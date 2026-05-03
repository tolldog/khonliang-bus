"""Tests for bus_wait_for_event — long-poll semantics for Claude notifications."""

from __future__ import annotations

import time


def test_wait_returns_existing_unacked_event(client):
    """If there's already an unacked event, wait returns immediately."""
    client.post("/v1/publish", json={"topic": "pr.review", "payload": {"pr": 42}, "source": "test"})
    client.post("/v1/publish", json={"topic": "pr.merge", "payload": {"pr": 42}, "source": "test"})

    t0 = time.monotonic()
    r = client.post("/v1/wait", json={
        "topics": ["pr.review"],
        "subscriber_id": "claude-1",
        "timeout": 5.0,
    }).json()
    elapsed = time.monotonic() - t0

    assert r["status"] == "matched"
    assert r["event"]["topic"] == "pr.review"
    assert r["event"]["payload"] == {"pr": 42}
    assert elapsed < 1.0


def test_wait_advances_ack_pointer(client):
    """ack_on_return=true advances the subscriber's ack position."""
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 1}, "source": "s"})
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 2}, "source": "s"})

    r1 = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "claude-ack", "timeout": 1.0,
    }).json()
    assert r1["event"]["payload"] == {"n": 1}

    r2 = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "claude-ack", "timeout": 1.0,
    }).json()
    assert r2["event"]["payload"] == {"n": 2}


def test_wait_no_ack_when_disabled(client):
    client.post("/v1/publish", json={"topic": "t", "payload": {"x": 1}, "source": "s"})
    r1 = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "peek", "timeout": 1.0, "ack_on_return": False,
    }).json()
    r2 = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "peek", "timeout": 1.0, "ack_on_return": False,
    }).json()
    assert r1["event"]["id"] == r2["event"]["id"]


def test_wait_times_out_when_no_match(client):
    t0 = time.monotonic()
    r = client.post("/v1/wait", json={
        "topics": ["never.fires"], "subscriber_id": "claude-t", "timeout": 0.5,
    }).json()
    elapsed = time.monotonic() - t0
    assert r["status"] == "timeout"
    assert r["event"] is None
    assert 0.4 < elapsed < 2.0


def test_wait_empty_topics_matches_any(client):
    client.post("/v1/publish", json={"topic": "random.topic", "payload": {"x": 1}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": [], "subscriber_id": "any", "timeout": 1.0,
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["topic"] == "random.topic"


def test_wait_generates_subscriber_id_if_empty(client):
    client.post("/v1/publish", json={"topic": "t", "payload": {}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "", "timeout": 1.0,
    }).json()
    assert r["subscriber_id"].startswith("waiter-")


def test_wait_wakes_on_new_publish(tmp_path):
    """A blocked waiter wakes when a publish fires — tested against the
    BusServer directly (TestClient can't do concurrent HTTP calls cleanly).
    """
    import asyncio
    from bus.db import BusDB
    from bus.server import BusServer, WaitRequest, PublishRequest

    db = BusDB(str(tmp_path / "wake-test.db"))
    bus = BusServer(db, config={"bus_url": "http://localhost:9999"})

    async def scenario():
        # Start the waiter with a long timeout
        wait_task = asyncio.create_task(bus.wait_for_event(WaitRequest(
            topics=["delayed.event"],
            subscriber_id="waiter",
            timeout=5.0,
        )))

        # Give the waiter a moment to park on the Event
        await asyncio.sleep(0.1)
        assert not wait_task.done(), "waiter should be blocked, not done"

        # Publish — should wake the waiter
        t0 = asyncio.get_running_loop().time()
        await bus.publish(PublishRequest(
            topic="delayed.event",
            payload={"arrived": True},
            source="test",
        ))
        result = await asyncio.wait_for(wait_task, timeout=2.0)
        elapsed = asyncio.get_running_loop().time() - t0
        return result, elapsed

    result, elapsed = asyncio.run(scenario())
    assert result["status"] == "matched"
    assert result["event"]["payload"] == {"arrived": True}
    assert elapsed < 1.0  # woke quickly after publish


def test_wait_filters_by_topic(client):
    client.post("/v1/publish", json={"topic": "ignore.me", "payload": {"no": True}, "source": "s"})
    client.post("/v1/publish", json={"topic": "want.this", "payload": {"yes": True}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["want.this"], "subscriber_id": "filter", "timeout": 1.0,
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["topic"] == "want.this"


def test_wait_multiple_topics_returns_earliest(client):
    client.post("/v1/publish", json={"topic": "topic.a", "payload": {"n": 1}, "source": "s"})
    client.post("/v1/publish", json={"topic": "topic.b", "payload": {"n": 2}, "source": "s"})
    client.post("/v1/publish", json={"topic": "topic.c", "payload": {"n": 3}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["topic.b", "topic.c"], "subscriber_id": "multi", "timeout": 1.0,
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["topic"] == "topic.b"


def test_wait_cursor_now_skips_existing_backlog(client):
    """``cursor='now'`` pins a fresh subscriber to the current high-water
    mark, so retained backlog isn't replayed one-event-per-call. Closes
    fr_bus_3db58f0b — surfaced by dog_ce53165f."""
    # Seed three events that a fresh subscriber would otherwise drain.
    client.post("/v1/publish", json={"topic": "old.topic", "payload": {"n": 1}, "source": "s"})
    client.post("/v1/publish", json={"topic": "old.topic", "payload": {"n": 2}, "source": "s"})
    client.post("/v1/publish", json={"topic": "old.topic", "payload": {"n": 3}, "source": "s"})

    # Fresh subscriber + cursor='now' + short timeout: there are no
    # forward-going events, so this must time out instead of returning
    # the seeded backlog.
    r = client.post("/v1/wait", json={
        "topics": ["old.topic"],
        "subscriber_id": "fresh-now",
        "timeout": 0.2,
        "cursor": "now",
    }).json()
    assert r["status"] == "timeout"

    # Default cursor on a *different* fresh subscriber DOES see the
    # backlog. (Using a different ``subscriber_id`` keeps each
    # assertion trivially independent — neither subscriber affects
    # the other's matcher state. ``wait_for_event`` only writes a
    # subscription row on a matched-and-acked return, so even reusing
    # ``fresh-now`` would have worked here, but a separate id makes
    # the contract under test obvious.) The point of the assertion
    # is that cursor controls whether backlog replays for a fresh
    # subscriber.
    r = client.post("/v1/wait", json={
        "topics": ["old.topic"],
        "subscriber_id": "fresh-default",
        "timeout": 1.0,
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["payload"] == {"n": 1}


def test_wait_cursor_latest_is_synonym_for_now(client):
    """Both spellings are honored so a caller that mentally maps to
    'latest' (e.g. coming from Kafka conventions) doesn't trip up."""
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 1}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["t"],
        "subscriber_id": "fresh-latest",
        "timeout": 0.2,
        "cursor": "LATEST",  # case-insensitive
    }).json()
    assert r["status"] == "timeout"


def test_wait_cursor_now_returns_event_published_after_call(tmp_path):
    """``cursor='now'`` does NOT silence forward events — only backlog.
    A publish that happens after the wait begins must wake the waiter.

    Tested directly against ``BusServer`` (same pattern as
    ``test_wait_wakes_on_new_publish``) rather than via TestClient +
    ``run_in_executor`` — this file already notes that TestClient
    can't do concurrent HTTP calls cleanly, and the run_in_executor
    pattern was flagged as flaky on shared loops.
    """
    import asyncio
    from bus.db import BusDB
    from bus.server import BusServer, PublishRequest, WaitRequest

    db = BusDB(str(tmp_path / "cursor-now-wake.db"))
    bus = BusServer(db, config={"bus_url": "http://localhost:9999"})

    async def scenario():
        # Seed pre-existing backlog the cursor='now' must skip.
        for n in range(3):
            await bus.publish(PublishRequest(
                topic="live", payload={"n": n}, source="s",
            ))

        # Start a cursor='now' waiter; ack pointer doesn't yet exist
        # for ``wake-now``, so default cursor would have replayed
        # backlog. cursor='now' pins the floor at the current
        # high-water mark.
        wait_task = asyncio.create_task(bus.wait_for_event(WaitRequest(
            topics=["live"],
            subscriber_id="wake-now",
            timeout=5.0,
            cursor="now",
        )))

        # Give the waiter a moment to park on the asyncio Event.
        await asyncio.sleep(0.1)
        assert not wait_task.done(), "waiter should be blocked on cursor='now'"

        # Publish a fresh event AFTER the wait is active. Must wake
        # the waiter — the snapshot was taken when wait_for_event
        # entered, so this id is strictly above the floor.
        await bus.publish(PublishRequest(
            topic="live", payload={"n": 99}, source="s",
        ))
        return await asyncio.wait_for(wait_task, timeout=2.0)

    r = asyncio.run(scenario())
    assert r["status"] == "matched"
    # Must be the post-wait publish, not seeded backlog n=0..2.
    assert r["event"]["payload"] == {"n": 99}


def test_wait_cursor_unknown_value_falls_back_to_default(client):
    """Misspelled / unknown cursor strings must not silently drop
    backlog — they fall through to the legacy replay-from-ack
    behaviour. Prevents a typo (`cursor='latests'`) from accidentally
    making a fresh subscriber miss retained backlog they wanted."""
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 1}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["t"],
        "subscriber_id": "typo",
        "timeout": 1.0,
        "cursor": "latests",  # typo — unknown value
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["payload"] == {"n": 1}


def test_wait_cursor_now_with_floor_above_ack_pointer(client):
    """floor > ack: a subscriber whose ack pointer is below the
    cursor='now' snapshot does NOT get backwards delivery to the
    intervening events — the floor wins.

    The "ack > floor" direction is impossible by construction:
    floor = ``max_message_id`` taken at wait time and the ack pointer
    is always ≤ max_message_id, so floor ≥ ack always holds. The
    floor == ack case is uninteresting (threshold is the same value
    either way). Only the floor > ack case has distinct
    observable behaviour, so it's the only direction tested here."""
    # Seed three events; subscriber 'sub' acks the first two via two
    # default-cursor waits.
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 1}, "source": "s"})
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 2}, "source": "s"})
    for expected in (1, 2):
        r = client.post("/v1/wait", json={
            "topics": ["t"], "subscriber_id": "sub", "timeout": 1.0,
        }).json()
        assert r["event"]["payload"] == {"n": expected}

    # Publish a third event; cursor='now' AT this point snapshots above
    # event 3, so the wait should time out (nothing forward-going yet).
    client.post("/v1/publish", json={"topic": "t", "payload": {"n": 3}, "source": "s"})
    r = client.post("/v1/wait", json={
        "topics": ["t"],
        "subscriber_id": "sub",
        "timeout": 0.2,
        "cursor": "now",
    }).json()
    assert r["status"] == "timeout"

    # Without cursor=now, the same subscriber still sees event n=3
    # (its ack pointer is at id=2, event 3 is unacked).
    r = client.post("/v1/wait", json={
        "topics": ["t"], "subscriber_id": "sub", "timeout": 1.0,
    }).json()
    assert r["event"]["payload"] == {"n": 3}


