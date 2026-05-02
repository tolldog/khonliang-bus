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

    # Default cursor on the same fresh subscriber DOES see the backlog.
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
    """
    # Need a real concurrent client+publisher; reuse the dedicated
    # concurrent harness from test_wait_wakes_on_new_publish.
    import asyncio
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(db_path=str(tmp_path / "cursor-now-wake.db"))

    async def run():
        with TestClient(app) as c:
            # Seed pre-existing backlog the cursor='now' must skip.
            for n in range(3):
                c.post("/v1/publish", json={"topic": "live", "payload": {"n": n}, "source": "s"})

            wait_done = asyncio.Event()
            result_holder = {}

            async def waiter():
                # Block in a thread so the TestClient's sync POST can run.
                import functools
                loop = asyncio.get_running_loop()
                def _do():
                    return c.post("/v1/wait", json={
                        "topics": ["live"],
                        "subscriber_id": "wake-now",
                        "timeout": 3.0,
                        "cursor": "now",
                    }).json()
                result_holder["r"] = await loop.run_in_executor(None, _do)
                wait_done.set()

            task = asyncio.create_task(waiter())
            await asyncio.sleep(0.1)
            # Publish a fresh event AFTER the wait is active.
            c.post("/v1/publish", json={"topic": "live", "payload": {"n": 99}, "source": "s"})
            await asyncio.wait_for(wait_done.wait(), timeout=4.0)
            await task
            return result_holder["r"]

    r = asyncio.run(run())
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


def test_wait_cursor_now_with_existing_subscriber_history(client):
    """If a subscriber has acked through id=2 and cursor='now' snapshots
    above that, the floor wins (no backwards delivery). And vice
    versa — if the subscriber has acked past the snapshot, the ack
    pointer wins. Confirms the MAX(ack, floor) semantic."""
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
