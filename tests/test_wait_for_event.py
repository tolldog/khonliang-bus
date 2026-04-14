"""Tests for bus_wait_for_event — long-poll semantics for Claude notifications."""

from __future__ import annotations

import threading
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
