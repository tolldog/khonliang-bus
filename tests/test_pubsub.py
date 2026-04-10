"""Tests for pub/sub: publish, ack, nack."""

from __future__ import annotations


def test_publish_returns_id(client):
    r = client.post("/v1/publish", json={
        "topic": "test.event",
        "payload": {"key": "val"},
        "source": "test",
    }).json()
    assert "id" in r
    assert r["topic"] == "test.event"


def test_publish_multiple_messages(client):
    for i in range(5):
        client.post("/v1/publish", json={
            "topic": "t",
            "payload": {"n": i},
            "source": "test",
        })
    # Verify via status — we don't have a direct messages endpoint but
    # the DB will have them. Use the internal DB via the app.
    # For now just verify publish didn't error:
    r = client.post("/v1/publish", json={
        "topic": "t",
        "payload": {"n": 5},
        "source": "test",
    }).json()
    assert r["id"] > 5  # IDs are monotonic


def test_ack(client):
    msg = client.post("/v1/publish", json={
        "topic": "t", "payload": {}, "source": "s",
    }).json()
    r = client.post("/v1/ack", json={
        "subscriber_id": "sub1",
        "message_id": msg["id"],
        "topic": "t",
    }).json()
    assert r["status"] == "acked"


def test_nack_rolls_back_ack(client):
    """NACK should roll back the ack so the message gets redelivered."""
    msg1 = client.post("/v1/publish", json={"topic": "t", "payload": {"n": 1}, "source": "s"}).json()
    msg2 = client.post("/v1/publish", json={"topic": "t", "payload": {"n": 2}, "source": "s"}).json()

    # Ack both
    client.post("/v1/ack", json={"subscriber_id": "sub1", "message_id": msg2["id"], "topic": "t"})

    # NACK msg2 — should roll back so msg2 gets redelivered
    r = client.post("/v1/nack", json={
        "subscriber_id": "sub1",
        "message_id": msg2["id"],
        "topic": "t",
        "reason": "processing failed",
    }).json()
    assert r["status"] == "nacked"
    assert r["redelivery_from"] == msg2["id"]
