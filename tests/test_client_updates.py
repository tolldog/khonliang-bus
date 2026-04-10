"""Tests for the updated Python client: nack, deregister, close behavior."""

from __future__ import annotations

from tests.conftest import register_test_agent


def test_nack_endpoint(client):
    """BusClient.nack() calls /v1/nack on the bus."""
    register_test_agent(client, "sub1")
    msg = client.post("/v1/publish", json={
        "topic": "t", "payload": {"n": 1}, "source": "test",
    }).json()

    # ACK then NACK
    client.post("/v1/ack", json={
        "subscriber_id": "sub1", "message_id": msg["id"], "topic": "t",
    })
    r = client.post("/v1/nack", json={
        "subscriber_id": "sub1",
        "message_id": msg["id"],
        "topic": "t",
        "reason": "test nack",
    }).json()
    assert r["status"] == "nacked"
    assert r["redelivery_from"] == msg["id"]


def test_deregister_endpoint(client):
    """BusClient.deregister() calls /v1/deregister on the bus."""
    register_test_agent(client, "agent1")
    assert len(client.get("/v1/services").json()) == 1

    client.post("/v1/deregister", json={"id": "agent1"})
    assert len(client.get("/v1/services").json()) == 0
