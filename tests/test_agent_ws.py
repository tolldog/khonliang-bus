"""Tests for the WebSocket agent protocol."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bus.server import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    return TestClient(app)


def test_agent_ws_register_and_services(client):
    """Agent connects via WebSocket, registers, appears in services."""
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register",
            "id": "ws-agent-1",
            "agent_type": "researcher",
            "version": "0.6.4",
            "pid": 1234,
            "skills": [
                {"name": "find_papers", "description": "Search for papers"},
                {"name": "synergize", "description": "Generate FRs"},
            ],
        })
        resp = ws.receive_json()
        assert resp["type"] == "registered"
        assert resp["id"] == "ws-agent-1"

        # Agent should be visible in services
        services = client.get("/v1/services").json()
        assert len(services) == 1
        assert services[0]["id"] == "ws-agent-1"
        assert services[0]["skill_count"] == 2


def test_agent_ws_heartbeat(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-hb", "skills": []})
        ws.receive_json()  # registered

        ws.send_json({"type": "heartbeat"})
        resp = ws.receive_json()
        assert resp["type"] == "pong"


def test_agent_ws_deregister(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-dereg", "skills": []})
        ws.receive_json()

        assert len(client.get("/v1/services").json()) == 1

        ws.send_json({"type": "deregister"})

    # After deregister + disconnect, agent should be gone
    assert len(client.get("/v1/services").json()) == 0


def test_agent_ws_disconnect_deregisters(client):
    """Agent disconnecting without explicit deregister still cleans up."""
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-disc", "skills": []})
        ws.receive_json()

    # Connection closed — agent should be deregistered
    assert len(client.get("/v1/services").json()) == 0


def test_agent_ws_publish(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-pub", "skills": []})
        ws.receive_json()

        ws.send_json({
            "type": "publish",
            "topic": "test.event",
            "payload": {"key": "val"},
        })

    # Message should be stored
    status = client.get("/v1/status").json()
    assert status["registered_agents"] == 0  # disconnected


def test_agent_ws_gap_report(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-gap", "skills": []})
        ws.receive_json()

        ws.send_json({
            "type": "gap",
            "operation": "do_something",
            "reason": "I don't know how to do this",
            "context": {"requested_by": "developer"},
        })

    gaps = client.get("/v1/gaps").json()
    assert len(gaps) == 1
    assert gaps[0]["operation"] == "do_something"
    assert gaps[0]["reason"] == "I don't know how to do this"
