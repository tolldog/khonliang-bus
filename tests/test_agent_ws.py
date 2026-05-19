"""Tests for the WebSocket agent protocol."""

from __future__ import annotations

import json
import threading
import time

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


def test_agent_ws_feedback_report(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "ws-feedback", "skills": []})
        ws.receive_json()

        ws.send_json({
            "type": "feedback",
            "kind": "friction",
            "operation": "run_tests",
            "category": "token",
            "severity": "medium",
            "message": "raw pytest output was too large",
            "context": {"raw_bytes": 100000},
            "suggestion": "store raw output as artifact",
        })

    feedback = client.get("/v1/feedback", params={"kind": "friction"}).json()
    assert len(feedback) == 1
    assert feedback[0]["agent_id"] == "ws-feedback"
    assert feedback[0]["category"] == "token"
    assert feedback[0]["context"]["raw_bytes"] == 100000


def test_agent_ws_request_round_trip(tmp_path):
    """Full WS round-trip: bus dispatches a request to the agent, agent responds.

    Uses ``with TestClient(app) as client`` so that HTTP requests and the WS
    handler share the same anyio event loop (portal), which is required for
    asyncio.Future-based request/response correlation to work in tests.
    """
    app = create_app(db_path=str(tmp_path / "test.db"))
    registered = threading.Event()

    def agent(ws):
        ws.send_json({
            "type": "register",
            "id": "ws-roundtrip",
            "agent_type": "echo",
            "skills": [{"name": "echo", "description": "echo args"}],
        })
        ws.receive_json()  # registered confirmation
        registered.set()

        # Wait for a request from the bus then echo it back
        msg = ws.receive_json()
        assert msg["type"] == "request"
        assert msg["operation"] == "echo"
        ws.send_json({
            "type": "response",
            "correlation_id": msg["correlation_id"],
            "result": {"echoed": msg["args"]},
        })

    with TestClient(app) as client:
        with client.websocket_connect("/v1/agent") as ws:
            t = threading.Thread(target=agent, args=(ws,), daemon=True)
            t.start()
            registered.wait(timeout=2)

            r = client.post("/v1/request", json={
                "agent_id": "ws-roundtrip",
                "operation": "echo",
                "args": {"msg": "hello"},
                "timeout": 5.0,
            }).json()

            t.join(timeout=5)
            assert not t.is_alive(), "agent thread did not finish in time"

    assert "result" in r, f"expected result, got: {r}"
    assert r["result"] == {"echoed": {"msg": "hello"}}


def test_agent_ws_async_request_creates_result_artifact(tmp_path):
    """Async requests return a receipt and persist the eventual result."""
    app = create_app(db_path=str(tmp_path / "test.db"))
    registered = threading.Event()

    def agent(ws):
        ws.send_json({
            "type": "register",
            "id": "ws-async",
            "agent_type": "echo",
            "skills": [{"name": "echo", "description": "echo args"}],
        })
        ws.receive_json()
        registered.set()

        msg = ws.receive_json()
        assert msg["type"] == "request"
        assert msg["operation"] == "echo"
        ws.send_json({
            "type": "response",
            "correlation_id": msg["correlation_id"],
            "result": {"echoed": msg["args"]},
        })

    with TestClient(app) as client:
        with client.websocket_connect("/v1/agent") as ws:
            t = threading.Thread(target=agent, args=(ws,), daemon=True)
            t.start()
            assert registered.wait(timeout=2), "agent did not register in time"

            receipt = client.post("/v1/request", json={
                "agent_id": "ws-async",
                "operation": "echo",
                "args": {"msg": "hello"},
                "timeout": 5.0,
                "async_mode": True,
            }).json()

            assert receipt["status"] == "accepted"
            assert receipt["trace_id"].startswith("t-")
            artifact_id = receipt["artifact_id"]
            assert receipt["artifact"] == {
                "id": artifact_id,
                "kind": "async_request_result",
                "available": False,
            }

            t.join(timeout=5)
            assert not t.is_alive(), "agent thread did not finish in time"

            deadline = time.monotonic() + 5
            meta = {"error": "artifact not found"}
            while time.monotonic() < deadline:
                meta = client.get(f"/v1/artifacts/{artifact_id}").json()
                if "error" not in meta:
                    break
                time.sleep(0.05)

            assert "error" not in meta
            assert meta["kind"] == "async_request_result"
            assert meta["producer"] == "ws-async"
            assert meta["trace_id"] == receipt["trace_id"]
            assert meta["metadata"]["status"] == "ok"

            content = client.get(f"/v1/artifacts/{artifact_id}/content").json()
            payload = json.loads(content["text"])
            assert payload["status"] == "ok"
            assert payload["result"] == {"echoed": {"msg": "hello"}}

def test_ws_register_persists_welcome(client):
    """fr_khonliang-bus_f96722dd: WS register path must persist welcome
    alongside the registration — parity with the HTTP path. Without this
    coverage, a regression that only broke WS-side persistence would
    slip through (no other test exercises that code path).
    """
    welcome = {
        "agent_id": "ws-welcomed",
        "agent_type": "researcher",
        "version": "0.1.0",
        "role": "test fixture",
        "skill_count": 1,
    }
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register",
            "id": "ws-welcomed",
            "agent_type": "researcher",
            "version": "0.1.0",
            "pid": 4242,
            "skills": [{"name": "find", "description": "d"}],
            "welcome": welcome,
        })
        ws.receive_json()  # registered

    # After disconnect: welcome still retrievable (survives-deregister
    # invariant), agent removed from services.
    body = client.get("/v1/agents/ws-welcomed/welcome").json()
    assert body["welcome"] == welcome


def test_ws_register_without_welcome_stores_nothing(client):
    """Backward compat: pre-PR#25 bus-lib agents on the WS path omit
    welcome — bus stores nothing; GET returns 404."""
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register",
            "id": "ws-legacy",
            "skills": [],
        })
        ws.receive_json()
    assert client.get("/v1/agents/ws-legacy/welcome").status_code == 404


def test_ws_register_ignores_non_dict_welcome(client):
    """Defensive: a WS client sending a non-dict welcome (string, list,
    int, ...) must not break registration. Bus silently ignores the field;
    register succeeds; GET returns 404."""
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register",
            "id": "ws-bad-welcome",
            "skills": [],
            "welcome": "not a dict",
        })
        ws.receive_json()
    assert client.get("/v1/agents/ws-bad-welcome/welcome").status_code == 404


def test_ws_register_oversized_welcome_still_registers(client, db):
    """Parity with HTTP path: oversized welcome from one agent must not
    block its registration. The welcome is logged + skipped; the agent
    appears in services + skills (with the registration intact); only the
    welcome catalog is empty for that agent."""
    oversized = {"agent_id": "x", "padding": "x" * (db.MAX_WELCOME_BYTES + 1024)}
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register",
            "id": "ws-big-welcome",
            "skills": [{"name": "do_thing"}],
            "welcome": oversized,
        })
        ws.receive_json()  # registered — not blocked
        services = client.get("/v1/services").json()
        assert any(s["id"] == "ws-big-welcome" for s in services)
    # But the welcome got dropped.
    assert client.get("/v1/agents/ws-big-welcome/welcome").status_code == 404

