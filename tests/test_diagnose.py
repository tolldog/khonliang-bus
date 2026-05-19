"""Tests for ``bus.diagnose`` — structured per-agent failure-mode probe.

Covers the v1 verdict matrix from fr_khonliang-bus_8fe376c7: ``ok``,
``crashed``, ``agent_wedged`` (three sub-cases), ``not_registered``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bus.db import BusDB
from bus.server import BusServer, create_app


@pytest.fixture
def fake_alive(monkeypatch):
    """Configurable ``_pid_alive`` truth table — same shape as the boot-reconciliation tests."""
    alive_pids: set[int] = set()

    def _check(pid: int) -> bool:
        return pid in alive_pids

    monkeypatch.setattr("bus.server._pid_alive", _check)
    return alive_pids


def _register(db: BusDB, agent_id: str, pid: int, skills_n: int = 2) -> None:
    db.register_agent(
        agent_id=agent_id,
        agent_type="test",
        callback_url="http://localhost:9999",
        pid=pid,
        version="0.1.0",
        skills=[
            {"name": f"skill_{i}", "description": "", "parameters": {}}
            for i in range(skills_n)
        ],
    )


def _bus(db: BusDB) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999"})


async def test_diagnose_not_registered(tmp_path, fake_alive):
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    d = await bus.diagnose("ghost")

    assert d["agent_id"] == "ghost"
    assert d["verdict"] == "not_registered"
    assert d["pid"] is None
    assert d["bus_registry"]["registered"] is False
    assert d["bus_registry"]["skill_count"] == 0
    assert d["health_probe"]["ok"] is False
    assert "install" in d["recommendation"]
    assert "ghost" in d["recommendation"]


async def test_diagnose_crashed(tmp_path, fake_alive):
    """Registered, but PID dead AND no WebSocket connection — process is gone."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "agent-1", pid=12345)
    bus = _bus(db)
    # fake_alive empty: 12345 is dead. No entry in _agent_connections.

    d = await bus.diagnose("agent-1")

    assert d["verdict"] == "crashed"
    assert d["pid"] is None  # dead PID surfaces as None
    assert d["bus_registry"]["registered"] is True
    assert d["bus_registry"]["skill_count"] == 2
    assert d["health_probe"]["ok"] is False
    assert "bus_start_agent" in d["recommendation"]


async def test_diagnose_wedged_no_ws(tmp_path, fake_alive):
    """Process alive but no WebSocket — agent isn't talking to bus."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "agent-1", pid=12345)
    fake_alive.add(12345)
    bus = _bus(db)

    d = await bus.diagnose("agent-1")

    assert d["verdict"] == "agent_wedged"
    assert d["pid"] == 12345  # alive PID surfaces
    assert d["health_probe"]["error"] == "no WebSocket connection"
    assert "bus_restart_agent" in d["recommendation"]


async def test_diagnose_wedged_probe_timeout(tmp_path, fake_alive, monkeypatch):
    """WebSocket connected, but health probe times out — wedged in a request."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "agent-1", pid=12345)
    fake_alive.add(12345)
    bus = _bus(db)
    bus._agent_connections["agent-1"] = object()  # sentinel — `is_agent_ws_connected` returns True

    async def _timeout(**_kw):
        return {"error": "timeout", "correlation_id": "diag-x"}

    monkeypatch.setattr(bus, "send_request_to_agent_ws", _timeout)

    d = await bus.diagnose("agent-1")

    assert d["verdict"] == "agent_wedged"
    assert d["health_probe"]["ok"] is False
    assert d["health_probe"]["error"] == "timeout"
    assert d["health_probe"]["latency_ms"] is not None
    assert "bus_restart_agent" in d["recommendation"]


async def test_diagnose_wedged_probe_error(tmp_path, fake_alive, monkeypatch):
    """WebSocket connected, probe returned a non-timeout error — agent responded but unhealthy."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "agent-1", pid=12345)
    fake_alive.add(12345)
    bus = _bus(db)
    bus._agent_connections["agent-1"] = object()

    async def _err(**_kw):
        return {"error": "db locked"}

    monkeypatch.setattr(bus, "send_request_to_agent_ws", _err)

    d = await bus.diagnose("agent-1")

    assert d["verdict"] == "agent_wedged"
    assert d["health_probe"]["error"] == "db locked"
    assert "bus_restart_agent" in d["recommendation"]


async def test_diagnose_ok(tmp_path, fake_alive, monkeypatch):
    """All four checks pass: PID alive, WS connected, probe returns clean."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "agent-1", pid=12345)
    fake_alive.add(12345)
    bus = _bus(db)
    bus._agent_connections["agent-1"] = object()

    async def _ok(**_kw):
        return {"status": "ok"}

    monkeypatch.setattr(bus, "send_request_to_agent_ws", _ok)

    d = await bus.diagnose("agent-1")

    assert d["verdict"] == "ok"
    assert d["health_probe"]["ok"] is True
    assert d["health_probe"]["latency_ms"] is not None
    assert d["pid"] == 12345
    assert d["recommendation"] == "no action needed"


async def test_diagnose_detail_full_same_shape_as_brief(tmp_path, fake_alive):
    """``detail`` is accepted for API stability with future fields; v1 returns same shape."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    brief = await bus.diagnose("ghost", detail="brief")
    full = await bus.diagnose("ghost", detail="full")

    assert brief.keys() == full.keys()
    assert brief["verdict"] == full["verdict"]


# -- HTTP endpoint integration -----------------------------------------------


def test_diagnose_endpoint_returns_envelope(tmp_path):
    """``GET /v1/diagnose/{agent_id}`` returns the Diagnosis JSON for unknown agents
    (the simplest path — no WebSocket / process state to fake at the HTTP layer)."""
    app = create_app(db_path=str(tmp_path / "test.db"))
    client = TestClient(app)

    r = client.get("/v1/diagnose/ghost")

    assert r.status_code == 200
    body = r.json()
    assert body["agent_id"] == "ghost"
    assert body["verdict"] == "not_registered"
    assert "bus_registry" in body
    assert "health_probe" in body
    assert "recommendation" in body


def test_diagnose_endpoint_accepts_detail_query(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    client = TestClient(app)

    r = client.get("/v1/diagnose/ghost", params={"detail": "full"})

    assert r.status_code == 200
    assert r.json()["verdict"] == "not_registered"
