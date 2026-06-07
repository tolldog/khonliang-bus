"""Active WS health probe for bus_services (fr_khonliang-bus_7bf5ce84 #3).

get_services derives liveness passively (dead/stale) from PID + heartbeat.
get_services_probed adds an ACTIVE per-agent WS health-ping and reports
``unreachable`` for an up-looking agent that doesn't answer — a wedged process.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from bus.db import BusDB
from bus.server import BusServer, create_app


def _bus(db: BusDB, **cfg) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _register(db: BusDB, agent_id: str = "a1", pid: int | None = None) -> None:
    db.register_agent(
        agent_id=agent_id,
        agent_type="test",
        callback_url="http://localhost:9001",
        pid=os.getpid() if pid is None else pid,
        version="0.1.0",
        skills=[{"name": "health_check", "description": "", "parameters": {}}],
    )


def _areturn(value):
    async def _f(*args, **kwargs):
        return value
    return _f


def _svc(entries, agent_id="a1"):
    return {s["id"]: s for s in entries}[agent_id]


@pytest.mark.asyncio
async def test_probe_keeps_healthy_when_reachable(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # live pid, fresh heartbeat → 'healthy'
    bus = _bus(db)
    monkeypatch.setattr(bus, "_probe_agent_reachable", _areturn(True))
    svc = _svc(await bus.get_services_probed())
    assert svc["status"] == "healthy"
    assert svc["probe_ok"] is True


@pytest.mark.asyncio
async def test_probe_marks_unreachable_when_health_check_fails(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # passively 'healthy'...
    bus = _bus(db)
    monkeypatch.setattr(bus, "_probe_agent_reachable", _areturn(False))
    svc = _svc(await bus.get_services_probed())
    assert svc["status"] == "unreachable"  # ...but the active probe says no
    assert svc["probe_ok"] is False


@pytest.mark.asyncio
async def test_probe_skips_definitively_gone_entries(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, pid=-1)  # derived 'dead' — nothing to confirm
    bus = _bus(db)
    called: list[str] = []

    async def _record(agent_id, timeout):
        called.append(agent_id)
        return True

    monkeypatch.setattr(bus, "_probe_agent_reachable", _record)
    svc = _svc(await bus.get_services_probed())
    assert svc["status"] == "dead"
    assert "probe_ok" not in svc
    assert called == []  # dead entries are not probed


@pytest.mark.asyncio
async def test_probe_exception_is_isolated_as_unreachable(tmp_path, monkeypatch):
    # A single agent's probe raising must not fail the whole listing — that
    # agent is reported unreachable, the others are unaffected.
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, "a1")
    _register(db, "a2")
    bus = _bus(db)

    async def _flaky(agent_id, timeout):
        if agent_id == "a1":
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(bus, "_probe_agent_reachable", _flaky)
    entries = {s["id"]: s for s in await bus.get_services_probed()}
    assert entries["a1"]["status"] == "unreachable"
    assert entries["a2"]["status"] == "healthy"


@pytest.mark.asyncio
async def test_probe_agent_reachable_false_without_ws(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)
    # No WebSocket connection registered → not reachable (and never probes a
    # missing socket).
    assert await _bus(db)._probe_agent_reachable("a1", 1.0) is False


def test_rest_services_probe_query(tmp_path):
    # End-to-end via the route: an HTTP-registered agent (no WS) is 'healthy'
    # without probing but 'unreachable' with ?probe=true.
    app = create_app(db_path=str(tmp_path / "bus.db"))
    client = TestClient(app)
    client.post("/v1/register", json={
        "id": "a1", "callback": "http://localhost:9001", "pid": os.getpid(),
        "version": "0.1.0",
        "skills": [{"name": "health_check", "description": "", "parameters": {}}],
    })
    plain = {s["id"]: s for s in client.get("/v1/services").json()}["a1"]
    assert plain["status"] == "healthy"
    probed = {s["id"]: s for s in client.get("/v1/services?probe=true").json()}["a1"]
    assert probed["status"] == "unreachable"
    assert probed["probe_ok"] is False
