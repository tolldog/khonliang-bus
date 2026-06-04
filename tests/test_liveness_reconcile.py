"""Write-back liveness reconciliation (fr_khonliang-bus_7bf5ce84 follow-up).

get_services / start_agent / bus_welcome derive liveness at read time, but the
SQL-only routing readers (``get_healthy_agent_for_type`` — request routing, the
orchestrator, flows) read the stored ``status`` column directly. reconcile_liveness
periodically persists the derived state so those readers stop selecting dead/stale
agents. Run each tick of the supervision loop.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from bus.db import BusDB
from bus.server import BusServer


def _bus(db: BusDB, **cfg) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _register(db: BusDB, agent_id: str = "a1", pid: int | None = None, agent_type: str = "test") -> None:
    db.register_agent(
        agent_id=agent_id,
        agent_type=agent_type,
        callback_url="http://localhost:9001",
        pid=os.getpid() if pid is None else pid,
        version="0.1.0",
        skills=[{"name": "do_thing", "description": "", "parameters": {}}],
    )


def _set_heartbeat(db: BusDB, agent_id: str, when_sql: str) -> None:
    with db.conn() as c:
        c.execute("UPDATE registrations SET last_heartbeat = ? WHERE id = ?", (when_sql, agent_id))


def test_reconcile_marks_dead_pid(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, pid=-1)  # negative PID → derived dead (never probed)
    result = _bus(db).reconcile_liveness()
    assert result == {"updated": {"a1": "dead"}}
    assert db.get_registration("a1")["status"] == "dead"


def test_reconcile_marks_stale_heartbeat(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # live pid
    _set_heartbeat(db, "a1", "2000-01-01 00:00:00")  # ancient
    _bus(db).reconcile_liveness()
    assert db.get_registration("a1")["status"] == "stale"


def test_reconcile_noops_fresh_healthy(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # live pid, fresh heartbeat
    result = _bus(db).reconcile_liveness()
    assert result == {"updated": {}}
    assert db.get_registration("a1")["status"] == "healthy"


def test_reconcile_excludes_dead_agent_from_type_routing(tmp_path):
    """The payoff: after reconcile, get_healthy_agent_for_type (the SQL reader
    used by request routing / orchestrator / flows) no longer returns an agent
    whose process is gone."""
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, "alive", pid=os.getpid(), agent_type="worker")
    _register(db, "dead", pid=-1, agent_type="worker")
    bus = _bus(db)

    bus.reconcile_liveness()

    assert db.get_registration("dead")["status"] == "dead"
    picked = db.get_healthy_agent_for_type("worker")
    assert picked is not None and picked["id"] == "alive"


def test_reconcile_is_idempotent(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, pid=-1)
    bus = _bus(db)
    assert bus.reconcile_liveness() == {"updated": {"a1": "dead"}}
    assert bus.reconcile_liveness() == {"updated": {}}  # already 'dead'; no re-write


@pytest.mark.asyncio
async def test_supervision_loop_runs_reconcile(tmp_path, monkeypatch):
    """The periodic loop drives reconcile_liveness, not just supervise_once."""
    db = BusDB(str(tmp_path / "bus.db"))
    bus = _bus(db)

    fired_twice = asyncio.Event()
    calls = 0

    def _count():
        nonlocal calls
        calls += 1
        if calls >= 2:
            fired_twice.set()
        return {"updated": {}}

    monkeypatch.setattr(bus, "reconcile_liveness", _count)
    task = bus.start_supervisor(interval=0.1)
    try:
        await asyncio.wait_for(fired_twice.wait(), timeout=2.0)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    assert calls >= 2
