"""Live-liveness derivation for get_services (fr_khonliang-bus_7bf5ce84).

bug_khonliang-bus_529d12df: bus_services echoed the stored catalog status, so a
registration row stayed ``healthy`` from its last heartbeat write even after the
process died. get_services now derives state at read time from two signals,
strongest first — a registered PID gone from /proc → ``dead``; otherwise a
``healthy`` row whose ``last_heartbeat`` exceeds a configurable threshold →
``stale``. It only downgrades; an already-unhealthy row is never resurrected.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import bus.server as server_mod
from bus.db import BusDB
from bus.server import BusServer


def _bus(db: BusDB, **cfg) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _register(db: BusDB, agent_id: str = "a1", pid: int | None = None) -> None:
    # Default to this test process's PID so _pid_alive() is True and the
    # heartbeat path (not the dead-PID path) is what's under test.
    db.register_agent(
        agent_id=agent_id,
        agent_type="test",
        callback_url="http://localhost:9001",
        pid=os.getpid() if pid is None else pid,
        version="0.1.0",
        skills=[{"name": "do_thing", "description": "", "parameters": {}}],
    )


def _set_heartbeat(db: BusDB, agent_id: str, when_sql: str) -> None:
    with db.conn() as c:
        c.execute(
            "UPDATE registrations SET last_heartbeat = ? WHERE id = ?",
            (when_sql, agent_id),
        )


def _svc(bus: BusServer, agent_id: str = "a1") -> dict:
    return {s["id"]: s for s in bus.get_services()}[agent_id]


# -- helper --------------------------------------------------------------


def test_heartbeat_age_parses_utc_text():
    base = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert BusServer._heartbeat_age_s("2026-05-30 12:00:00", now=base + 60) == 60.0


def test_heartbeat_age_none_when_missing_or_unparseable():
    assert BusServer._heartbeat_age_s(None) is None
    assert BusServer._heartbeat_age_s("") is None
    assert BusServer._heartbeat_age_s("not-a-timestamp") is None


# -- heartbeat-freshness path (live PID) --------------------------------


def test_fresh_heartbeat_reports_healthy(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # register stamps last_heartbeat = now; pid = live
    svc = _svc(_bus(db))
    assert svc["status"] == "healthy"
    assert svc["heartbeat_age_s"] is not None


def test_stale_heartbeat_downgrades_healthy_to_stale(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)
    _set_heartbeat(db, "a1", "2000-01-01 00:00:00")  # ancient
    svc = _svc(_bus(db))
    assert svc["status"] == "stale"
    assert svc["heartbeat_age_s"] > 90


def test_threshold_is_configurable(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)
    _set_heartbeat(db, "a1", "2000-01-01 00:00:00")  # ancient
    # A huge threshold means even an ancient heartbeat is still healthy —
    # proves the config value is honored, not a hardcoded default.
    svc = _svc(_bus(db, heartbeat_stale_threshold_s=10**12))
    assert svc["status"] == "healthy"


def test_does_not_upgrade_non_healthy_rows(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)
    db.set_agent_status("a1", "unhealthy")  # live pid + fresh hb, but unhealthy
    svc = _svc(_bus(db))
    assert svc["status"] == "unhealthy"


# -- dead-PID path -------------------------------------------------------


def test_dead_pid_reports_dead(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)  # fresh heartbeat, but pretend the process is gone
    monkeypatch.setattr(server_mod, "_pid_alive", lambda pid: False)
    svc = _svc(_bus(db))
    assert svc["status"] == "dead"


def test_dead_pid_wins_over_stale_heartbeat(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db)
    _set_heartbeat(db, "a1", "2000-01-01 00:00:00")  # would be 'stale'...
    monkeypatch.setattr(server_mod, "_pid_alive", lambda pid: False)
    svc = _svc(_bus(db))
    assert svc["status"] == "dead"  # ...but a dead PID is the stronger signal


def test_pid_zero_falls_back_to_heartbeat(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _register(db, pid=0)  # WS registration without a PID — skip the /proc check
    _set_heartbeat(db, "a1", "2000-01-01 00:00:00")
    svc = _svc(_bus(db))
    assert svc["status"] == "stale"


# -- start_agent guard consumes the same derivation ----------------------


def _install(db: BusDB, agent_id: str = "a1") -> None:
    db.install_agent(
        agent_id=agent_id,
        agent_type="test",
        command="/bin/true",
        args=[],
        cwd="/tmp",
        config="/tmp/cfg.yaml",
    )


def test_start_agent_already_running_only_when_derived_live(tmp_path):
    db = BusDB(str(tmp_path / "bus.db"))
    _install(db)
    _register(db)  # healthy row, live pid
    bus = _bus(db)
    assert bus.start_agent("a1")["status"] == "already_running"


def test_start_agent_restarts_dead_healthy_row(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "bus.db"))
    _install(db)
    _register(db)  # stored 'healthy', but the process is gone
    bus = _bus(db)
    monkeypatch.setattr(server_mod, "_pid_alive", lambda pid: False)
    # Don't actually spawn — assert the guard fell through to a (re)start.
    monkeypatch.setattr(bus, "_start_process", lambda installed: {"id": "a1", "status": "started"})
    assert bus.start_agent("a1")["status"] == "started"
