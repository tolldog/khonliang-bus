"""Boot-time PID reconciliation: stale registrations get reaped before bus accepts traffic.

Regression coverage for fr_khonliang-bus_5c58c4e9 / bug_khonliang-bus_529d12df —
host reboot kills every agent process, bus's persisted registrations stay marked
``healthy`` with stale PIDs, ``start_agent`` returns ``already_running`` against
PIDs that no longer exist.
"""

from __future__ import annotations

import os

from bus.db import BusDB
from bus.server import BusServer


def _register(db: BusDB, agent_id: str, pid: int) -> None:
    db.register_agent(
        agent_id=agent_id,
        agent_type="test",
        callback_url="http://localhost:9999",
        pid=pid,
        version="0.1.0",
        skills=[{"name": "do_thing", "description": "", "parameters": {}}],
    )


def _bus(db: BusDB) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999"})


def test_reconcile_clears_dead_pid(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "dead-agent", pid=999_999)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 0}
    assert db.get_registration("dead-agent") is None


def test_reconcile_keeps_live_pid(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "live-agent", pid=os.getpid())

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 0, "kept": 1}
    assert db.get_registration("live-agent") is not None


def test_reconcile_mixed(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "dead", pid=999_999)
    _register(db, "live", pid=os.getpid())

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 1}
    assert db.get_registration("dead") is None
    assert db.get_registration("live") is not None


def test_reconcile_treats_zero_pid_as_dead(tmp_path):
    """A registration with pid=0 has no real process; ``os.kill(0, 0)`` would
    target the whole process group, which is the wrong semantics."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "no-pid", pid=0)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 0}
    assert db.get_registration("no-pid") is None


def test_reconcile_treats_negative_pid_as_dead(tmp_path):
    """``/v1/register`` accepts any integer PID; a negative value is truthy in
    Python but ``os.kill(-1, ...)`` signals every process the caller can
    reach and ``os.kill(-N, ...)`` targets a whole process group. Either
    would let an invalid persisted PID survive boot reconciliation."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "neg-pid", pid=-1)
    _register(db, "neg-group", pid=-99999)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 2, "kept": 0}
    assert db.get_registration("neg-pid") is None
    assert db.get_registration("neg-group") is None


def test_start_agent_no_longer_already_running_after_reconcile(tmp_path):
    """Load-bearing regression: a stale ``healthy`` registration with a dead PID
    used to short-circuit ``start_agent`` to ``already_running``; reconciliation
    must clear that path before the operator's first ``bus_start_agent`` call."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    db.install_agent(
        agent_id="my-agent",
        agent_type="test",
        command="/bin/true",
        args=[],
        cwd="/tmp",
        config="/tmp/cfg.yaml",
    )
    _register(db, "my-agent", pid=999_999)
    db.heartbeat("my-agent")  # marks status='healthy'

    bus = _bus(db)

    pre = bus.start_agent("my-agent")
    assert pre.get("status") == "already_running", f"pre-reconcile state should be the bug: {pre}"

    bus.reconcile_on_boot()

    after = bus.start_agent("my-agent")
    assert after.get("status") != "already_running", f"post-reconcile should not short-circuit: {after}"
