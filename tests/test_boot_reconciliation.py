"""Boot-time PID reconciliation: stale registrations get reaped before bus accepts traffic.

Regression coverage for fr_khonliang-bus_5c58c4e9 / bug_khonliang-bus_529d12df —
host reboot kills every agent process, bus's persisted registrations stay marked
``healthy`` with stale PIDs, ``start_agent`` returns ``already_running`` against
PIDs that no longer exist.
"""

from __future__ import annotations

import pytest

from bus.db import BusDB
from bus.server import BusServer, _pid_alive


@pytest.fixture
def fake_alive(monkeypatch):
    """Replace ``bus.server._pid_alive`` with a configurable truth table.

    Avoids any dependency on real OS PIDs — on hosts with ``kernel.pid_max``
    raised above the test's chosen "dead" PID, a real process could occupy
    that slot and flake the test.
    """
    alive_pids: set[int] = set()

    def _check(pid: int) -> bool:
        return pid in alive_pids

    monkeypatch.setattr("bus.server._pid_alive", _check)
    return alive_pids


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


def test_reconcile_clears_dead_pid(tmp_path, fake_alive):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "dead-agent", pid=12345)
    # fake_alive empty: 12345 is treated as dead.

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 0}
    assert db.get_registration("dead-agent") is None


def test_reconcile_keeps_live_pid(tmp_path, fake_alive):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "live-agent", pid=12345)
    fake_alive.add(12345)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 0, "kept": 1}
    assert db.get_registration("live-agent") is not None


def test_reconcile_mixed(tmp_path, fake_alive):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "dead", pid=12345)
    _register(db, "live", pid=23456)
    fake_alive.add(23456)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 1}
    assert db.get_registration("dead") is None
    assert db.get_registration("live") is not None


def test_reconcile_treats_zero_pid_as_dead(tmp_path, fake_alive):
    """A registration with pid=0 has no real process; ``os.kill(0, 0)`` would
    target the whole process group, which is the wrong semantics. The
    ``pid > 0`` guard fires before ``_pid_alive`` is even called."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _register(db, "no-pid", pid=0)

    result = _bus(db).reconcile_on_boot()

    assert result == {"pids_reaped": 1, "kept": 0}
    assert db.get_registration("no-pid") is None


def test_reconcile_treats_negative_pid_as_dead(tmp_path, fake_alive):
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


def test_start_agent_no_longer_already_running_after_reconcile(tmp_path, fake_alive, monkeypatch):
    """A stale ``healthy`` registration with a dead PID must not short-circuit
    ``start_agent`` to ``already_running``. This is now defended at two layers:
    (1) ``start_agent`` derives liveness at call time (fr_khonliang-bus_7bf5ce84),
        so even BEFORE reconcile a dead-PID row falls through to a real (re)start
        — this also covers a mid-life crash, which boot reconciliation (boot-only)
        does not; and
    (2) boot reconciliation deregisters the stale row outright."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    db.install_agent(
        agent_id="my-agent",
        agent_type="test",
        command="/bin/true",
        args=[],
        cwd="/tmp",
        config="/tmp/cfg.yaml",
    )
    _register(db, "my-agent", pid=12345)
    db.heartbeat("my-agent")  # marks status='healthy'
    # fake_alive empty: 12345 is dead.

    bus = _bus(db)
    # Observe the guard's decision without spawning a real process.
    monkeypatch.setattr(bus, "_start_process", lambda installed: {"id": "my-agent", "status": "started"})

    # Layer 1: the liveness-aware guard refuses the false 'already_running' and
    # falls through to the (stubbed) restart path — assert the exact status so
    # the test fails if the guard ever stops reaching _start_process.
    pre = bus.start_agent("my-agent")
    assert pre.get("status") == "started", f"guard should fall through to restart a dead-PID row: {pre}"

    # Layer 2: reconcile deregisters the stale row outright.
    bus.reconcile_on_boot()
    assert db.get_registration("my-agent") is None
    after = bus.start_agent("my-agent")
    assert after.get("status") == "started", f"post-reconcile should restart, not short-circuit: {after}"


# -- _pid_alive helper semantics ---------------------------------------------


def test_pid_alive_returns_false_on_process_lookup_error(monkeypatch):
    def _raise(pid, sig):
        raise ProcessLookupError(pid)

    monkeypatch.setattr("os.kill", _raise)
    assert _pid_alive(12345) is False


def test_pid_alive_returns_true_on_permission_error(monkeypatch):
    """``PermissionError`` from ``os.kill`` means the PID exists but we can't
    signal it (different uid). Must NOT be treated as dead — would cause boot
    reconciliation to wrongly deregister live agents owned by other users."""

    def _raise(pid, sig):
        raise PermissionError(pid)

    monkeypatch.setattr("os.kill", _raise)
    assert _pid_alive(12345) is True


def test_pid_alive_returns_true_on_success(monkeypatch):
    monkeypatch.setattr("os.kill", lambda pid, sig: None)
    assert _pid_alive(12345) is True
