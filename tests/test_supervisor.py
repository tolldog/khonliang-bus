"""v1 supervision (restart-on-crash) for bus-spawned agent subprocesses.

Covers fr_khonliang-bus_dc4ef3e9 v1: detect a dead Popen in
``self._processes`` and re-launch via the existing install path. No
backoff or max-failures threshold — those are explicit follow-ups.
"""

from __future__ import annotations

import asyncio

import pytest

from bus.db import BusDB
from bus.server import BusServer


class _FakePopen:
    """Subprocess.Popen-shaped fake — controllable exit state."""

    def __init__(self, pid: int = 0, alive: bool = True, returncode: int = 0):
        self.pid = pid or id(self)
        self._alive = alive
        self.returncode = returncode if not alive else None

    def poll(self):
        return None if self._alive else self.returncode

    def die(self, code: int = 1) -> None:
        self._alive = False
        self.returncode = code


def _bus(db: BusDB) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999"})


def _install(db: BusDB, agent_id: str, command: str = "/bin/true") -> None:
    db.install_agent(
        agent_id=agent_id,
        agent_type="test",
        command=command,
        args=[],
        cwd="/tmp",
        config="/tmp/cfg.yaml",
    )


def test_supervise_once_alive_process_is_noop(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)
    bus._processes["a1"] = _FakePopen(alive=True)

    result = bus.supervise_once()

    assert result["restarted"] == []
    assert result["alive"] == ["a1"]
    assert result["lost"] == {}
    assert bus._supervisor_restart_counts == {}


def _patch_fake_start_process(monkeypatch, bus: BusServer) -> list[str]:
    """Make ``_start_process`` return a fake-Popen success without spawning
    a real subprocess. Returns the list that gets populated with agent_ids
    each fake-start call processes."""
    spawned: list[str] = []

    def _fake_start(installed):
        new_proc = _FakePopen(alive=True)
        with bus._processes_lock:
            bus._processes[installed["id"]] = new_proc
        spawned.append(installed["id"])
        return {"id": installed["id"], "pid": new_proc.pid, "status": "started"}

    monkeypatch.setattr(bus, "_start_process", _fake_start)
    return spawned


def test_supervise_once_restarts_dead_process(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    fake = _FakePopen(alive=False, returncode=1)
    bus._processes["a1"] = fake
    spawned = _patch_fake_start_process(monkeypatch, bus)

    result = bus.supervise_once()

    assert result["restarted"] == ["a1"]
    assert result["lost"] == {}
    assert spawned == ["a1"]
    # Dead Popen replaced with a fresh fake. ``_processes`` still has the
    # agent — the new Popen IS the restart.
    assert "a1" in bus._processes
    assert bus._processes["a1"] is not fake
    assert bus._supervisor_restart_counts["a1"] == 1


def test_supervise_once_skips_uninstalled_agent(tmp_path):
    """If the agent was uninstalled mid-supervision, don't try to relaunch it."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)
    bus._processes["ghost"] = _FakePopen(alive=False)
    # No install row for "ghost".

    result = bus.supervise_once()

    assert result["restarted"] == []
    assert "ghost" in result["lost"]
    assert "no longer installed" in result["lost"]["ghost"]
    assert "ghost" not in bus._processes


def test_supervise_once_records_restart_failure(tmp_path):
    """If the relaunch itself fails (bad command etc.), record in ``lost`` not ``restarted``."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "broken", command="/no/such/binary/for/supervise")
    bus = _bus(db)
    bus._processes["broken"] = _FakePopen(alive=False)

    result = bus.supervise_once()

    assert result["restarted"] == []
    assert "broken" in result["lost"]
    assert result["lost"]["broken"]  # non-empty error string
    assert bus._supervisor_restart_counts.get("broken", 0) == 0


def test_supervise_once_counts_repeated_restarts(tmp_path, monkeypatch):
    """Restart counter accumulates across calls so operators can spot a flapping agent."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "flapper")
    bus = _bus(db)
    _patch_fake_start_process(monkeypatch, bus)

    # First crash + restart.
    bus._processes["flapper"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    # Second crash + restart.
    bus._processes["flapper"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()

    assert bus._supervisor_restart_counts["flapper"] == 2


def test_supervise_once_clears_stale_registration_before_restart(tmp_path, monkeypatch):
    """Without deregister, ``start_agent`` would short-circuit to
    ``already_running`` because the prior crash left a 'healthy' registration."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    _patch_fake_start_process(monkeypatch, bus)

    # Simulate a prior successful run: agent registered + heartbeat'd healthy
    # with the SAME pid that's now dead, so the PID-match check upholds
    # ownership and the deregister fires.
    fake = _FakePopen(pid=99999, alive=False, returncode=1)
    db.register_agent(
        agent_id="a1",
        agent_type="test",
        callback_url="http://localhost:9999",
        pid=fake.pid,
        version="0.1.0",
        skills=[],
    )
    db.heartbeat("a1")
    bus._processes["a1"] = fake

    result = bus.supervise_once()

    assert "a1" in result["restarted"]
    # The stale 'healthy' row WAS cleared. The fake ``_start_process`` doesn't
    # re-register, so the table stays empty afterwards.
    assert db.get_registration("a1") is None


def test_supervise_once_skips_when_active_ws_connection_with_zero_pid(tmp_path, monkeypatch):
    """WS register payloads can legitimately carry ``pid=0``
    (see ``handle_agent_ws``). The replacement guard must short-circuit on
    an active WS connection regardless of the registration's pid, so a
    pid-0 replacement doesn't get its row wiped."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    _patch_fake_start_process(monkeypatch, bus)

    crashed = _FakePopen(pid=11111, alive=False, returncode=1)
    bus._processes["a1"] = crashed

    # WS-registered replacement with pid=0 + active WS connection.
    db.register_agent(
        agent_id="a1",
        agent_type="test",
        callback_url="ws://...",
        pid=0,
        version="0.1.0",
        skills=[{"name": "do_thing", "description": "", "parameters": {}}],
    )
    bus._agent_connections["a1"] = object()  # active WS sentinel

    result = bus.supervise_once()

    assert "a1" in result["alive"]
    assert "a1" not in result["restarted"]
    reg = db.get_registration("a1")
    assert reg is not None
    assert len(db.get_skills("a1")) == 1
    assert "a1" not in bus._processes


def test_supervise_once_skips_when_replacement_is_already_registered(tmp_path, monkeypatch):
    """If something else already registered against the same ``agent_id``
    with a different PID between the crash and this sweep, the supervisor
    must NOT deregister it — the replacement is canonical now."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    spawned = _patch_fake_start_process(monkeypatch, bus)

    crashed = _FakePopen(pid=11111, alive=False, returncode=1)
    bus._processes["a1"] = crashed

    # Replacement instance registered with a DIFFERENT pid before our sweep.
    db.register_agent(
        agent_id="a1",
        agent_type="test",
        callback_url="http://localhost:9999",
        pid=22222,
        version="0.1.0",
        skills=[{"name": "do_thing", "description": "", "parameters": {}}],
    )

    result = bus.supervise_once()

    # The agent is reported alive (the replacement) and we did NOT
    # trigger a restart — would have wiped the replacement's skills.
    assert "a1" in result["alive"]
    assert "a1" not in result["restarted"]
    assert spawned == []  # restart never fired
    reg = db.get_registration("a1")
    assert reg is not None
    assert int(reg["pid"]) == 22222
    assert len(db.get_skills("a1")) == 1
    assert "a1" not in bus._processes


def test_supervise_once_does_not_restart_after_concurrent_stop(tmp_path, monkeypatch):
    """If a concurrent stop_agent / restart pops the tracked entry between the
    snapshot and the per-agent re-check, the supervisor must treat the agent as
    no longer its own and NOT restart it — restarting would undo an operator
    stop (the ``current is None`` branch of supervise_once)."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    spawned = _patch_fake_start_process(monkeypatch, bus)

    class _SelfPoppingPopen(_FakePopen):
        # poll() runs after the snapshot; popping here makes the re-check
        # under the lock see ``current is None`` (a concurrent stop won the
        # race to remove the entry).
        def poll(self):
            bus._processes.pop("a1", None)
            return self.returncode

    bus._processes["a1"] = _SelfPoppingPopen(pid=11111, alive=False, returncode=1)

    result = bus.supervise_once()

    assert spawned == []  # restart never fired
    assert "a1" not in result["restarted"]
    assert "a1" not in bus._processes


async def test_start_supervisor_returns_running_task_and_is_idempotent(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    task = bus.start_supervisor(interval=10.0)
    same = bus.start_supervisor(interval=10.0)

    assert task is same
    assert not task.done()

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_start_supervisor_rejects_non_positive_interval(tmp_path):
    """``asyncio.sleep`` raises on negative and effectively spins on zero —
    either would silently disable supervision. Reject loudly at config time."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    with pytest.raises(ValueError, match="supervisor interval"):
        bus.start_supervisor(interval=0)
    with pytest.raises(ValueError, match="supervisor interval"):
        bus.start_supervisor(interval=-1.0)
    with pytest.raises(ValueError, match="supervisor interval"):
        bus.start_supervisor(interval=0.001)  # below the minimum


def test_start_supervisor_reads_interval_from_config(tmp_path):
    """Without an explicit kwarg, the supervisor reads ``supervisor_interval_s``
    from bus config — and the same validation guard applies."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bad = BusServer(db, config={"supervisor_interval_s": 0})
    with pytest.raises(ValueError, match="supervisor interval"):
        bad.start_supervisor()


async def test_supervision_loop_runs_supervise_once(tmp_path, monkeypatch):
    """Smoke test: the loop calls supervise_once on its interval cadence.

    Event-driven completion rather than ``asyncio.sleep(N) → assert count``
    so the test is robust against heavily-loaded CI runners — we wait for
    the mock to fire twice, capped by a 2s timeout, instead of sleeping a
    fixed window and hoping.
    """
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    calls = 0
    fired_twice = asyncio.Event()

    def _count():
        nonlocal calls
        calls += 1
        if calls >= 2:
            fired_twice.set()
        return {"restarted": [], "alive": [], "lost": {}}

    monkeypatch.setattr(bus, "supervise_once", _count)

    # Minimum allowed interval — the validation guard is part of the surface
    # this test exercises end-to-end.
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
