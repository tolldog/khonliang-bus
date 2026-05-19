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


def test_supervise_once_restarts_dead_process(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    fake = _FakePopen(alive=False, returncode=1)
    bus._processes["a1"] = fake

    result = bus.supervise_once()

    assert result["restarted"] == ["a1"]
    assert result["lost"] == {}
    # Dead Popen replaced with a fresh real subprocess. _processes still
    # has the agent — the new Popen IS the restart.
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


def test_supervise_once_counts_repeated_restarts(tmp_path):
    """Restart counter accumulates across calls so operators can spot a flapping agent."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "flapper")
    bus = _bus(db)

    # First crash + restart.
    bus._processes["flapper"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    # Second crash + restart.
    bus._processes["flapper"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()

    assert bus._supervisor_restart_counts["flapper"] == 2


def test_supervise_once_clears_stale_registration_before_restart(tmp_path):
    """Without deregister, ``start_agent`` would short-circuit to
    ``already_running`` because the prior crash left a 'healthy' registration."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)

    # Simulate a prior successful run: agent registered + heartbeat'd healthy.
    db.register_agent(
        agent_id="a1",
        agent_type="test",
        callback_url="http://localhost:9999",
        pid=99999,
        version="0.1.0",
        skills=[],
    )
    db.heartbeat("a1")
    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)

    result = bus.supervise_once()

    assert "a1" in result["restarted"]
    # Stale registration was cleared so the new launch could proceed.
    # The new launch immediately Popens /bin/true — by the time we check,
    # the registrations table reflects what _start_process put back, which
    # is nothing (no re-register until the agent connects over WS, which
    # /bin/true won't do). The load-bearing assertion: it didn't short-circuit.


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

    task = bus.start_supervisor(interval=0.01)
    try:
        await asyncio.wait_for(fired_twice.wait(), timeout=2.0)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert calls >= 2
