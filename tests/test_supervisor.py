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

    # subprocess.Popen-shaped lifecycle no-ops so _stop_process can terminate a
    # (fake) live process without spawning a real one.
    def terminate(self) -> None:
        self.die(-15)

    def kill(self) -> None:
        self.die(-9)

    def wait(self, timeout=None):
        return self.returncode


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
    """Restart counter accumulates across calls so operators can spot a flapping
    agent — but only once each crash is past the backoff window (advance the
    injectable clock between crashes)."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "flapper")
    bus = _bus(db)
    _patch_fake_start_process(monkeypatch, bus)
    clock = {"t": 1000.0}
    bus._now = lambda: clock["t"]

    # First crash + restart (immediate).
    bus._processes["flapper"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    # Second crash, past the first backoff (backoff_s[0] = 1s) → restart again.
    clock["t"] += 2.0
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


# ---------------------------------------------------------------------------
# Exponential backoff + give-up (fr_khonliang-bus_dc4ef3e9 follow-up)
# ---------------------------------------------------------------------------


def _bus_cfg(db: BusDB, **cfg) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _crash(bus: BusServer, agent_id: str, code: int = 1) -> None:
    """Mark the agent's current tracked process dead in place."""
    bus._processes[agent_id].die(code)


def test_backoff_window_skips_immediate_recrash(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[5.0, 30.0])
    _patch_fake_start_process(monkeypatch, bus)
    clock = {"t": 100.0}
    bus._now = lambda: clock["t"]

    # First crash → immediate restart.
    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    r1 = bus.supervise_once()
    assert r1["restarted"] == ["a1"]

    # Re-crash within the 5s window → backing_off, NOT restarted.
    _crash(bus, "a1")
    r2 = bus.supervise_once()
    assert r2["restarted"] == []
    assert r2["backing_off"] == ["a1"]
    assert bus._supervisor_restart_counts["a1"] == 1  # unchanged

    # Past the window → restart again.
    clock["t"] += 6.0
    r3 = bus.supervise_once()
    assert r3["restarted"] == ["a1"]
    assert bus._supervisor_restart_counts["a1"] == 2


def test_backoff_escalates_per_schedule(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[1.0, 5.0, 30.0], supervisor_max_restarts=10)
    _patch_fake_start_process(monkeypatch, bus)
    clock = {"t": 0.0}
    bus._now = lambda: clock["t"]

    # Restart #1 → next window = backoff_s[0] = 1s.
    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    assert bus._supervisor_backoff["a1"]["next_attempt_at"] == 1.0

    # +1s, restart #2 → next window = backoff_s[1] = 5s.
    clock["t"] = 1.0
    _crash(bus, "a1")
    bus.supervise_once()
    assert bus._supervisor_backoff["a1"]["next_attempt_at"] == 6.0

    # +5s, restart #3 → next window = backoff_s[2] = 30s (capped at last).
    clock["t"] = 6.0
    _crash(bus, "a1")
    bus.supervise_once()
    assert bus._supervisor_backoff["a1"]["next_attempt_at"] == 36.0
    assert bus._supervisor_restart_counts["a1"] == 3


def test_gives_up_after_max_restarts(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=2)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 100.0  # frozen; backoff_s=[0] keeps every sweep eligible

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()           # restart #1
    _crash(bus, "a1")
    bus.supervise_once()           # restart #2
    _crash(bus, "a1")
    result = bus.supervise_once()  # restarts == 2 == max → give up

    assert "a1" in result["gave_up"]
    assert result["restarted"] == []
    assert "a1" in bus._autostart_failures
    assert "gave up" in bus._autostart_failures["a1"]
    assert "a1" not in bus._processes          # stopped supervising
    assert "a1" not in bus._supervisor_backoff


def test_gave_up_agent_surfaced_as_autostart_failed(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=1)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()   # restart #1
    _crash(bus, "a1")
    bus.supervise_once()   # give up

    services = bus.get_services()
    row = next(s for s in services if s["id"] == "a1")
    assert row["status"] == "autostart_failed"
    assert "gave up" in row["autostart_error"]


def test_recovery_window_resets_counter(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[1.0], supervisor_recovery_window_s=100.0)
    _patch_fake_start_process(monkeypatch, bus)
    clock = {"t": 0.0}
    bus._now = lambda: clock["t"]

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()  # restart #1; the fake start leaves it alive
    assert bus._supervisor_backoff["a1"]["restarts"] == 1

    # Survives past the recovery window → next sweep (alive) resets the counter.
    clock["t"] = 150.0
    result = bus.supervise_once()
    assert result["alive"] == ["a1"]
    assert "a1" not in bus._supervisor_backoff


def test_dead_agent_gives_up_even_when_backoff_exceeds_recovery_window(tmp_path):
    """A permanently-dead agent must still hit the give-up ceiling even when the
    elapsed time between sweeps exceeds the recovery window. Recovery is only
    proven by an ALIVE observation — resetting on a still-dead sweep would let a
    broken agent dodge the ceiling forever once backoff >= the window."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "broken", command="/no/such/binary/for/supervise")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=2,
                   supervisor_recovery_window_s=1.0)
    clock = {"t": 0.0}
    bus._now = lambda: clock["t"]

    bus._processes["broken"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()              # attempt #1 (fails), restarts=1
    clock["t"] = 5.0                  # 5s > 1s recovery window, but still DEAD
    bus.supervise_once()              # attempt #2 (fails), restarts=2 — NOT reset
    clock["t"] = 10.0
    result = bus.supervise_once()     # restarts == max → give up

    assert "broken" in result["gave_up"]
    assert "broken" in bus._autostart_failures


def test_scalar_backoff_config_accepted(tmp_path):
    """A constant backoff (scalar) config must not abort bus startup — the
    other supervisor_*_s knobs are scalars too."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = BusServer(db, config={"supervisor_backoff_s": 5})
    assert bus._supervisor_backoff_s == [5.0]


def test_string_scalar_backoff_config(tmp_path):
    """A string scalar ("30" / "0.5") from quoted-YAML / env config is one
    value, not iterated char-by-char."""
    db = BusDB(str(tmp_path / "b.db"))
    assert BusServer(db, config={"supervisor_backoff_s": "30"})._supervisor_backoff_s == [30.0]
    db2 = BusDB(str(tmp_path / "b2.db"))
    assert BusServer(db2, config={"supervisor_backoff_s": "0.5"})._supervisor_backoff_s == [0.5]


def test_string_restart_on_crash_false_disables(tmp_path):
    """supervisor_restart_on_crash="false" must disable restarts — bool("false")
    would otherwise be truthy."""
    db = BusDB(str(tmp_path / "b.db"))
    assert BusServer(db, config={"supervisor_restart_on_crash": "false"})._supervisor_restart_on_crash is False
    db2 = BusDB(str(tmp_path / "b2.db"))
    assert BusServer(db2, config={"supervisor_restart_on_crash": "true"})._supervisor_restart_on_crash is True


def test_restart_failure_keeps_supervising_then_gives_up(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "broken", command="/no/such/binary/for/supervise")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=2)
    bus._now = lambda: 0.0  # backoff_s=[0] → always eligible

    bus._processes["broken"] = _FakePopen(alive=False, returncode=1)
    r1 = bus.supervise_once()  # restart attempt #1 fails
    assert "broken" in r1["lost"]
    # The dead Popen is LEFT in _processes so the next sweep retries it.
    assert "broken" in bus._processes
    assert bus._supervisor_backoff["broken"]["restarts"] == 1

    r2 = bus.supervise_once()  # attempt #2 fails
    assert "broken" in r2["lost"]
    assert bus._supervisor_backoff["broken"]["restarts"] == 2

    r3 = bus.supervise_once()  # restarts == max → give up
    assert "broken" in r3["gave_up"]
    assert "broken" not in bus._processes


def test_restart_on_crash_false_disables_restart(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_restart_on_crash=False)
    _patch_fake_start_process(monkeypatch, bus)

    dead = _FakePopen(alive=False, returncode=1)
    bus._processes["a1"] = dead
    result = bus.supervise_once()

    assert result["would_restart"] == ["a1"]
    assert result["restarted"] == []
    assert bus._processes["a1"] is dead  # left in place, untouched
    assert bus._supervisor_restart_counts == {}


def test_manual_start_clears_give_up_state(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=1)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()  # restart #1
    _crash(bus, "a1")
    bus.supervise_once()  # give up
    assert "a1" in bus._autostart_failures

    # Operator manually starts it → clears the failed surface + backoff state.
    bus.start_agent("a1")
    assert "a1" not in bus._autostart_failures
    assert "a1" not in bus._supervisor_backoff


def test_manual_start_failure_keeps_failed_state(tmp_path):
    """A failed manual retry must NOT clear the failure surface — that would
    hide a still-down agent from /v1/services and reset its restart budget."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "broken", command="/no/such/binary/for/supervise")
    bus = _bus(db)
    # Simulate a prior supervisor give-up.
    bus._autostart_failures["broken"] = "supervisor gave up after 5 consecutive restarts"

    result = bus.start_agent("broken")  # _start_process fails (bad binary)

    assert "error" in result
    assert "broken" in bus._autostart_failures  # outage still visible


def test_manual_start_success_clears_failed_state(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus(db)
    _patch_fake_start_process(monkeypatch, bus)
    bus._autostart_failures["a1"] = "supervisor gave up after 5 consecutive restarts"
    bus._supervisor_backoff["a1"] = {"restarts": 5, "next_attempt_at": 0.0, "last_restart_at": 0.0}

    result = bus.start_agent("a1")

    assert result["status"] == "started"
    assert "a1" not in bus._autostart_failures
    assert "a1" not in bus._supervisor_backoff


def test_uninstall_clears_phantom_autostart_failed(tmp_path, monkeypatch):
    """A supervisor give-up writes _autostart_failures; uninstalling the agent
    must drop it from /v1/services (no phantom service)."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=1)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    _crash(bus, "a1")
    bus.supervise_once()  # give up
    assert any(s["id"] == "a1" for s in bus.get_services())

    bus.uninstall_agent("a1")

    assert "a1" not in bus._autostart_failures
    assert not any(s["id"] == "a1" for s in bus.get_services())


def test_get_services_skips_uninstalled_failure_entry(tmp_path):
    """Read-time guard: even a leftover _autostart_failures entry for an agent
    that's no longer installed is not surfaced."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)
    bus._autostart_failures["ghost"] = "supervisor gave up"  # never installed

    assert not any(s["id"] == "ghost" for s in bus.get_services())


async def test_registration_clears_give_up_state(tmp_path, monkeypatch):
    """An agent that gave up and then recovers OUT OF BAND (registers directly,
    not via start_agent) must clear its stale autostart_failed record — else the
    old reason re-surfaces on /v1/services once the replacement later
    deregisters."""
    from bus.server import RegisterRequest

    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[0.0], supervisor_max_restarts=1)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(bus, "_publish_event", _noop)

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()
    _crash(bus, "a1")
    bus.supervise_once()  # give up
    assert "a1" in bus._autostart_failures

    await bus.register_agent(RegisterRequest(id="a1", callback="http://x", pid=123))

    assert "a1" not in bus._autostart_failures
    # (give-up already popped backoff; this just confirms it's gone)
    assert "a1" not in bus._supervisor_backoff


async def test_registration_preserves_active_backoff_counter(tmp_path, monkeypatch):
    """A supervisor-restarted agent re-registers immediately; registration must
    NOT zero its consecutive-restart counter, or backoff + the give-up ceiling
    would never engage for a crash-loop that keeps registering."""
    from bus.server import RegisterRequest

    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[5.0], supervisor_max_restarts=3)
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(bus, "_publish_event", _noop)

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()  # supervisor restart → restarts=1
    assert bus._supervisor_backoff["a1"]["restarts"] == 1

    # The restarted agent registers (the normal post-restart handshake).
    await bus.register_agent(RegisterRequest(id="a1", callback="ws", pid=99))

    assert bus._supervisor_backoff["a1"]["restarts"] == 1  # preserved


def test_stop_clears_backoff_state(tmp_path, monkeypatch):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    bus = _bus_cfg(db, supervisor_backoff_s=[5.0])
    _patch_fake_start_process(monkeypatch, bus)
    bus._now = lambda: 0.0

    bus._processes["a1"] = _FakePopen(alive=False, returncode=1)
    bus.supervise_once()  # restart → backoff state set
    assert "a1" in bus._supervisor_backoff

    bus.stop_agent("a1")
    assert "a1" not in bus._supervisor_backoff


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
