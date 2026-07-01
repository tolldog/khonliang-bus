"""Lazy hot-load Phase 1 (fr_khonliang-bus_c81f7ab5).

Launch-on-demand: a skill call to a dead lazy-eligible agent triggers a launch,
waits for register (bounded), then dispatches. Covers ACs 1,2,3,5,6. The
idle-shutdown reaper + start_only flag (AC#4) are Phase 2.
"""

from __future__ import annotations

import asyncio

import pytest

from bus.db import BusDB
from bus.server import BusServer, RequestMessage


class _FakePopen:
    def __init__(self, pid: int = 0, alive: bool = True, returncode: int = 0):
        self.pid = pid or id(self)
        self._alive = alive
        self.returncode = returncode if not alive else None

    def poll(self):
        return None if self._alive else self.returncode

    def die(self, code: int = 1) -> None:
        self._alive = False
        self.returncode = code

    def terminate(self):
        self.die(-15)

    def kill(self):
        self.die(-9)

    def wait(self, timeout=None):
        return self.returncode


def _bus(tmp_path, **cfg) -> BusServer:
    db = BusDB(str(tmp_path / "bus.db"))
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _install(db: BusDB, agent_id: str, agent_type: str = "test") -> None:
    db.install_agent(agent_id=agent_id, agent_type=agent_type, command="/bin/true",
                     args=[], cwd="/tmp", config="/tmp/c.yaml")


def _patch_launch(bus: BusServer, register: bool = True):
    """Make _start_process a fake success: track the spawned agent, optionally
    write a registration row so the launch poll sees it come live. Returns the
    call-count list."""
    calls: list[str] = []

    def _fake(installed):
        agent_id = installed["id"]
        calls.append(agent_id)
        with bus._processes_lock:
            bus._processes[agent_id] = _FakePopen(alive=True)
        if register:
            # pid=0 (WS-style) → _derive_live_status falls to heartbeat, not dead.
            bus.db.register_agent(agent_id=agent_id, agent_type="test",
                                  callback_url="ws", pid=0)
        return {"id": agent_id, "pid": 0, "status": "started"}

    bus._start_process = _fake
    return calls


# ---------------------------------------------------------------------------
# AC#1 — config
# ---------------------------------------------------------------------------


def test_lazy_config_parses_str_and_dict_forms(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a", {"agent_id": "b", "idle_shutdown_s": 600}])
    assert set(bus._lazy_config) == {"a", "b"}
    assert bus._lazy_config["b"]["idle_shutdown_s"] == 600


# ---------------------------------------------------------------------------
# _resolve_lazy_target
# ---------------------------------------------------------------------------


def test_resolve_lazy_target_by_id(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    assert bus._resolve_lazy_target(RequestMessage(agent_id="a", operation="x")) == "a"
    assert bus._resolve_lazy_target(RequestMessage(agent_id="other", operation="x")) is None


def test_resolve_lazy_target_requires_install(tmp_path):
    """A lazy_config entry whose agent was uninstalled is not launchable — don't
    take the cold-start path (it'd only produce a synthetic 'not installed')."""
    bus = _bus(tmp_path, lazy_eligible=["a"])  # 'a' NOT installed
    assert bus._resolve_lazy_target(RequestMessage(agent_id="a", operation="x")) is None


def test_resolve_lazy_target_by_type_single_match(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a", agent_type="reviewer")
    assert bus._resolve_lazy_target(RequestMessage(agent_type="reviewer", operation="x")) == "a"


def test_resolve_lazy_target_by_type_ambiguous_is_none(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a", "b"])
    _install(bus.db, "a", agent_type="reviewer")
    _install(bus.db, "b", agent_type="reviewer")
    # Two lazy agents of the same type → don't guess which to start.
    assert bus._resolve_lazy_target(RequestMessage(agent_type="reviewer", operation="x")) is None


def test_lazy_reachable_matrix(tmp_path, monkeypatch):
    """_lazy_agent_reachable: registered AND live (registration required)."""
    import bus.server as srv
    bus = _bus(tmp_path, lazy_eligible=["a"])

    assert bus._lazy_agent_reachable("a", None) is False  # no reg → not reachable

    monkeypatch.setattr(srv, "_pid_alive", lambda pid: False)
    assert bus._lazy_agent_reachable("a", {"pid": 4242, "status": "healthy"}) is False  # dead pid
    monkeypatch.setattr(srv, "_pid_alive", lambda pid: True)
    assert bus._lazy_agent_reachable("a", {"pid": 4242, "status": "healthy"}) is True   # live pid

    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: True)
    assert bus._lazy_agent_reachable("a", {"pid": 0, "status": "healthy"}) is True   # WS live
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: False)
    assert bus._lazy_agent_reachable("a", {"pid": 0, "status": "healthy"}) is False  # pid0, no ws, no proc
    bus._processes["a"] = _FakePopen(alive=True)
    assert bus._lazy_agent_reachable("a", {"pid": 0, "status": "healthy"}) is True   # live spawned process


def test_lazy_needs_launch_avoids_duplicate(tmp_path, monkeypatch):
    """_lazy_needs_launch: launch only when NOT reachable AND no live process —
    a live process with a dropped registration is mid-startup, not a launch cue
    (else we'd spawn a duplicate)."""
    bus = _bus(tmp_path, lazy_eligible=["a"])
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: False)

    assert bus._lazy_needs_launch("a", None) is True  # no reg, no proc → launch

    bus._processes["a"] = _FakePopen(alive=True)  # process coming up, reg dropped
    assert bus._lazy_needs_launch("a", None) is False  # don't duplicate a live process


async def test_do_lazy_launch_no_duplicate_when_process_alive(tmp_path, monkeypatch):
    """If a spawned process is still alive but its registration dropped,
    _do_lazy_launch must NOT spawn a second one — it polls for the existing
    process to (re)register."""
    bus = _bus(tmp_path, lazy_eligible=["a"], lazy_launch_timeout_s=0.05)
    _install(bus.db, "a")
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: False)
    bus._processes["a"] = _FakePopen(alive=True)  # running; reg dropped
    calls: list[str] = []
    bus._start_process = lambda inst: calls.append(inst["id"])
    bus._stop_process = lambda aid: None  # don't kill the fake in teardown

    result = await bus._do_lazy_launch("a")

    assert calls == []                       # no duplicate spawn
    assert "did not register" in result["error"]  # polled the existing proc, never registered


# ---------------------------------------------------------------------------
# AC#2/#3 — launch + wait + timeout
# ---------------------------------------------------------------------------


async def test_do_lazy_launch_success(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    _patch_launch(bus, register=True)

    result = await bus._do_lazy_launch("a")

    assert result == {}
    assert bus.db.get_registration("a") is not None


async def test_do_lazy_launch_timeout_stops_and_errors(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"], lazy_launch_timeout_s=0.15)
    _install(bus.db, "a")
    _patch_launch(bus, register=False)  # spawns but never registers
    stopped: list[str] = []
    bus._stop_process = lambda aid: stopped.append(aid)

    result = await bus._do_lazy_launch("a")

    assert "did not register" in result["error"]
    assert stopped == ["a"]  # half-started process cleaned up


async def test_lazy_launch_timeout_clears_stale_registration(tmp_path, monkeypatch):
    """On timeout the row must be deregistered, not just the process killed — a
    WS pid=0 row can't be derived-dead, so leaving it would black-hole later
    agent_id calls onto a dead callback."""
    import bus.server as srv

    monkeypatch.setattr(srv, "_pid_alive", lambda pid: False)  # any pid reads dead
    bus = _bus(tmp_path, lazy_eligible=["a"], lazy_launch_timeout_s=0.05)
    _install(bus.db, "a")

    def _fake(installed):
        # Simulate a stale/broken registration that never becomes live.
        bus.db.register_agent(agent_id="a", agent_type="test", callback_url="ws", pid=4242)
        with bus._processes_lock:
            bus._processes["a"] = _FakePopen(alive=True)
        return {"id": "a", "pid": 4242, "status": "started"}

    bus._start_process = _fake

    result = await bus._do_lazy_launch("a")

    assert "did not register" in result["error"]
    assert bus.db.get_registration("a") is None  # stale row cleared


async def test_do_lazy_launch_clears_stale_ws_reg_and_spawns(tmp_path, monkeypatch):
    """A stale pid=0 WS registration from a prior exited instance must be cleared
    so start_agent actually spawns — otherwise its pid-derived already_running
    guard would no-op and never relaunch."""
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    # Stale row: registered pid=0, but no WS connection and no live process → down.
    bus.db.register_agent(agent_id="a", agent_type="test", callback_url="ws", pid=0)
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: False)
    calls = _patch_launch(bus, register=True)  # spawn re-registers it live

    result = await bus._do_lazy_launch("a")

    assert result == {}
    assert calls == ["a"]  # actually spawned (not a no-op already_running)


async def test_do_lazy_launch_pid0_reg_without_reachability_times_out(tmp_path, monkeypatch):
    """A pid=0 WS row whose socket dropped is non-dead but NOT reachable — the
    poll must not report success (which would then fail against a dead callback);
    it should hit the launch timeout."""
    bus = _bus(tmp_path, lazy_eligible=["a"], lazy_launch_timeout_s=0.05)
    _install(bus.db, "a")
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: False)

    def _fake(installed):
        # Registers pid=0 but leaves NO live process → not reachable.
        bus.db.register_agent(agent_id="a", agent_type="test", callback_url="ws", pid=0)
        return {"id": "a", "pid": 0, "status": "started"}

    bus._start_process = _fake

    result = await bus._do_lazy_launch("a")

    assert "did not register" in result["error"]  # not a false success


async def test_do_lazy_launch_not_installed(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])  # not installed
    result = await bus._do_lazy_launch("a")
    assert "not installed" in result["error"]


async def test_do_lazy_launch_spawn_failure_is_transient(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    bus._start_process = lambda installed: {"id": "a", "error": "boom"}

    result = await bus._do_lazy_launch("a")

    assert "failed" in result["error"]
    # Transient — must NOT leave a sticky autostart_failed row (stays lazy).
    assert "a" not in bus._autostart_failures


async def test_lazy_launch_dedups_concurrent_callers(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    calls = _patch_launch(bus, register=True)

    results = await asyncio.gather(bus._lazy_launch("a"), bus._lazy_launch("a"))

    assert results == [{}, {}]
    assert calls == ["a"]  # single launch shared by both callers


# ---------------------------------------------------------------------------
# AC#6 — no autostart, no supervisor restart
# ---------------------------------------------------------------------------


def test_autostart_skips_lazy_agents(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["lazy1"])
    _install(bus.db, "lazy1")
    _install(bus.db, "eager1")
    started: list[str] = []
    bus._start_process = lambda inst: (started.append(inst["id"]), {"id": inst["id"], "pid": 1, "status": "started"})[1]

    result = bus.autostart_installed_agents()

    assert "eager1" in started
    assert "lazy1" not in started
    assert "lazy1" in result["skipped"]


def test_supervisor_never_restarts_lazy_agent(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    started: list[str] = []
    bus._start_process = lambda inst: started.append(inst["id"])

    bus._processes["a"] = _FakePopen(alive=False, returncode=1)  # crashed lazy agent
    result = bus.supervise_once()

    assert started == []                     # not restarted
    assert result["restarted"] == []
    assert "a" not in bus._processes          # stopped tracking; next call relaunches


# ---------------------------------------------------------------------------
# AC#5 — bus_welcome state
# ---------------------------------------------------------------------------


def test_bus_welcome_shows_lazy_eligible_state(tmp_path):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")  # installed but never started

    w = bus.get_bus_welcome(detail="brief")
    entry = next(e for e in w["agents"] if e["agent_id"] == "a")
    assert entry["state"] == "lazy_eligible"  # not 'cataloged_dead'


# ---------------------------------------------------------------------------
# handle_request trigger
# ---------------------------------------------------------------------------


async def test_handle_request_triggers_lazy_launch_for_lazy_target(tmp_path, monkeypatch):
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a")
    launched: list[str] = []

    async def _fake_lazy(agent_id):
        # Simulate a successful launch: register the agent so handle_request
        # re-resolves it and proceeds past the no-healthy guard.
        launched.append(agent_id)
        bus.db.register_agent(agent_id=agent_id, agent_type="test", callback_url="ws", pid=0)
        return {}

    monkeypatch.setattr(bus, "_lazy_launch", _fake_lazy)

    # Stub the post-resolution dispatch so the test exercises only the lazy
    # trigger + re-resolution, not WS/callback routing.
    async def _fake_dispatch(*a, **k):
        return {"ok": True, "dispatched": True}
    monkeypatch.setattr(bus, "_dispatch_resolved_request", _fake_dispatch)

    result = await bus.handle_request(RequestMessage(agent_id="a", operation="ping"))
    assert launched == ["a"]
    assert result.get("dispatched") is True  # got past resolution to dispatch


async def test_agent_type_reresolves_by_type_after_lazy_launch(tmp_path, monkeypatch):
    """An agent_type request that cold-starts a lazy agent must re-run normal
    type selection afterward — not force the lazy instance (another healthy
    agent of the type may have appeared during the launch window)."""
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a", agent_type="reviewer")

    calls = {"n": 0}
    other = {"id": "b", "agent_type": "reviewer", "pid": 0, "status": "healthy", "callback_url": "ws"}

    def _staged_type(atype):
        calls["n"] += 1
        return None if calls["n"] == 1 else other  # none first (triggers lazy), b on re-resolve

    monkeypatch.setattr(bus.db, "get_healthy_agent_for_type", _staged_type)

    async def _fake_lazy(agent_id):
        return {}
    monkeypatch.setattr(bus, "_lazy_launch", _fake_lazy)

    dispatched = {}

    async def _fake_dispatch(**kwargs):
        dispatched["agent_id"] = kwargs["agent_id"]
        dispatched["reg"] = kwargs["reg"]
        return {"ok": True}
    monkeypatch.setattr(bus, "_dispatch_resolved_request", _fake_dispatch)

    await bus.handle_request(RequestMessage(agent_type="reviewer", operation="x"))

    assert calls["n"] == 2                 # re-resolved by TYPE, not forced to lazy_id
    assert dispatched["agent_id"] == "b"   # normal type selection won


async def test_agent_type_does_not_relaunch_reachable_lazy_agent(tmp_path, monkeypatch):
    """For an agent_type request, the launch decision must use the lazy agent's
    OWN registration — not the type-resolved reg (None when the only match is
    stale/unhealthy) — else a still-reachable WS instance gets relaunched."""
    bus = _bus(tmp_path, lazy_eligible=["a"])
    _install(bus.db, "a", agent_type="reviewer")
    bus.db.register_agent(agent_id="a", agent_type="reviewer", callback_url="ws", pid=0)
    # Type routing finds nothing healthy, but the agent IS reachable over WS.
    monkeypatch.setattr(bus.db, "get_healthy_agent_for_type", lambda t: None)
    monkeypatch.setattr(bus, "is_agent_ws_connected", lambda aid: True)

    launched: list[str] = []

    async def _fake_lazy(agent_id):
        launched.append(agent_id)
        return {}
    monkeypatch.setattr(bus, "_lazy_launch", _fake_lazy)

    await bus.handle_request(RequestMessage(agent_type="reviewer", operation="x"))

    assert launched == []  # own reg is reachable → no duplicate launch


async def test_handle_request_no_lazy_for_non_lazy_agent(tmp_path, monkeypatch):
    bus = _bus(tmp_path)  # nothing lazy
    launched: list[str] = []
    monkeypatch.setattr(bus, "_lazy_launch", lambda aid: launched.append(aid))

    result = await bus.handle_request(RequestMessage(agent_id="ghost", operation="x"))

    assert launched == []
    assert "no healthy agent" in result["error"]
