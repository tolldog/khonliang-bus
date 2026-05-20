"""FastAPI server for the khonliang-bus agent platform.

Endpoints organized by concern:
  - Agent lifecycle: install, uninstall, start, stop, restart
  - Agent registration: register, deregister, heartbeat, services
  - Request/reply: route requests to agent callback URLs
  - Pub/sub: publish, subscribe (WebSocket), ack, nack
  - Sessions: create, get, message, suspend, resume, delete
  - Observability: health, trace, status
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import threading
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from bus.artifacts import ArtifactStore, view_response
from bus.db import BusDB
from bus.flows import FlowEngine
from bus.orchestrator import Orchestrator
from bus.scheduler import SchedulerIntegration
from bus.versions import validate_collaboration_requirements
from bus.webhooks import build_topic, summarize, verify_signature

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------


class InstallRequest(BaseModel):
    agent_type: str
    id: str
    command: str
    args: list[str] = []
    cwd: str = ""
    config: str = ""


class RegisterRequest(BaseModel):
    id: str
    callback: str
    pid: int
    version: str = ""
    skills: list[dict[str, Any]] = []
    collaborations: list[dict[str, Any]] = []
    # Launch metadata extension, since khonliang-bus-lib PR #24
    # (fr_khonliang-bus-lib_2cfc0de6, _cccaa6a9). Both optional —
    # pre-PR#24 bus-lib clients omit them and provenance reports
    # ``registration_type="unknown"``.
    launch_spec: dict[str, Any] | None = None
    launch_info: dict[str, Any] | None = None


class HeartbeatRequest(BaseModel):
    id: str


class RequestMessage(BaseModel):
    agent_id: str | None = None
    agent_type: str | None = None
    operation: str
    args: dict[str, Any] = {}
    timeout: float = 30.0
    trace_id: str = ""
    session_id: str | None = None
    response_mode: str = "raw"  # validated in handle_request
    async_mode: bool = False


class PublishRequest(BaseModel):
    topic: str
    payload: Any = None
    source: str = ""


class WaitRequest(BaseModel):
    """Long-poll for events. Holds the connection until a matching event
    fires, the timeout elapses, or an already-pending event is returned.

    - ``topics``: list of topic strings to match on. Empty list matches all.
    - ``subscriber_id``: stable ID for this waiter. Used to track acked
      messages so the same event isn't re-delivered across calls.
    - ``timeout``: max seconds to wait before returning empty.
    - ``ack_on_return``: if true (default), advance the subscriber's ack
      pointer to the returned event so the next wait call skips it.
    - ``cursor``: where to start matching from. Default ``""`` replays
      unacked history (legacy semantics — a fresh subscriber sees every
      retained event). ``"now"`` / ``"latest"`` snapshot the current
      max message id and only return events published *after* that
      point, so a fresh subscriber doesn't drain days-old backlog one
      event per call. Per-call only — the snapshot is taken at the
      start of this wait and not persisted.
    """

    topics: list[str] = []
    subscriber_id: str = ""
    timeout: float = 30.0
    ack_on_return: bool = True
    cursor: str = ""


class AckRequest(BaseModel):
    subscriber_id: str
    message_id: int
    topic: str


class NackRequest(BaseModel):
    subscriber_id: str
    message_id: int
    topic: str
    reason: str = ""


class SessionCreateRequest(BaseModel):
    agent_id: str
    task: str = ""


class SessionMessageRequest(BaseModel):
    message: str
    args: dict[str, Any] = {}


class FlowRequest(BaseModel):
    flow_id: str
    args: dict[str, Any] = {}
    trace_id: str = ""
    # Per-step timeout (seconds) for ``FlowEngine._call_agent``. When unset,
    # the engine's built-in default applies. Propagated from the MCP adapter's
    # ``_mcp_timeout`` control-plane hint on flow invocations so slow inner
    # agent steps don't get truncated at the engine's default 30s.
    timeout: float | None = None


class GapReport(BaseModel):
    agent_id: str
    operation: str
    reason: str
    context: dict[str, Any] = {}


class FeedbackReport(BaseModel):
    agent_id: str
    kind: str
    operation: str = ""
    area: str = ""
    category: str = ""
    severity: str = ""
    message: str = ""
    context: dict[str, Any] = {}
    suggestion: str = ""
    fingerprint: str = ""


class ArtifactCreateRequest(BaseModel):
    kind: str
    title: str
    content: str
    producer: str = ""
    session_id: str = ""
    trace_id: str = ""
    content_type: str = "text/plain"
    metadata: dict[str, Any] = {}
    source_artifacts: list[str] = []
    id: str = ""
    ttl: str | None = None


class ArtifactDistillRequest(BaseModel):
    mode: str = "brief"
    purpose: str = ""
    max_chars: int = 4000
    cache: bool = True
    cache_ttl_seconds: int | None = None


class ArtifactDistillManyRequest(BaseModel):
    ids: list[str]
    purpose: str = ""
    max_chars: int = 8000


VALID_RESPONSE_MODES = {"raw", "extracted", "distilled"}
VALID_GAP_STATUSES = {"open", "reviewed", "dismissed"}
VALID_FEEDBACK_KINDS = {"gap", "friction", "suggestion"}
VALID_FRICTION_CATEGORIES = {
    "token", "latency", "round_trip", "routing", "format", "retry", "fallback"
}
VALID_FEEDBACK_SEVERITIES = {"", "low", "medium", "high"}
VALID_VERDICTS = {"accept", "push_back", "escalate"}


class EvaluateRequest(BaseModel):
    trace_id: str
    verdict: str  # accept | push_back | escalate
    reason: str = ""
    retry_with: dict[str, Any] = {}  # extra context for push_back


class SessionContextUpdate(BaseModel):
    public_ctx: dict[str, Any] | None = None
    private_ctx: dict[str, Any] | None = None


class OrchestrateRequest(BaseModel):
    task: str
    context: dict[str, Any] = {}
    model: str = ""
    trace_id: str = ""


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class BusServer:
    """The bus service. Create via :meth:`create_app`."""

    def __init__(self, db: BusDB, config: dict[str, Any] | None = None):
        self.db = db
        self.config = config or {}
        self._processes: dict[str, subprocess.Popen] = {}  # agent_id → process
        # FastAPI runs sync ``def`` lifecycle handlers (install/start/stop/
        # restart) in a thread pool while the supervisor loop runs on the
        # event loop — both mutate ``_processes``. A ``threading.Lock``
        # works for both (sync threadpool callers and async-loop callers
        # use the same ``with`` syntax) and keeps the lock surface small
        # compared to flipping every lifecycle handler to ``async def``.
        self._processes_lock = threading.Lock()
        # agent_id → error message; populated by autostart_installed_agents()
        # on bus boot. Reset on each bus restart (runtime-only, not persisted).
        self._autostart_failures: dict[str, str] = {}
        # agent_id → count of times the supervisor re-started this agent in
        # the current bus lifetime. Reset on each bus restart. v1: pure
        # counter for visibility / log signal — no max-failures cap or
        # backoff (sibling fr_khonliang-bus_dc4ef3e9 follow-up).
        self._supervisor_restart_counts: dict[str, int] = {}
        self._supervisor_task: asyncio.Task | None = None
        self._http = httpx.AsyncClient(timeout=30.0)
        self._subscribers: dict[str, list[WebSocket]] = {}  # topic → websockets
        self._agent_connections: dict[str, WebSocket] = {}  # agent_id → WebSocket
        self._pending_responses: dict[str, asyncio.Future] = {}  # correlation_id → Future
        self._pending_agent: dict[str, str] = {}  # correlation_id → agent_id
        self._async_request_tasks: set[asyncio.Task] = set()
        async_limit = int(self.config.get("async_request_concurrency", 8))
        self._async_request_semaphore = asyncio.Semaphore(max(1, async_limit))
        # Long-poll waiters: event fires on any new publish so waiters can re-check
        self._publish_notify: asyncio.Event | None = None
        self._retry_config = self.config.get("retry", {
            "delay": 2.0,
            "max_attempts": 3,
            "backoff": "exponential",
        })
        self.flow_engine = FlowEngine(db, self._http)
        self.artifacts = ArtifactStore(db)
        self.orchestrator = Orchestrator(
            db, self._http,
            ollama_url=self.config.get("ollama_url", "http://localhost:11434"),
            default_model=self.config.get("orchestrator_model", "qwen2.5:7b"),
            bus_url=self.config.get("bus_url", "http://localhost:8787"),
        )
        self.scheduler = SchedulerIntegration(db)

    async def shutdown(self) -> None:
        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
            except Exception:
                # Don't blanket-swallow: a real supervisor crash should be
                # diagnosable in the bus log. Preserve the traceback.
                logger.exception("Supervisor task raised during shutdown")
            self._supervisor_task = None
        if self._async_request_tasks:
            for task in list(self._async_request_tasks):
                task.cancel()
            await asyncio.gather(*self._async_request_tasks, return_exceptions=True)
        await self._http.aclose()

    def reconcile_on_boot(self) -> dict:
        """Drop registrations whose tracked PID is no longer alive.

        Bus restarts faster than agent processes do. Across a host reboot,
        agent processes die but persisted registrations stay marked
        ``healthy`` with stale PIDs — so :meth:`start_agent` returns
        ``already_running`` against PIDs that no longer exist and operators
        must :meth:`restart_agent` to break the no-op cycle (fr_khonliang-bus_5c58c4e9).
        """
        pids_reaped = 0
        kept = 0
        for reg in self.db.get_registrations():
            pid = reg.get("pid")
            # Guard pid > 0 before _pid_alive: os.kill(0, ...) signals the
            # whole process group, os.kill(-N, ...) signals a process group
            # by id — neither is the per-agent semantics this skill needs.
            if pid and int(pid) > 0 and _pid_alive(int(pid)):
                kept += 1
                continue
            self.db.deregister_agent(reg["id"])
            pids_reaped += 1
            logger.info("Reconciled agent %s on boot (pid=%s not alive)", reg["id"], pid)
        logger.info("Boot reconciliation: pids_reaped=%d kept=%d", pids_reaped, kept)
        return {"pids_reaped": pids_reaped, "kept": kept}

    def autostart_installed_agents(self) -> dict:
        """Walk ``installed_agents`` and launch each via :meth:`start_agent`.

        Called from the FastAPI lifespan after :meth:`reconcile_on_boot` —
        bus is listening and the registrations table is clean, so each
        ``start_agent`` call gets a fair shot.

        Failure of any one agent's launch does NOT abort the loop. Errors
        are accumulated in ``self._autostart_failures`` and surfaced on the
        ``/v1/services`` response so an operator inspecting the running
        platform sees what didn't come up. The dict resets on each bus
        restart (runtime state only, not persisted) so a transient failure
        doesn't permanently stigmatize an agent across restarts.

        Idempotent across bus restarts: :meth:`start_agent` returns
        ``already_running`` for any registration the boot-time reconciliation
        kept (none, by construction — boot reconciliation clears the
        registration table — but the guard means a partial pre-boot crash
        can't double-launch on the next pass).

        See fr_khonliang-bus_fc904c3e. v1 source-of-truth is the
        ``installed_agents`` table itself; YAML-config allowlist /
        autostart-disable-per-agent is a follow-up FR. Lazy-eligible
        agents (sibling FR fr_khonliang-bus_c81f7ab5) will opt out via
        the same future config when that lands.
        """
        self._autostart_failures.clear()
        started: list[str] = []
        skipped: list[str] = []
        for installed in self.db.get_installed_agents():
            agent_id = installed["id"]
            try:
                result = self.start_agent(agent_id)
            except Exception as e:
                self._autostart_failures[agent_id] = str(e)
                logger.warning("autostart: %s raised %s", agent_id, e)
                continue
            status = result.get("status")
            if status == "started":
                started.append(agent_id)
                logger.info("autostart: %s started (pid=%s)", agent_id, result.get("pid"))
            elif status == "already_running":
                skipped.append(agent_id)
            else:
                # ``start_agent`` returns ``{error: ...}`` on launch failure
                # (subprocess.Popen exception) or ``{error: 'not installed'}``
                # — neither has ``status == 'started'``.
                err = result.get("error", f"unexpected start_agent result: {result!r}")
                self._autostart_failures[agent_id] = err
                logger.warning("autostart: %s failed — %s", agent_id, err)
        logger.info(
            "Autostart complete: started=%d skipped=%d failed=%d",
            len(started), len(skipped), len(self._autostart_failures),
        )
        return {
            "started": started,
            "skipped": skipped,
            "failed": dict(self._autostart_failures),
        }

    def supervise_once(self) -> dict:
        """Single-pass supervision sweep: walk ``self._processes`` and re-launch
        any whose underlying subprocess has exited.

        Only attaches to processes the CURRENT bus instance spawned (per
        ``feedback_supervision_asymmetry_bus_vs_agents`` — agents stay
        ephemeral; bus is systemd-managed). Across a bus restart, the
        ``_processes`` dict is fresh and supervision picks up whatever
        autostart spawned in this lifetime.

        v1 scope (fr_khonliang-bus_dc4ef3e9): detect + restart. No backoff,
        no per-agent max-failures threshold, no operator notification — all
        explicit follow-ups. Restart count tracked in
        ``self._supervisor_restart_counts`` for log / visibility signal.

        Returns ``{restarted: [...], alive: [...], lost: {id: reason}}``.
        ``lost`` covers cases where the agent was found dead but the
        restart attempt also failed.
        """
        restarted: list[str] = []
        alive: list[str] = []
        lost: dict[str, str] = {}
        # Snapshot under the lock so the supervisor's view of the world
        # doesn't change underfoot while a threadpool-served lifecycle
        # handler mutates ``_processes``. Inspecting dead Popens and
        # calling ``_start_process`` happen outside the lock — they
        # don't need it (Popen.poll is per-process) and ``_start_process``
        # re-acquires the lock for its own write.
        with self._processes_lock:
            tracked = list(self._processes.items())
        for agent_id, proc in tracked:
            if proc.poll() is None:
                alive.append(agent_id)
                continue
            # Process exited. Pop the dead entry before relaunching so the
            # new Popen replaces it cleanly.
            exit_code = proc.returncode
            crashed_pid = proc.pid
            with self._processes_lock:
                # Re-check under the lock: a concurrent stop_agent may have
                # already removed/replaced the entry.
                current = self._processes.get(agent_id)
                if current is proc:
                    self._processes.pop(agent_id, None)
                elif current is not None:
                    # Someone else replaced our tracked Popen with a new one
                    # — that's the new canonical process. Don't touch it.
                    alive.append(agent_id)
                    continue
                # current is None: another sweep already cleared us.
            installed = self.db.get_installed_agent(agent_id)
            if installed is None:
                # Agent was uninstalled while supervised — don't relaunch.
                lost[agent_id] = "no longer installed"
                logger.warning("Supervisor: %s exited (code=%s) and is no longer installed", agent_id, exit_code)
                continue
            # If a different instance has already registered against this
            # agent_id (operator manually started a replacement between the
            # crash and this sweep, or another supervisor instance got there
            # first), don't touch its registration. WS-register payloads
            # may legitimately carry ``pid=0`` (see ``handle_agent_ws``
            # default), so the PID-mismatch check alone isn't sufficient
            # — also short-circuit if there's an active WebSocket
            # connection for this agent_id, which always means a live
            # peer is talking to the bus right now.
            reg = self.db.get_registration(agent_id)
            ws_active = self.is_agent_ws_connected(agent_id)
            different_pid = (
                reg is not None
                and reg.get("pid")
                and int(reg["pid"]) != crashed_pid
            )
            if ws_active or different_pid:
                logger.info(
                    "Supervisor: %s exited (code=%s, pid=%s) but a replacement is present (ws=%s, reg_pid=%s); not disturbing",
                    agent_id, exit_code, crashed_pid, ws_active,
                    (reg or {}).get("pid"),
                )
                alive.append(agent_id)
                continue
            # Clear any stale registration (our own crashed instance's row)
            # so start_agent doesn't see a leftover 'healthy' row from
            # before the crash and short-circuit to ``already_running``.
            self.db.deregister_agent(agent_id)
            result = self._start_process(installed)
            if result.get("status") == "started":
                count = self._supervisor_restart_counts.get(agent_id, 0) + 1
                self._supervisor_restart_counts[agent_id] = count
                restarted.append(agent_id)
                logger.info(
                    "Supervisor: %s exited (code=%s), restarted (pid=%s, restart_count=%d)",
                    agent_id, exit_code, result.get("pid"), count,
                )
            else:
                err = result.get("error", f"unexpected _start_process result: {result!r}")
                lost[agent_id] = err
                logger.warning(
                    "Supervisor: %s exited (code=%s); restart failed: %s",
                    agent_id, exit_code, err,
                )
        return {"restarted": restarted, "alive": alive, "lost": lost}

    async def _supervision_loop(self, interval: float) -> None:
        """Background task: call :meth:`supervise_once` every ``interval`` seconds.

        Cancelled in :meth:`shutdown`. A v1 deliberate trade-off: poll
        rather than SIGCHLD because the supervised set is small (~10 agents)
        and we want bus restart semantics to remain trivial (no signal
        handler ownership tangle with FastAPI / uvicorn).
        """
        logger.info("Supervisor loop started (interval=%.1fs)", interval)
        try:
            while True:
                try:
                    self.supervise_once()
                except Exception:
                    # ``logger.exception`` preserves the traceback in the
                    # bus log — important because the supervisor swallows
                    # the error and keeps looping, so the only signal of a
                    # genuine bug here is the log line.
                    logger.exception("Supervisor sweep raised")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Supervisor loop cancelled")
            raise

    # Minimum supervisor sweep cadence. Below this, ``asyncio.sleep`` either
    # raises (negative) or effectively spins (zero, very small positive),
    # which would peg the event loop and flood logs. 0.1s is small enough
    # for tight tests but still a true delay.
    _SUPERVISOR_MIN_INTERVAL_S: float = 0.1

    def start_supervisor(self, interval: float | None = None) -> asyncio.Task:
        """Launch the supervision loop as a background task. Idempotent —
        returns the existing task if one is already running.

        Raises ``ValueError`` if ``interval`` (or
        ``config.supervisor_interval_s``) is below
        :attr:`_SUPERVISOR_MIN_INTERVAL_S` — a non-positive interval
        would silently disable supervision by spinning the event loop
        or raising deep inside ``asyncio.sleep``.
        """
        if self._supervisor_task is not None and not self._supervisor_task.done():
            return self._supervisor_task
        raw = (
            interval
            if interval is not None
            else self.config.get("supervisor_interval_s", 5.0)
        )
        try:
            secs = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"supervisor interval must be a positive number, got {raw!r}"
            ) from exc
        if secs < self._SUPERVISOR_MIN_INTERVAL_S:
            raise ValueError(
                f"supervisor interval must be >= {self._SUPERVISOR_MIN_INTERVAL_S}s, got {secs}"
            )
        self._supervisor_task = asyncio.create_task(self._supervision_loop(secs))
        return self._supervisor_task

    async def diagnose(self, agent_id: str, detail: str = "brief") -> dict:
        """Probe an agent across multiple failure axes.

        v1 covers bus-side fields only — registration view, WebSocket
        health probe, verdict synthesis. Adapter-side routing sync,
        agent-internal worker state, and recent logs are deferred to
        follow-up FRs (the full Diagnosis envelope is in
        fr_khonliang-bus_8fe376c7).

        ``detail`` is accepted for API stability with future fields but
        currently behaves the same for ``brief`` and ``full``.

        The health probe sends ``operation="health_check"`` over the
        agent's WebSocket. ``health_check`` is part of every agent's
        always-available built-in skill set (provided by
        ``khonliang_bus.BaseAgent``), so any agent that successfully
        registered with the bus will accept it. Agents that override
        the base class without re-providing ``health_check`` will
        diagnose as ``agent_wedged`` here — desired behavior: a
        non-conforming agent IS broken from the bus's point of view.

        The ``pid`` field always reflects the registered PID (so
        operators can grep logs / kernel messages even after a crash).
        ``pid_alive`` is the live ``/proc`` check on that PID.
        """
        reg = self.db.get_registration(agent_id)
        if not reg:
            return {
                "agent_id": agent_id,
                "pid": None,
                "pid_alive": False,
                "bus_registry": {
                    "registered": False,
                    "skill_count": 0,
                    "last_heartbeat": None,
                },
                "health_probe": {
                    "ok": False,
                    "latency_ms": None,
                    "error": "not registered",
                },
                "verdict": "not_registered",
                "recommendation": (
                    f"bus_install_agent(id='{agent_id}', ...) then "
                    f"bus_start_agent('{agent_id}')"
                ),
            }

        pid = reg.get("pid")
        pid_int = int(pid) if pid is not None else 0
        pid_alive = pid_int > 0 and _pid_alive(pid_int)
        pid_known_dead = pid_int > 0 and not pid_alive
        ws_connected = self.is_agent_ws_connected(agent_id)
        skills = self.db.get_skills(agent_id)
        has_health_check = any(s.get("name") == "health_check" for s in skills)

        bus_registry = {
            "registered": True,
            "skill_count": len(skills),
            "last_heartbeat": reg.get("last_heartbeat"),
        }

        # Short-circuit: if the registered PID exists in /proc terms but is
        # gone, the agent IS crashed regardless of any stale entry that may
        # still linger in ``_agent_connections``. Skip the probe — a write
        # to a dead WS would only churn ``correlation_id`` and recover the
        # same verdict the caller will get faster from here.
        if pid_known_dead:
            return {
                "agent_id": agent_id,
                "pid": pid,
                "pid_alive": False,
                "bus_registry": bus_registry,
                "health_probe": {
                    "ok": False,
                    "latency_ms": None,
                    "error": f"pid {pid} not alive (skipped probe)",
                },
                "verdict": "crashed",
                "recommendation": f"bus_start_agent('{agent_id}')",
            }

        health_probe: dict = {"ok": False, "latency_ms": None, "error": None}
        if ws_connected:
            correlation_id = f"diag-{uuid.uuid4().hex[:12]}"
            t0 = time.monotonic()
            result = await self.send_request_to_agent_ws(
                agent_id=agent_id,
                operation="health_check",
                args={},
                correlation_id=correlation_id,
                timeout=5.0,
            )
            latency_ms = int((time.monotonic() - t0) * 1000)
            if isinstance(result, dict) and "error" in result:
                health_probe = {"ok": False, "latency_ms": latency_ms, "error": result["error"]}
            else:
                health_probe = {"ok": True, "latency_ms": latency_ms, "error": None}
        else:
            health_probe["error"] = "no WebSocket connection"

        if health_probe["ok"]:
            verdict = "ok"
            recommendation = "no action needed"
        elif ws_connected and not has_health_check:
            # Probe failed against an agent that's connected over WS but
            # doesn't advertise the ``health_check`` skill. That skill is
            # provided by ``khonliang_bus.BaseAgent`` for every conforming
            # agent, so a missing one is itself the diagnosis: this agent
            # isn't bus-lib-compliant. Recommend fixing the agent, not
            # restarting the bus, so callers don't chase a phantom wedge.
            # Gated on ``ws_connected`` — if the WS is also down, the WS is
            # the load-bearing issue and falls through to the wedged branch
            # below.
            verdict = "agent_wedged"
            recommendation = (
                f"{agent_id} did not register a 'health_check' skill; "
                f"diagnose v1 cannot probe it. Upgrade to khonliang-bus-lib "
                f"BaseAgent or register a health_check skill on the agent."
            )
        else:
            # PID is alive (we'd have short-circuited otherwise) but the
            # probe failed — agent process is running but isn't responding
            # cleanly, OR the WS is down. Folded into one wedged branch in
            # v1; sub-verdicts (``wedged_in_request`` vs ``worker_down``)
            # belong with the worker / adapter sub-field FRs, not here.
            verdict = "agent_wedged"
            recommendation = f"bus_restart_agent('{agent_id}')"

        return {
            "agent_id": agent_id,
            "pid": pid,
            "pid_alive": pid_alive,
            "bus_registry": bus_registry,
            "health_probe": health_probe,
            "verdict": verdict,
            "recommendation": recommendation,
        }

    # -- agent lifecycle --

    def install_agent(self, req: InstallRequest) -> dict:
        self.db.install_agent(
            agent_id=req.id,
            agent_type=req.agent_type,
            command=req.command,
            args=req.args,
            cwd=req.cwd or ".",
            config=req.config,
        )
        logger.info("Installed agent %s (type=%s)", req.id, req.agent_type)
        return {"id": req.id, "status": "installed"}

    def uninstall_agent(self, agent_id: str) -> dict:
        self._stop_process(agent_id)
        if self.db.uninstall_agent(agent_id):
            self.db.deregister_agent(agent_id)
            logger.info("Uninstalled agent %s", agent_id)
            return {"id": agent_id, "status": "uninstalled"}
        return {"id": agent_id, "status": "not_found"}

    def start_agent(self, agent_id: str) -> dict:
        installed = self.db.get_installed_agent(agent_id)
        if not installed:
            return {"id": agent_id, "error": "not installed"}
        reg = self.db.get_registration(agent_id)
        if reg and reg["status"] == "healthy":
            return {"id": agent_id, "status": "already_running"}
        return self._start_process(installed)

    def stop_agent(self, agent_id: str) -> dict:
        self._stop_process(agent_id)
        self.db.deregister_agent(agent_id)
        return {"id": agent_id, "status": "stopped"}

    def restart_agent(self, agent_id: str) -> dict:
        self.stop_agent(agent_id)
        return self.start_agent(agent_id)

    def _start_process(self, installed: dict) -> dict:
        agent_id = installed["id"]
        bus_url = self.config.get("bus_url", "http://localhost:8787")
        cmd = [installed["command"]] + installed["args"] + [
            "--id", agent_id,
            "--bus", bus_url,
            "--config", installed["config"],
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=installed["cwd"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            with self._processes_lock:
                self._processes[agent_id] = proc
            logger.info("Started agent %s (pid=%d)", agent_id, proc.pid)
            return {"id": agent_id, "pid": proc.pid, "status": "started"}
        except Exception as e:
            logger.error("Failed to start agent %s: %s", agent_id, e)
            return {"id": agent_id, "error": str(e)}

    def _stop_process(self, agent_id: str) -> None:
        with self._processes_lock:
            proc = self._processes.pop(agent_id, None)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            logger.info("Stopped agent %s (pid=%d)", agent_id, proc.pid)

    # -- agent registration --

    async def register_agent(self, req: RegisterRequest) -> dict:
        self.db.register_agent(
            agent_id=req.id,
            agent_type=req.id.rsplit("-", 1)[0] if "-" in req.id else req.id,
            callback_url=req.callback,
            pid=req.pid,
            version=req.version,
            skills=req.skills,
            collaborations=req.collaborations,
            launch_spec=req.launch_spec,
            launch_info=req.launch_info,
        )
        logger.info("Registered agent %s at %s (pid=%d, %d skills)", req.id, req.callback, req.pid, len(req.skills))
        # Notify subscribers of registry change
        await self._publish_event("bus.registry_changed", {"agent_id": req.id, "action": "registered"})
        return {"id": req.id, "status": "registered"}

    async def deregister_agent(self, agent_id: str) -> dict:
        if self.db.deregister_agent(agent_id):
            await self._publish_event("bus.registry_changed", {"agent_id": agent_id, "action": "deregistered"})
            return {"id": agent_id, "status": "deregistered"}
        return {"id": agent_id, "status": "not_found"}

    def heartbeat(self, req: HeartbeatRequest) -> dict:
        if self.db.heartbeat(req.id):
            return {"id": req.id, "status": "ok"}
        return {"id": req.id, "status": "not_registered"}

    # -- request/reply --

    async def handle_request(self, req: RequestMessage) -> dict:
        trace_id = req.trace_id or f"t-{uuid.uuid4().hex[:8]}"

        if req.response_mode not in VALID_RESPONSE_MODES:
            return {"error": f"invalid response_mode: {req.response_mode!r}. Must be one of {VALID_RESPONSE_MODES}", "trace_id": trace_id}

        # Resolve agent: by ID or by type
        if req.agent_id:
            reg = self.db.get_registration(req.agent_id)
        elif req.agent_type:
            reg = self.db.get_healthy_agent_for_type(req.agent_type)
        else:
            return {"error": "agent_id or agent_type required", "trace_id": trace_id}

        if not reg:
            return {"error": f"no healthy agent found for {req.agent_id or req.agent_type}", "trace_id": trace_id}

        agent_id = reg["id"]

        # Record trace (include args for push-back replay)
        self.db.record_trace_step(trace_id, step=1, agent_id=agent_id, operation=req.operation, args=req.args)
        t0 = time.monotonic()
        correlation_id = f"req-{uuid.uuid4().hex[:12]}"

        if req.async_mode:
            artifact_id = f"art_{uuid.uuid4().hex[:12]}"
            duration_ms = int((time.monotonic() - t0) * 1000)
            self.db.finish_trace_step(
                trace_id,
                step=1,
                status="accepted",
                duration_ms=duration_ms,
            )
            task = asyncio.create_task(
                self._run_async_request_task(
                    req=req,
                    reg=reg,
                    agent_id=agent_id,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    artifact_id=artifact_id,
                )
            )
            self._async_request_tasks.add(task)
            task.add_done_callback(self._finish_async_request_task)
            return {
                "status": "accepted",
                "trace_id": trace_id,
                "artifact_id": artifact_id,
                "artifact": {
                    "id": artifact_id,
                    "kind": "async_request_result",
                    "available": False,
                },
                "message": "request accepted; result artifact will be created when complete",
            }

        return await self._dispatch_resolved_request(
            req=req,
            reg=reg,
            agent_id=agent_id,
            trace_id=trace_id,
            correlation_id=correlation_id,
            t0=t0,
        )

    async def _run_async_request_task(
        self,
        *,
        req: RequestMessage,
        reg: dict[str, Any],
        agent_id: str,
        trace_id: str,
        correlation_id: str,
        artifact_id: str,
    ) -> None:
        async with self._async_request_semaphore:
            await self._complete_async_request(
                req=req,
                reg=reg,
                agent_id=agent_id,
                trace_id=trace_id,
                correlation_id=correlation_id,
                artifact_id=artifact_id,
            )

    async def _complete_async_request(
        self,
        *,
        req: RequestMessage,
        reg: dict[str, Any],
        agent_id: str,
        trace_id: str,
        correlation_id: str,
        artifact_id: str,
    ) -> None:
        t0 = time.monotonic()
        try:
            response = await self._dispatch_resolved_request(
                req=req,
                reg=reg,
                agent_id=agent_id,
                trace_id=trace_id,
                correlation_id=correlation_id,
                t0=t0,
            )
        except Exception as e:
            duration_ms = int((time.monotonic() - t0) * 1000)
            self.db.finish_trace_step(
                trace_id,
                step=1,
                status="failed",
                duration_ms=duration_ms,
                error=str(e),
            )
            response = {"error": str(e), "trace_id": trace_id}
            logger.exception("Async request %s failed before artifact creation", trace_id)

        self._store_async_request_artifact(
            req=req,
            agent_id=agent_id,
            trace_id=trace_id,
            artifact_id=artifact_id,
            response=response,
        )

    async def _dispatch_resolved_request(
        self,
        *,
        req: RequestMessage,
        reg: dict[str, Any],
        agent_id: str,
        trace_id: str,
        correlation_id: str,
        t0: float,
    ) -> dict:
        # Prefer WebSocket if the agent is connected that way
        if self.is_agent_ws_connected(agent_id):
            result = await self.send_request_to_agent_ws(
                agent_id=agent_id,
                operation=req.operation,
                args=req.args,
                correlation_id=correlation_id,
                trace_id=trace_id,
                session_id=req.session_id,
                response_mode=req.response_mode,
                timeout=req.timeout,
            )
            duration_ms = int((time.monotonic() - t0) * 1000)
            if "error" in result:
                self.db.finish_trace_step(trace_id, step=1, status="failed", duration_ms=duration_ms, error=str(result["error"]))
                return {"error": result["error"], "trace_id": trace_id}
            self.db.finish_trace_step(trace_id, step=1, status="ok", duration_ms=duration_ms)
            return {"result": result.get("result"), "trace_id": trace_id}

        # Fall back to HTTP callback
        callback_url = reg["callback_url"]
        payload = {
            "operation": req.operation,
            "args": req.args,
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "session_id": req.session_id,
            "response_mode": req.response_mode,
        }

        attempt = 0
        max_attempts = int(self._retry_config.get("max_attempts", 3))
        delay = float(self._retry_config.get("delay", 2.0))

        while attempt < max_attempts:
            attempt += 1
            try:
                resp = await self._http.post(
                    f"{callback_url}/v1/handle",
                    json=payload,
                    timeout=req.timeout,
                )
                duration_ms = int((time.monotonic() - t0) * 1000)

                if resp.status_code == 200:
                    result = resp.json()
                    self.db.finish_trace_step(trace_id, step=1, status="ok", duration_ms=duration_ms)
                    return {"result": result.get("result"), "trace_id": trace_id}

                try:
                    error_body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"error": resp.text}
                except (json.JSONDecodeError, ValueError):
                    error_body = {"error": resp.text}
                retryable = error_body.get("retryable", False)

                if not retryable or attempt >= max_attempts:
                    self.db.finish_trace_step(trace_id, step=1, status="failed", duration_ms=duration_ms, error=str(error_body))
                    if attempt >= max_attempts:
                        self.db.add_dead_letter(
                            topic=f"request.{req.operation}",
                            payload=req.args,
                            source="bus",
                            agent_id=agent_id,
                            operation=req.operation,
                            error=str(error_body),
                            attempts=attempt,
                        )
                    return {"error": error_body.get("error", "agent error"), "trace_id": trace_id}

            except httpx.TimeoutException:
                duration_ms = int((time.monotonic() - t0) * 1000)
                if attempt >= max_attempts:
                    self.db.finish_trace_step(trace_id, step=1, status="timeout", duration_ms=duration_ms)
                    self.db.add_dead_letter(
                        topic=f"request.{req.operation}",
                        payload=req.args,
                        source="bus",
                        agent_id=agent_id,
                        operation=req.operation,
                        error=f"timeout after {attempt} attempts",
                        attempts=attempt,
                    )
                    return {"error": f"timeout after {attempt} attempts", "trace_id": trace_id}

            except httpx.ConnectError:
                duration_ms = int((time.monotonic() - t0) * 1000)
                self.db.set_agent_status(agent_id, "unhealthy")
                if attempt >= max_attempts:
                    self.db.finish_trace_step(trace_id, step=1, status="failed", duration_ms=duration_ms, error="connection refused")
                    return {"error": "agent unreachable", "trace_id": trace_id}

            # Backoff before retry
            if self._retry_config.get("backoff") == "exponential":
                await asyncio.sleep(delay * (2 ** (attempt - 1)))
            else:
                await asyncio.sleep(delay)

        return {"error": "max retries exceeded", "trace_id": trace_id}

    def _store_async_request_artifact(
        self,
        *,
        req: RequestMessage,
        agent_id: str,
        trace_id: str,
        artifact_id: str,
        response: dict[str, Any],
    ) -> None:
        status = "failed" if "error" in response else "ok"
        payload = {
            "status": status,
            "trace_id": trace_id,
            "agent_id": agent_id,
            "operation": req.operation,
            "session_id": req.session_id,
            "response_mode": req.response_mode,
            "result": response.get("result"),
            "error": response.get("error"),
        }
        try:
            self.artifacts.create(
                kind="async_request_result",
                title=f"Async result for {agent_id}.{req.operation}",
                content=json.dumps(payload, indent=2, sort_keys=True),
                producer=agent_id,
                session_id=req.session_id or "",
                trace_id=trace_id,
                content_type="application/json",
                metadata={
                    "status": status,
                    "agent_id": agent_id,
                    "operation": req.operation,
                    "response_mode": req.response_mode,
                },
                artifact_id=artifact_id,
            )
        except Exception:
            logger.exception("Failed to store async request artifact %s", artifact_id)

    def _finish_async_request_task(self, task: asyncio.Task) -> None:
        self._async_request_tasks.discard(task)
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            logger.error(
                "Background async request task failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    # -- pub/sub --

    async def publish(self, req: PublishRequest) -> dict:
        msg_id = self.db.publish_message(req.topic, req.payload, req.source)
        # Notify WebSocket subscribers
        await self._push_to_subscribers(req.topic, msg_id)
        # Wake any long-poll waiters so they can re-check their topic filters
        self._notify_waiters()
        return {"id": msg_id, "topic": req.topic}

    def _notify_waiters(self) -> None:
        """Signal all active long-poll waiters that a new event was published."""
        if self._publish_notify is not None:
            # Swap in a fresh event so the current one fires once and waiters
            # that re-wait get a new Event object to await on.
            old = self._publish_notify
            self._publish_notify = asyncio.Event()
            old.set()

    async def wait_for_event(self, req: WaitRequest) -> dict:
        """Long-poll for the next event matching ``topics`` for ``subscriber_id``.

        Returns the first unacked event that matches a subscribed topic. If
        no match exists, waits up to ``timeout`` seconds for a new publish.
        Advances the subscriber's ack pointer if ``ack_on_return`` is true.

        ``req.cursor`` controls the starting point for matching:
          - ``""`` (default) — match from the subscriber's last-acked
            id (replays retained backlog for a fresh subscriber).
          - ``"now"`` / ``"latest"`` — snapshot the current high-water
            mark and require ``m.id > snapshot``. A fresh subscriber
            only sees events published after the wait begins. Per-call
            floor; not persisted into the subscriptions table.
        """
        # Lazy-init so there's always a current event object
        if self._publish_notify is None:
            self._publish_notify = asyncio.Event()

        topics = req.topics or []  # empty = match any
        subscriber_id = req.subscriber_id or f"waiter-{uuid.uuid4().hex[:8]}"
        deadline = time.monotonic() + max(0.0, float(req.timeout))

        # Cursor='now'|'latest' pins the matcher's floor at the current
        # max message id so this wait only sees forward-going publishes.
        # Anything else (including legacy "" / unset) falls back to the
        # subscriber's ack pointer alone.
        cursor = (req.cursor or "").strip().lower()
        min_id = self.db.max_message_id() if cursor in ("now", "latest") else 0

        while True:
            # Check each subscribed topic for unacked messages
            match = self._find_next_matching(subscriber_id, topics, min_id)
            if match is not None:
                if req.ack_on_return:
                    self.db.ack_message(subscriber_id, match["topic"], match["id"])
                return {
                    "event": match,
                    "subscriber_id": subscriber_id,
                    "status": "matched",
                }

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {
                    "event": None,
                    "subscriber_id": subscriber_id,
                    "status": "timeout",
                }

            # Wait for the next publish (or timeout)
            current_notify = self._publish_notify
            try:
                await asyncio.wait_for(current_notify.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return {
                    "event": None,
                    "subscriber_id": subscriber_id,
                    "status": "timeout",
                }
            # Loop and re-check — a publish fired, there may be a match now

    def _find_next_matching(
        self, subscriber_id: str, topics: list[str], min_id: int = 0,
    ) -> dict | None:
        """Return the earliest unacked message matching any of ``topics``
        for ``subscriber_id``, or None.

        If ``topics`` is empty, match any topic on the bus.
        ``min_id`` is the per-call floor used by ``cursor='now'``; 0 keeps
        the legacy replay-from-ack behaviour. Delegates to
        :meth:`BusDB.find_earliest_unacked` — a single indexed query
        instead of per-topic round-trips.
        """
        return self.db.find_earliest_unacked(subscriber_id, topics or None, min_id)

    async def _push_to_subscribers(self, topic: str, msg_id: int) -> None:
        msgs = self.db.get_messages(topic, after_id=msg_id - 1, limit=1)
        if not msgs:
            return
        msg = msgs[0]
        for ws in list(self._subscribers.get(topic, [])):
            try:
                await ws.send_json(msg)
            except Exception:
                self._subscribers[topic].remove(ws)

    async def _publish_event(self, topic: str, payload: Any) -> None:
        msg_id = self.db.publish_message(topic, payload, "bus")
        await self._push_to_subscribers(topic, msg_id)
        self._notify_waiters()

    def ack(self, req: AckRequest) -> dict:
        self.db.ack_message(req.subscriber_id, req.topic, req.message_id)
        return {"status": "acked"}

    def nack(self, req: NackRequest) -> dict:
        # NACK: roll back the ack so the message is redelivered on next
        # poll/subscribe. We set last_acked_id to one below the NACKed
        # message so it (and any after it) get redelivered.
        prev_id = max(0, req.message_id - 1)
        self.db.ack_message(req.subscriber_id, req.topic, prev_id)
        logger.info("NACK from %s on %s msg %d: %s", req.subscriber_id, req.topic, req.message_id, req.reason)
        return {"status": "nacked", "redelivery_from": prev_id + 1}

    async def subscribe_ws(self, ws: WebSocket, topic: str, subscriber_id: str) -> None:
        """WebSocket subscription handler. Caller must accept() first."""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(ws)

        # Send unacknowledged messages first
        last_acked = self.db.get_last_acked(subscriber_id, topic)
        backlog = self.db.get_messages(topic, after_id=last_acked)
        for msg in backlog:
            await ws.send_json(msg)

        try:
            while True:
                data = await ws.receive_text()
                # Client can send ack/nack over the WebSocket too
                try:
                    cmd = json.loads(data)
                    if cmd.get("type") == "ack":
                        self.db.ack_message(subscriber_id, topic, cmd["message_id"])
                except (json.JSONDecodeError, KeyError):
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            if topic in self._subscribers:
                self._subscribers[topic] = [
                    s for s in self._subscribers[topic] if s != ws
                ]

    # -- sessions --

    def create_session(self, req: SessionCreateRequest) -> dict:
        session_id = f"sess-{uuid.uuid4().hex[:8]}"
        self.db.create_session(session_id, req.agent_id)
        return {"session_id": session_id, "agent_id": req.agent_id, "status": "active"}

    def get_session(self, session_id: str) -> dict:
        s = self.db.get_session(session_id)
        if not s:
            return {"error": "session not found"}
        return s

    async def session_message(self, session_id: str, req: SessionMessageRequest) -> dict:
        s = self.db.get_session(session_id)
        if not s:
            return {"error": "session not found"}
        if s["status"] != "active":
            return {"error": f"session is {s['status']}, not active"}
        # Route to the session's agent as a request
        return await self.handle_request(RequestMessage(
            agent_id=s["agent_id"],
            operation="session_message",
            args={"message": req.message, "session_id": session_id, **req.args},
            session_id=session_id,
        ))

    def suspend_session(self, session_id: str) -> dict:
        if not self.db.get_session(session_id):
            return {"error": "session not found"}
        self.db.update_session(session_id, status="suspended")
        return {"session_id": session_id, "status": "suspended"}

    def resume_session(self, session_id: str) -> dict:
        if not self.db.get_session(session_id):
            return {"error": "session not found"}
        self.db.update_session(session_id, status="active")
        return {"session_id": session_id, "status": "active"}

    def archive_session(self, session_id: str) -> dict:
        if not self.db.get_session(session_id):
            return {"error": "session not found"}
        self.db.update_session(session_id, status="archived")
        return {"session_id": session_id, "status": "archived"}

    # -- health monitoring --

    def check_agent_health(self, agent_id: str) -> dict:
        reg = self.db.get_registration(agent_id)
        if not reg:
            return {"id": agent_id, "status": "not_registered"}

        pid = reg["pid"]
        pid_alive = _pid_alive(pid)

        if not pid_alive:
            self.db.set_agent_status(agent_id, "dead")
            return {"id": agent_id, "status": "dead", "pid": pid}

        return {"id": agent_id, "status": reg["status"], "pid": pid}

    def get_services(self) -> list[dict]:
        regs = self.db.get_registrations()
        registered_ids = {reg["id"] for reg in regs}
        result = []
        for reg in regs:
            skills = self.db.get_skills(reg["id"])
            result.append({
                "id": reg["id"],
                "agent_type": reg["agent_type"],
                "status": reg["status"],
                "version": reg["version"],
                "callback_url": reg["callback_url"],
                "pid": reg["pid"],
                "skill_count": len(skills),
                "skills": [s["name"] for s in skills],
            })
        # Surface autostart failures alongside live registrations so the
        # services list reflects the FULL post-boot picture, not just the
        # successes. Skip any agent that DID end up registered (a retry
        # after autostart succeeded — the live entry is canonical).
        # Per fr_khonliang-bus_fc904c3e acceptance #3.
        for agent_id, error in self._autostart_failures.items():
            if agent_id in registered_ids:
                continue
            installed = self.db.get_installed_agent(agent_id)
            result.append({
                "id": agent_id,
                "agent_type": (installed or {}).get("agent_type", "unknown"),
                "status": "autostart_failed",
                "version": "",
                "callback_url": "",
                "pid": None,
                "skill_count": 0,
                "skills": [],
                "autostart_error": error,
            })
        return result

    def get_agent_provenance(self, agent_id: str, redact_sensitive: bool = False) -> dict:
        """Surface what process is currently serving ``agent_id`` and whether
        it matches the canonical install (``installed_agents``).

        Implements ``fr_khonliang-bus_aa096048`` Tier 1: joins the runtime
        ``registrations`` row (carrying the bus-lib launch_spec / launch_info
        handshake, since khonliang-bus-lib PR #24) against the canonical
        ``installed_agents`` row and reports ``match``: bool.

        **Disclosure profile.** This surface exposes process-level metadata
        (cwd, executable path, config path, commit_sha, branch, dirty flag,
        pid). The bus binds 0.0.0.0 with no authentication, so the HTTP
        route is **redacted by default** — operators must explicitly opt
        INTO full disclosure via the ``provenance_disclose_full: true``
        bus config flag (typical for local-trusted hosts, per the
        ``feedback_no_sandboxing_in_local_trusted_env`` memory).

        The in-process call site (this method, called directly from
        Python within the same process) defaults ``redact_sensitive=False``
        — i.e. unredacted — because the trust boundary is process-internal.
        The HTTP route inverts that via the config flag described above.

        When ``redact_sensitive=True``, the ``process.cwd``,
        ``process.executable``, ``process.config``, and the entire
        ``code`` block are replaced with placeholders so a network
        caller learns only the ``registration_type`` + ``match`` verdict.

        Returns:
            ``{agent_id, registration_type, process, code, canonical_install,
            match, notes}``.

            ``registration_type`` ∈ ``{"canonical", "adhoc", "unknown", "none"}``:
            - ``canonical``: a runtime registration exists, ``launch_spec``
              fields all match the ``installed_agents`` row exactly.
            - ``adhoc``: a runtime registration exists but ``launch_spec``
              diverges from the canonical install (or no canonical install
              exists for this agent_id).
            - ``unknown``: a runtime registration exists but the agent did
              not include ``launch_spec`` (older bus-lib).
            - ``none``: no runtime registration; the agent is not connected.

            ``match`` is True/False when ``registration_type`` is canonical/adhoc
            *and* the canonical install exists; None when the comparison can't
            be made (unknown / none / no canonical row).
        """
        runtime = self.db.get_registration(agent_id)
        installed = self.db.get_installed_agent(agent_id)

        # ``get_installed_agent`` already runs ``_row_to_dict`` which parses
        # the ``args`` JSON column into a list. Keep a typed reference for the
        # match comparison; tolerate the legacy string shape defensively
        # (e.g. if a future caller hands us a raw row).
        canonical_install: dict | None = None
        canonical_args: list | None = None
        if installed:
            canonical_install = dict(installed)
            args_field = canonical_install.get("args")
            if isinstance(args_field, list):
                canonical_args = args_field
            elif isinstance(args_field, str):
                try:
                    canonical_args = json.loads(args_field)
                    canonical_install["args"] = canonical_args
                except (json.JSONDecodeError, TypeError):
                    canonical_args = None

        if not runtime:
            return _maybe_redact_provenance({
                "agent_id": agent_id,
                "registration_type": "none",
                "process": None,
                "code": None,
                "canonical_install": canonical_install,
                "match": None,
                "notes": [],
            }, redact_sensitive)

        spec = runtime.get("launch_spec")
        info = runtime.get("launch_info")
        notes: list[str] = []

        process: dict | None = {
            "pid": runtime.get("pid"),
            "executable": spec.get("executable") if isinstance(spec, dict) else None,
            "args": spec.get("args") if isinstance(spec, dict) else None,
            "cwd": spec.get("cwd") if isinstance(spec, dict) else None,
            "config": spec.get("config") if isinstance(spec, dict) else None,
            "started_at": info.get("started_at") if isinstance(info, dict) else None,
        }

        code: dict | None = None
        if isinstance(info, dict):
            code = {
                "commit_sha": info.get("commit_sha"),
                "branch": info.get("branch"),
                "dirty": info.get("dirty"),
                # behind_main is not computed bus-side; downstream consumers
                # (e.g. developer.fleet_inventory) compare against ~/dev mains.
                "behind_main": None,
            }

        # Determine registration_type + match.
        if not isinstance(spec, dict):
            registration_type = "unknown"
            match: bool | None = None
            notes.append("agent did not report launch_spec (pre-PR#24 bus-lib?)")
        elif canonical_install is None:
            registration_type = "adhoc"
            match = None
            notes.append("no canonical install row for this agent_id")
        else:
            match = (
                spec.get("executable") == canonical_install.get("command")
                and spec.get("args") == canonical_args
                and spec.get("cwd") == canonical_install.get("cwd")
                and spec.get("config") == canonical_install.get("config")
            )
            registration_type = "canonical" if match else "adhoc"

        return _maybe_redact_provenance({
            "agent_id": agent_id,
            "registration_type": registration_type,
            "process": process,
            "code": code,
            "canonical_install": canonical_install,
            "match": match,
            "notes": notes,
        }, redact_sensitive)

    def get_platform_status(self) -> dict:
        installed = self.db.get_installed_agents()
        regs = self.db.get_registrations()
        skills = self.db.get_skills()
        validated = self.get_validated_flows()
        return {
            "installed_agents": len(installed),
            "registered_agents": len(regs),
            "healthy": sum(1 for r in regs if r["status"] == "healthy"),
            "unhealthy": sum(1 for r in regs if r["status"] != "healthy"),
            "total_skills": len(skills),
            "total_flows": len(validated["available"]) + len(validated["unavailable"]),
            "available_flows": len(validated["available"]),
            "unavailable_flows": len(validated["unavailable"]),
        }

    # -- artifacts --

    def create_artifact(self, req: ArtifactCreateRequest) -> dict:
        try:
            return self.artifacts.create(
                kind=req.kind,
                title=req.title,
                content=req.content,
                producer=req.producer,
                session_id=req.session_id,
                trace_id=req.trace_id,
                content_type=req.content_type,
                metadata=req.metadata,
                source_artifacts=req.source_artifacts,
                artifact_id=req.id,
                ttl=req.ttl,
            )
        except ValueError as e:
            return {"error": str(e)}

    def list_artifacts(
        self,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> list[dict]:
        return self.artifacts.list(
            session_id=session_id,
            kind=kind,
            producer=producer,
            limit=limit,
        )

    def artifact_metadata(self, artifact_id: str) -> dict:
        meta = self.artifacts.metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        return meta

    def artifact_view(self, artifact_id: str, view: str, **kwargs: Any) -> dict:
        meta = self.artifacts.metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            if view == "head":
                return view_response(meta, self.artifacts.head(artifact_id, **kwargs))
            if view == "tail":
                return view_response(meta, self.artifacts.tail(artifact_id, **kwargs))
            if view == "get":
                return view_response(meta, self.artifacts.get(artifact_id, **kwargs))
            if view == "grep":
                return view_response(meta, self.artifacts.grep(artifact_id, **kwargs))
            if view == "excerpt":
                return view_response(meta, self.artifacts.excerpt(artifact_id, **kwargs))
        except ValueError as e:
            return {"error": str(e)}
        return {"error": f"unknown artifact view: {view}"}

    def distill_artifact(self, artifact_id: str, req: ArtifactDistillRequest) -> dict:
        try:
            return self.artifacts.distill(
                artifact_id,
                mode=req.mode,
                purpose=req.purpose,
                max_chars=req.max_chars,
                cache=req.cache,
                cache_ttl_seconds=req.cache_ttl_seconds,
            )
        except KeyError:
            return {"error": "artifact not found"}
        except ValueError as e:
            return {"error": str(e)}

    def distill_many_artifacts(self, req: ArtifactDistillManyRequest) -> dict:
        try:
            return self.artifacts.distill_many(
                req.ids,
                purpose=req.purpose,
                max_chars=req.max_chars,
            )
        except ValueError as e:
            return {"error": str(e)}
        except KeyError as e:
            return {"error": f"artifact not found: {e.args[0]}"}

    # -- flow orchestration (Step 6) --

    async def execute_flow(self, req: FlowRequest) -> dict:
        return await self.flow_engine.execute(
            req.flow_id, req.args, req.trace_id, step_timeout=req.timeout,
        )

    # -- session context (Step 5) --

    def update_session_context(self, session_id: str, req: SessionContextUpdate) -> dict:
        s = self.db.get_session(session_id)
        if not s:
            return {"error": "session not found"}
        public = json.dumps(req.public_ctx) if req.public_ctx is not None else None
        private = json.dumps(req.private_ctx) if req.private_ctx is not None else None
        self.db.update_session(session_id, public_ctx=public, private_ctx=private)
        return {"session_id": session_id, "status": "context_updated"}

    def get_session_context(self, session_id: str, scope: str = "public") -> dict:
        s = self.db.get_session(session_id)
        if not s:
            return {"error": "session not found"}
        result: dict[str, Any] = {
            "session_id": session_id,
            "status": s["status"],
        }
        if s.get("public_ctx"):
            try:
                result["public"] = json.loads(s["public_ctx"])
            except (json.JSONDecodeError, TypeError):
                result["public"] = s["public_ctx"]
        if scope == "private" and s.get("private_ctx"):
            try:
                result["private"] = json.loads(s["private_ctx"])
            except (json.JSONDecodeError, TypeError):
                result["private"] = s["private_ctx"]
        return result

    # -- skill registry + version gates (Step 2) --

    def get_all_skills(self) -> list[dict]:
        """All skills across all registered agents, namespaced by agent_id."""
        skills = self.db.get_skills()
        for s in skills:
            if isinstance(s.get("parameters"), str):
                try:
                    s["parameters"] = json.loads(s["parameters"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return skills

    def get_validated_flows(self) -> dict:
        """Validate all declared flows against current registrations.

        Returns ``{"available": [...], "unavailable": [...]}`` where each
        flow includes its validation status and any unmet requirements.
        """
        flows = self.db.get_flows()
        regs = self.db.get_registrations()
        available = []
        unavailable = []

        for flow in flows:
            requires = flow.get("requires", {})
            if isinstance(requires, str):
                try:
                    requires = json.loads(requires)
                except (json.JSONDecodeError, TypeError):
                    requires = {}

            # requires can be a list of agent_types (no version gate)
            # or a dict of agent_type → version gate
            if isinstance(requires, list):
                requires = {t: ">=0.0.0" for t in requires}

            met, diagnostics = validate_collaboration_requirements(requires, regs)
            entry = {
                "name": flow["name"],
                "declared_by": flow["declared_by"],
                "description": flow.get("description", ""),
                "requires": requires,
                "valid": met,
            }
            if met:
                available.append(entry)
            else:
                entry["unmet"] = diagnostics
                unavailable.append(entry)

        return {"available": available, "unavailable": unavailable}

    def get_interaction_matrix(self) -> dict:
        """Build the interaction matrix from current registrations.

        Shows solo skill counts per agent and validated collaborative
        flows between agents.
        """
        regs = self.db.get_registrations()
        validated = self.get_validated_flows()

        agents = {}
        for reg in regs:
            skills = self.db.get_skills(reg["id"])
            agents[reg["id"]] = {
                "agent_type": reg["agent_type"],
                "version": reg["version"],
                "status": reg["status"],
                "solo_skills": len(skills),
            }

        collaborations = []
        for flow in validated["available"]:
            collaborations.append({
                "name": flow["name"],
                "declared_by": flow["declared_by"],
                "requires": flow["requires"],
                "status": "available",
            })
        for flow in validated["unavailable"]:
            collaborations.append({
                "name": flow["name"],
                "declared_by": flow["declared_by"],
                "requires": flow["requires"],
                "status": "unavailable",
                "unmet": flow.get("unmet", []),
            })

        return {
            "agents": agents,
            "collaborations": collaborations,
        }


    # -- agent WebSocket connections --

    async def handle_agent_ws(self, ws: WebSocket) -> None:
        """Handle a persistent agent WebSocket connection.

        Protocol (all JSON over one WebSocket):

        Agent → Bus:
          {"type": "register", "id": "...", "version": "...", "skills": [...], "collaborations": [...],
           "launch_spec": {...} | null,    # how to respawn (executable, args, cwd, config); since bus-lib PR #24
           "launch_info": {...} | null}    # runtime snapshot (started_at, commit_sha, branch, dirty); since bus-lib PR #24
          {"type": "heartbeat"}
          {"type": "response", "correlation_id": "...", "result": {...}}
          {"type": "error", "correlation_id": "...", "error": "...", "retryable": bool}
          {"type": "publish", "topic": "...", "payload": {...}}
          {"type": "gap", "operation": "...", "reason": "...", "context": {...}}
          {"type": "feedback", "kind": "gap|friction|suggestion", ...}
          {"type": "deregister"}

        Bus → Agent:
          {"type": "registered", "id": "..."}
          {"type": "request", "operation": "...", "args": {...}, "correlation_id": "...", "trace_id": "...", "session_id": null, "response_mode": "raw"}
          {"type": "ping"}
        """
        await ws.accept()
        agent_id: str | None = None

        try:
            while True:
                data = await ws.receive_json()
                msg_type = data.get("type", "")

                if msg_type == "register":
                    agent_id = data["id"]
                    self._agent_connections[agent_id] = ws
                    # Store registration in DB.
                    # launch_spec / launch_info: optional handshake extension
                    # contributed by khonliang-bus-lib PR #24
                    # (fr_khonliang-bus-lib_2cfc0de6, _cccaa6a9). Older bus-lib
                    # versions omit them; we pass None through unchanged so
                    # get_agent_provenance can report unknown match status.
                    self.db.register_agent(
                        agent_id=agent_id,
                        agent_type=data.get("agent_type", agent_id.rsplit("-", 1)[0] if "-" in agent_id else agent_id),
                        callback_url=f"ws://connected",  # WebSocket — no callback URL needed
                        pid=data.get("pid", 0),
                        version=data.get("version", ""),
                        skills=data.get("skills", []),
                        collaborations=data.get("collaborations", []),
                        launch_spec=data.get("launch_spec"),
                        launch_info=data.get("launch_info"),
                    )
                    await ws.send_json({"type": "registered", "id": agent_id})
                    logger.info("Agent %s connected via WebSocket (%d skills)", agent_id, len(data.get("skills", [])))
                    await self._publish_event("bus.registry_changed", {"agent_id": agent_id, "action": "registered"})

                elif msg_type == "heartbeat":
                    if agent_id:
                        self.db.heartbeat(agent_id)
                    await ws.send_json({"type": "pong"})

                elif msg_type == "response":
                    cid = data.get("correlation_id", "")
                    future = self._pending_responses.pop(cid, None)
                    self._pending_agent.pop(cid, None)
                    if future and not future.done():
                        future.set_result(data)

                elif msg_type == "error":
                    cid = data.get("correlation_id", "")
                    future = self._pending_responses.pop(cid, None)
                    self._pending_agent.pop(cid, None)
                    if future and not future.done():
                        future.set_result(data)

                elif msg_type == "publish":
                    topic = data.get("topic", "")
                    payload = data.get("payload")
                    source = agent_id or "unknown"
                    msg_id = self.db.publish_message(topic, payload, source)
                    await self._push_to_subscribers(topic, msg_id)
                    self._notify_waiters()

                elif msg_type == "gap":
                    if agent_id:
                        self.db.report_gap(
                            agent_id=agent_id,
                            operation=data.get("operation", ""),
                            reason=data.get("reason", ""),
                            context=data.get("context", {}),
                        )

                elif msg_type == "feedback":
                    if agent_id:
                        self.report_feedback(FeedbackReport(
                            agent_id=agent_id,
                            kind=data.get("kind", ""),
                            operation=data.get("operation", ""),
                            area=data.get("area", ""),
                            category=data.get("category", ""),
                            severity=data.get("severity", ""),
                            message=data.get("message", ""),
                            context=data.get("context", {}),
                            suggestion=data.get("suggestion", ""),
                            fingerprint=data.get("fingerprint", ""),
                        ))

                elif msg_type == "deregister":
                    break

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.warning("Agent WebSocket error for %s: %s", agent_id, e)
        finally:
            if agent_id:
                self._agent_connections.pop(agent_id, None)
                # Cancel only pending requests belonging to this agent
                to_cancel = [
                    cid for cid, aid in self._pending_agent.items()
                    if aid == agent_id
                ]
                for cid in to_cancel:
                    self._pending_agent.pop(cid, None)
                    fut = self._pending_responses.pop(cid, None)
                    if fut and not fut.done():
                        fut.set_result({"error": f"agent {agent_id} disconnected"})
                self.db.deregister_agent(agent_id)
                logger.info("Agent %s disconnected (%d pending requests cancelled)", agent_id, len(to_cancel))
                await self._publish_event("bus.registry_changed", {"agent_id": agent_id, "action": "deregistered"})

    async def send_request_to_agent_ws(
        self,
        agent_id: str,
        operation: str,
        args: dict,
        correlation_id: str,
        trace_id: str = "",
        session_id: str | None = None,
        response_mode: str = "raw",
        timeout: float = 30.0,
    ) -> dict:
        """Send a request to an agent over its WebSocket connection."""
        ws = self._agent_connections.get(agent_id)
        if not ws:
            return {"error": f"agent {agent_id} not connected via WebSocket"}

        # Create a Future for the response
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_responses[correlation_id] = future
        self._pending_agent[correlation_id] = agent_id

        # Send the request
        try:
            await ws.send_json({
                "type": "request",
                "operation": operation,
                "args": args,
                "correlation_id": correlation_id,
                "trace_id": trace_id,
                "session_id": session_id,
                "response_mode": response_mode,
            })
        except Exception:
            self._pending_responses.pop(correlation_id, None)
            self._pending_agent.pop(correlation_id, None)
            return {"error": f"agent {agent_id} disconnected during send"}

        # Wait for response with timeout
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_responses.pop(correlation_id, None)
            self._pending_agent.pop(correlation_id, None)
            return {"error": "timeout", "correlation_id": correlation_id}

    def is_agent_ws_connected(self, agent_id: str) -> bool:
        return agent_id in self._agent_connections

    # -- gap reporting --

    def report_gap(self, req: GapReport) -> dict:
        gap_id = self.db.report_gap(
            agent_id=req.agent_id,
            operation=req.operation,
            reason=req.reason,
            context=req.context,
        )
        logger.info("Gap reported by %s: %s — %s", req.agent_id, req.operation, req.reason)
        return {"gap_id": gap_id, "status": "open"}

    def get_gaps(self, status: str = "open") -> list[dict]:
        return self.db.get_gaps(status)

    def update_gap(self, gap_id: int, status: str) -> dict:
        if status not in VALID_GAP_STATUSES:
            return {"error": f"invalid status: {status!r}. Must be one of {VALID_GAP_STATUSES}"}
        # Check gap exists
        with self.db.conn() as c:
            row = c.execute("SELECT id FROM gaps WHERE id = ?", (gap_id,)).fetchone()
            if not row:
                return {"error": f"gap {gap_id} not found"}
        self.db.update_gap_status(gap_id, status)
        return {"gap_id": gap_id, "status": status}

    def report_feedback(self, req: FeedbackReport) -> dict:
        if req.kind not in VALID_FEEDBACK_KINDS:
            return {"error": f"invalid kind: {req.kind!r}. Must be one of {VALID_FEEDBACK_KINDS}"}
        if req.severity not in VALID_FEEDBACK_SEVERITIES:
            return {"error": f"invalid severity: {req.severity!r}. Must be one of {VALID_FEEDBACK_SEVERITIES}"}
        if req.kind == "friction" and req.category not in VALID_FRICTION_CATEGORIES:
            return {"error": f"invalid friction category: {req.category!r}. Must be one of {VALID_FRICTION_CATEGORIES}"}

        message = req.message
        if req.kind == "gap":
            message = message or req.context.get("what_was_needed", "")
        elif req.kind == "friction":
            message = message or req.suggestion or req.category
        elif req.kind == "suggestion":
            message = message or req.suggestion or req.area
        if not message:
            return {"error": "message is required"}

        result = self.db.report_feedback(
            agent_id=req.agent_id,
            kind=req.kind,
            operation=req.operation,
            area=req.area,
            category=req.category,
            severity=req.severity,
            message=message,
            context=req.context,
            suggestion=req.suggestion,
            fingerprint=req.fingerprint,
        )
        logger.info("Feedback reported by %s: %s %s", req.agent_id, req.kind, message)
        return result

    def get_feedback(
        self,
        agent_id: str = "",
        kind: str = "",
        status: str = "open",
        since: str = "",
        limit: int = 50,
    ) -> list[dict]:
        return self.db.get_feedback(
            agent_id=agent_id,
            kind=kind,
            status=status,
            since=since,
            limit=limit,
        )

    # -- response evaluation --

    async def evaluate_response(self, req: EvaluateRequest) -> dict:
        if req.verdict not in VALID_VERDICTS:
            return {"error": f"invalid verdict: {req.verdict!r}. Must be one of {VALID_VERDICTS}", "invalid_input": True}

        self.db.record_evaluation(
            trace_id=req.trace_id,
            verdict=req.verdict,
            reason=req.reason,
            retry_with=req.retry_with,
        )

        if req.verdict == "push_back":
            # Replay the original request, merging original args with retry_with
            trace = self.db.get_trace(req.trace_id)
            if trace:
                last_step = trace[-1]
                original_args = last_step.get("args", {}) or {}
                merged_args = {**original_args, **req.retry_with}
                logger.info(
                    "Push-back on %s.%s — retrying with merged context",
                    last_step.get("agent_id", "?"),
                    last_step.get("operation", "?"),
                )
                return await self.handle_request(RequestMessage(
                    agent_id=last_step.get("agent_id"),
                    operation=last_step.get("operation", ""),
                    args=merged_args,
                    trace_id=f"{req.trace_id}-retry",
                ))
            return {"error": "no trace found to retry", "trace_id": req.trace_id}

        if req.verdict == "escalate":
            gap_id = self.db.report_gap(
                agent_id="bus",
                operation=f"escalation from {req.trace_id}",
                reason=req.reason,
                context=req.retry_with,
            )
            return {"escalated": True, "gap_id": gap_id, "trace_id": req.trace_id}

        return {"verdict": req.verdict, "trace_id": req.trace_id}


_REDACTED = "<redacted>"
_PROCESS_SENSITIVE = ("executable", "cwd", "config")
_INSTALL_SENSITIVE = ("command", "cwd", "config")


def _redact_field(payload: dict, key: str) -> None:
    """Replace ``payload[key]`` with the standard placeholder if the field
    is present (i.e. not None / not absent). Type-agnostic: applies to any
    non-None value (str, list, dict, number) so a malicious agent can't
    bypass redaction by sending an unexpected JSON shape.
    """
    if payload.get(key) is not None:
        payload[key] = _REDACTED


def _maybe_redact_provenance(payload: dict, redact: bool) -> dict:
    """Apply the redaction policy to a provenance payload at every return
    path. Used by ``BusServer.get_agent_provenance`` so the early-return
    case (``registration_type == "none"``) cannot bypass the same masking
    the main flow applies.

    Fields cleared (when ``redact=True``):
      - ``process.executable``, ``process.cwd``, ``process.config``,
        ``process.args`` (regardless of type — any non-None value becomes
        the placeholder).
      - ``code`` block dropped entirely (commit_sha + branch + dirty are
        all disclosure-sensitive).
      - ``canonical_install.command``, ``.cwd``, ``.config``, ``.args``
        replaced the same way.

    Fields preserved (treated as the public verdict):
      - ``agent_id``, ``registration_type``, ``match``, ``notes``.
      - ``process.pid``, ``process.started_at``.
      - ``canonical_install.agent_type``, ``.installed_at``.
    """
    if not redact:
        return payload

    process = payload.get("process")
    if isinstance(process, dict):
        for k in _PROCESS_SENSITIVE:
            _redact_field(process, k)
        _redact_field(process, "args")

    # Drop the entire code block; consumers needing commit info must
    # use a trusted call path.
    if payload.get("code") is not None:
        payload["code"] = None

    install = payload.get("canonical_install")
    if isinstance(install, dict):
        for k in _INSTALL_SENSITIVE:
            _redact_field(install, k)
        _redact_field(install, "args")

    return payload


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The PID exists but we can't signal it (different uid). Treating
        # this as "dead" would let boot reconciliation deregister a live
        # agent whenever the bus runs under a different account.
        return True
    return True


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------


def create_app(db_path: str = "data/bus.db", config: dict[str, Any] | None = None) -> FastAPI:
    db = BusDB(db_path)
    bus = BusServer(db, config)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        bus.reconcile_on_boot()
        bus.autostart_installed_agents()
        bus.start_supervisor()
        yield
        await bus.shutdown()

    app = FastAPI(title="khonliang-bus", version="0.2.0", lifespan=lifespan)

    # -- agent lifecycle --

    @app.post("/v1/install")
    def install(req: InstallRequest):
        return bus.install_agent(req)

    @app.delete("/v1/install/{agent_id}")
    def uninstall(agent_id: str):
        return bus.uninstall_agent(agent_id)

    @app.get("/v1/install")
    def list_installed():
        return bus.db.get_installed_agents()

    @app.post("/v1/install/{agent_id}/start")
    def start(agent_id: str):
        return bus.start_agent(agent_id)

    @app.post("/v1/install/{agent_id}/stop")
    def stop(agent_id: str):
        return bus.stop_agent(agent_id)

    @app.post("/v1/install/{agent_id}/restart")
    def restart(agent_id: str):
        return bus.restart_agent(agent_id)

    # -- agent registration --

    @app.post("/v1/register")
    async def register(req: RegisterRequest):
        return await bus.register_agent(req)

    @app.post("/v1/deregister")
    async def deregister(req: HeartbeatRequest):  # same shape: just { id }
        return await bus.deregister_agent(req.id)

    @app.post("/v1/heartbeat")
    def heartbeat(req: HeartbeatRequest):
        return bus.heartbeat(req)

    @app.get("/v1/services")
    def services():
        return bus.get_services()

    @app.get("/v1/diagnose/{agent_id}")
    async def diagnose(agent_id: str, detail: str = "brief"):
        # No auth / rate-limit — same posture as every other bus control-plane
        # route. khonliang-bus assumes a single-host, local-trusted environment
        # (see project_dev_fleet_convention + feedback_no_sandboxing_in_local_trusted_env);
        # security is an explicit non-goal for v1. Network-exposure hardening
        # is a single platform-wide FR, not per-route.
        if detail not in ("brief", "full"):
            raise HTTPException(
                status_code=400,
                detail=f"detail must be 'brief' or 'full', got {detail!r}",
            )
        return await bus.diagnose(agent_id, detail=detail)

    @app.get("/v1/agent/{agent_id}/provenance")
    def agent_provenance(agent_id: str):
        # fr_khonliang-bus_aa096048: canonical-vs-ad-hoc registration state
        # joined against installed_agents.
        #
        # **HTTP route is redacted by default.** The bus binds 0.0.0.0 with
        # no authentication, so any network peer that can reach this port
        # can read whatever the route returns. Redact-by-default means a
        # forgotten exposure (or a future Tailscale Funnel rule that adds
        # 8788 alongside webhooks) doesn't silently leak host/source
        # metadata — operators must explicitly opt INTO disclosure.
        #
        # Local-trusted hosts (the typical khonliang dev environment, per
        # ``feedback_no_sandboxing_in_local_trusted_env``) opt in by
        # setting ``provenance_disclose_full: true`` in the bus config.
        # The in-process call site (``BusServer.get_agent_provenance``)
        # always supports unredacted output via the ``redact_sensitive``
        # kwarg, so callers reaching the bus over IPC / shared memory
        # / same-process imports remain unaffected.
        disclose_full = bool(bus.config.get("provenance_disclose_full", False))
        return bus.get_agent_provenance(
            agent_id, redact_sensitive=not disclose_full,
        )

    # -- request/reply --

    @app.post("/v1/request")
    async def request(req: RequestMessage):
        return await bus.handle_request(req)

    # -- pub/sub --

    @app.post("/v1/publish")
    async def publish(req: PublishRequest):
        return await bus.publish(req)

    # -- external webhooks --

    @app.post("/v1/webhooks/github")
    async def github_webhook(request: Request):
        """Receive a GitHub webhook and publish it as a bus event.

        GitHub posts JSON with ``X-GitHub-Event`` and (optionally)
        ``X-Hub-Signature-256``. We verify the signature against the
        configured secret, then publish to ``github.<event>[.<action>]``
        with a summarized payload plus the full GitHub body.

        Auth:
          - Normal: ``github_webhook_secret`` (config) or
            ``GITHUB_WEBHOOK_SECRET`` (env) must be set. Invalid
            signature → 401.
          - Dev-only: if no secret is set AND
            ``github_webhook_allow_unsigned`` is True in config, unsigned
            requests are accepted. Otherwise, no-secret deployments
            reject all posts with 503 to prevent forged-event footguns.
        """
        raw = await request.body()
        sig = request.headers.get("X-Hub-Signature-256", "")
        event_type = request.headers.get("X-GitHub-Event", "unknown")
        delivery_id = request.headers.get("X-GitHub-Delivery", "")

        secret = bus.config.get("github_webhook_secret", "") or os.environ.get(
            "GITHUB_WEBHOOK_SECRET", ""
        )
        allow_unsigned = bool(bus.config.get("github_webhook_allow_unsigned", False))

        if not secret and not allow_unsigned:
            logger.warning(
                "GitHub webhook rejected — no secret configured and "
                "github_webhook_allow_unsigned is not set (delivery=%s)",
                delivery_id,
            )
            return JSONResponse(
                status_code=503,
                content={"error": "webhook receiver not configured — set github_webhook_secret or github_webhook_allow_unsigned"},
            )

        if not verify_signature(secret, raw, sig):
            logger.warning("GitHub webhook signature check failed (delivery=%s)", delivery_id)
            return JSONResponse(status_code=401, content={"error": "invalid signature"})

        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            return JSONResponse(status_code=400, content={"error": f"invalid JSON: {e}"})

        if not isinstance(payload, dict):
            return JSONResponse(
                status_code=400,
                content={"error": f"webhook body must be a JSON object (got {type(payload).__name__})"},
            )

        topic = build_topic(event_type, payload)
        summary = summarize(event_type, payload)
        summary["delivery_id"] = delivery_id

        # Publish the summary + the full payload so subscribers can
        # pick whichever level of detail they need.
        result = await bus.publish(PublishRequest(
            topic=topic,
            payload={"summary": summary, "github": payload},
            source="github-webhook",
        ))
        logger.info("GitHub webhook %s (delivery=%s) → %s msg=%s", event_type, delivery_id, topic, result.get("id"))
        return {"status": "published", "topic": topic, "message_id": result.get("id"), "delivery_id": delivery_id}

    @app.post("/v1/ack")
    def ack(req: AckRequest):
        return bus.ack(req)

    @app.post("/v1/nack")
    def nack(req: NackRequest):
        return bus.nack(req)

    @app.post("/v1/wait")
    async def wait_for_event(req: WaitRequest):
        return await bus.wait_for_event(req)

    @app.websocket("/v1/agent")
    async def agent_ws(ws: WebSocket):
        await bus.handle_agent_ws(ws)

    @app.websocket("/v1/subscribe")
    async def subscribe(ws: WebSocket):
        await ws.accept()
        data = await ws.receive_text()
        params = json.loads(data)
        await bus.subscribe_ws(
            ws,
            topic=params["topic"],
            subscriber_id=params["subscriber_id"],
        )

    # -- sessions --

    @app.post("/v1/session")
    def create_session(req: SessionCreateRequest):
        return bus.create_session(req)

    @app.get("/v1/session/{session_id}")
    def get_session(session_id: str):
        return bus.get_session(session_id)

    @app.post("/v1/session/{session_id}/message")
    async def session_message(session_id: str, req: SessionMessageRequest):
        return await bus.session_message(session_id, req)

    @app.post("/v1/session/{session_id}/context")
    def update_context(session_id: str, req: SessionContextUpdate):
        return bus.update_session_context(session_id, req)

    @app.get("/v1/session/{session_id}/context")
    def get_context(session_id: str, scope: str = "public"):
        return bus.get_session_context(session_id, scope)

    @app.post("/v1/session/{session_id}/suspend")
    def suspend_session(session_id: str):
        return bus.suspend_session(session_id)

    @app.post("/v1/session/{session_id}/resume")
    def resume_session(session_id: str):
        return bus.resume_session(session_id)

    @app.delete("/v1/session/{session_id}")
    def archive_session(session_id: str):
        return bus.archive_session(session_id)

    # -- skill registry (Step 2) --

    @app.get("/v1/skills")
    def skills(agent_id: str | None = None):
        if agent_id:
            return bus.db.get_skills(agent_id)
        return bus.get_all_skills()

    @app.get("/v1/flows")
    def flows():
        return bus.get_validated_flows()

    @app.get("/v1/topics")
    def topics(prefix: str = "", limit: int = 200):
        """Introspect the event surface — every topic ever published
        with per-topic metadata. Returns a JSON list of topic objects,
        same shape as ``GET /v1/skills`` (``GET /v1/flows`` returns
        ``{available, unavailable}`` instead — different envelope, so
        this endpoint follows the ``/v1/skills`` shape rather than
        the ``/v1/flows`` one).

        Closes fr_bus_7b2d41d2 (surfaced by dog_ce53165f): an MCP
        session can discover canonical topic strings without reading
        source.
        """
        return bus.db.list_topics(prefix=prefix, limit=limit)

    @app.post("/v1/flow")
    async def execute_flow(req: FlowRequest):
        return await bus.execute_flow(req)

    @app.get("/v1/matrix")
    def matrix():
        return bus.get_interaction_matrix()

    # -- artifacts --

    @app.post("/v1/artifacts")
    def create_artifact(req: ArtifactCreateRequest):
        result = bus.create_artifact(req)
        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])
        return result

    @app.get("/v1/artifacts")
    def list_artifacts(
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ):
        return bus.list_artifacts(
            session_id=session_id,
            kind=kind,
            producer=producer,
            limit=limit,
        )

    @app.get("/v1/artifacts/{artifact_id}")
    def artifact_metadata(artifact_id: str):
        result = bus.artifact_metadata(artifact_id)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.get("/v1/artifacts/{artifact_id}/head")
    def artifact_head(artifact_id: str, lines: int = 80, max_chars: int = 4000):
        result = bus.artifact_view(
            artifact_id, "head", lines=lines, max_chars=max_chars
        )
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.get("/v1/artifacts/{artifact_id}/tail")
    def artifact_tail(artifact_id: str, lines: int = 80, max_chars: int = 4000):
        result = bus.artifact_view(
            artifact_id, "tail", lines=lines, max_chars=max_chars
        )
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.get("/v1/artifacts/{artifact_id}/content")
    def artifact_content(artifact_id: str, offset: int = 0, max_chars: int = 4000):
        result = bus.artifact_view(
            artifact_id, "get", offset=offset, max_chars=max_chars
        )
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.get("/v1/artifacts/{artifact_id}/grep")
    def artifact_grep(
        artifact_id: str,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ):
        result = bus.artifact_view(
            artifact_id,
            "grep",
            pattern=pattern,
            context_lines=context_lines,
            max_matches=max_matches,
            max_chars=max_chars,
        )
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.get("/v1/artifacts/{artifact_id}/excerpt")
    def artifact_excerpt(
        artifact_id: str,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ):
        result = bus.artifact_view(
            artifact_id,
            "excerpt",
            start_line=start_line,
            end_line=end_line,
            max_chars=max_chars,
        )
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result

    @app.post("/v1/artifacts/distill_many")
    def artifact_distill_many(req: ArtifactDistillManyRequest):
        result = bus.distill_many_artifacts(req)
        if "error" in result:
            status = 404 if "not found" in result["error"] else 422
            raise HTTPException(status_code=status, detail=result["error"])
        return result

    @app.post("/v1/artifacts/{artifact_id}/distill")
    def artifact_distill(artifact_id: str, req: ArtifactDistillRequest):
        result = bus.distill_artifact(artifact_id, req)
        if "error" in result:
            status = 404 if "not found" in result["error"] else 422
            raise HTTPException(status_code=status, detail=result["error"])
        return result

    # -- observability --

    # -- gaps --

    @app.post("/v1/gap")
    def report_gap(req: GapReport):
        return bus.report_gap(req)

    @app.get("/v1/gaps")
    def get_gaps(status: str = "open"):
        return bus.get_gaps(status)

    @app.patch("/v1/gap/{gap_id}")
    def update_gap(gap_id: int, status: str):
        result = bus.update_gap(gap_id, status)
        if "error" in result:
            error = result["error"]
            if "not found" in error:
                raise HTTPException(status_code=404, detail=error)
            raise HTTPException(status_code=422, detail=error)
        return result

    @app.post("/v1/feedback")
    def report_feedback(req: FeedbackReport):
        result = bus.report_feedback(req)
        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])
        return result

    @app.get("/v1/feedback")
    def get_feedback(
        agent_id: str = "",
        kind: str = "",
        status: str = "open",
        since: str = "",
        limit: int = 50,
    ):
        return bus.get_feedback(
            agent_id=agent_id,
            kind=kind,
            status=status,
            since=since,
            limit=limit,
        )

    # -- response evaluation --

    @app.post("/v1/evaluate")
    async def evaluate_response(req: EvaluateRequest):
        result = await bus.evaluate_response(req)
        if result.get("invalid_input"):
            raise HTTPException(status_code=422, detail=result["error"])
        return result

    @app.get("/v1/evaluations/{trace_id}")
    def get_evaluations(trace_id: str):
        return bus.db.get_evaluations(trace_id)

    # -- orchestrator --

    @app.post("/v1/orchestrate")
    async def orchestrate(req: OrchestrateRequest):
        return await bus.orchestrator.orchestrate(
            task=req.task,
            context=req.context,
            model=req.model,
            trace_id=req.trace_id,
        )

    # -- scheduler --

    @app.get("/v1/models")
    def models():
        return bus.scheduler.get_model_profiles()

    @app.get("/v1/session/{session_id}/model")
    def session_model(session_id: str):
        return bus.scheduler.get_session_recommendation(session_id)

    # -- observability --

    @app.get("/v1/health")
    def health():
        return {"status": "ok", "version": "0.2.0"}

    @app.get("/v1/trace/{trace_id}")
    def trace(trace_id: str):
        return bus.db.get_trace(trace_id)

    @app.get("/v1/status")
    def status():
        return bus.get_platform_status()

    return app
