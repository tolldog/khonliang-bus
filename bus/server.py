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
import math
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

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
from bus import webhook_install

logger = logging.getLogger(__name__)

#: Agent ids the bus reserves for itself. ``bus`` is the bus's own catalog
#: identity (fr_khonliang-bus_6638f4dc) — bus_welcome renders it and
#: bus_skills(agent_id="bus") returns the built-in tool catalog, so a real
#: agent must not claim the name (it would create ambiguous filter semantics).
RESERVED_AGENT_IDS = frozenset({"bus"})


@lru_cache(maxsize=1)
def load_bus_self_welcome() -> dict:
    """The bus's own welcome blob (``bus/welcome.json``).

    Static content loaded lazily on first call and cached, so ``bus_welcome`` can render
    the bus as a first-class participant in its own catalog without touching
    the agent registry (fr_khonliang-bus_6638f4dc). The declared ``bus_*`` skill
    list is asserted against the adapter's actually-registered tools by
    ``tests/test_bus_self_welcome.py`` so it can't silently drift.
    """
    with Path(__file__).with_name("welcome.json").open(encoding="utf-8") as f:
        return json.load(f)


def bus_self_skill_names() -> list[str]:
    """Flattened list of every skill the bus declares in its welcome catalog."""
    cats = load_bus_self_welcome().get("skills_by_category", {})
    return [name for skills in cats.values() for name in skills]


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
    # Welcome extension, since khonliang-bus-lib PR #25
    # (fr_khonliang-bus_f96722dd). Bus persists into the survives-deregister
    # welcomes table; older bus-lib agents omit it and the bus stores nothing
    # (queries return None until the agent re-registers with the new lib).
    welcome: dict[str, Any] | None = None


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
        # Wall-clock start time of this BusServer instance — used by
        # ``get_bus_welcome`` to report ``bus_uptime_s`` so cold-start
        # callers know whether the bus just rebooted (likely dead agents
        # haven't been respawned yet) vs has been up long enough that
        # missing agents are real outages. fr_khonliang-bus_37498850.
        self._started_at = time.time()
        # agent_id → count of times the supervisor re-started this agent in
        # the current bus lifetime (cumulative; reset only on bus restart) —
        # log/visibility signal, distinct from the consecutive-restart counter
        # in ``_supervisor_backoff`` below which drives backoff + give-up.
        self._supervisor_restart_counts: dict[str, int] = {}
        # Per-agent backoff/give-up state (fr_khonliang-bus_dc4ef3e9 follow-up),
        # keyed by agent_id → {restarts, next_attempt_at, last_restart_at}:
        #   restarts        — consecutive supervisor restart attempts not yet
        #                     cleared by a sustained-liveness recovery; indexes
        #                     the backoff schedule and drives the give-up ceiling.
        #   next_attempt_at — earliest wall-clock the next restart may run.
        #   last_restart_at — for the recovery-window reset.
        # Runtime-only: AC #6 default is no persistence across bus restarts, so a
        # fresh dict each boot is correct.
        self._supervisor_backoff: dict[str, dict] = {}
        # Injectable clock so deterministic tests drive backoff windows without
        # sleeping. Production reads wall-clock.
        self._now = time.time
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
        # Supervisor backoff/give-up policy (fr_khonliang-bus_dc4ef3e9). Flat
        # config keys, consistent with ``supervisor_interval_s``:
        #   supervisor_restart_on_crash  — global auto-restart kill-switch
        #       (True default preserves v1 behaviour). Per-entry gating is a
        #       follow-up — installed_agents has no per-agent metadata column.
        #   supervisor_backoff_s         — cooldown applied AFTER each restart
        #       before the next attempt, indexed by restart count and capped at
        #       the last element.
        #   supervisor_max_restarts      — consecutive-restart ceiling; on reach,
        #       the bus gives up and marks the agent autostart_failed.
        #   supervisor_recovery_window_s — sustained liveness that resets the
        #       consecutive-restart counter.
        # Parse explicitly: config may arrive string-valued (quoted YAML /
        # env-backed), where bool("false") is truthy — so an operator's
        # "false"/"0" must actually disable restarts, not silently enable them.
        _rc = self.config.get("supervisor_restart_on_crash", True)
        if isinstance(_rc, str):
            self._supervisor_restart_on_crash = _rc.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self._supervisor_restart_on_crash = bool(_rc)
        raw_backoff = self.config.get("supervisor_backoff_s", [1.0, 5.0, 30.0, 300.0])
        # Accept a scalar (constant backoff) as well as a schedule list — the
        # other supervisor_*_s knobs are scalars, so a single number is a
        # plausible config and shouldn't abort startup. A string scalar ("30")
        # must be one value, not iterated char-by-char into [3.0, 0.0].
        if isinstance(raw_backoff, (int, float, str)):
            raw_backoff = [raw_backoff]

        def _san_backoff(x) -> float:
            # A non-finite (NaN/Inf) or negative backoff would corrupt the
            # ``now < next_attempt_at`` gate (NaN comparisons are always False;
            # Inf never elapses; negative fires instantly in the past). Coerce
            # any such value to 0.0 (immediate retry) rather than letting bad
            # config wedge the supervisor.
            v = float(x)
            return v if (math.isfinite(v) and v >= 0) else 0.0

        self._supervisor_backoff_s = [_san_backoff(x) for x in raw_backoff] or [1.0]
        self._supervisor_max_restarts = max(
            1, int(self.config.get("supervisor_max_restarts", 5))
        )
        self._supervisor_recovery_window_s = float(
            self.config.get("supervisor_recovery_window_s", 300.0)
        )
        # Lazy hot-load (fr_khonliang-bus_c81f7ab5): agents started on the FIRST
        # skill call to a dead agent, not kept hot. Keyed by agent_id; the agent
        # must be INSTALLED (the launch spec is reused, same as autostart). An
        # agent listed here is NOT autostarted and NOT supervisor-restarted
        # ("exactly one mode" + one launch per call). Config forms accepted:
        #   lazy_eligible: [heavy-reviewer, {agent_id: worker, idle_shutdown_s: 600}]
        self._lazy_config: dict[str, dict] = {}
        for entry in self.config.get("lazy_eligible", []) or []:
            if isinstance(entry, str):
                self._lazy_config[entry] = {}
            elif isinstance(entry, dict) and entry.get("agent_id"):
                self._lazy_config[str(entry["agent_id"])] = entry
        # Wait-for-register budget on a lazy launch — distinct from the request's
        # own dispatch timeout (we wait up to this for the agent to come live,
        # THEN dispatch with the caller's timeout).
        self._lazy_launch_timeout_s = float(self.config.get("lazy_launch_timeout_s", 30.0))
        # In-flight lazy launches: agent_id -> asyncio.Task, so concurrent callers
        # for the same dead agent share ONE launch instead of racing to spawn.
        self._lazy_launches: dict[str, asyncio.Task] = {}
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

        Also purges any catalog rows for RESERVED_AGENT_IDS: reservation now
        blocks new install/register, but an UPGRADED deployment could already
        hold a real agent named ``bus`` that would otherwise be silently
        shadowed by the synthetic self-catalog (fr_khonliang-bus_6638f4dc).
        """
        purged = self._purge_reserved_agents()
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
        logger.info("Boot reconciliation: pids_reaped=%d kept=%d purged_reserved=%d", pids_reaped, kept, purged)
        # Return shape unchanged (pids_reaped/kept) — the reserved purge is a
        # boot-hygiene side effect logged above, not part of the public result.
        return {"pids_reaped": pids_reaped, "kept": kept}

    def _purge_reserved_agents(self) -> int:
        """Remove any catalog rows (registration + install + welcome) for a
        RESERVED_AGENT_ID left over from before the reservation existed, so an
        upgraded deployment doesn't have a real agent shadowed by the synthetic
        bus self-catalog. Returns the count purged."""
        purged = 0
        for agent_id in RESERVED_AGENT_IDS:
            had = (
                self.db.get_registration(agent_id) is not None
                or self.db.get_installed_agent(agent_id) is not None
                or self.db.get_agent_welcome(agent_id) is not None
            )
            if not had:
                continue
            # Purge the CATALOG rows only — deliberately do NOT signal the
            # persisted PID. A bare stored PID can't prove ownership, and after a
            # reboot / PID reuse ``_pid_alive`` may be true for an unrelated
            # process under the same UID; killing it would be worse than the row
            # cleanup. This matches the normal boot reconciliation above, which
            # also only deregisters stale-PID rows and never signals them.
            # (_stop_process still terminates a process THIS instance spawned.)
            self._stop_process(agent_id)
            self.db.deregister_agent(agent_id)
            self.db.uninstall_agent(agent_id)
            # Welcomes survive deregister, so clear it explicitly — otherwise
            # /v1/agents/<id>/welcome + /v1/agents/welcomes still leak the stale
            # blob for the reserved id.
            self.db.delete_agent_welcome(agent_id)
            purged += 1
            logger.warning(
                "Purged reserved agent id %r from the catalog on boot "
                "(reserved for the bus's own self-catalog)", agent_id,
            )
        return purged

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
            # Lazy-eligible agents opt out of autostart — they're launched on the
            # first skill call instead ("exactly one mode", fr_khonliang-bus_c81f7ab5).
            if agent_id in self._lazy_config:
                skipped.append(agent_id)
                continue
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

    def _drop_autostart_failure(self, agent_id: str) -> None:
        """Drop a stale ``autostart_failed`` REASON when the agent comes alive.

        Called from both registration paths (HTTP + WS): registration proves the
        agent is up, so a prior give-up/boot-failure reason is stale and must not
        re-surface on /v1/services when a later replacement deregisters.

        Deliberately does NOT touch ``_supervisor_backoff`` — a
        supervisor-restarted agent re-registers immediately, so zeroing its
        consecutive-restart counter on every registration would defeat backoff +
        the give-up ceiling. The counter is reset only by sustained liveness
        (the recovery window, in :meth:`supervise_once`) or explicit operator
        action (``start_agent`` on spawn success / ``_stop_process`` on stop).
        """
        self._autostart_failures.pop(agent_id, None)


    def supervise_once(self) -> dict:
        """Single-pass supervision sweep: walk ``self._processes`` and re-launch
        any whose underlying subprocess has exited, with exponential backoff and
        a give-up ceiling (fr_khonliang-bus_dc4ef3e9).

        Only attaches to processes the CURRENT bus instance spawned (per
        ``feedback_supervision_asymmetry_bus_vs_agents`` — agents stay
        ephemeral; bus is systemd-managed). Across a bus restart, the
        ``_processes`` dict is fresh and supervision picks up whatever
        autostart spawned in this lifetime.

        Backoff: the first crash restarts immediately; each subsequent crash
        waits ``supervisor_backoff_s[min(restarts, last)]`` before the next
        attempt (capped at the last element). After ``supervisor_max_restarts``
        consecutive restarts the bus gives up — marks the agent
        ``autostart_failed`` (surfaced via ``bus_services``) and stops trying.
        An agent that stays alive for ``supervisor_recovery_window_s`` since its
        last restart has its consecutive-restart counter reset (so the ceiling
        counts a crash-loop, not lifetime crashes). ``effective backoff`` is
        ``max(backoff_s[i], supervisor_interval_s)`` since the sweep only gates
        on the window — it never sleeps inside the loop. Setting
        ``supervisor_restart_on_crash=False`` disables auto-restart globally;
        dead agents are then reported under ``would_restart`` but left down.

        The dead Popen is deliberately LEFT in ``_processes`` while backing off
        or after a failed restart, so the next sweep re-sees it; only a give-up
        or an uninstall removes it (a successful restart overwrites the entry).

        Returns ``{restarted, alive, lost, backing_off, gave_up, would_restart}``.
        ``lost`` covers a dead agent whose restart attempt also failed (it stays
        supervised and is retried after backoff); ``gave_up`` is terminal.
        """
        now = self._now()
        restarted: list[str] = []
        alive: list[str] = []
        lost: dict[str, str] = {}
        backing_off: list[str] = []
        gave_up: dict[str, str] = {}
        would_restart: list[str] = []
        # Snapshot under the lock so the supervisor's view of the world doesn't
        # change underfoot while a threadpool-served lifecycle handler mutates
        # ``_processes``. Inspecting dead Popens and calling ``_start_process``
        # happen outside the lock — Popen.poll is per-process and
        # ``_start_process`` re-acquires the lock for its own write.
        with self._processes_lock:
            tracked = list(self._processes.items())
        for agent_id, proc in tracked:
            st = self._supervisor_backoff.get(agent_id)
            if proc.poll() is None:
                # Alive. Recovery is detected ONLY by OBSERVING the agent up past
                # the window: clear the consecutive-restart counter so the give-up
                # ceiling counts a crash-LOOP, not lifetime crashes. We never reset
                # on a dead sweep — a dead process can't be distinguished from one
                # that crashed immediately and sat dead, and resetting there would
                # let a permanently-broken agent dodge the give-up ceiling once
                # backoff_s reaches the window (each post-window sweep would zero
                # the counter). This requires supervisor_interval_s <
                # supervisor_recovery_window_s so an alive sweep lands during
                # recovery — true by default (5s << 300s).
                if st and now - st["last_restart_at"] >= self._supervisor_recovery_window_s:
                    self._supervisor_backoff.pop(agent_id, None)
                    logger.info(
                        "Supervisor: %s stable %.0fs since last restart; reset backoff counter",
                        agent_id, self._supervisor_recovery_window_s,
                    )
                alive.append(agent_id)
                continue

            exit_code = proc.returncode
            crashed_pid = proc.pid

            # Lazy-eligible agents get ONE launch per skill call — the supervisor
            # never restarts them (fr_khonliang-bus_c81f7ab5 AC#6). Stop tracking
            # the dead Popen and move on; the next skill call re-launches it. This
            # is a clean early-exit, deliberately kept OUT of the backoff/give-up
            # cells. Keyed off _lazy_config (not a launching-set) so a sweep that
            # lands mid-launch is still correct.
            if agent_id in self._lazy_config:
                with self._processes_lock:
                    if self._processes.get(agent_id) is proc:
                        self._processes.pop(agent_id, None)
                self._supervisor_backoff.pop(agent_id, None)
                continue

            # Concurrency re-check WITHOUT popping: a concurrent stop/restart may
            # have replaced (new Popen) or removed (None) our tracked entry.
            # Sweeps are serialized on the supervisor task, so this is never a
            # competing supervisor pass.
            with self._processes_lock:
                current = self._processes.get(agent_id)
                if current is not proc:
                    if current is not None:
                        # A replacement is the canonical process now.
                        alive.append(agent_id)
                    # Either way our crashed instance is no longer canonical;
                    # drop any backoff state we held for it.
                    self._supervisor_backoff.pop(agent_id, None)
                    continue

            installed = self.db.get_installed_agent(agent_id)
            if installed is None:
                # Uninstalled while supervised — stop tracking, don't relaunch.
                with self._processes_lock:
                    if self._processes.get(agent_id) is proc:
                        self._processes.pop(agent_id, None)
                self._supervisor_backoff.pop(agent_id, None)
                lost[agent_id] = "no longer installed"
                logger.warning("Supervisor: %s exited (code=%s) and is no longer installed", agent_id, exit_code)
                continue

            # A replacement registered out-of-band (operator manually started a
            # copy, or another instance got there first). WS-register payloads
            # may legitimately carry ``pid=0`` (see ``handle_agent_ws`` default),
            # so the PID-mismatch check alone isn't sufficient — also short-circuit
            # on an active WebSocket, which always means a live peer is talking to
            # the bus right now. Stop tracking our dead Popen and don't disturb
            # the replacement.
            reg = self.db.get_registration(agent_id)
            ws_active = self.is_agent_ws_connected(agent_id)
            different_pid = (
                reg is not None
                and reg.get("pid")
                and int(reg["pid"]) != crashed_pid
            )
            if ws_active or different_pid:
                with self._processes_lock:
                    if self._processes.get(agent_id) is proc:
                        self._processes.pop(agent_id, None)
                self._supervisor_backoff.pop(agent_id, None)
                logger.info(
                    "Supervisor: %s exited (code=%s, pid=%s) but a replacement is present (ws=%s, reg_pid=%s); not disturbing",
                    agent_id, exit_code, crashed_pid, ws_active,
                    (reg or {}).get("pid"),
                )
                alive.append(agent_id)
                continue

            # Still inside the backoff window AND below the give-up ceiling: skip
            # the restart this sweep, leaving the dead Popen so a later sweep
            # re-sees it. The ``restarts < max`` guard matters: once the ceiling
            # is reached, a further crash must fall through to the give-up path
            # promptly rather than waiting out one more (up to 300s) cooldown
            # first. We deliberately DON'T deregister the crashed row here — it
            # stays visible on get_services/diagnose (as a derived-'dead' agent)
            # instead of vanishing for the whole cooldown; type-based routing
            # still excludes it (reconcile_liveness marks it dead). A replacement
            # that comes up during the window is caught by the guard above on the
            # NEXT sweep (which pops the dead Popen).
            if (
                st
                and now < st["next_attempt_at"]
                and st["restarts"] < self._supervisor_max_restarts
            ):
                backing_off.append(agent_id)
                continue

            # Global kill-switch: report but don't restart (leave the dead Popen
            # so the state stays visible across sweeps).
            if not self._supervisor_restart_on_crash:
                would_restart.append(agent_id)
                continue

            restarts = st["restarts"] if st else 0

            # Give-up ceiling reached: stop supervising and surface the agent as
            # ``autostart_failed`` (reusing the boot-autostart failure surface so
            # bus_services renders it uniformly).
            if restarts >= self._supervisor_max_restarts:
                # Re-validate just before the destructive give-up: a replacement
                # may have registered in the window since the ws/pid guard above.
                # Give-up both deregisters the row (dropping skills/flows) AND
                # stops supervising, so wiping a healthy replacement would
                # silently undo a recovery. Re-fetch the registration here rather
                # than trusting the earlier ``reg`` snapshot.
                reg_now = self.db.get_registration(agent_id)
                replacement_up = self.is_agent_ws_connected(agent_id) or (
                    reg_now is not None
                    and reg_now.get("pid")
                    and int(reg_now["pid"]) != crashed_pid
                )
                with self._processes_lock:
                    if self._processes.get(agent_id) is proc:
                        self._processes.pop(agent_id, None)
                if replacement_up:
                    # A healthy replacement is up — stop tracking our dead Popen
                    # but don't deregister it or mark it failed.
                    self._supervisor_backoff.pop(agent_id, None)
                    logger.info(
                        "Supervisor: %s hit the restart ceiling but a replacement is now registered; not marking failed",
                        agent_id,
                    )
                    alive.append(agent_id)
                    continue
                reason = (
                    f"supervisor gave up after {restarts} consecutive restarts "
                    f"(last exit code={exit_code})"
                )
                self._autostart_failures[agent_id] = reason
                self._supervisor_backoff.pop(agent_id, None)
                if reg_now is not None:
                    self.db.deregister_agent(agent_id)
                gave_up[agent_id] = reason
                logger.error("Supervisor: %s — %s; marking autostart_failed", agent_id, reason)
                continue

            # At the actual restart instant (window elapsed), re-validate for a
            # replacement (same TOCTOU the give-up path guards): one could have
            # registered between the ws/pid guard above and here on another
            # threadpool thread. If so, abort — don't deregister it or spawn a
            # duplicate; stop tracking our dead Popen and treat as alive.
            reg_now = self.db.get_registration(agent_id)
            if self.is_agent_ws_connected(agent_id) or (
                reg_now is not None
                and reg_now.get("pid")
                and int(reg_now["pid"]) != crashed_pid
            ):
                with self._processes_lock:
                    if self._processes.get(agent_id) is proc:
                        self._processes.pop(agent_id, None)
                self._supervisor_backoff.pop(agent_id, None)
                alive.append(agent_id)
                continue
            # No replacement — restart. We deliberately do NOT deregister the
            # crashed row first: a successful respawn re-registers (INSERT OR
            # REPLACE overwrites it), and if the spawn FAILS the row stays visible
            # on get_services (as derived-'dead') for the whole backoff period
            # instead of the agent vanishing until eventual give-up. Routing is
            # unaffected — reconcile_liveness marks the stale row dead, and
            # start_agent's guard reads derived liveness, so neither resolves it.
            result = self._start_process(installed)  # overwrites _processes[id] on success
            backoff_s = self._supervisor_backoff_s[
                min(restarts, len(self._supervisor_backoff_s) - 1)
            ]
            next_at = now + backoff_s
            if result.get("status") == "started":
                self._supervisor_backoff[agent_id] = {
                    "restarts": restarts + 1,
                    "next_attempt_at": next_at,
                    "last_restart_at": now,
                }
                self._supervisor_restart_counts[agent_id] = (
                    self._supervisor_restart_counts.get(agent_id, 0) + 1
                )
                restarted.append(agent_id)
                logger.info(
                    "Supervisor: %s exited (code=%s), restarted (pid=%s, attempt=%d/%d, next_backoff=%gs)",
                    agent_id, exit_code, result.get("pid"), restarts + 1,
                    self._supervisor_max_restarts, backoff_s,
                )
            else:
                # Restart FAILED: _start_process did not add a new Popen, so the
                # old dead Popen remains in _processes and a later sweep retries
                # after backoff. Still count the attempt so we escalate toward
                # give-up rather than retrying a broken launch forever.
                self._supervisor_backoff[agent_id] = {
                    "restarts": restarts + 1,
                    "next_attempt_at": next_at,
                    "last_restart_at": now,
                }
                err = result.get("error", f"unexpected _start_process result: {result!r}")
                lost[agent_id] = err
                logger.warning(
                    "Supervisor: %s exited (code=%s); restart attempt %d/%d failed: %s (retry in %.0fs)",
                    agent_id, exit_code, restarts + 1, self._supervisor_max_restarts, err, backoff_s,
                )
        return {
            "restarted": restarted,
            "alive": alive,
            "lost": lost,
            "backing_off": backing_off,
            "gave_up": gave_up,
            "would_restart": would_restart,
        }

    def reconcile_liveness(self) -> dict:
        """Persist derived live state to the stored registration status so the
        db-SQL readers that filter on ``status='healthy'`` stop selecting agents
        whose process is gone or whose heartbeat is stale.

        The read-time derivation (``get_services`` / ``start_agent`` /
        ``bus_welcome``) reflects liveness immediately, but the SQL-only
        consumers — ``get_healthy_agent_for_type`` used by request routing in
        :meth:`handle_request`, the orchestrator, and collaboration flows — read
        the stored column directly. Without this write-back they keep routing
        work to dead/stale agents (bug_khonliang-bus_529d12df). Run periodically
        from :meth:`_supervision_loop`.

        Idempotent and self-healing: only writes when the derived status differs
        from what's stored, and a recovered agent's next heartbeat resets its
        status to 'healthy' (which also refreshes ``last_heartbeat``). Returns
        ``{updated: {agent_id: new_status}}``.

        The write is a compare-and-set against the ``(status, last_heartbeat,
        pid)`` observed for the row: reconcile runs off-loop (asyncio.to_thread)
        while /v1/heartbeat and re-register run in the FastAPI threadpool, so an
        unconditional write could clobber a fresh heartbeat that landed between
        the read and the write and wrongly downgrade a now-live agent. When the
        CAS misses (the row changed under us) the downgrade is skipped — the next
        tick re-evaluates against the new state.
        """
        updated: dict[str, str] = {}
        for reg in self.db.get_registrations():
            derived = self._derive_live_status(reg)
            if derived != reg.get("status") and self.db.set_agent_status_cas(
                reg["id"],
                derived,
                expected_status=reg.get("status"),
                expected_last_heartbeat=reg.get("last_heartbeat"),
                expected_pid=reg.get("pid"),
            ):
                updated[reg["id"]] = derived
        if updated:
            logger.info("Liveness reconcile updated %d agent(s): %s", len(updated), updated)
        return {"updated": updated}

    async def _supervision_loop(self, interval: float) -> None:
        """Background task: every ``interval`` seconds, run :meth:`supervise_once`
        (restart crashed bus-spawned agents) then :meth:`reconcile_liveness`
        (persist derived stale/dead status so SQL routing readers stay honest).

        Cancelled in :meth:`shutdown`. A v1 deliberate trade-off: poll
        rather than SIGCHLD because the supervised set is small (~10 agents)
        and we want bus restart semantics to remain trivial (no signal
        handler ownership tangle with FastAPI / uvicorn).
        """
        logger.info("Supervisor loop started (interval=%.1fs)", interval)
        try:
            while True:
                try:
                    # Run the sweep in a worker thread: supervise_once does
                    # sqlite I/O and may spawn subprocesses, which would
                    # otherwise block the event loop (WS/HTTP handling) for
                    # the duration. ``_processes_lock`` is a threading.Lock
                    # precisely so this off-loop access stays safe alongside
                    # the threadpool-served lifecycle endpoints.
                    await asyncio.to_thread(self.supervise_once)
                except Exception:
                    # ``logger.exception`` preserves the traceback in the
                    # bus log — important because the supervisor swallows
                    # the error and keeps looping, so the only signal of a
                    # genuine bug here is the log line.
                    logger.exception("Supervisor sweep raised")
                try:
                    # Persist derived liveness so the SQL-only routing readers
                    # (get_healthy_agent_for_type) exclude dead/stale agents
                    # (fr_khonliang-bus_7bf5ce84). Also off-loop (sqlite writes),
                    # and isolated from the sweep so one failing doesn't skip
                    # the other.
                    await asyncio.to_thread(self.reconcile_liveness)
                except Exception:
                    logger.exception("Liveness reconcile raised")
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
        if req.id in RESERVED_AGENT_IDS:
            return {"id": req.id, "error": f"'{req.id}' is a reserved agent id"}
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
        # Drop any recorded autostart/supervisor-give-up failure so an
        # uninstalled agent doesn't linger on the autostart_failed surface.
        self._autostart_failures.pop(agent_id, None)
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
        # Guard on DERIVED liveness, not the stored status. start_agent must not
        # spawn a duplicate of an agent whose process is already PRESENT —
        # _start_process never stops/probes an existing PID first, so a second
        # copy would race to register the same id. Any non-'dead' derivation
        # means a process is (or is presumed) up — 'healthy', 'stale' (alive but
        # late on heartbeats), or 'unhealthy' (alive but degraded) — so only a
        # 'dead' row (PID gone) falls through to a real (re)start, the runtime
        # form of the post-reboot no-op (bug_khonliang-bus_529d12df,
        # dog_8b70cbe3). To force a restart of a live-but-degraded agent, use
        # restart_agent (stop + start).
        if reg and self._derive_live_status(reg) != "dead":
            return {"id": agent_id, "status": "already_running"}
        result = self._start_process(installed)
        # On a successful SPAWN, reset only the backoff budget so the operator's
        # manual restart starts fresh. Deliberately do NOT clear the
        # autostart_failed REASON here — the new process hasn't registered yet
        # and may die during init, so the outage must stay visible on
        # /v1/services until registration proves the agent is actually back
        # (``_drop_autostart_failure`` on the register path clears it then).
        if result.get("status") == "started":
            self._supervisor_backoff.pop(agent_id, None)
        elif result.get("error"):
            # The manual (re)start failed to SPAWN. restart_agent's _stop_process
            # already removed the dead Popen + backoff state, so without this the
            # agent would vanish from /v1/services AND from supervision (no
            # registration, no _processes entry, no backoff) — silently unrecovered.
            # Record the failure so it stays visible as autostart_failed until an
            # operator fixes it and a start actually succeeds (registration clears it).
            self._autostart_failures[agent_id] = f"start failed: {result['error']}"
        return result

    def stop_agent(self, agent_id: str) -> dict:
        self._stop_process(agent_id)
        # Explicit stop = intentionally down: clear any supervisor give-up record
        # so it doesn't linger as a synthetic autostart_failed outage. Do this in
        # stop_agent, NOT _stop_process — restart_agent must NOT clear it (a
        # failed restart has to keep the outage visible; the failure is cleared
        # only when the replacement actually registers).
        self._autostart_failures.pop(agent_id, None)
        self.db.deregister_agent(agent_id)
        return {"id": agent_id, "status": "stopped"}

    def restart_agent(self, agent_id: str) -> dict:
        # Deliberately NOT stop_agent: keep any autostart_failed record until the
        # replacement registers, so a restart that fails to come up stays visible
        # on /v1/services instead of hiding the outage.
        self._stop_process(agent_id)
        self.db.deregister_agent(agent_id)
        return self.start_agent(agent_id)

    # -- lazy hot-load (fr_khonliang-bus_c81f7ab5) --

    def _resolve_lazy_target(self, req: "RequestMessage") -> str | None:
        """The lazy-eligible agent_id a request should trigger a launch for, else
        None. Direct ``agent_id``: the id itself if lazy-eligible. By
        ``agent_type``: only when EXACTLY ONE installed lazy-eligible agent
        matches the type (an ambiguous multi-match falls through to normal
        no-healthy rather than guessing which one to start).

        Both paths require an INSTALL row — a lazy_config entry whose agent was
        uninstalled is not launchable (reusing its launch spec is the whole
        point), so it must not take the cold-start path.
        """
        if req.agent_id:
            return (
                req.agent_id
                if req.agent_id in self._lazy_config
                and self.db.get_installed_agent(req.agent_id) is not None
                else None
            )
        if req.agent_type:
            matches = [
                aid for aid in self._lazy_config
                if (inst := self.db.get_installed_agent(aid))
                and inst.get("agent_type") == req.agent_type
            ]
            return matches[0] if len(matches) == 1 else None
        return None

    def _lazy_process_alive(self, agent_id: str) -> bool:
        """Whether a process THIS bus spawned for the agent is still running."""
        with self._processes_lock:
            proc = self._processes.get(agent_id)
        return proc is not None and proc.poll() is None

    def _lazy_agent_reachable(self, agent_id: str, reg: dict | None) -> bool:
        """Whether a lazy agent can serve a request NOW — registered AND live.

        pid-derived liveness alone is insufficient: a lazy agent registers over
        WebSocket with pid=0 (``handle_agent_ws`` default), which
        _derive_live_status never marks 'dead'. For the pid-less case reachability
        is an active WS connection OR a live bus-spawned process backing the row.
        Registration is REQUIRED — a spawned-but-unregistered process can't yet
        serve, so this gates the launch poll (don't dispatch before register).
        """
        if reg is None:
            return False
        if self._derive_live_status(reg) == "dead":
            return False
        pid = reg.get("pid")
        if pid and int(pid) > 0:
            return True  # real live local pid
        return self.is_agent_ws_connected(agent_id) or self._lazy_process_alive(agent_id)

    async def _lazy_launch(self, agent_id: str) -> dict:
        """Launch a dead lazy-eligible agent and wait for it to register.

        Returns ``{}`` once the agent is live, or ``{"error": ...}`` on
        not-installed / launch-failure / register-timeout. Concurrent callers for
        the same agent share ONE launch task so a burst of cold calls can't race
        to spawn duplicates.
        """
        task = self._lazy_launches.get(agent_id)
        if task is None:
            task = asyncio.create_task(self._do_lazy_launch(agent_id))
            self._lazy_launches[agent_id] = task
            task.add_done_callback(lambda t: self._lazy_launches.pop(agent_id, None))
        # shield: a cancelled caller must not cancel the shared launch that other
        # waiters still depend on.
        return await asyncio.shield(task)

    async def _do_lazy_launch(self, agent_id: str) -> dict:
        installed = self.db.get_installed_agent(agent_id)
        if installed is None:
            return {"error": f"lazy agent {agent_id!r} is not installed"}
        reg = self.db.get_registration(agent_id)
        if self._lazy_agent_reachable(agent_id, reg):
            return {}  # already up (concurrent launch / WS reconnect)
        # Spawn ONLY if no process is already coming up — a live spawned process
        # with a dropped/absent registration is mid-startup, so relaunching would
        # duplicate it (codex). In that case, skip the spawn and just poll for it
        # to register.
        if not self._lazy_process_alive(agent_id):
            # A stale (unreachable) registration would make start_agent's
            # pid-derived 'already_running' guard no-op without spawning — clear
            # it first so the launch actually happens.
            if reg is not None:
                self._stop_process(agent_id)
                self.db.deregister_agent(agent_id)
            result = self.start_agent(agent_id)
            if result.get("error"):
                # Transient: keep the agent lazy_eligible (next call retries)
                # rather than leaving a sticky autostart_failed row.
                self._autostart_failures.pop(agent_id, None)
                return {"error": f"lazy launch of {agent_id!r} failed: {result['error']}"}
        # Poll until the agent is actually REACHABLE (registered AND live) — the
        # spawned process, or the one that was already coming up. Async-sleep
        # between cheap reads — never block the request loop.
        deadline = self._now() + self._lazy_launch_timeout_s
        while self._now() < deadline:
            reg = self.db.get_registration(agent_id)
            if self._lazy_agent_reachable(agent_id, reg):
                return {}
            await asyncio.sleep(0.1)
        # Deadline passed. One last check — it may have come reachable in the
        # final tick; if so, succeed rather than kill a working agent.
        reg = self.db.get_registration(agent_id)
        if self._lazy_agent_reachable(agent_id, reg):
            return {}
        # Genuinely not up — stop the half-started process AND clear any stale
        # registration. A WS agent registers with pid=0, which _derive_live_status
        # can't mark 'dead', so leaving the row would black-hole later agent_id
        # calls onto a dead callback until the heartbeat ages out. Deregister for
        # a clean slate so the next call relaunches (AC#3).
        self._stop_process(agent_id)
        self.db.deregister_agent(agent_id)
        return {
            "error": (
                f"lazy agent {agent_id!r} did not register within "
                f"{self._lazy_launch_timeout_s:g}s"
            )
        }

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
        # Operator-/lifecycle-initiated stop (stop_agent, uninstall, restart):
        # drop supervisor backoff state so the agent isn't treated as mid-crash-
        # loop on its next start. The give-up failure surface is cleared by the
        # CALLERS instead (stop_agent / uninstall_agent) — restart_agent must
        # keep it until the replacement registers. The supervisor never calls
        # _stop_process, so this can't race its own bookkeeping.
        self._supervisor_backoff.pop(agent_id, None)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            logger.info("Stopped agent %s (pid=%d)", agent_id, proc.pid)

    # -- agent registration --

    async def register_agent(self, req: RegisterRequest) -> dict:
        if req.id in RESERVED_AGENT_IDS:
            return {
                "id": req.id,
                "error": f"'{req.id}' is a reserved agent id (the bus's own catalog)",
            }
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
        self._drop_autostart_failure(req.id)
        # Persist welcome (survives-deregister) so the bus_welcome super-skill
        # and GET /v1/agents/<id>/welcome work after the agent process exits.
        # fr_khonliang-bus_f96722dd. An oversized welcome must not break the
        # HTTP register response — log and proceed without it (parity with
        # the WS path).
        if req.welcome is not None:
            try:
                self.db.set_agent_welcome(req.id, req.welcome)
            except ValueError as e:
                logger.warning(
                    "Welcome rejected during HTTP register for %s: %s",
                    req.id, e,
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

        # Lazy hot-load (fr_khonliang-bus_c81f7ab5): if the target is a
        # dead-or-absent lazy-eligible agent, launch it on demand, wait for it to
        # register, then re-resolve and dispatch normally. The derived-liveness
        # check is consulted ONLY for a lazy candidate — a bus-spawned local agent
        # where os.kill(pid,0) is reliable — so it can also re-launch an agent
        # whose row lingered after a crash. Non-lazy routing is unchanged.
        lazy_id = self._resolve_lazy_target(req)
        if lazy_id:
            # Decide on the LAZY AGENT'S OWN registration (its source of truth),
            # not the type-resolved ``reg``. Launch (or JOIN an in-flight launch)
            # whenever the lazy agent isn't reachable — _lazy_launch dedups so a
            # burst of cold calls shares one launch and later callers WAIT rather
            # than get no-healthy; _do_lazy_launch won't duplicate a live process.
            # For an agent_type request, skip entirely if type routing already
            # resolved a healthy agent (``reg`` set) — don't cold-start a dormant
            # worker when another instance already serves the type.
            own_reg = self.db.get_registration(lazy_id)
            # "Already served" means a DIFFERENT agent covers the type — not the
            # lazy agent's own (possibly stale 'healthy') row: get_healthy_agent_
            # for_type can still return a crashed lazy agent until reconcile marks
            # it dead, so treating that as served would skip the relaunch and
            # dispatch to a dead callback. When the type resolves to the lazy
            # agent itself, its own reachability (below) governs the relaunch.
            already_served = (
                req.agent_type is not None
                and reg is not None
                and reg.get("id") != lazy_id
            )
            if not already_served and not self._lazy_agent_reachable(lazy_id, own_reg):
                launched = await self._lazy_launch(lazy_id)
                if launched.get("error"):
                    return {**launched, "trace_id": trace_id}
                # Re-resolve the SAME way the original request did (by type for a
                # type request — another healthy agent may have appeared).
                reg = (
                    self.db.get_registration(req.agent_id)
                    if req.agent_id
                    else self.db.get_healthy_agent_for_type(req.agent_type)
                )

        if not reg:
            return {"error": f"no healthy agent found for {req.agent_id or req.agent_type}", "trace_id": trace_id}

        # NB: direct agent_id resolution intentionally does NOT gate on
        # _derive_live_status. That check is os.kill(pid, 0) on the LOCAL pid
        # namespace, which is only meaningful for bus-spawned agents — a remote
        # agent that registers a positive pid from another host/container would
        # be false-flagged 'dead' and become unreachable. So a crashed
        # bus-spawned agent that is still registered during supervisor backoff
        # gets a failed dispatch against its (dead) callback rather than a clean
        # no-healthy; type-based routing still excludes it via reconcile_liveness.
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

        # Reuse the derived-liveness logic rather than probing the PID directly:
        # _derive_live_status guards pid<=0 (a bare _pid_alive(0) would probe the
        # caller's OWN process group, and negative PIDs probe a group), and keeps
        # this endpoint consistent with /v1/services + start_agent.
        status = self._derive_live_status(reg)
        if status == "dead":
            self.db.set_agent_status(agent_id, "dead")
        return {"id": agent_id, "status": status, "pid": reg["pid"]}

    #: Default heartbeat-freshness threshold (seconds). Beyond this age since
    #: the last heartbeat, a row the catalog still marks 'healthy' is reported
    #: as 'stale' — its process is gone or wedged even though the last status
    #: write said healthy. ~3 missed beats at a 30s cadence; tune via
    #: config['heartbeat_stale_threshold_s'].
    _DEFAULT_HEARTBEAT_STALE_S: float = 90.0

    @staticmethod
    def _heartbeat_age_s(last_heartbeat: str | None, now: float | None = None) -> float | None:
        """Seconds since ``last_heartbeat`` (a SQLite ``datetime('now')`` UTC
        text stamp, ``'YYYY-MM-DD HH:MM:SS'``), or ``None`` when the value is
        missing/unparseable. ``now`` (epoch seconds, UTC) is injectable for
        deterministic tests."""
        if not last_heartbeat:
            return None
        try:
            dt = datetime.strptime(last_heartbeat, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            # Defensive: last_heartbeat is always machine-written by SQLite
            # datetime('now'), so this should never fire. Log at debug (not
            # error) to surface the offending value for diagnosis without
            # spamming — get_services parses on every services listing.
            logger.debug("unparseable last_heartbeat %r; treating age as unknown", last_heartbeat)
            return None
        ref = now if now is not None else time.time()
        return ref - dt.timestamp()

    def _heartbeat_threshold_s(self) -> float:
        """``config['heartbeat_stale_threshold_s']`` as a float, falling back to
        the default on a missing/malformed value. A bad config value must not
        raise — _derive_live_status runs on every /v1/services read and inside
        start_agent. Memoized (config is fixed at construction) so the parse and
        the warn-on-bad-value happen once, not per-agent-per-request."""
        cached = getattr(self, "_heartbeat_threshold_cache", None)
        if cached is not None:
            return cached
        raw = self.config.get("heartbeat_stale_threshold_s", self._DEFAULT_HEARTBEAT_STALE_S)
        val: float | None = None
        # ``float()`` is too permissive on its own: bool is an int subclass
        # (float(True)==1.0), and "nan"/"inf" parse without raising — nan would
        # silently DISABLE staleness (age > nan is always False) and a <=0 value
        # would mark every agent stale. Accept only a finite, strictly-positive,
        # non-bool number; anything else falls back to the default.
        if not isinstance(raw, bool):
            try:
                parsed = float(raw)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None and math.isfinite(parsed) and parsed > 0:
                val = parsed
        if val is None:
            logger.warning(
                "invalid heartbeat_stale_threshold_s %r; using default %ss",
                raw, self._DEFAULT_HEARTBEAT_STALE_S,
            )
            val = self._DEFAULT_HEARTBEAT_STALE_S
        self._heartbeat_threshold_cache = val
        return val

    def _derive_live_status(self, reg: dict) -> str:
        """Live per-agent state derived from PID liveness + heartbeat freshness
        — the SINGLE source of truth for "is this agent really up" used by
        get_services, the start_agent ``already_running`` guard, and (read-time)
        bus_welcome (fr_khonliang-bus_7bf5ce84 / bug_khonliang-bus_529d12df).

        Two signals, strongest first; both only DOWNGRADE a row's stored status
        — an already-worse row is never resurrected to 'healthy':
          1. dead  — no live process for the registered PID, i.e. _pid_alive
                     (os.kill(pid, 0)) finds none — the same primitive boot
                     reconciliation and diagnose use. A negative PID is treated
                     as dead WITHOUT probing (os.kill(-N, …) targets a process
                     group). pid=0 (WS registration without a PID) skips this and
                     falls through to (2).
          2. stale — a 'healthy' row whose last_heartbeat is older than the
                     configurable freshness threshold.
        """
        stored = reg.get("status")
        pid = reg.get("pid")
        pid_int = int(pid) if pid else 0
        # A negative PID is a malformed registration — os.kill(-N, 0) targets a
        # process GROUP, so never probe it; treat as dead (matches boot
        # reconciliation, which reaps negative PIDs). pid_int == 0 (WS reg
        # without a PID) skips the check and falls through to the heartbeat
        # signal.
        if pid_int < 0:
            return "dead"
        if pid_int > 0 and not _pid_alive(pid_int):
            return "dead"
        if stored == "healthy":
            age = self._heartbeat_age_s(reg.get("last_heartbeat"))
            if age is not None and age > self._heartbeat_threshold_s():
                return "stale"
        return stored

    def get_services(self) -> list[dict]:
        regs = self.db.get_registrations()
        registered_ids = {reg["id"] for reg in regs}
        result = []
        for reg in regs:
            skills = self.db.get_skills(reg["id"])
            # Live state derived at read time so the services list never reports
            # a 'healthy' agent whose process is gone / heartbeat is stale
            # (bug_khonliang-bus_529d12df). See _derive_live_status.
            age = self._heartbeat_age_s(reg.get("last_heartbeat"))
            status = self._derive_live_status(reg)
            result.append({
                "id": reg["id"],
                "agent_type": reg["agent_type"],
                "status": status,
                "version": reg["version"],
                "callback_url": reg["callback_url"],
                "pid": reg["pid"],
                "skill_count": len(skills),
                "skills": [s["name"] for s in skills],
                "last_heartbeat": reg.get("last_heartbeat"),
                "heartbeat_age_s": round(age, 1) if age is not None else None,
            })
        # Surface autostart failures alongside live registrations so the
        # services list reflects the FULL post-boot picture, not just the
        # successes. Skip any agent that DID end up registered (a retry
        # after autostart succeeded — the live entry is canonical).
        # Per fr_khonliang-bus_fc904c3e acceptance #3.
        #
        # Snapshot the dict: the supervisor worker thread now writes
        # _autostart_failures continuously on give-up (fr_khonliang-bus_dc4ef3e9),
        # while this iteration runs on the FastAPI request thread. ``list(...)``
        # materializes in one C call with no intervening bytecode, so a
        # concurrent give-up can't raise "dictionary changed size during
        # iteration" here.
        for agent_id, error in list(self._autostart_failures.items()):
            if agent_id in registered_ids:
                continue
            installed = self.db.get_installed_agent(agent_id)
            if installed is None:
                # Agent was uninstalled after the failure was recorded (e.g. a
                # supervisor give-up followed by uninstall). Don't surface a
                # phantom service for something no longer installed.
                continue
            result.append({
                "id": agent_id,
                "agent_type": (installed or {}).get("agent_type", "unknown"),
                "status": "autostart_failed",
                "version": "",
                "callback_url": "",
                "pid": None,
                "skill_count": 0,
                "skills": [],
                # Keep the entry shape uniform with live registrations so
                # /v1/services clients can read these fields unconditionally.
                "last_heartbeat": None,
                "heartbeat_age_s": None,
                "autostart_error": error,
            })
        return result

    #: Per-agent timeout (seconds) for the active WS health probe, and the cap on
    #: how many agents are probed concurrently — so one wedged agent can't stall
    #: the whole ``bus_services(probe=true)`` listing.
    _PROBE_TIMEOUT_S: float = 5.0
    _PROBE_CONCURRENCY: int = 8

    async def _probe_agent_reachable(self, agent_id: str, timeout: float) -> bool:
        """Active liveness probe: send ``health_check`` over the agent's
        WebSocket and return True iff it answers within ``timeout``. False when
        there is no live WS connection or the probe errors/times out. Mirrors the
        health probe in :meth:`diagnose` (health_check is a BaseAgent built-in,
        so any conforming agent accepts it)."""
        if not self.is_agent_ws_connected(agent_id):
            return False
        result = await self.send_request_to_agent_ws(
            agent_id=agent_id,
            operation="health_check",
            args={},
            correlation_id=f"probe-{uuid.uuid4().hex[:12]}",
            timeout=timeout,
        )
        return not (isinstance(result, dict) and "error" in result)

    async def get_services_probed(
        self, timeout: float | None = None, concurrency: int | None = None
    ) -> list[dict]:
        """:meth:`get_services` plus an ACTIVE per-agent WS health probe
        (fr_khonliang-bus_7bf5ce84 acceptance #3). For each entry the passive
        derivation reports as up ('healthy'/'stale'), send a ``health_check`` and,
        if it doesn't answer in time, downgrade it to ``unreachable`` — a wedged
        agent whose PID may be alive but that can't serve. 'dead' / 'unhealthy' /
        'autostart_failed' entries are left as-is (nothing to confirm).

        Bounded fan-out (a semaphore) + per-agent timeout keep one wedged agent
        from stalling the listing. Each probed entry gains a ``probe_ok`` bool.
        """
        services = self.get_services()
        t = timeout if timeout is not None else self._PROBE_TIMEOUT_S
        sem = asyncio.Semaphore(max(1, concurrency or self._PROBE_CONCURRENCY))

        async def _probe(entry: dict) -> None:
            if entry["status"] not in ("healthy", "stale"):
                return
            try:
                async with sem:
                    ok = await self._probe_agent_reachable(entry["id"], t)
            except Exception:
                # One agent's probe blowing up must not fail the whole listing
                # (the bounded fan-out already keeps it from stalling). Treat an
                # unexpected error the same as a failed probe: unreachable.
                logger.exception("Active probe of %s raised", entry["id"])
                ok = False
            entry["probe_ok"] = ok
            if not ok:
                entry["status"] = "unreachable"

        await asyncio.gather(*(_probe(e) for e in services))
        return services

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

    #: Wire schema version for ``get_bus_welcome``. Bump when the response
    #: shape changes in a way callers must adapt to (additive fields don't
    #: warrant a bump — clients should ignore unknown keys per
    #: ``feedback_cheap_irreversible_principle``). The top-level ``bus`` field
    #: (fr_khonliang-bus_6638f4dc) is purely additive, so it does NOT bump this.
    BUS_WELCOME_SCHEMA_VERSION = 1

    def get_bus_welcome(self, detail: str = "brief") -> dict:
        """One-call cold-start discovery of the platform.

        Joins three data sources into a single response a fresh Claude
        session (or any cold-context caller) can use to learn what's
        registered, what each agent does, what state it's in, and what
        to do next:

        - ``registrations`` + ``self._autostart_failures`` (live state,
          via :meth:`get_services` — single source of truth so
          ``bus_welcome.agents[].state`` and ``bus_services`` never
          disagree, per ``fr_khonliang-bus_37498850`` acceptance #3).
        - ``installed_agents`` (canonical catalog — surfaces dead agents
          callers may want to ``bus_start_agent``).
        - ``welcomes`` (per-agent editorial content cached by bus-lib
          on register, ``fr_khonliang-bus_f96722dd``).

        Args:
            detail: ``"brief"`` (default) returns role + state + skill_count
                per agent; ``"full"`` adds mission, full skills list, and
                the welcome's editorial fields (boundaries, entry_points,
                guide_skill).

        Returns:
            See ``fr_khonliang-bus_37498850`` for the canonical shape.
            ``platform`` always includes ``schema_version`` so consumers
            can branch on it.

        No live-probe is performed — state comes from heartbeat-driven
        ``registrations.status`` + ``installed_agents`` + the failed-
        autostart table. Bus_welcome is meant to be cheap (single call
        every cold-start session); explicit liveness checks live on
        ``bus_services`` / dedicated probe endpoints.
        """
        if detail not in {"brief", "full"}:
            return {"error": f"detail must be one of brief|full (got {detail!r})"}

        services = self.get_services()
        services_by_id = {s["id"]: s for s in services}
        installed = self.db.get_installed_agents()
        installed_by_id = {a["id"]: a for a in installed}
        welcomes = {w["agent_id"]: w for w in self.db.list_agent_welcomes()}

        agents: list[dict] = []
        # Union of all known agent_ids — registered, installed, or
        # welcome-only. Sorted for deterministic output.
        all_ids = sorted(
            set(services_by_id) | set(installed_by_id) | set(welcomes)
        )

        for agent_id in all_ids:
            # A reserved id (e.g. ``bus``) is rendered by the synthetic ``bus``
            # field below, not as a pseudo-agent — skip any residual catalog row
            # (e.g. a welcome that survived deregister) so it isn't double-listed.
            if agent_id in RESERVED_AGENT_IDS:
                continue
            svc = services_by_id.get(agent_id)
            inst = installed_by_id.get(agent_id)
            welcome_record = welcomes.get(agent_id)
            welcome_dict = (welcome_record or {}).get("welcome") or {}

            # State: prefer the runtime svc.status (single source of truth
            # per FR acceptance #3); fall back to cataloged_dead when the
            # agent is installed but absent from runtime.
            if svc:
                state = svc["status"]  # 'healthy' / 'unhealthy' / 'dead' /
                                       # 'autostart_failed' (from get_services)
            elif inst:
                state = "cataloged_dead"
            else:
                # Welcome-only: an agent that registered, published a
                # welcome, then deregistered without an install row. Rare
                # but possible (ad-hoc registrations per
                # ``feedback_adhoc_agents_are_first_class``).
                state = "deregistered"

            # A down lazy-eligible agent isn't really 'dead' — it launches on the
            # next skill call. Surface it as ``lazy_eligible`` so callers know the
            # skills are available on demand (fr_khonliang-bus_c81f7ab5 AC#5). A
            # LIVE lazy agent keeps its runtime status.
            if inst and agent_id in self._lazy_config and state in {
                "cataloged_dead", "dead", "deregistered"
            }:
                state = "lazy_eligible"

            entry: dict = {
                "agent_id": agent_id,
                "agent_type": (
                    (svc or {}).get("agent_type")
                    or (inst or {}).get("agent_type")
                    or welcome_dict.get("agent_type", "unknown")
                ),
                "role": welcome_dict.get("role", ""),
                # Prefer the live svc count; only fall back to the cached
                # welcome when svc omits it. ``is not None`` (not truthiness)
                # so a live ``0`` is preserved rather than masked by the cache.
                "skill_count": (
                    live_skill_count
                    if (live_skill_count := (svc or {}).get("skill_count")) is not None
                    else welcome_dict.get("skill_count", 0)
                ),
                "state": state,
                "pid": (svc or {}).get("pid"),
                "welcome_cached_at": (welcome_record or {}).get("updated_at"),
            }
            if detail == "full":
                entry["mission"] = welcome_dict.get("mission", "")
                # Skills list: prefer the live one (richer than the cached
                # welcome's catalog summary), fall back to welcome's
                # skills_by_category, last fall back to skill_count only.
                if svc and svc.get("skills"):
                    entry["skills"] = svc["skills"]
                elif welcome_dict.get("skills_by_category"):
                    entry["skills_by_category"] = welcome_dict["skills_by_category"]
                # Editorial fields (only present when the agent populated them).
                for k in ("boundaries", "entry_points", "guide_skill"):
                    if k in welcome_dict:
                        entry[k] = welcome_dict[k]
                # Autostart error, when applicable.
                if svc and svc.get("autostart_error"):
                    entry["autostart_error"] = svc["autostart_error"]
            agents.append(entry)

        # suggested_next: hints that turn the response into a runnable
        # decision tree for a cold-start LLM.
        suggested: list[str] = []
        dead_with_install = [
            a["agent_id"] for a in agents
            if a["state"] in {"cataloged_dead", "dead", "autostart_failed"}
            and a["agent_id"] in installed_by_id
        ]
        if dead_with_install:
            suggested.append(
                f"bus_start_agent(agent_id=<id>) for any of "
                f"{dead_with_install} before invoking their skills"
            )
        live = [a["agent_id"] for a in agents if a["state"] == "healthy"]
        if live and detail == "brief":
            suggested.append(
                "bus_welcome(detail='full') for mission + skill catalog "
                "per agent"
            )
        if live:
            suggested.append(
                "<agent_id>.welcome for richer drill-in once an agent is live"
            )
        suggested.append("bus_skills(agent_id=<id>) for full per-agent skill schemas")
        suggested.append("bus_topics for the event surface (subscribe via bus_wait_for_event)")

        # The bus as a first-class entry in its own catalog (symmetric with
        # agents, per fr_khonliang-bus_6638f4dc) — a sibling top-level ``bus``
        # field rather than an entry in ``agents[]``, so consumers iterating the
        # agent list don't get a non-dispatchable pseudo-agent (the bus has no
        # callback and is never a routing target). Content is synthesized from
        # bus/welcome.json at read time — no registry/catalog write.
        blob = load_bus_self_welcome()
        skill_names = bus_self_skill_names()
        bus_entry: dict = {
            "kind": "bus",
            "identity": blob.get("identity", ""),
            "role": blob.get("role", ""),
            "skill_count": len(skill_names),
            # If this call is being answered, the bus process is up.
            "state": "healthy",
        }
        if detail == "full":
            # Copy nested structures out of the cached blob so a caller mutating
            # the response can't corrupt the process-lifetime cache.
            bus_entry["skills_by_category"] = {
                cat: list(names)
                for cat, names in blob.get("skills_by_category", {}).items()
            }
            for k in ("boundaries", "suggested_next"):
                val = blob.get(k)
                if val:
                    bus_entry[k] = list(val) if isinstance(val, list) else val

        return {
            "platform": {
                "name": "khonliang",
                "identity": (
                    "externalized-memory ecosystem; bus = durable hub, "
                    "agents = peer services"
                ),
                "bus_uptime_s": int(time.time() - self._started_at),
                "schema_version": self.BUS_WELCOME_SCHEMA_VERSION,
            },
            "bus": bus_entry,
            "agents": agents,
            "suggested_next": suggested,
        }

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
                    if agent_id in RESERVED_AGENT_IDS:
                        # Reserved for the bus's own catalog — reject and close
                        # rather than let a real agent shadow it.
                        await ws.send_json({
                            "type": "register_error",
                            "error": f"'{agent_id}' is a reserved agent id (the bus's own catalog)",
                        })
                        agent_id = None
                        await ws.close()
                        return
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
                    self._drop_autostart_failure(agent_id)
                    # Persist welcome to the survives-deregister catalog so
                    # cold-start LLMs + bus_welcome super-skill see it even
                    # after the agent process exits.
                    # fr_khonliang-bus_f96722dd. Only ``dict`` is honored —
                    # ignore stray non-dict shapes a future client might send.
                    # An oversized welcome from one agent must not block
                    # registration of others, so size-limit failures are
                    # logged and the registration proceeds without the
                    # welcome (the agent stays callable; its welcome can
                    # be retried via POST /v1/agents/<id>/welcome).
                    welcome = data.get("welcome")
                    if isinstance(welcome, dict):
                        try:
                            self.db.set_agent_welcome(agent_id, welcome)
                        except ValueError as e:
                            logger.warning(
                                "Welcome rejected during WS register for %s: %s",
                                agent_id, e,
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
    async def services(probe: bool = False):
        # probe=true adds an active WS health-ping per agent and reports
        # 'unreachable' for ones that don't answer (fr_khonliang-bus_7bf5ce84
        # acceptance #3). Default false: the cheap read-time derivation only.
        if probe:
            return await bus.get_services_probed()
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

    @app.get("/v1/agents/{agent_id}/welcome")
    def agent_welcome(agent_id: str):
        # fr_khonliang-bus_f96722dd: read the persisted welcome blob. Survives
        # deregister + bus restart; returns 404 if the agent has never
        # published one. The bus_welcome super-skill consumes this directly.
        record = bus.db.get_agent_welcome(agent_id)
        if record is None:
            raise HTTPException(
                status_code=404,
                detail=f"no welcome on file for agent_id={agent_id!r}",
            )
        return record

    @app.post("/v1/agents/{agent_id}/welcome")
    def agent_welcome_set(agent_id: str, payload: dict[str, Any]):
        # Reserved ids (e.g. ``bus``) can't hold an agent welcome — the bus's own
        # welcome is synthesized from bus/welcome.json (fr_khonliang-bus_6638f4dc).
        if agent_id in RESERVED_AGENT_IDS:
            raise HTTPException(
                status_code=400,
                detail=f"'{agent_id}' is a reserved agent id",
            )
        # Late-update / operator-override path for the welcome catalog
        # (fr_khonliang-bus_f96722dd "POST /v1/agents/<id>/welcome").
        # Accepts a JSON dict; persists it as the agent's welcome. Idempotent:
        # the same payload posted twice yields one row (most-recent-wins).
        #
        # FastAPI's pydantic-based body parsing already returns 422 for
        # non-object JSON bodies (the ``payload: dict[str, Any]`` annotation
        # is the schema). The only validation we still own is the size cap:
        # explicit 413 for oversized welcomes, since a bus client may want
        # to distinguish "rejected for size" from generic 422.
        try:
            bus.db.set_agent_welcome(agent_id, payload)
        except ValueError as e:
            raise HTTPException(status_code=413, detail=str(e))
        return {"agent_id": agent_id, "status": "stored"}

    @app.get("/v1/welcome")
    def bus_welcome(detail: str = "brief"):
        # fr_khonliang-bus_37498850: one-call cold-start discovery — platform
        # info + per-agent {role, mission, skills, state}. Defaults to brief;
        # pass ?detail=full for the full skill catalog + editorial fields.
        # Reject invalid detail with 400 (mirrors /v1/diagnose) rather than
        # forwarding get_bus_welcome's error dict as an HTTP 200 body.
        if detail not in ("brief", "full"):
            raise HTTPException(
                status_code=400,
                detail=f"detail must be 'brief' or 'full', got {detail!r}",
            )
        return bus.get_bus_welcome(detail=detail)

    @app.get("/v1/agents/welcomes")
    def list_welcomes():
        # Fleet-wide list, ordered by agent_id. Used by the bus_welcome
        # super-skill (sibling fr_khonliang-bus_37498850) to build cold-start
        # briefings. Empty list (not 404) when no welcomes have been published.
        return bus.db.list_agent_welcomes()

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

    # -- outbound webhook management (install / audit / repair) --
    #
    # These routes call the GitHub API with an admin:repo_hook token to
    # CREATE/PATCH/DELETE repo webhooks pointing back at this bus. The token
    # is a real write credential, and the bus binds 0.0.0.0 with no auth, so
    # the whole token-backed surface is gated behind an explicit opt-in
    # (``github_webhook_admin`` config or ``KHONLIANG_BUS_WEBHOOK_ADMIN`` env)
    # — same posture as the provenance disclose flag. ``check_funnel`` is the
    # one exception: it needs no token (it only probes the bus's own public
    # URL), so it stays ungated as a plain reachability check.

    def _webhook_admin_enabled() -> bool:
        if bool(bus.config.get("github_webhook_admin", False)):
            return True
        return os.environ.get("KHONLIANG_BUS_WEBHOOK_ADMIN", "").strip().lower() in {
            "1", "true", "yes", "on",
        }

    def _resolve_webhook_public_url() -> str:
        return (
            bus.config.get("github_webhook_public_url", "")
            or os.environ.get("GITHUB_WEBHOOK_PUBLIC_URL", "")
        )

    def _resolve_webhook_context(
        *, require_deliverable: bool = False
    ) -> tuple[str, "webhook_install.HookConfig"]:
        """Resolve (token, HookConfig) for a token-backed management route.

        Raises ``HTTPException`` (FastAPI renders ``{"detail": ...}``) on any
        precondition failure: 403 if admin is disabled, 400 if no token /
        public URL / the URL fails shape validation.

        A valid canonical ``github_webhook_public_url`` is a HARD prerequisite
        for EVERY route, audits included: ``find_canonical_match`` /
        ``find_orphan_hooks`` classify a repo's hooks by the canonical URL's
        host+path, and ``HookConfig`` can't be built without it — so there is
        no meaningful audit to run when it's unset. (Raw, unclassified hook
        enumeration would be a separate feature, not a lenient audit.)

        ``require_deliverable`` (set by mutating callers) is the ONLY leniency
        axis: it additionally refuses when the bus's own RECEIVER would reject
        every delivery (no secret + unsigned disallowed). Read-only audits and
        dry-runs leave it False so they still work on a receiver that is
        secret-misconfigured — but they still require the public URL above.
        """
        if not _webhook_admin_enabled():
            raise HTTPException(
                status_code=403,
                detail=(
                    "webhook admin disabled — set github_webhook_admin: true "
                    "in bus config or KHONLIANG_BUS_WEBHOOK_ADMIN=1"
                ),
            )
        token = (
            bus.config.get("github_token", "")
            or os.environ.get("GITHUB_TOKEN", "")
            or os.environ.get("GH_TOKEN", "")
        )
        if not token:
            raise HTTPException(
                status_code=400,
                detail=(
                    "no GitHub token — set github_token (config) or "
                    "GITHUB_TOKEN / GH_TOKEN env (needs admin:repo_hook scope)"
                ),
            )
        public_url = _resolve_webhook_public_url()
        secret = bus.config.get("github_webhook_secret", "") or os.environ.get(
            "GITHUB_WEBHOOK_SECRET", ""
        )
        # Refuse to configure a hook the bus's own receiver would reject on
        # every delivery: with no secret AND github_webhook_allow_unsigned
        # false (the default), /v1/webhooks/github answers 503 to everything,
        # so an install would "succeed" while wiring up a permanently-broken
        # webhook. Require a secret, or explicit unsigned dev mode. Only the
        # mutating callers gate on THIS (the secret/unsigned state); audits and
        # dry-runs skip it. (They still require the public URL above.)
        allow_unsigned = bool(bus.config.get("github_webhook_allow_unsigned", False))
        if require_deliverable and not secret and not allow_unsigned:
            raise HTTPException(
                status_code=400,
                detail=(
                    "refusing to install: the bus receiver has no "
                    "github_webhook_secret and github_webhook_allow_unsigned is "
                    "false, so every delivery would be rejected (503). Set "
                    "github_webhook_secret (recommended) or enable "
                    "github_webhook_allow_unsigned for localhost dev."
                ),
            )
        try:
            hook_config = webhook_install.HookConfig(target_url=public_url, secret=secret)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"github_webhook_public_url invalid ({e}) — these routes "
                    "classify/configure repo hooks against the canonical URL, "
                    "so it must be a valid HTTPS /v1/webhooks/github URL"
                    if public_url
                    else "github_webhook_public_url is unset — set the HTTPS "
                    "/v1/webhooks/github URL in bus config; every management "
                    "route (audit included) compares against it"
                ),
            )
        return token, hook_config

    def _resolve_webhook_owner(owner: str | None) -> str:
        resolved = (owner or bus.config.get("github_owner", "") or "").strip()
        if not resolved:
            raise HTTPException(
                status_code=400,
                detail="no owner — pass 'owner' or set github_owner in bus config",
            )
        return resolved

    def _require_fleet_prefix(body: dict) -> str:
        """Resolve a fleet ``prefix``, refusing the empty string.

        ``list_org_repos`` matches by ``name.startswith(prefix)``, so ``""``
        would match EVERY repo under the owner — turning an intended
        ``khonliang-*`` rollout into an account-wide hook create/repair. Force
        an explicit non-empty prefix.
        """
        prefix = _str_field(body, "prefix", default="khonliang-")
        if not prefix:
            raise HTTPException(
                status_code=400,
                detail=(
                    "'prefix' must be non-empty — it matches repos by "
                    "startswith, so an empty prefix would match every repo "
                    "under the owner"
                ),
            )
        return prefix

    async def _run_webhook_op(coro):
        """Await a primitive coroutine, mapping its failure modes to HTTP.

        ``ValueError`` (e.g. empty fleet) → 400; ``httpx.HTTPStatusError``
        (GitHub rejected the call — bad scope, missing repo) → 502 carrying
        the upstream status. Each raises ``HTTPException`` (FastAPI renders
        ``{"detail": ...}``).
        """
        try:
            return await coro
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502,
                detail=f"github HTTP {e.response.status_code}: {e.response.text[:200]}",
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"github request error: {e}")

    async def _require_object_body(request: Request) -> dict:
        """Parse the request body as a JSON object or raise 400.

        Without this, malformed JSON (``request.json()`` raises) or a valid
        non-object payload (``[]`` / ``"x"`` — ``.get`` then ``AttributeError``)
        would surface as a 500 instead of a client-error 400.
        """
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="request body must be valid JSON")
        if not isinstance(body, dict):
            raise HTTPException(
                status_code=400,
                detail=f"request body must be a JSON object, got {type(body).__name__}",
            )
        return body

    def _str_field(
        body: dict, name: str, *, default: str | None = None, required: bool = False
    ) -> str | None:
        """Extract a string field or raise 400. An absent key falls back to
        ``default``; a present-but-wrong-type value (``123``, explicit
        ``null``) is a client error, not a 500."""
        if name not in body:
            if required:
                raise HTTPException(status_code=400, detail=f"'{name}' is required")
            return default
        val = body[name]
        if not isinstance(val, str):
            raise HTTPException(
                status_code=400,
                detail=f"'{name}' must be a string, got {type(val).__name__}",
            )
        if required and not val:
            raise HTTPException(status_code=400, detail=f"'{name}' is required")
        return val

    def _bool_field(body: dict, name: str, *, default: bool = False) -> bool:
        """Extract a JSON boolean or raise 400. Avoids Python truthiness
        silently turning ``{"dry_run": "false"}`` into a (true) dry run."""
        val = body.get(name, default)
        if not isinstance(val, bool):
            raise HTTPException(
                status_code=400,
                detail=f"'{name}' must be a boolean, got {type(val).__name__}",
            )
        return val

    @app.post("/v1/webhooks/manage/install")
    async def webhook_manage_install(request: Request):
        body = await _require_object_body(request)
        repo = _str_field(body, "repo", required=True)
        dry_run = _bool_field(body, "dry_run")
        token, hook_config = _resolve_webhook_context(require_deliverable=not dry_run)
        async with webhook_install.make_client(token) as client:
            return await _run_webhook_op(
                webhook_install.install_one(client, repo, hook_config, dry_run=dry_run)
            )

    @app.post("/v1/webhooks/manage/install_fleet")
    async def webhook_manage_install_fleet(request: Request):
        body = await _require_object_body(request)
        dry_run = _bool_field(body, "dry_run")
        prefix = _require_fleet_prefix(body)
        token, hook_config = _resolve_webhook_context(require_deliverable=not dry_run)
        owner = _resolve_webhook_owner(_str_field(body, "owner"))
        async with webhook_install.make_client(token) as client:
            return await _run_webhook_op(
                webhook_install.install_fleet(
                    client, owner, hook_config, prefix=prefix, dry_run=dry_run
                )
            )

    @app.post("/v1/webhooks/manage/audit")
    async def webhook_manage_audit(request: Request):
        body = await _require_object_body(request)
        repo = _str_field(body, "repo", required=True)
        token, hook_config = _resolve_webhook_context()
        async with webhook_install.make_client(token) as client:
            result = await _run_webhook_op(
                webhook_install.audit_one(client, repo, hook_config)
            )
        return webhook_install._audit_to_dict(result)

    @app.post("/v1/webhooks/manage/audit_fleet")
    async def webhook_manage_audit_fleet(request: Request):
        body = await _require_object_body(request)
        prefix = _require_fleet_prefix(body)
        token, hook_config = _resolve_webhook_context()
        owner = _resolve_webhook_owner(_str_field(body, "owner"))
        async with webhook_install.make_client(token) as client:
            return await _run_webhook_op(
                webhook_install.audit_fleet(client, owner, hook_config, prefix=prefix)
            )

    @app.post("/v1/webhooks/manage/repair")
    async def webhook_manage_repair(request: Request):
        body = await _require_object_body(request)
        repo = _str_field(body, "repo", required=True)
        token, hook_config = _resolve_webhook_context(require_deliverable=True)
        async with webhook_install.make_client(token) as client:
            return await _run_webhook_op(
                webhook_install.repair_one(client, repo, hook_config)
            )

    def _redact_url(url: str) -> str:
        """Strip credentials / query / fragment, keeping scheme://host[:port]/path.

        ``check_funnel`` is ungated, and github_webhook_public_url can validly
        carry userinfo (``https://user:tok@host/...``) or a ``?token=`` query
        — both pass validate_target_url (it only checks scheme/host/path). The
        ungated response must not echo those secrets back, so the host+path
        (all an operator needs to confirm the funnel target) is all we return.
        """
        try:
            p = urlparse(url)
            if not p.scheme or not p.hostname:
                return ""
            # ``p.port`` validates the port lazily and raises ValueError on a
            # non-numeric one — kept inside the try so redaction never crashes
            # the caller (validate_target_url already rejects this upstream).
            host = p.hostname + (f":{p.port}" if p.port else "")
            return f"{p.scheme}://{host}{p.path}"
        except Exception:
            return ""

    @app.get("/v1/webhooks/manage/check_funnel")
    async def webhook_manage_check_funnel():
        """Probe the configured public webhook URL for bus reachability.

        Ungated and token-free: it only POSTs a deliberately-invalid body to
        the bus's own ``/v1/webhooks/github`` (rejected pre-publish), so it
        leaks no repo state and needs no admin credential. The echoed URL is
        redacted to host+path so an ungated caller can't read back any
        userinfo / query-string token embedded in the configured URL.
        """
        public_url = _resolve_webhook_public_url()
        if not public_url:
            raise HTTPException(
                status_code=400,
                detail="github_webhook_public_url is unset",
            )
        # A bad URL *shape* is a local config error, not a reachability result:
        # reject it with 400 (like the other routes) so a client's
        # raise_for_status() doesn't misread a misconfig as "bus unreachable".
        # Don't echo the raw URL in the error (ungated endpoint).
        try:
            webhook_install.validate_target_url(public_url)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=(
                    "github_webhook_public_url is set but not a valid HTTPS "
                    "/v1/webhooks/github URL"
                ),
            )
        result = await webhook_install.check_url_reachable(public_url)
        if isinstance(result, dict) and isinstance(result.get("url"), str):
            result["url"] = _redact_url(result["url"])
        return result

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
