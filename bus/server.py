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
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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
    """

    topics: list[str] = []
    subscriber_id: str = ""
    timeout: float = 30.0
    ack_on_return: bool = True


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


class GapReport(BaseModel):
    agent_id: str
    operation: str
    reason: str
    context: dict[str, Any] = {}


VALID_RESPONSE_MODES = {"raw", "extracted", "distilled"}
VALID_GAP_STATUSES = {"open", "reviewed", "dismissed"}
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
        self._http = httpx.AsyncClient(timeout=30.0)
        self._subscribers: dict[str, list[WebSocket]] = {}  # topic → websockets
        self._agent_connections: dict[str, WebSocket] = {}  # agent_id → WebSocket
        self._pending_responses: dict[str, asyncio.Future] = {}  # correlation_id → Future
        self._pending_agent: dict[str, str] = {}  # correlation_id → agent_id
        # Long-poll waiters: event fires on any new publish so waiters can re-check
        self._publish_notify: asyncio.Event | None = None
        self._retry_config = self.config.get("retry", {
            "delay": 2.0,
            "max_attempts": 3,
            "backoff": "exponential",
        })
        self.flow_engine = FlowEngine(db, self._http)
        self.orchestrator = Orchestrator(
            db, self._http,
            ollama_url=self.config.get("ollama_url", "http://localhost:11434"),
            default_model=self.config.get("orchestrator_model", "qwen2.5:7b"),
            bus_url=self.config.get("bus_url", "http://localhost:8787"),
        )
        self.scheduler = SchedulerIntegration(db)

    async def shutdown(self) -> None:
        await self._http.aclose()

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
            self._processes[agent_id] = proc
            logger.info("Started agent %s (pid=%d)", agent_id, proc.pid)
            return {"id": agent_id, "pid": proc.pid, "status": "started"}
        except Exception as e:
            logger.error("Failed to start agent %s: %s", agent_id, e)
            return {"id": agent_id, "error": str(e)}

    def _stop_process(self, agent_id: str) -> None:
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
        """
        # Lazy-init so there's always a current event object
        if self._publish_notify is None:
            self._publish_notify = asyncio.Event()

        topics = req.topics or []  # empty = match any
        subscriber_id = req.subscriber_id or f"waiter-{uuid.uuid4().hex[:8]}"
        deadline = time.monotonic() + max(0.0, float(req.timeout))

        while True:
            # Check each subscribed topic for unacked messages
            match = self._find_next_matching(subscriber_id, topics)
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
        self, subscriber_id: str, topics: list[str]
    ) -> dict | None:
        """Return the earliest unacked message matching any of ``topics``
        for ``subscriber_id``, or None.

        If ``topics`` is empty, match any topic on the bus.
        Delegates to :meth:`BusDB.find_earliest_unacked` — a single
        indexed query instead of per-topic round-trips.
        """
        return self.db.find_earliest_unacked(subscriber_id, topics or None)

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
        return result

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

    # -- flow orchestration (Step 6) --

    async def execute_flow(self, req: FlowRequest) -> dict:
        return await self.flow_engine.execute(req.flow_id, req.args, req.trace_id)

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
          {"type": "register", "id": "...", "version": "...", "skills": [...], "collaborations": [...]}
          {"type": "heartbeat"}
          {"type": "response", "correlation_id": "...", "result": {...}}
          {"type": "error", "correlation_id": "...", "error": "...", "retryable": bool}
          {"type": "publish", "topic": "...", "payload": {...}}
          {"type": "gap", "operation": "...", "reason": "...", "context": {...}}
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
                    # Store registration in DB
                    self.db.register_agent(
                        agent_id=agent_id,
                        agent_type=data.get("agent_type", agent_id.rsplit("-", 1)[0] if "-" in agent_id else agent_id),
                        callback_url=f"ws://connected",  # WebSocket — no callback URL needed
                        pid=data.get("pid", 0),
                        version=data.get("version", ""),
                        skills=data.get("skills", []),
                        collaborations=data.get("collaborations", []),
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


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------


def create_app(db_path: str = "data/bus.db", config: dict[str, Any] | None = None) -> FastAPI:
    db = BusDB(db_path)
    bus = BusServer(db, config)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
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

    @app.post("/v1/flow")
    async def execute_flow(req: FlowRequest):
        return await bus.execute_flow(req)

    @app.get("/v1/matrix")
    def matrix():
        return bus.get_interaction_matrix()

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
