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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from bus.db import BusDB
from bus.flows import FlowEngine
from bus.versions import validate_collaboration_requirements

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


class PublishRequest(BaseModel):
    topic: str
    payload: Any = None
    source: str = ""


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


class SessionContextUpdate(BaseModel):
    public_ctx: dict[str, Any] | None = None
    private_ctx: dict[str, Any] | None = None


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
        self._retry_config = self.config.get("retry", {
            "delay": 2.0,
            "max_attempts": 3,
            "backoff": "exponential",
        })
        self.flow_engine = FlowEngine(db, self._http)

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
        callback_url = reg["callback_url"]

        # Record trace
        self.db.record_trace_step(trace_id, step=1, agent_id=agent_id, operation=req.operation)
        t0 = time.monotonic()

        # Forward to agent
        correlation_id = f"req-{uuid.uuid4().hex[:12]}"
        payload = {
            "operation": req.operation,
            "args": req.args,
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "session_id": req.session_id,
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
        return {"id": msg_id, "topic": req.topic}

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

    @app.post("/v1/ack")
    def ack(req: AckRequest):
        return bus.ack(req)

    @app.post("/v1/nack")
    def nack(req: NackRequest):
        return bus.nack(req)

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
