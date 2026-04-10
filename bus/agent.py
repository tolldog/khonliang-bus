"""BaseAgent — the base class every agent inherits from.

Handles lifecycle boilerplate (port binding, registration, heartbeat,
shutdown) so agent authors focus on skills, not plumbing.

Usage::

    class ResearcherAgent(BaseAgent):
        agent_type = "researcher"
        module_name = "researcher.agent"

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.pipeline = create_pipeline(self.config_path)

        def register_skills(self):
            return [
                Skill("find_papers", "Search arxiv", {"query": {"type": "string"}}),
            ]

        @handler("find_papers")
        async def find_papers(self, args):
            return await self.pipeline.search(args["query"])

    if __name__ == "__main__":
        agent = ResearcherAgent.from_cli()
        asyncio.run(agent.start())
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
from fastapi import FastAPI
import uvicorn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Skill descriptor
# ---------------------------------------------------------------------------


@dataclass
class Skill:
    """A skill this agent can handle."""

    name: str
    description: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    since: str = ""


@dataclass
class Collaboration:
    """A multi-agent flow this agent declares."""

    name: str
    description: str = ""
    requires: dict[str, str] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Handler decorator
# ---------------------------------------------------------------------------

_HANDLER_ATTR = "_bus_handler_name"


def handler(operation: str) -> Callable:
    """Decorator marking a method as a skill handler.

    Usage::

        @handler("find_papers")
        async def find_papers(self, args):
            return {"papers": [...]}
    """

    def decorator(fn: Callable) -> Callable:
        setattr(fn, _HANDLER_ATTR, operation)
        return fn

    return decorator


# ---------------------------------------------------------------------------
# BaseAgent
# ---------------------------------------------------------------------------


class BaseAgent:
    """Base class for bus agents. Subclass and override skills + handlers."""

    agent_type: str = "base"
    module_name: str = "agent"

    def __init__(
        self,
        agent_id: str,
        bus_url: str,
        config_path: str = "",
    ):
        self.agent_id = agent_id
        self.bus_url = bus_url.rstrip("/")
        self.config_path = config_path
        self._http = httpx.AsyncClient(timeout=30.0)
        self._heartbeat_task: asyncio.Task | None = None
        self._server: uvicorn.Server | None = None
        self._port: int = 0
        self._handlers: dict[str, Callable] = {}
        self._collect_handlers()

    def _collect_handlers(self) -> None:
        """Discover @handler-decorated methods."""
        for name in dir(self):
            method = getattr(self, name, None)
            if callable(method) and hasattr(method, _HANDLER_ATTR):
                op = getattr(method, _HANDLER_ATTR)
                self._handlers[op] = method

    # -- override these --

    def register_skills(self) -> list[Skill]:
        """Return the skills this agent provides. Override in subclass."""
        return []

    def register_collaborations(self) -> list[Collaboration]:
        """Return collaborative flows this agent declares. Override in subclass."""
        return []

    # -- lifecycle --

    @classmethod
    def from_cli(cls, argv: list[str] | None = None) -> "BaseAgent":
        """Parse --id, --bus, --config from command line and construct."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--id", required=True)
        parser.add_argument("--bus", required=True)
        parser.add_argument("--config", default="")
        parser.add_argument("command", nargs="?")  # install / uninstall
        args = parser.parse_args(argv)

        agent = cls(
            agent_id=args.id,
            bus_url=args.bus,
            config_path=args.config,
        )

        if args.command == "install":
            agent._do_install()
            sys.exit(0)
        elif args.command == "uninstall":
            agent._do_uninstall()
            sys.exit(0)

        return agent

    async def start(self) -> None:
        """Bind port, register with bus, start heartbeat, serve requests."""
        app = self._build_app()
        config = uvicorn.Config(app, host="0.0.0.0", port=0, log_level="warning")
        self._server = uvicorn.Server(config)

        # Start the server in background to discover the assigned port
        serve_task = asyncio.create_task(self._server.serve())

        # Wait for the server to start and discover the port
        while not self._server.started:
            await asyncio.sleep(0.05)

        for sock in self._server.servers[0].sockets:
            self._port = sock.getsockname()[1]
            break

        logger.info("Agent %s listening on port %d", self.agent_id, self._port)

        # Register with bus
        await self._register()

        # Start heartbeat loop
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Handle signals for clean shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        await serve_task

    async def shutdown(self) -> None:
        """Deregister and stop."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        try:
            await self._http.post(
                f"{self.bus_url}/v1/deregister",
                json={"id": self.agent_id},
            )
        except Exception:
            pass
        if self._server:
            self._server.should_exit = True
        await self._http.aclose()
        logger.info("Agent %s shut down", self.agent_id)

    # -- install/uninstall --

    def _do_install(self) -> None:
        """Synchronous install — called from from_cli when command is 'install'."""
        with httpx.Client(timeout=10.0) as client:
            r = client.post(f"{self.bus_url}/v1/install", json={
                "agent_type": self.agent_type,
                "id": self.agent_id,
                "command": sys.executable,
                "args": ["-m", self.module_name],
                "cwd": os.getcwd(),
                "config": os.path.abspath(self.config_path) if self.config_path else "",
            })
            print(json.dumps(r.json(), indent=2))

    def _do_uninstall(self) -> None:
        """Synchronous uninstall."""
        with httpx.Client(timeout=10.0) as client:
            r = client.request("DELETE", f"{self.bus_url}/v1/install/{self.agent_id}")
            print(json.dumps(r.json(), indent=2))

    # -- registration --

    async def _register(self) -> None:
        skills = self.register_skills()
        collabs = self.register_collaborations()
        callback = f"http://localhost:{self._port}"

        payload = {
            "id": self.agent_id,
            "callback": callback,
            "pid": os.getpid(),
            "version": getattr(self, "version", "0.0.0"),
            "skills": [
                {
                    "name": s.name,
                    "description": s.description,
                    "parameters": s.parameters,
                    "since": s.since,
                }
                for s in skills
            ],
            "collaborations": [
                {
                    "name": c.name,
                    "description": c.description,
                    "requires": c.requires,
                    "steps": c.steps,
                }
                for c in collabs
            ],
        }

        try:
            r = await self._http.post(f"{self.bus_url}/v1/register", json=payload)
            r.raise_for_status()
            logger.info("Registered with bus: %s", r.json())
        except Exception as e:
            raise RuntimeError(
                f"Agent {self.agent_id} failed to register with bus at "
                f"{self.bus_url}: {e}. The bus must be running before "
                f"agents can start."
            ) from e

    # -- heartbeat --

    async def _heartbeat_loop(self, interval: float = 30.0) -> None:
        while True:
            try:
                await asyncio.sleep(interval)
                await self._http.post(
                    f"{self.bus_url}/v1/heartbeat",
                    json={"id": self.agent_id},
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Heartbeat failed: %s", e)

    # -- request handling --

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.post("/v1/handle")
        async def handle(request: dict):
            operation = request.get("operation", "")
            args = request.get("args", {})
            correlation_id = request.get("correlation_id", "")

            handler_fn = self._handlers.get(operation)
            if not handler_fn:
                return {
                    "correlation_id": correlation_id,
                    "error": f"unknown operation: {operation}",
                    "retryable": False,
                }

            try:
                result = await handler_fn(args)
                return {
                    "correlation_id": correlation_id,
                    "result": result,
                }
            except Exception as e:
                logger.exception("Handler %s failed", operation)
                return {
                    "correlation_id": correlation_id,
                    "error": str(e),
                    "retryable": True,
                }

        @app.get("/v1/health")
        def health():
            return {"status": "ok", "agent_id": self.agent_id}

        return app

    # -- pub/sub helpers --

    async def publish(self, topic: str, payload: Any) -> dict:
        """Publish an event to the bus."""
        r = await self._http.post(
            f"{self.bus_url}/v1/publish",
            json={"topic": topic, "payload": payload, "source": self.agent_id},
        )
        return r.json()

    async def nack(self, message_id: int, topic: str, reason: str = "") -> dict:
        """Negative-acknowledge a message for redelivery."""
        r = await self._http.post(
            f"{self.bus_url}/v1/nack",
            json={
                "subscriber_id": self.agent_id,
                "message_id": message_id,
                "topic": topic,
                "reason": reason,
            },
        )
        return r.json()

    # -- from_mcp migration helper --

    @classmethod
    def from_mcp(
        cls,
        mcp_server,
        agent_type: str,
        agent_id: str = "",
        bus_url: str = "",
        config_path: str = "",
    ) -> "BaseAgent":
        """Wrap an existing FastMCP server's tools as bus handlers.

        Introspects the server's registered tools and creates a BaseAgent
        subclass with @handler for each tool. The tool functions stay
        identical — only the transport changes from stdio to bus HTTP.

        This is a migration bridge. The end state is native @handler
        methods, not wrapped MCP tools.
        """
        import asyncio as _aio

        # Introspect MCP tools
        tools = _aio.run(mcp_server.list_tools())

        # Build dynamic subclass
        handlers = {}
        skills = []

        for tool in tools:
            tool_name = tool.name
            skills.append(Skill(
                name=tool_name,
                description=tool.description or "",
                parameters={},  # could extract from tool.inputSchema
            ))

            # Create a handler that calls the MCP tool
            async def _make_handler(tn):
                async def _handler(self_inner, args):
                    result = await mcp_server.call_tool(tn, args)
                    # MCP tools return [TextContent, ...] — extract text
                    if isinstance(result, tuple) and len(result) == 2:
                        meta = result[1]
                        if isinstance(meta, dict) and "result" in meta:
                            return {"result": meta["result"]}
                    if isinstance(result, list) and result:
                        first = result[0]
                        if hasattr(first, "text"):
                            return {"result": first.text}
                    return {"result": str(result)}
                return _handler

            handler_fn = _aio.run(_make_handler(tool_name))
            setattr(handler_fn, _HANDLER_ATTR, tool_name)
            handlers[tool_name] = handler_fn

        # Create the subclass
        agent_cls = type(
            f"{agent_type.title()}Agent",
            (cls,),
            {
                "agent_type": agent_type,
                **{f"_handle_{n}": fn for n, fn in handlers.items()},
            },
        )

        # Override register_skills
        def _register_skills(self_inner):
            return skills
        agent_cls.register_skills = _register_skills

        return agent_cls(
            agent_id=agent_id or f"{agent_type}-primary",
            bus_url=bus_url,
            config_path=config_path,
        )
