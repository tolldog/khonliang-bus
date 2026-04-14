"""Bus-MCP adapter — makes the bus the single MCP Claude connects to.

A thin FastMCP bridge that:
  1. On startup: fetches registered agents + skills from the bus
  2. Generates @mcp.tool() for each skill (namespaced: agent_id.skill_name)
  3. Generates @mcp.tool() for each validated collaborative flow
  4. Routes tool calls through the bus via POST /v1/request
  5. Subscribes to bus.registry_changed for dynamic tool refresh

Invocation::

    python -m bus.mcp_adapter --bus http://localhost:8787

Or in .mcp.json::

    {
      "mcpServers": {
        "khonliang": {
          "command": "python",
          "args": ["-m", "bus.mcp_adapter", "--bus", "http://localhost:8787"]
        }
      }
    }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

import httpx
from mcp.server.fastmcp import Context, FastMCP

logger = logging.getLogger(__name__)


class BusMCPAdapter:
    """Translates between MCP (stdio, Claude-facing) and the bus (HTTP)."""

    def __init__(self, bus_url: str):
        self.bus_url = bus_url.rstrip("/")
        self._http = httpx.Client(timeout=30.0)
        self._async_http = httpx.AsyncClient(timeout=30.0)
        self.mcp = FastMCP("khonliang-bus")
        self._registered_tools: set[str] = set()

    def build(self) -> FastMCP:
        """Fetch bus state and generate MCP tools. Call once before run."""
        self._register_bus_tools()
        self._register_skill_tools()
        self._register_flow_tools()
        return self.mcp

    def _register_bus_tools(self) -> None:
        """Register built-in bus management tools."""
        mcp = self.mcp
        adapter = self

        def _format_lifecycle(op: str, agent_id: str, result: dict) -> str:
            """Terse, structured response for lifecycle ops.

            Distinguishes transport failures (no ``id`` in the envelope,
            because ``_async_post`` caught an exception before reaching the
            bus) from bus-reported outcomes (always include ``id``):

            - transport error              → "error[{op}]: <msg>"
            - bus-reported 'not installed' → "{id}: {op} not installed"
            - bus-reported spawn error     → "{id}: {op} error: <msg>"
            - already running              → "{id}: {op} already_running"
            - normal success               → "{id}: {op} {status}[ pid={pid}]"

            ``op`` is included in every line so failures and successes carry
            the operation context (useful when a caller batches or logs).
            """
            # Transport error: _async_post returned {"error": ...} without id
            if "id" not in result and "error" in result:
                return f"error[{op}]: {result['error']}"

            err = result.get("error")
            if err == "not installed":
                return f"{agent_id}: {op} not installed"
            if err:
                # Bus-scoped failure (e.g., subprocess spawn exception)
                return f"{agent_id}: {op} error: {err}"

            status = result.get("status", "unknown")
            pid = result.get("pid")
            if pid is not None:
                return f"{agent_id}: {op} {status} pid={pid}"
            return f"{agent_id}: {op} {status}"

        @mcp.tool()
        async def bus_services() -> str:
            """List all registered agents with their skills and status."""
            services = adapter._get("/v1/services")
            if not services:
                return "no agents registered"
            lines = []
            for svc in services:
                status = "✓" if svc["status"] == "healthy" else "✗"
                lines.append(
                    f"  {status} {svc['id']} (v{svc.get('version', '?')}, "
                    f"{svc['skill_count']} skills)"
                )
            return "=== AGENTS ===\n" + "\n".join(lines)

        @mcp.tool()
        async def bus_status() -> str:
            """Platform status: agent counts, skill counts, flow availability."""
            s = adapter._get("/v1/status")
            lines = [
                f"agents: {s['registered_agents']} registered "
                f"({s['healthy']} healthy, {s['unhealthy']} unhealthy)",
                f"installed: {s['installed_agents']}",
                f"skills: {s['total_skills']}",
                f"flows: {s['available_flows']} available, "
                f"{s['unavailable_flows']} unavailable",
            ]
            return "\n".join(lines)

        @mcp.tool()
        async def bus_matrix() -> str:
            """Interaction matrix: agents, solo skills, collaborative flows."""
            m = adapter._get("/v1/matrix")
            lines = ["=== AGENTS ==="]
            for aid, info in m.get("agents", {}).items():
                lines.append(
                    f"  {aid} ({info['agent_type']} v{info.get('version', '?')}) "
                    f"— {info['solo_skills']} skills [{info['status']}]"
                )
            lines.append("")
            lines.append("=== COLLABORATIONS ===")
            for collab in m.get("collaborations", []):
                reqs = ", ".join(f"{k}{v}" for k, v in collab.get("requires", {}).items())
                status = "✓" if collab["status"] == "available" else "✗"
                line = f"  {status} {collab['name']} [{reqs}]"
                if collab.get("unmet"):
                    line += f" — {'; '.join(collab['unmet'])}"
                lines.append(line)
            return "\n".join(lines)

        @mcp.tool()
        async def bus_flows() -> str:
            """List available and unavailable collaborative flows with diagnostics."""
            f = adapter._get("/v1/flows")
            lines = []
            if f.get("available"):
                lines.append("=== AVAILABLE ===")
                for flow in f["available"]:
                    reqs = ", ".join(f"{k}{v}" for k, v in flow.get("requires", {}).items())
                    lines.append(f"  {flow['name']} [{reqs}] — {flow.get('description', '')}")
            if f.get("unavailable"):
                lines.append("=== UNAVAILABLE ===")
                for flow in f["unavailable"]:
                    lines.append(f"  {flow['name']} — {'; '.join(flow.get('unmet', []))}")
            return "\n".join(lines) if lines else "no flows declared"

        @mcp.tool()
        async def bus_wait_for_event(
            topics: str = "",
            subscriber_id: str = "claude-mcp",
            timeout: float = 30.0,
        ) -> str:
            """Long-poll for the next bus event matching ``topics``.

            Inverts polling: instead of Claude calling ``bus_events``
            repeatedly, Claude opens this tool and the bus holds the
            connection until a matching event fires (or timeout).

            Args:
                topics: Comma-separated topic names. Empty = match any topic.
                subscriber_id: Stable ID so the bus doesn't re-deliver
                    the same event across wait calls. Default "claude-mcp".
                timeout: Max seconds to wait (default 30).

            Returns a string describing the event or "timeout".
            """
            topic_list = [t.strip() for t in topics.split(",") if t.strip()] if topics else []
            # Add a 5-second buffer to the HTTP transport timeout so the server
            # always has room to return its graceful {"status":"timeout"} before
            # the HTTP client fires its own timeout.
            result = await adapter._async_post(
                "/v1/wait",
                {
                    "topics": topic_list,
                    "subscriber_id": subscriber_id,
                    "timeout": timeout,
                    "ack_on_return": True,
                },
                http_timeout=timeout + 5,
            )
            if result.get("error"):
                return f"bus error: {result['error']}"
            # Use the subscriber_id the server returned (it may have
            # generated one if the caller passed an empty string).
            returned_sid = result.get("subscriber_id", subscriber_id)
            if result.get("status") == "timeout":
                return f"timeout after {timeout}s (subscriber={returned_sid})"
            event = result.get("event") or {}
            payload = event.get("payload")
            try:
                payload_str = json.dumps(payload, indent=2) if not isinstance(payload, str) else payload
            except (TypeError, ValueError):
                payload_str = str(payload)
            return (
                f"event id={event.get('id', '?')} topic={event.get('topic', '?')} "
                f"source={event.get('source', '?')} at={event.get('created_at', '?')}\n"
                f"{payload_str}"
            )

        @mcp.tool()
        async def bus_trace(trace_id: str) -> str:
            """Query execution trace for a request or flow."""
            steps = adapter._get(f"/v1/trace/{trace_id}")
            if not steps:
                return f"no trace found for {trace_id}"
            lines = [f"trace: {trace_id}"]
            for step in steps:
                dur = f"{step['duration_ms']}ms" if step.get("duration_ms") else "..."
                err = f" — {step['error']}" if step.get("error") else ""
                lines.append(
                    f"  step {step['step']}: {step.get('agent_id', '?')}.{step.get('operation', '?')} "
                    f"[{step.get('status', 'pending')} {dur}]{err}"
                )
            return "\n".join(lines)

        @mcp.tool()
        async def bus_start_agent(agent_id: str, ctx: Context | None = None) -> str:
            """Start an installed agent. Idempotent — returns 'already_running' if up.

            Thin wrapper over POST /v1/install/{id}/start. The bus owns
            subprocess spawning and registration tracking; this tool just
            triggers it and formats the structured response.

            After the start completes, adapter refreshes its skill-tool
            registry (the agent may advertise new skills) and fires
            ``tools/list_changed`` so the MCP client re-lists tools.
            """
            result = await adapter._async_post(f"/v1/install/{agent_id}/start", {})
            response = _format_lifecycle("start", agent_id, result)
            await adapter._refresh_and_notify(ctx)
            return response

        @mcp.tool()
        async def bus_stop_agent(agent_id: str, ctx: Context | None = None) -> str:
            """Stop a running agent and deregister it from the bus.

            Thin wrapper over POST /v1/install/{id}/stop. Refreshes the
            adapter's skill-tool registry afterwards (the stopped agent's
            tools go away) and fires ``tools/list_changed``.
            """
            result = await adapter._async_post(f"/v1/install/{agent_id}/stop", {})
            response = _format_lifecycle("stop", agent_id, result)
            await adapter._refresh_and_notify(ctx)
            return response

        @mcp.tool()
        async def bus_restart_agent(agent_id: str, ctx: Context | None = None) -> str:
            """Restart an agent (stop + start). Returns final status and new pid.

            Useful for picking up code changes without external supervision.
            Thin wrapper over POST /v1/install/{id}/restart. Refreshes the
            adapter's skill-tool registry afterwards (restarted agents may
            advertise new or different skills after a code change) and
            fires ``tools/list_changed``.
            """
            result = await adapter._async_post(f"/v1/install/{agent_id}/restart", {})
            response = _format_lifecycle("restart", agent_id, result)
            await adapter._refresh_and_notify(ctx)
            return response

        @mcp.tool()
        async def bus_refresh_skills(ctx: Context | None = None) -> str:
            """Refresh the adapter's skill-tool registry against current bus state.

            Closes a loop: when an agent restarts outside the MCP lifecycle
            surface (e.g. manual respawn in an iTerm pane) and registers
            new skills, MCP clients don't see them until this refresh runs.
            Normally called automatically after ``bus_start_agent`` /
            ``bus_stop_agent`` / ``bus_restart_agent``; expose it as a
            manual escape hatch for external-lifecycle cases.

            Returns a summary of added/removed tools.
            """
            diff = adapter.refresh_skills()
            if ctx is not None and (diff["added"] or diff["removed"]):
                await adapter._notify_list_changed(ctx)
            added = len(diff["added"])
            removed = len(diff["removed"])
            if not (added or removed):
                return "no changes"
            parts = []
            if added:
                parts.append(f"+{added}: {', '.join(diff['added'])}")
            if removed:
                parts.append(f"-{removed}: {', '.join(diff['removed'])}")
            return " | ".join(parts)

        @mcp.tool()
        async def bus_skills(agent_id: str = "") -> str:
            """List skills. Optionally filter by agent_id."""
            params = {"agent_id": agent_id} if agent_id else {}
            skills = adapter._get("/v1/skills", params=params)
            if not skills:
                return "no skills registered"
            lines = []
            current_agent = ""
            for s in skills:
                if s["agent_id"] != current_agent:
                    current_agent = s["agent_id"]
                    lines.append(f"\n  {current_agent}:")
                lines.append(f"    {s['name']} — {s.get('description', '')}")
            return "\n".join(lines).strip()

        @mcp.tool()
        async def bus_artifact_list(
            session_id: str = "",
            kind: str = "",
            producer: str = "",
            limit: int = 20,
        ) -> str:
            """List artifact metadata. Does not return artifact content."""
            result = adapter._get(
                "/v1/artifacts",
                params={
                    "session_id": session_id,
                    "kind": kind,
                    "producer": producer,
                    "limit": limit,
                },
            )
            return json.dumps(result or [], indent=2)

        @mcp.tool()
        async def bus_artifact_metadata(id: str) -> str:
            """Return artifact metadata, size, kind, producer, and refs."""
            result = adapter._get(f"/v1/artifacts/{id}")
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_head(
            id: str,
            lines: int = 80,
            max_chars: int = 4000,
        ) -> str:
            """Bounded beginning of a text artifact."""
            result = adapter._get(
                f"/v1/artifacts/{id}/head",
                params={"lines": lines, "max_chars": max_chars},
            )
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_tail(
            id: str,
            lines: int = 80,
            max_chars: int = 4000,
        ) -> str:
            """Bounded end of a text artifact."""
            result = adapter._get(
                f"/v1/artifacts/{id}/tail",
                params={"lines": lines, "max_chars": max_chars},
            )
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_get(
            id: str,
            offset: int = 0,
            max_chars: int = 4000,
        ) -> str:
            """Bounded character window from a text artifact."""
            result = adapter._get(
                f"/v1/artifacts/{id}/content",
                params={"offset": offset, "max_chars": max_chars},
            )
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_grep(
            id: str,
            pattern: str,
            context_lines: int = 10,
            max_matches: int = 10,
            max_chars: int = 4000,
        ) -> str:
            """Bounded pattern excerpts from a text artifact."""
            result = adapter._get(
                f"/v1/artifacts/{id}/grep",
                params={
                    "pattern": pattern,
                    "context_lines": context_lines,
                    "max_matches": max_matches,
                    "max_chars": max_chars,
                },
            )
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_excerpt(
            id: str,
            start_line: int,
            end_line: int,
            max_chars: int = 4000,
        ) -> str:
            """Bounded explicit line range from a text artifact."""
            result = adapter._get(
                f"/v1/artifacts/{id}/excerpt",
                params={
                    "start_line": start_line,
                    "end_line": end_line,
                    "max_chars": max_chars,
                },
            )
            return json.dumps(result or {"error": "artifact not found"}, indent=2)

        @mcp.tool()
        async def bus_artifact_distill(
            id: str,
            mode: str = "brief",
            purpose: str = "",
            max_chars: int = 4000,
        ) -> str:
            """Create and return a bounded distillation artifact."""
            result = await adapter._async_post(
                f"/v1/artifacts/{id}/distill",
                {"mode": mode, "purpose": purpose, "max_chars": max_chars},
            )
            return json.dumps(result, indent=2)

        @mcp.tool()
        async def bus_artifact_distill_many(
            ids: str,
            purpose: str = "",
            max_chars: int = 8000,
        ) -> str:
            """Distill several artifacts. Pass ids as comma-separated artifact IDs."""
            artifact_ids = [i.strip() for i in ids.split(",") if i.strip()]
            result = await adapter._async_post(
                "/v1/artifacts/distill_many",
                {"ids": artifact_ids, "purpose": purpose, "max_chars": max_chars},
            )
            return json.dumps(result, indent=2)

    def _register_skill_tools(self) -> None:
        """Generate an @mcp.tool for each registered agent skill."""
        services = self._get("/v1/services")
        for svc in (services or []):
            if svc["status"] != "healthy":
                continue
            agent_id = svc["id"]
            for skill_name in svc.get("skills", []):
                self._register_one_skill(agent_id, skill_name)

    # ------------------------------------------------------------------
    # Dynamic refresh — pick up new skills when agents reload
    # ------------------------------------------------------------------

    def refresh_skills(self) -> dict[str, list[str]]:
        """Reconcile the MCP tool registry against current bus state.

        Queries ``/v1/services``, computes the diff against the set of
        currently-registered skill tools, and:

        - Adds an ``@mcp.tool`` for each new skill (via ``_register_one_skill``)
        - Removes the ``@mcp.tool`` for each skill whose agent went away

        Returns ``{"added": [tool_name, ...], "removed": [tool_name, ...]}``.

        Does NOT fire ``tools/list_changed`` on its own — callers with an
        MCP ``Context`` in hand should call ``_notify_list_changed`` to
        push the notification to connected clients.
        """
        services = self._get("/v1/services") or []
        # Build the set of currently-live skill tool names
        live: set[str] = set()
        for svc in services:
            if svc.get("status") != "healthy":
                continue
            agent_id = svc["id"]
            for skill_name in svc.get("skills", []):
                live.add(f"{agent_id}.{skill_name}")

        # Skill tools (namespaced agent_id.skill_name) vs bus/flow tools (no dot
        # or a single-component flow name). We only reconcile skill tools here;
        # bus_* and collaborative flows are owned elsewhere.
        currently_registered = {
            t for t in self._registered_tools
            if "." in t and not t.startswith("bus_")
        }

        added: list[str] = []
        for tool_name in sorted(live - currently_registered):
            agent_id, _, skill_name = tool_name.partition(".")
            self._register_one_skill(agent_id, skill_name)
            added.append(tool_name)

        removed: list[str] = []
        for tool_name in sorted(currently_registered - live):
            try:
                self.mcp.remove_tool(tool_name)
            except Exception as e:
                logger.warning("Failed to remove tool %s: %s", tool_name, e)
                continue
            self._registered_tools.discard(tool_name)
            removed.append(tool_name)

        if added or removed:
            logger.info(
                "refresh_skills: +%d -%d (added=%s removed=%s)",
                len(added), len(removed), added, removed,
            )
        return {"added": added, "removed": removed}

    async def _refresh_and_notify(self, ctx: Context | None) -> None:
        """Refresh skill tools, then push ``tools/list_changed`` if we have a ctx.

        Called by lifecycle handlers after start/stop/restart — these are
        the common triggers for a skill-set change. ``ctx`` is the MCP
        request context; without it we can't fire the notification (no
        session handle), but the refresh itself still happens.
        """
        diff = self.refresh_skills()
        if ctx is not None and (diff["added"] or diff["removed"]):
            await self._notify_list_changed(ctx)

    async def _notify_list_changed(self, ctx: Context) -> None:
        """Fire MCP ``notifications/tools/list_changed`` to the active session.

        Best-effort: some transports / clients don't support the notification,
        and the session may not expose ``send_tool_list_changed`` on all
        library versions. A failure here is not fatal to the overall
        lifecycle op; log and continue.
        """
        try:
            send = getattr(ctx.session, "send_tool_list_changed", None)
            if send is not None:
                await send()
        except Exception as e:
            logger.warning("tools/list_changed notification failed: %s", e)

    def _register_one_skill(self, agent_id: str, skill_name: str) -> None:
        tool_name = f"{agent_id}.{skill_name}"
        if tool_name in self._registered_tools:
            return
        self._registered_tools.add(tool_name)

        adapter = self

        # Fetch skill description from the registry
        skills = self._get("/v1/skills", params={"agent_id": agent_id})
        description = ""
        for s in (skills or []):
            if s["name"] == skill_name:
                description = s.get("description", "")
                break

        # Dynamic tool — takes kwargs as JSON string since we don't know
        # the schema at generation time (progressive disclosure).
        @self.mcp.tool(name=tool_name, description=description or f"{agent_id} skill: {skill_name}")
        async def _skill_proxy(args: str = "{}") -> str:
            """Route to bus agent. Pass args as JSON string."""
            try:
                parsed_args = json.loads(args) if isinstance(args, str) else args
            except json.JSONDecodeError:
                return f"error: invalid JSON args: {args}"

            result = await adapter._async_request(agent_id, skill_name, parsed_args)
            if "error" in result:
                return f"error: {result['error']}"
            r = result.get("result", "")
            return r if isinstance(r, str) else json.dumps(r, indent=2)

    def _register_flow_tools(self) -> None:
        """Generate an @mcp.tool for each available collaborative flow."""
        flows = self._get("/v1/flows")
        for flow in (flows or {}).get("available", []):
            self._register_one_flow(flow)

    def _register_one_flow(self, flow: dict) -> None:
        flow_name = flow["name"]
        if flow_name in self._registered_tools:
            return
        self._registered_tools.add(flow_name)

        adapter = self
        description = flow.get("description", "") or f"Collaborative flow: {flow_name}"

        @self.mcp.tool(name=flow_name, description=description)
        async def _flow_proxy(args: str = "{}") -> str:
            """Execute a collaborative flow. Pass args as JSON string."""
            try:
                parsed_args = json.loads(args) if isinstance(args, str) else args
            except json.JSONDecodeError:
                return f"error: invalid JSON args: {args}"

            result = await adapter._async_post("/v1/flow", {
                "flow_id": flow_name,
                "args": parsed_args,
            })
            if "error" in result:
                return f"error: {result['error']}"
            r = result.get("result", "")
            return r if isinstance(r, str) else json.dumps(r, indent=2)

    # -- HTTP helpers --

    def _get(self, path: str, params: dict | None = None) -> Any:
        try:
            r = self._http.get(f"{self.bus_url}{path}", params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Bus request failed: GET %s: %s", path, e)
            return None

    async def _async_request(self, agent_id: str, operation: str, args: dict) -> dict:
        try:
            r = await self._async_http.post(
                f"{self.bus_url}/v1/request",
                json={
                    "agent_id": agent_id,
                    "operation": operation,
                    "args": args,
                    "timeout": 30,
                },
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    async def _async_post(self, path: str, body: dict, http_timeout: float | None = None) -> dict:
        try:
            kwargs: dict = {"json": body}
            if http_timeout is not None:
                kwargs["timeout"] = httpx.Timeout(http_timeout)
            r = await self._async_http.post(f"{self.bus_url}{path}", **kwargs)
            return r.json()
        except Exception as e:
            return {"error": str(e)}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="khonliang-bus MCP adapter")
    parser.add_argument("--bus", default="http://localhost:8787", help="Bus URL")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    adapter = BusMCPAdapter(args.bus)
    mcp = adapter.build()

    logger.info(
        "Bus-MCP adapter started. %d tools registered. Bus: %s",
        len(adapter._registered_tools) + 20,  # +20 for built-in bus tools
        args.bus,
    )

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
