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
from mcp.server.fastmcp import FastMCP

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

    def _register_skill_tools(self) -> None:
        """Generate an @mcp.tool for each registered agent skill."""
        services = self._get("/v1/services")
        for svc in (services or []):
            if svc["status"] != "healthy":
                continue
            agent_id = svc["id"]
            for skill_name in svc.get("skills", []):
                self._register_one_skill(agent_id, skill_name)

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

    async def _async_post(self, path: str, body: dict) -> dict:
        try:
            r = await self._async_http.post(f"{self.bus_url}{path}", json=body)
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
        len(adapter._registered_tools) + 6,  # +6 for bus_* tools
        args.bus,
    )

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
