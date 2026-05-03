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
import math
import os
import sys
from typing import Any

import httpx
from mcp.server.fastmcp import Context, FastMCP

from bus.response_envelope import (
    build_response_envelope,
    dumps_envelope,
    extract_response_budget,
    serialize_result,
)

logger = logging.getLogger(__name__)

# Library fallback. Raised from 30 -> 60 after empirical evidence that 14-16B
# local Ollama reviewers legitimately run 30-35s and 30s truncated mid-flight
# (FR fr_khonliang_a3dc662d). 60s is a tighter ceiling than the 120s default
# most skills want; adapter config + per-call hint take precedence.
DEFAULT_MCP_TIMEOUT_S: float = 60.0

# Skill-arg control-plane hint. Stripped from args before forwarding so the
# skill handler never sees it.
MCP_TIMEOUT_ARG: str = "_mcp_timeout"


class BusMCPAdapter:
    """Translates between MCP (stdio, Claude-facing) and the bus (HTTP).

    Per-call timeout precedence (high → low). When set, ``mcp_timeout``
    or the legacy in-args hint applies symmetrically: it caps the
    adapter→bus transport AND is forwarded as ``FlowRequest.timeout``
    on flow calls / the bus's per-request ``timeout`` field on skill
    calls, so the bus-side cap matches the adapter-side cap.

    1. Top-level ``mcp_timeout`` kwarg on the auto-generated MCP tool
       (visible in the tool's JSON schema, so Claude can discover it
       without prior knowledge of the in-args hint). FastMCP rejects
       parameter names starting with ``_``, so the schema-level kwarg
       drops the underscore that the in-args hint and the library
       constant still carry.
    2. Per-call ``_mcp_timeout`` hint inside the skill-or-flow JSON
       ``args`` string (legacy / undiscoverable but still honored).
       Stripped before forwarding to the skill handler.

    When NO per-call override is supplied, the adapter default
    (``default_timeout_s``) and library fallback only cap the
    adapter→bus transport, NOT the server-side execution:

    3. Adapter default set at construction (``default_timeout_s``),
       normally resolved from ``KHONLIANG_MCP_DEFAULT_TIMEOUT`` env
       var in ``main()``. Skill calls pass it as the bus's per-request
       ``timeout`` field too. **Flow calls do not** — without an
       override, ``FlowRequest.timeout`` is omitted and the engine
       falls back to its own built-in per-step cap, so the adapter
       default acts purely as the transport-side ceiling on flows.
    4. Library fallback (``DEFAULT_MCP_TIMEOUT_S`` = 60s) — same
       caveat as (3) for flows.

    A per-skill default from the ``Skill`` descriptor is a deliberate
    future extension (would require a bus-lib + registry schema
    change) tracked separately; today the per-call hint covers slow
    skills.
    """

    def __init__(self, bus_url: str, default_timeout_s: float | None = None):
        self.bus_url = bus_url.rstrip("/")
        self.default_timeout_s = (
            float(default_timeout_s)
            if default_timeout_s is not None
            else DEFAULT_MCP_TIMEOUT_S
        )
        # Construct httpx clients with the adapter default timeout as the
        # client-level cap. Call sites that need a longer window (or shorter)
        # pass a per-request ``timeout=`` kwarg which overrides the client
        # default, so slow skills can run to completion while short calls
        # still return quickly.
        self._http = httpx.Client(timeout=httpx.Timeout(self.default_timeout_s))
        self._async_http = httpx.AsyncClient(timeout=httpx.Timeout(self.default_timeout_s))
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
            cursor: str = "",
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
                cursor: Where to start matching from. ``""`` (default)
                    replays unacked history (legacy semantics — a fresh
                    subscriber sees retained backlog). ``"now"`` /
                    ``"latest"`` snapshot the current high-water mark
                    and only return events published after this call
                    begins; useful for "wake me when the next thing
                    happens" without draining backlog one-event-per-call.
                    Per-call floor; not persisted.

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
                    "cursor": cursor,
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
        async def bus_feedback(
            agent_id: str = "",
            kind: str = "",
            status: str = "open",
            since: str = "",
            limit: int = 20,
        ) -> str:
            """Query structured agent feedback reports."""
            reports = adapter._get(
                "/v1/feedback",
                params={
                    "agent_id": agent_id,
                    "kind": kind,
                    "status": status,
                    "since": since,
                    "limit": limit,
                },
            )
            return json.dumps(reports or [], indent=2)

        # Read-side ``bus_artifact_*`` MCP tools were retired with
        # khonliang-store Phase 4c (`fr_khonliang-bus_9151395d`).
        # The store agent now owns the canonical artifact read
        # surface — operators reach the same data through
        # ``store-primary.artifact_list / .artifact_metadata /
        # .artifact_get / .artifact_head / .artifact_tail /
        # .artifact_grep / .artifact_excerpt`` (registered
        # dynamically by ``_register_skill_tools`` from the
        # ``store-primary`` agent's skill list). The bus's
        # ``/v1/artifacts/*`` HTTP routes stay reachable so
        # ``khonliang-store``'s composite read fallback
        # (``BusBackedArtifactStore``) keeps working until
        # operators have finished the migration via
        # ``store-primary.artifact_migrate_from_bus``.
        #
        # ``bus_artifact_distill`` and ``bus_artifact_distill_many``
        # below are NOT part of this deprecation — they create new
        # artifacts, which store doesn't have an equivalent skill
        # for yet (Phase 5 territory).

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
        # the schema at generation time (progressive disclosure). The
        # explicit ``mcp_timeout`` kwarg is part of the schema so Claude
        # can override the default cap without prior knowledge of the
        # legacy in-args hint convention. FastMCP forbids underscore-
        # prefixed parameter names, so the schema-level kwarg drops the
        # underscore that the in-args hint still carries.
        @self.mcp.tool(name=tool_name, description=description or f"{agent_id} skill: {skill_name}")
        async def _skill_proxy(args: str = "{}", mcp_timeout: int | None = None) -> str:
            """Route to bus agent. Pass args as JSON string.

            ``mcp_timeout`` (seconds): override the adapter's default
            request timeout for this single call. Use it for skills that
            legitimately run >60s (long ingest, deep distill, slow review).
            Wins over an ``_mcp_timeout`` value embedded in ``args``.
            """
            try:
                parsed_args = json.loads(args) if isinstance(args, str) else args
            except json.JSONDecodeError:
                return f"error: invalid JSON args: {args}"
            if not isinstance(parsed_args, dict):
                return "error: args must be a JSON object"
            budget = extract_response_budget(parsed_args)
            timeout = adapter._resolve_timeout(mcp_timeout, parsed_args)

            result = await adapter._async_request(
                agent_id, skill_name, parsed_args, timeout=timeout,
            )
            if "error" in result:
                return adapter._format_error(result)
            return await adapter._format_tool_result(
                producer=agent_id,
                operation=skill_name,
                result=result.get("result", ""),
                budget=budget,
            )

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
        async def _flow_proxy(args: str = "{}", mcp_timeout: int | None = None) -> str:
            """Execute a collaborative flow. Pass args as JSON string.

            ``mcp_timeout`` (seconds): caps the adapter→bus transport AND
            is forwarded to ``FlowRequest.timeout`` so ``FlowEngine._call_agent``
            uses it as the per-step cap rather than its built-in 30s default.
            Without that plumbing, a flow whose inner agent step takes >30s
            would be truncated inside the engine even when the outer MCP
            timeout is generous. Wins over an ``_mcp_timeout`` value
            embedded in ``args``.
            """
            try:
                parsed_args = json.loads(args) if isinstance(args, str) else args
            except json.JSONDecodeError:
                return f"error: invalid JSON args: {args}"
            if not isinstance(parsed_args, dict):
                return "error: args must be a JSON object"
            budget = extract_response_budget(parsed_args)
            timeout = adapter._resolve_timeout(mcp_timeout, parsed_args)

            flow_body: dict[str, Any] = {"flow_id": flow_name, "args": parsed_args}
            if timeout is not None:
                flow_body["timeout"] = timeout

            result = await adapter._async_post(
                "/v1/flow",
                flow_body,
                http_timeout=(timeout or adapter.default_timeout_s) + 5.0,
            )
            if "error" in result:
                return adapter._format_error(result)
            return await adapter._format_tool_result(
                producer="flow",
                operation=flow_name,
                result=result.get("result", ""),
                budget=budget,
            )

    # -- Control-plane helpers --

    def _coerce_timeout(self, raw: Any, source: str = MCP_TIMEOUT_ARG) -> float | None:
        """Validate a raw timeout value from either the top-level
        ``mcp_timeout`` kwarg or the legacy in-args ``_mcp_timeout`` hint.
        ``source`` is the spelling used in the warning logs so an operator
        can see which ingress path supplied a bad value (the two spellings
        differ because FastMCP rejects underscore-prefixed kwarg names).
        Returns None when absent or invalid (adapter default applies).

        Rejects ``nan`` and ``inf`` for parity with ``_resolve_default_timeout``:
        ``json.loads`` happily parses ``NaN``/``Infinity`` and either would
        propagate down to ``httpx.Timeout(...)`` with bad consequences
        (``inf`` effectively disables the cap; ``nan`` raises later)."""
        if raw is None:
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid %s value (not numeric): %r", source, raw)
            return None
        if not math.isfinite(value):
            logger.warning("Invalid %s value (not finite): %r", source, raw)
            return None
        if value <= 0:
            logger.warning("Invalid %s value (non-positive): %r", source, raw)
            return None
        return value

    def _pop_timeout_hint(self, args: dict) -> float | None:
        """Extract and remove the ``_mcp_timeout`` control-plane hint from
        skill args so it isn't forwarded to the skill handler. Returns None
        when absent or invalid (adapter default applies)."""
        return self._coerce_timeout(args.pop(MCP_TIMEOUT_ARG, None), MCP_TIMEOUT_ARG)

    def _resolve_timeout(
        self, top_level: int | float | None, args: dict
    ) -> float | None:
        """Pick the effective per-call timeout for a skill or flow call.

        Top-level ``mcp_timeout`` kwarg (visible in the tool's JSON schema)
        wins over the legacy ``_mcp_timeout`` hint (hidden inside the args
        JSON string). Either path strips the hint from ``args`` so the
        skill handler never sees it. Invalid values silently fall through
        to the next tier; warning logs label the bad value with the source
        spelling so it's clear which ingress was supplied."""
        # Always pop the in-args hint to keep it out of the forwarded args,
        # even when the top-level kwarg ultimately wins.
        in_args = self._pop_timeout_hint(args)
        top = self._coerce_timeout(top_level, "mcp_timeout")
        return top if top is not None else in_args

    def _format_error(self, result: dict) -> str:
        """Render a bus error dict as a non-empty MCP string. Preserves the
        ``timed_out`` marker and trace_id (when present) so callers can tell
        a transport truncation apart from a skill-level failure."""
        parts: list[str] = [f"error: {result.get('error', 'unknown error')}"]
        if result.get("timed_out"):
            parts.append("timed_out=true")
            if (timeout_s := result.get("timeout_s")) is not None:
                parts.append(f"timeout_s={timeout_s}")
        if (trace_id := result.get("trace_id")):
            parts.append(f"trace_id={trace_id}")
        return " ".join(parts)

    # -- HTTP helpers --

    def _get(self, path: str, params: dict | None = None) -> Any:
        try:
            r = self._http.get(f"{self.bus_url}{path}", params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Bus request failed: GET %s: %s", path, e)
            return None

    async def _async_request(
        self,
        agent_id: str,
        operation: str,
        args: dict,
        timeout: float | None = None,
    ) -> dict:
        """POST to /v1/request. Timeout applies to both httpx transport and the
        bus-side ``timeout`` field (so the server's bus→agent call doesn't cap
        before ours does). When the bus returns a JSON body, callers may
        receive bus-supplied fields such as ``trace_id``. If the httpx
        transport itself times out, this method instead returns a synthetic
        dict with ``timed_out=True`` and ``timeout_s`` but no bus response
        metadata (no ``trace_id``) — the request never reached the bus or the
        response never made it back.
        """
        resolved = float(timeout) if timeout is not None else self.default_timeout_s
        # Give the bus a small buffer to return its own timeout envelope
        # gracefully before the client-side transport gives up.
        transport_timeout = resolved + 5.0
        try:
            r = await self._async_http.post(
                f"{self.bus_url}/v1/request",
                json={
                    "agent_id": agent_id,
                    "operation": operation,
                    "args": args,
                    "timeout": resolved,
                },
                timeout=httpx.Timeout(transport_timeout),
            )
            return r.json()
        except httpx.TimeoutException as e:
            logger.warning(
                "MCP transport timeout after %.1fs for %s.%s: %s",
                transport_timeout, agent_id, operation, e,
            )
            return {
                "error": f"mcp transport timeout after {transport_timeout:.1f}s",
                "timed_out": True,
                "timeout_s": resolved,
            }
        except Exception as e:
            return {"error": str(e)}

    async def _async_post(self, path: str, body: dict, http_timeout: float | None = None) -> dict:
        try:
            kwargs: dict = {"json": body}
            if http_timeout is not None:
                kwargs["timeout"] = httpx.Timeout(http_timeout)
            r = await self._async_http.post(f"{self.bus_url}{path}", **kwargs)
            return r.json()
        except httpx.TimeoutException as e:
            effective = http_timeout if http_timeout is not None else self.default_timeout_s
            logger.warning(
                "MCP transport timeout after %.1fs for POST %s: %s",
                effective, path, e,
            )
            return {
                "error": f"mcp transport timeout after {effective:.1f}s",
                "timed_out": True,
                "timeout_s": effective,
            }
        except Exception as e:
            return {"error": str(e)}

    async def _format_tool_result(
        self,
        *,
        producer: str,
        operation: str,
        result: Any,
        budget,
    ) -> str:
        """Return a bounded envelope for dynamic skill and flow outputs."""
        text, content_type = serialize_result(result)
        artifact = None
        if len(text) > budget.max_chars:
            artifact = await self._async_post(
                "/v1/artifacts",
                {
                    "kind": "tool_result",
                    "title": f"{producer}.{operation} result",
                    "content": text,
                    "producer": producer,
                    "content_type": content_type,
                    "metadata": {
                        "operation": operation,
                        "mcp_tool": f"{producer}.{operation}",
                    },
                },
            )
            if artifact and "error" in artifact:
                logger.warning(
                    "Failed to store artifact for %s.%s: %s",
                    producer,
                    operation,
                    artifact["error"],
                )
                artifact = None

        envelope = build_response_envelope(
            ok=True,
            status="ok",
            producer=producer,
            operation=operation,
            text=text,
            budget=budget,
            artifact=artifact,
            content_type=content_type,
        )
        return dumps_envelope(envelope)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _resolve_default_timeout(cli_value: float | None) -> float:
    """Resolve the adapter-level default timeout from CLI / env / library.

    Precedence: explicit ``--default-timeout`` CLI arg, then
    ``KHONLIANG_MCP_DEFAULT_TIMEOUT`` env var, then ``DEFAULT_MCP_TIMEOUT_S``.
    Invalid env values log a warning and fall back to the library default.
    An invalid CLI value (non-positive or non-finite) raises ``ValueError``
    — the CLI is an explicit operator choice, so we fail loudly rather than
    silently falling back the way env does.
    """
    if cli_value is not None:
        value = float(cli_value)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f"--default-timeout must be a positive finite number, got {cli_value!r}"
            )
        return value
    env = os.environ.get("KHONLIANG_MCP_DEFAULT_TIMEOUT")
    if env:
        try:
            value = float(env)
            if math.isfinite(value) and value > 0:
                return value
            logger.warning(
                "KHONLIANG_MCP_DEFAULT_TIMEOUT must be a positive finite "
                "number, got %r — using default",
                env,
            )
        except ValueError:
            logger.warning(
                "KHONLIANG_MCP_DEFAULT_TIMEOUT not numeric (%r) — using default",
                env,
            )
    return DEFAULT_MCP_TIMEOUT_S


def main():
    parser = argparse.ArgumentParser(description="khonliang-bus MCP adapter")
    parser.add_argument("--bus", default="http://localhost:8787", help="Bus URL")
    parser.add_argument(
        "--default-timeout",
        type=float,
        default=None,
        help=(
            "Adapter-level default timeout (seconds). For skill calls, "
            "applies symmetrically to the adapter→bus transport AND the "
            "bus's per-request timeout field. For flow calls, applies "
            "only to the +5s-buffered transport cap; "
            "``FlowRequest.timeout`` is omitted when no per-call "
            "override is supplied, and in that omitted case the engine "
            "falls back to its built-in per-step cap (the adapter "
            "default does NOT propagate to per-step). Overrides "
            "KHONLIANG_MCP_DEFAULT_TIMEOUT env var. A per-call override "
            "is symmetric on both surfaces: the top-level "
            "``mcp_timeout`` kwarg on a skill or flow tool, or the legacy "
            "``_mcp_timeout`` hint embedded in the ``args`` JSON string."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    try:
        default_timeout = _resolve_default_timeout(args.default_timeout)
    except ValueError as exc:
        parser.error(str(exc))
    adapter = BusMCPAdapter(args.bus, default_timeout_s=default_timeout)
    mcp = adapter.build()

    logger.info(
        "Bus-MCP adapter started. %d tools registered. Bus: %s Default timeout: %.1fs",
        len(adapter._registered_tools) + 21,  # +21 for built-in bus tools
        args.bus,
        default_timeout,
    )

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
