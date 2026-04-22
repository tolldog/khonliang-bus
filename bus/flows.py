"""Flow orchestration engine — execute declared multi-agent flows.

Walks a flow definition step-by-step, resolving {{template}} variables
from prior step outputs. Supports:
  - DAG references: {{step.1.field}} and {{step.3.other_field}} in the same step
  - Sequential execution (steps run in order, not parallel — parallel is Step 6.5)
  - Per-step error handling with partial results
  - Trace recording per step via BusDB
  - Template resolution for nested fields: {{step.1.title}}, {{step.2.papers.0}}

Does NOT support (yet):
  - Parallel fan-out (Step 6.5)
  - on_failure handlers (Step 6.5)
  - Nested flows / workflow-as-step (Step 6.5)
  - Conditional branching (Step 6.5)
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from typing import Any

import httpx

from bus.db import BusDB

logger = logging.getLogger(__name__)

_TEMPLATE_RE = re.compile(r"\{\{(\w+(?:\.\w+)*)\}\}")


_DEFAULT_STEP_TIMEOUT_S: float = 30.0


class FlowEngine:
    """Execute multi-agent flows declared in the bus registry."""

    def __init__(self, db: BusDB, http: httpx.AsyncClient):
        self.db = db
        self._http = http

    async def execute(
        self,
        flow_name: str,
        args: dict[str, Any],
        trace_id: str = "",
        step_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Execute a named flow.

        Returns::

            {
                "status": "completed" | "partial" | "failed",
                "result": <final step output>,
                "steps_completed": N,
                "steps_total": M,
                "trace_id": "t-...",
                "error": "..." (if failed/partial),
            }
        """
        trace_id = trace_id or f"t-{uuid.uuid4().hex[:8]}"

        flow = self.db.get_flow(flow_name)
        if not flow:
            return {"status": "failed", "error": f"flow not found: {flow_name}", "trace_id": trace_id}

        steps = flow.get("steps", [])
        if isinstance(steps, str):
            try:
                steps = json.loads(steps)
            except json.JSONDecodeError:
                return {"status": "failed", "error": "invalid flow steps JSON", "trace_id": trace_id}

        if not steps:
            return {"status": "failed", "error": "flow has no steps", "trace_id": trace_id}

        # Execution context: step outputs indexed by step number (1-based)
        ctx: dict[str, Any] = {"args": args}
        step_outputs: dict[int, Any] = {}
        last_result: Any = None

        for i, step in enumerate(steps, 1):
            call = step.get("call", "")
            step_args = step.get("args", {})
            output_name = step.get("output", f"step_{i}")

            # Parse agent and operation from "agent_type.operation" format
            if "." in call:
                agent_ref, operation = call.rsplit(".", 1)
            else:
                agent_ref = ""
                operation = call

            # Resolve template variables in args
            resolved_args = _resolve_templates(step_args, ctx, step_outputs)

            # Find the agent to call
            agent = self._resolve_agent(agent_ref)
            if not agent:
                self.db.record_trace_step(trace_id, i, agent_id=agent_ref, operation=operation)
                self.db.finish_trace_step(trace_id, i, status="failed", duration_ms=0, error=f"no healthy agent for {agent_ref}")
                return {
                    "status": "partial",
                    "result": last_result,
                    "steps_completed": i - 1,
                    "steps_total": len(steps),
                    "trace_id": trace_id,
                    "error": f"step {i}: no healthy agent for {agent_ref}",
                }

            # Execute the step
            self.db.record_trace_step(trace_id, i, agent_id=agent["id"], operation=operation)
            t0 = time.monotonic()

            try:
                result = await self._call_agent(
                    agent, operation, resolved_args, timeout=step_timeout,
                )
                duration_ms = int((time.monotonic() - t0) * 1000)

                if "error" in result:
                    self.db.finish_trace_step(trace_id, i, status="failed", duration_ms=duration_ms, error=str(result["error"]))
                    return {
                        "status": "partial",
                        "result": last_result,
                        "steps_completed": i - 1,
                        "steps_total": len(steps),
                        "trace_id": trace_id,
                        "error": f"step {i} ({agent['id']}.{operation}): {result['error']}",
                    }

                step_result = result.get("result", result)
                self.db.finish_trace_step(trace_id, i, status="ok", duration_ms=duration_ms)

            except Exception as e:
                duration_ms = int((time.monotonic() - t0) * 1000)
                self.db.finish_trace_step(trace_id, i, status="failed", duration_ms=duration_ms, error=str(e))
                return {
                    "status": "partial",
                    "result": last_result,
                    "steps_completed": i - 1,
                    "steps_total": len(steps),
                    "trace_id": trace_id,
                    "error": f"step {i}: {e}",
                }

            # Store output for subsequent step references
            step_outputs[i] = step_result
            ctx[output_name] = step_result
            ctx[f"step_{i}"] = step_result
            last_result = step_result

        return {
            "status": "completed",
            "result": last_result,
            "steps_completed": len(steps),
            "steps_total": len(steps),
            "trace_id": trace_id,
        }

    def _resolve_agent(self, agent_ref: str) -> dict[str, Any] | None:
        """Resolve an agent reference to a registration.

        Tries: exact ID match first, then type-based lookup.
        """
        reg = self.db.get_registration(agent_ref)
        if reg and reg["status"] == "healthy":
            return reg
        return self.db.get_healthy_agent_for_type(agent_ref)

    async def _call_agent(
        self,
        agent: dict,
        operation: str,
        args: dict,
        timeout: float | None = None,
    ) -> dict:
        """Forward a request to an agent's callback URL.

        ``timeout`` overrides the per-step default when set. The default of
        30s matches the engine's historical cap; callers who need to extend
        it (e.g. local-Ollama reviewer steps that legitimately run longer)
        should supply a larger value via ``FlowRequest.timeout``.
        """
        correlation_id = f"flow-{uuid.uuid4().hex[:8]}"
        resolved_timeout = timeout if timeout is not None else _DEFAULT_STEP_TIMEOUT_S
        r = await self._http.post(
            f"{agent['callback_url']}/v1/handle",
            json={
                "operation": operation,
                "args": args,
                "correlation_id": correlation_id,
            },
            timeout=resolved_timeout,
        )
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}: {r.text}"}
        return r.json()


# ---------------------------------------------------------------------------
# Template resolution
# ---------------------------------------------------------------------------


def _resolve_templates(
    args: dict[str, Any],
    ctx: dict[str, Any],
    step_outputs: dict[int, Any],
) -> dict[str, Any]:
    """Resolve {{template}} variables in flow step args.

    Supports:
      - {{args.field}} — from the flow's input args
      - {{step.N.field}} — from step N's output (1-based)
      - {{output_name.field}} — from a named step output
      - {{step.N}} — the entire step output (for passing complex objects)
    """
    resolved = {}
    for key, value in args.items():
        if isinstance(value, str):
            resolved[key] = _resolve_string(value, ctx, step_outputs)
        elif isinstance(value, dict):
            resolved[key] = _resolve_templates(value, ctx, step_outputs)
        else:
            resolved[key] = value
    return resolved


def _resolve_string(
    template: str,
    ctx: dict[str, Any],
    step_outputs: dict[int, Any],
) -> Any:
    """Resolve a single template string. If the entire string is one
    template (e.g. ``"{{step.1}}"``), return the raw value (could be
    dict, list, etc.). If it contains mixed text + templates, do string
    interpolation.
    """
    # Check if the entire string is a single template
    match = _TEMPLATE_RE.fullmatch(template)
    if match:
        return _lookup(match.group(1), ctx, step_outputs)

    # Mixed text + templates → string interpolation
    def replacer(m):
        val = _lookup(m.group(1), ctx, step_outputs)
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val) if val is not None else ""

    return _TEMPLATE_RE.sub(replacer, template)


def _lookup(path: str, ctx: dict[str, Any], step_outputs: dict[int, Any]) -> Any:
    """Look up a dotted path in the context.

    ``step.1.title`` → step_outputs[1]["title"]
    ``args.path`` → ctx["args"]["path"]
    ``spec.title`` → ctx["spec"]["title"]
    """
    parts = path.split(".")

    # Handle "step.N" references
    if parts[0] == "step" and len(parts) >= 2:
        try:
            step_num = int(parts[1])
            obj = step_outputs.get(step_num)
            return _drill(obj, parts[2:])
        except (ValueError, IndexError):
            return None

    # Handle context references (args, named outputs)
    obj = ctx.get(parts[0])
    return _drill(obj, parts[1:])


def _drill(obj: Any, path: list[str]) -> Any:
    """Drill into a nested object by dotted path segments."""
    for key in path:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list):
            try:
                obj = obj[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            obj = getattr(obj, key, None)
    return obj
