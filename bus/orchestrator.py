"""Orchestrator agent — LLM-powered dynamic flow composition.

When a task doesn't fit a pre-declared static flow, the orchestrator
uses a local LLM to plan which agents to call, in what order, with
what arguments. It reads the skill catalog, plans a sequence, executes
it step by step, and adapts if intermediate results change the path.

The orchestrator is itself a bus endpoint — Claude calls it like any
other skill, and it coordinates the rest.

Usage::

    POST /v1/orchestrate
    {
        "task": "evaluate the next developer work unit against the research corpus",
        "context": {"project": "developer"},
        "model": "qwen2.5:7b"  // optional, defaults to config
    }
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import httpx

from bus.db import BusDB

logger = logging.getLogger(__name__)

DEFAULT_SYSTEM_PROMPT = """\
You are an orchestrator for a multi-agent platform. You have access to
registered agents and their skills. Your job is to decompose a task into
a sequence of agent skill calls and execute them.

Available agents and skills:
{catalog}

Respond with a JSON array of steps. Each step has:
  - "agent_type": which agent to call (by type, not ID)
  - "operation": which skill to invoke
  - "args": arguments dict
  - "output": a name for this step's result (referenced by later steps as {{name}})

Example:
[
  {{"agent_type": "researcher", "operation": "find_papers", "args": {{"query": "consensus algorithms"}}, "output": "papers"}},
  {{"agent_type": "developer", "operation": "evaluate_with_evidence", "args": {{"evidence": "{{papers}}"}}, "output": "evaluation"}}
]

Only use skills that are listed in the catalog. Do not invent skills.
Respond ONLY with the JSON array, no explanation.
"""


class Orchestrator:
    """LLM-powered dynamic flow composition."""

    def __init__(
        self,
        db: BusDB,
        http: httpx.AsyncClient,
        ollama_url: str = "http://localhost:11434",
        default_model: str = "qwen2.5:7b",
        bus_url: str = "http://localhost:8787",
    ):
        self.db = db
        self._http = http
        self.ollama_url = ollama_url.rstrip("/")
        self.default_model = default_model
        self.bus_url = bus_url.rstrip("/")

    async def orchestrate(
        self,
        task: str,
        context: dict[str, Any] | None = None,
        model: str = "",
        trace_id: str = "",
    ) -> dict[str, Any]:
        """Plan and execute a dynamic multi-agent flow for the given task.

        Steps:
          1. Build a catalog of available agents and skills
          2. Ask the LLM to plan a sequence of steps
          3. Execute each step via the bus request/reply mechanism
          4. Return the final result with trace
        """
        trace_id = trace_id or f"t-orch-{uuid.uuid4().hex[:8]}"
        model = model or self.default_model

        # 1. Build catalog
        catalog = self._build_catalog()
        if not catalog:
            return {"status": "failed", "error": "no agents registered", "trace_id": trace_id}

        # 2. Plan
        prompt = f"Task: {task}"
        if context:
            prompt += f"\nContext: {json.dumps(context)}"

        system = DEFAULT_SYSTEM_PROMPT.format(catalog=catalog)
        plan = await self._ask_llm(system, prompt, model)

        try:
            steps = json.loads(plan)
            if not isinstance(steps, list):
                return {"status": "failed", "error": f"LLM returned non-list: {type(steps).__name__}", "trace_id": trace_id}
        except json.JSONDecodeError as e:
            return {"status": "failed", "error": f"LLM returned invalid JSON: {e}", "plan_raw": plan, "trace_id": trace_id}

        if not steps:
            return {"status": "failed", "error": "LLM returned empty plan", "trace_id": trace_id}

        # 3. Execute
        step_outputs: dict[str, Any] = {}
        last_result: Any = None

        for i, step in enumerate(steps, 1):
            agent_type = step.get("agent_type", "")
            operation = step.get("operation", "")
            args = step.get("args", {})
            output_name = step.get("output", f"step_{i}")

            # Resolve template references in args
            resolved_args = self._resolve_refs(args, step_outputs)

            # Find a healthy agent of this type
            agent = self.db.get_healthy_agent_for_type(agent_type)
            if not agent:
                return {
                    "status": "partial",
                    "error": f"step {i}: no healthy agent for type {agent_type!r}",
                    "steps_completed": i - 1,
                    "plan": steps,
                    "trace_id": trace_id,
                }

            # Record trace
            self.db.record_trace_step(trace_id, i, agent_id=agent["id"], operation=operation, args=resolved_args)
            t0 = time.monotonic()

            # Execute via bus request
            callback_url = agent["callback_url"]
            use_ws_path = callback_url.startswith("ws:") or callback_url.startswith("wss:")
            try:
                if use_ws_path:
                    # WebSocket agents: route through the bus /v1/request endpoint
                    r = await self._http.post(
                        f"{self.bus_url}/v1/request",
                        json={
                            "agent_type": agent_type,
                            "operation": operation,
                            "args": resolved_args,
                            "timeout": 30,
                            "trace_id": trace_id,
                        },
                        timeout=30.0,
                    )
                else:
                    r = await self._http.post(
                        f"{callback_url}/v1/handle",
                        json={
                            "operation": operation,
                            "args": resolved_args,
                            "correlation_id": f"orch-{uuid.uuid4().hex[:8]}",
                            "trace_id": trace_id,
                        },
                        timeout=30.0,
                    )
                duration_ms = int((time.monotonic() - t0) * 1000)

                if r.status_code == 200:
                    result = r.json()
                    step_result = result.get("result", result)
                    self.db.finish_trace_step(trace_id, i, status="ok", duration_ms=duration_ms)
                else:
                    self.db.finish_trace_step(trace_id, i, status="failed", duration_ms=duration_ms, error=r.text)
                    return {
                        "status": "partial",
                        "error": f"step {i} ({agent_type}.{operation}): HTTP {r.status_code}",
                        "steps_completed": i - 1,
                        "trace_id": trace_id,
                    }
            except Exception as e:
                duration_ms = int((time.monotonic() - t0) * 1000)
                self.db.finish_trace_step(trace_id, i, status="failed", duration_ms=duration_ms, error=str(e))
                return {
                    "status": "partial",
                    "error": f"step {i}: {e}",
                    "steps_completed": i - 1,
                    "trace_id": trace_id,
                }

            step_outputs[output_name] = step_result
            last_result = step_result

        return {
            "status": "completed",
            "result": last_result,
            "steps_completed": len(steps),
            "steps_total": len(steps),
            "plan": steps,
            "trace_id": trace_id,
        }

    def _build_catalog(self) -> str:
        """Build a text catalog of registered agents and skills for the LLM."""
        services = self.db.get_registrations()
        if not services:
            return ""

        lines = []
        for svc in services:
            if svc.get("status") != "healthy":
                continue
            skills = self.db.get_skills(svc["id"])
            agent_type = svc.get("agent_type", svc["id"])
            lines.append(f"\nAgent: {agent_type} (v{svc.get('version', '?')})")
            for s in skills:
                desc = s.get("description", "")[:80]
                lines.append(f"  - {s['name']}: {desc}")

        return "\n".join(lines)

    async def _ask_llm(self, system: str, prompt: str, model: str) -> str:
        """Call Ollama to generate a plan."""
        try:
            r = await self._http.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": model,
                    "system": system,
                    "prompt": prompt,
                    "stream": False,
                },
                timeout=120.0,
            )
            r.raise_for_status()
            return r.json().get("response", "")
        except Exception as e:
            logger.error("Ollama call failed: %s", e)
            return "[]"

    def _resolve_refs(self, args: dict, outputs: dict[str, Any]) -> dict:
        """Replace {{name}} references in args with step outputs."""
        resolved = {}
        for key, value in args.items():
            if isinstance(value, str) and value.startswith("{{") and value.endswith("}}"):
                ref = value[2:-2]
                resolved[key] = outputs.get(ref, value)
            else:
                resolved[key] = value
        return resolved
