"""Tests for the flow orchestration engine — template resolution, execution, traces."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bus.flows import FlowEngine, _resolve_templates, _resolve_string
from bus.db import BusDB


# ---------------------------------------------------------------------------
# Template resolution unit tests
# ---------------------------------------------------------------------------


def test_resolve_simple_args_ref():
    ctx = {"args": {"path": "/my/spec.md"}}
    result = _resolve_string("{{args.path}}", ctx, {})
    assert result == "/my/spec.md"


def test_resolve_step_ref():
    step_outputs = {1: {"title": "My Spec", "sections": 5}}
    result = _resolve_string("{{step.1.title}}", {}, step_outputs)
    assert result == "My Spec"


def test_resolve_entire_step_as_object():
    step_outputs = {1: {"title": "Spec", "body": "full content"}}
    result = _resolve_string("{{step.1}}", {}, step_outputs)
    assert isinstance(result, dict)
    assert result["title"] == "Spec"


def test_resolve_mixed_text_and_templates():
    ctx = {"args": {"name": "world"}}
    result = _resolve_string("hello {{args.name}}!", ctx, {})
    assert result == "hello world!"


def test_resolve_nested_dict_args():
    ctx = {"args": {"path": "/spec.md"}}
    step_outputs = {1: {"title": "My Spec"}}
    args = {
        "path": "{{args.path}}",
        "query": "{{step.1.title}}",
        "nested": {"inner": "{{step.1.title}}"},
    }
    resolved = _resolve_templates(args, ctx, step_outputs)
    assert resolved["path"] == "/spec.md"
    assert resolved["query"] == "My Spec"
    assert resolved["nested"]["inner"] == "My Spec"


def test_resolve_named_output():
    ctx = {"spec": {"title": "Consensus", "fr": "fr_dev_12345678"}}
    result = _resolve_string("{{spec.title}}", ctx, {})
    assert result == "Consensus"


def test_resolve_missing_ref_returns_none():
    result = _resolve_string("{{step.99.missing}}", {}, {})
    assert result is None


def test_resolve_list_index():
    step_outputs = {1: {"papers": ["paper_a", "paper_b"]}}
    result = _resolve_string("{{step.1.papers.0}}", {}, step_outputs)
    assert result == "paper_a"


# ---------------------------------------------------------------------------
# Flow execution integration tests (with a mock agent)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_agent():
    """A simple FastAPI agent that echoes operations."""
    app = FastAPI()

    @app.post("/v1/handle")
    async def handle(request: dict):
        op = request.get("operation", "")
        args = request.get("args", {})
        cid = request.get("correlation_id", "")

        if op == "read_spec":
            return {"correlation_id": cid, "result": {
                "title": f"Spec: {args.get('path', '?')}",
                "sections": 5,
                "fr": "fr_dev_28a11ce2",
            }}
        elif op == "find_relevant":
            return {"correlation_id": cid, "result": {
                "count": 3,
                "papers": ["paper1", "paper2", "paper3"],
            }}
        elif op == "evaluate":
            return {"correlation_id": cid, "result": {
                "verdict": "approved",
                "evidence": args.get("evidence", "none"),
            }}
        elif op == "fail":
            return {"correlation_id": cid, "error": "intentional failure", "retryable": False}
        else:
            return {"correlation_id": cid, "result": {"echo": op, "args": args}}

    return TestClient(app)


@pytest.fixture
def flow_db(tmp_path):
    return BusDB(str(tmp_path / "flow-test.db"))


@pytest.fixture
def flow_engine(flow_db, mock_agent):
    """FlowEngine wired to a mock agent via ASGI transport."""
    transport = httpx.ASGITransport(app=mock_agent.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://testserver")
    return FlowEngine(flow_db, client)


def _register_agents(flow_db):
    """Register test agents pointing at http://testserver (the mock)."""
    flow_db.register_agent(
        "developer", "developer", "http://testserver", 1, "0.1.0",
        skills=[{"name": "read_spec"}, {"name": "evaluate"}, {"name": "fail"}],
    )
    flow_db.register_agent(
        "researcher", "researcher", "http://testserver", 2, "0.6.4",
        skills=[{"name": "find_relevant"}],
        collaborations=[{
            "name": "evaluate_spec",
            "requires": {"developer": ">=0.1.0"},
            "steps": [
                {"call": "developer.read_spec", "args": {"path": "{{args.path}}"}, "output": "spec"},
                {"call": "researcher.find_relevant", "args": {"query": "{{spec.title}}"}, "output": "papers"},
            ],
        }],
    )


def test_execute_simple_flow(flow_db, flow_engine):
    """A 2-step flow: read_spec → find_relevant, with step references."""
    _register_agents(flow_db)

    result = asyncio.run(flow_engine.execute("evaluate_spec", {"path": "/my/spec.md"}, trace_id="t-flow-1"))
    assert result["status"] == "completed"
    assert result["steps_completed"] == 2
    assert result["result"]["count"] == 3


def test_execute_dag_flow(flow_db, flow_engine):
    """A 3-step DAG: step 3 references outputs from steps 1 AND 2."""
    _register_agents(flow_db)

    with flow_db.conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flows (name, declared_by, description, requires, steps) VALUES (?, ?, ?, ?, ?)",
            ("dag_flow", "developer", "DAG test", "[]", json.dumps([
                {"call": "developer.read_spec", "args": {"path": "{{args.path}}"}, "output": "spec"},
                {"call": "researcher.find_relevant", "args": {"query": "{{spec.title}}"}, "output": "papers"},
                {"call": "developer.evaluate", "args": {
                    "evidence": "{{step.1.title}} + {{step.2.count}} papers",
                }, "output": "evaluation"},
            ])),
        )

    result = asyncio.run(flow_engine.execute("dag_flow", {"path": "/spec.md"}, trace_id="t-dag"))
    assert result["status"] == "completed"
    assert result["steps_completed"] == 3
    assert result["result"]["evidence"] == "Spec: /spec.md + 3 papers"


def test_execute_flow_with_failing_step(flow_db, flow_engine):
    """A flow where step 2 fails — returns partial result."""
    _register_agents(flow_db)

    with flow_db.conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flows (name, declared_by, description, requires, steps) VALUES (?, ?, ?, ?, ?)",
            ("failing_flow", "developer", "Fails at step 2", "[]", json.dumps([
                {"call": "developer.read_spec", "args": {"path": "{{args.path}}"}, "output": "spec"},
                {"call": "developer.fail", "args": {}},
            ])),
        )

    result = asyncio.run(flow_engine.execute("failing_flow", {"path": "/spec.md"}, trace_id="t-fail"))
    assert result["status"] == "partial"
    assert result["steps_completed"] == 1
    assert "intentional failure" in result["error"]


def test_execute_flow_records_trace(flow_db, flow_engine):
    """Verify trace entries are recorded for each step."""
    _register_agents(flow_db)

    with flow_db.conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flows (name, declared_by, description, requires, steps) VALUES (?, ?, ?, ?, ?)",
            ("traced_flow", "developer", "", "[]", json.dumps([
                {"call": "developer.read_spec", "args": {"path": "/x.md"}},
            ])),
        )

    result = asyncio.run(flow_engine.execute("traced_flow", {}, trace_id="t-traced"))
    trace = flow_db.get_trace("t-traced")
    assert len(trace) == 1
    assert trace[0]["status"] == "ok"
    assert trace[0]["operation"] == "read_spec"
    assert trace[0]["duration_ms"] is not None


def test_execute_unknown_flow(flow_db):
    import httpx
    engine = FlowEngine(flow_db, httpx.AsyncClient())
    result = asyncio.run(engine.execute("nonexistent", {}))
    assert result["status"] == "failed"
    assert "not found" in result["error"]


def test_execute_flow_forwards_step_timeout(flow_db, flow_engine):
    """``FlowRequest.timeout`` → ``FlowEngine.execute(step_timeout=...)`` →
    ``_call_agent(timeout=...)``. Without this plumbing the per-step httpx
    cap stays at the built-in 30s even when the MCP adapter passed a larger
    ``_mcp_timeout`` hint, and slow inner-agent calls get truncated.
    """
    _register_agents(flow_db)
    captured: list[float | None] = []

    async def recording_call_agent(agent, operation, args, timeout=None):
        captured.append(timeout)
        return {"result": {"echo": operation}}

    flow_engine._call_agent = recording_call_agent
    result = asyncio.run(flow_engine.execute(
        "evaluate_spec", {"path": "/x.md"}, trace_id="t-to", step_timeout=180.0,
    ))
    assert result["status"] == "completed"
    # Both steps in the evaluate_spec flow must see the same step_timeout.
    assert captured == [180.0, 180.0]


def test_execute_flow_default_step_timeout_when_unset(flow_db, flow_engine):
    """Without a ``step_timeout`` override, ``_call_agent`` receives None and
    falls back to its internal default (preserving legacy behavior for flows
    invoked outside the MCP adapter path)."""
    _register_agents(flow_db)
    captured: list[float | None] = []

    async def recording_call_agent(agent, operation, args, timeout=None):
        captured.append(timeout)
        return {"result": {"echo": operation}}

    flow_engine._call_agent = recording_call_agent
    asyncio.run(flow_engine.execute("evaluate_spec", {"path": "/x.md"}, trace_id="t-def"))
    assert captured == [None, None]


def test_execute_flow_missing_agent(flow_db, flow_engine):
    """Flow references an agent that isn't registered."""
    # Register a dummy agent so we can declare a flow (FK constraint)
    flow_db.register_agent("dummy", "dummy", "http://x", 1, "0.1.0")

    with flow_db.conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flows (name, declared_by, description, requires, steps) VALUES (?, ?, ?, ?, ?)",
            ("orphan_flow", "dummy", "", "[]", json.dumps([
                {"call": "ghost.do_thing", "args": {}},
            ])),
        )

    result = asyncio.run(flow_engine.execute("orphan_flow", {}))
    assert result["status"] == "partial"
    assert "no healthy agent" in result["error"]
