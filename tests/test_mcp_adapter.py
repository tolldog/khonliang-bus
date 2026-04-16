"""Tests for the bus-MCP adapter: tool generation, routing, bus tools."""

from __future__ import annotations

import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from bus.mcp_adapter import BusMCPAdapter
from bus.server import create_app


@pytest.fixture
def bus_client(tmp_path):
    """A running bus with test agents registered, accessed via TestClient."""
    app = create_app(db_path=str(tmp_path / "test-bus.db"))
    client = TestClient(app)

    # Register two agents with skills
    client.post("/v1/register", json={
        "id": "researcher-primary",
        "callback": "http://localhost:9001",
        "pid": 1,
        "version": "0.6.4",
        "skills": [
            {"name": "find_papers", "description": "Search for papers"},
            {"name": "synergize", "description": "Classify concepts"},
        ],
    })
    client.post("/v1/register", json={
        "id": "developer-primary",
        "callback": "http://localhost:9002",
        "pid": 2,
        "version": "0.1.0",
        "skills": [
            {"name": "read_spec", "description": "Parse a spec file"},
        ],
        "collaborations": [{
            "name": "evaluate_spec",
            "description": "Evaluate spec against corpus",
            "requires": {"researcher": ">=0.5.0"},
            "steps": [
                {"call": "developer.read_spec"},
                {"call": "researcher.find_papers"},
            ],
        }],
    })

    return client


@pytest.fixture
def adapter(bus_client, tmp_path):
    """An adapter pointing at the test bus."""
    # The adapter uses httpx.Client directly against the bus URL.
    # For testing, we monkey-patch it to use the TestClient instead.
    a = BusMCPAdapter("http://testserver")

    # Replace the sync HTTP client with one that routes through TestClient
    a._http = bus_client

    return a


def test_build_registers_bus_tools(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}

    # Bus management tools
    for bus_tool in (
        "bus_services", "bus_status", "bus_matrix", "bus_flows",
        "bus_trace", "bus_skills", "bus_artifact_list",
        "bus_artifact_metadata", "bus_artifact_head", "bus_artifact_tail",
        "bus_artifact_get", "bus_artifact_grep", "bus_artifact_excerpt",
        "bus_artifact_distill", "bus_artifact_distill_many",
        "bus_start_agent", "bus_stop_agent", "bus_restart_agent",
    ):
        assert bus_tool in tool_names, f"missing bus tool: {bus_tool}"


def test_build_registers_skill_tools(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}

    # Agent skill tools (namespaced by agent_id)
    assert "researcher-primary.find_papers" in tool_names
    assert "researcher-primary.synergize" in tool_names
    assert "developer-primary.read_spec" in tool_names


def test_build_registers_flow_tools(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}

    # Collaborative flow tool
    assert "evaluate_spec" in tool_names


def test_total_tool_count(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    # 20 bus tools (11 existing + 9 artifact tools) + 3 skills + 1 flow = 24
    assert len(tools) == 24


def test_bus_services_tool(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_services", {}))
    text = _extract_text(result)
    assert "researcher-primary" in text
    assert "developer-primary" in text


def test_bus_status_tool(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_status", {}))
    text = _extract_text(result)
    assert "agents: 2" in text
    assert "skills: 3" in text


def test_bus_matrix_tool(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_matrix", {}))
    text = _extract_text(result)
    assert "researcher-primary" in text
    assert "developer-primary" in text
    assert "evaluate_spec" in text


def test_bus_flows_tool(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_flows", {}))
    text = _extract_text(result)
    assert "evaluate_spec" in text
    assert "AVAILABLE" in text


def test_bus_skills_tool(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_skills", {}))
    text = _extract_text(result)
    assert "find_papers" in text
    assert "read_spec" in text


def test_bus_artifact_tail_tool(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    created = bus_client.post("/v1/artifacts", json={
        "kind": "pytest_log",
        "title": "pytest",
        "content": "\n".join(f"line {i}" for i in range(20)),
        "producer": "test",
    }).json()

    mcp = a.build()
    result = asyncio.run(mcp.call_tool(
        "bus_artifact_tail",
        {"id": created["id"], "lines": 2, "max_chars": 100},
    ))
    text = _extract_text(result)
    assert "line 18" in text
    assert "line 19" in text
    assert "pytest_log" in text


def test_skill_proxy_returns_response_envelope(adapter):
    async def mock_request(agent_id, operation, args):
        assert agent_id == "researcher-primary"
        assert operation == "find_papers"
        assert args == {"query": "context budgets"}
        return {"result": "Found 3 papers\nUse artifact-backed responses."}

    adapter._async_request = mock_request
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool(
        "researcher-primary.find_papers",
        {"args": json.dumps({"query": "context budgets"})},
    ))
    payload = json.loads(_extract_text(result))
    assert payload["ok"] is True
    assert payload["status"] == "ok"
    assert payload["summary"].startswith("researcher-primary.find_papers:")
    assert payload["content"] == "Found 3 papers\nUse artifact-backed responses."
    assert payload["artifact_ids"] == []
    assert payload["omitted"] is False


def test_skill_proxy_stores_large_result_as_artifact(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    large = "\n".join(f"FAILED test_{i}: AssertionError details" for i in range(1000))

    async def mock_request(agent_id, operation, args):
        assert "_response_budget_chars" not in args
        return {"result": large}

    async def mock_post(path, body, http_timeout=None):
        assert path == "/v1/artifacts"
        response = bus_client.post(path, json=body)
        assert response.status_code == 200
        return response.json()

    a._async_request = mock_request
    a._async_post = mock_post
    mcp = a.build()
    result = asyncio.run(mcp.call_tool(
        "developer-primary.read_spec",
        {"args": json.dumps({"_response_budget_chars": 1200})},
    ))
    text = _extract_text(result)
    payload = json.loads(text)
    assert len(text) < 4000
    assert payload["omitted"] is True
    assert payload["truncated"] is True
    assert payload["metrics"]["raw_chars"] == len(large)
    assert payload["metrics"]["inline_budget_chars"] == 1200
    assert payload["artifact_ids"]
    assert "FAILED test_999" not in text

    artifact = bus_client.get(f"/v1/artifacts/{payload['artifact_ids'][0]}").json()
    assert artifact["kind"] == "tool_result"
    assert artifact["producer"] == "developer-primary"
    assert artifact["size_bytes"] == len(large.encode("utf-8"))


def test_flow_proxy_stores_large_result_as_artifact(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    large = {"stdout": "\n".join(f"+ changed line {i}" for i in range(800))}

    async def mock_post(path, body, http_timeout=None):
        if path == "/v1/flow":
            assert body["args"] == {"path": "/tmp/spec.md"}
            return {"result": large}
        if path == "/v1/artifacts":
            response = bus_client.post(path, json=body)
            assert response.status_code == 200
            return response.json()
        raise AssertionError(path)

    a._async_post = mock_post
    mcp = a.build()
    result = asyncio.run(mcp.call_tool(
        "evaluate_spec",
        {"args": json.dumps({"path": "/tmp/spec.md", "_response_budget_chars": 1000})},
    ))
    text = _extract_text(result)
    payload = json.loads(text)
    assert len(text) < 4000
    assert payload["summary"].startswith("flow.evaluate_spec:")
    assert payload["artifact_ids"]
    assert payload["refs"][0]["kind"] == "tool_result"


def test_bus_trace_tool_empty(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_trace", {"trace_id": "nonexistent"}))
    text = _extract_text(result)
    assert "no trace" in text


def test_bus_wait_for_event_tool_registered(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    assert "bus_wait_for_event" in {t.name for t in tools}


def test_bus_wait_for_event_tool_formats_event(adapter):
    """When the bus returns a matched event, the tool formats it for Claude."""
    async def mock_post(path, body, http_timeout=None):
        assert path == "/v1/wait"
        return {
            "status": "matched",
            "subscriber_id": "claude-mcp",
            "event": {
                "id": 42,
                "topic": "pr.review",
                "source": "github-webhook",
                "created_at": "2026-04-12T10:00:00",
                "payload": {"pr": 123, "action": "opened"},
            },
        }
    adapter._async_post = mock_post

    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool(
        "bus_wait_for_event",
        {"topics": "pr.review,pr.merge", "timeout": 1.0},
    ))
    text = _extract_text(result)
    assert "pr.review" in text
    assert "github-webhook" in text
    assert "id=42" in text


def test_bus_wait_for_event_tool_formats_timeout(adapter):
    async def mock_post(path, body, http_timeout=None):
        return {"status": "timeout", "event": None, "subscriber_id": "claude-mcp"}
    adapter._async_post = mock_post

    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool(
        "bus_wait_for_event",
        {"topics": "nothing", "timeout": 0.5},
    ))
    text = _extract_text(result)
    assert "timeout" in text.lower()


# -- lifecycle tools --
#
# These mock _async_post so the tests stay focused on tool-layer behavior
# (formatting, error handling) rather than re-covering the HTTP plumbing
# which tests/test_lifecycle.py already verifies.

def _stub_async_post(adapter, response: dict) -> list[tuple[str, dict]]:
    """Replace adapter._async_post with a stub; returns a call-log list."""
    calls: list[tuple[str, dict]] = []

    async def _stub(path: str, body: dict, http_timeout: float | None = None) -> dict:
        calls.append((path, body))
        return response

    adapter._async_post = _stub
    return calls


def test_bus_start_agent_success(adapter):
    mcp = adapter.build()
    calls = _stub_async_post(
        adapter, {"id": "dev-researcher", "pid": 4242, "status": "started"}
    )
    result = asyncio.run(mcp.call_tool("bus_start_agent", {"agent_id": "dev-researcher"}))
    text = _extract_text(result)
    assert text.strip() == "dev-researcher: start started pid=4242"
    assert calls == [("/v1/install/dev-researcher/start", {})]


def test_bus_start_agent_already_running(adapter):
    mcp = adapter.build()
    _stub_async_post(adapter, {"id": "dev-researcher", "status": "already_running"})
    result = asyncio.run(mcp.call_tool("bus_start_agent", {"agent_id": "dev-researcher"}))
    text = _extract_text(result)
    assert text.strip() == "dev-researcher: start already_running"


def test_bus_start_agent_not_installed(adapter):
    mcp = adapter.build()
    _stub_async_post(adapter, {"id": "ghost", "error": "not installed"})
    result = asyncio.run(mcp.call_tool("bus_start_agent", {"agent_id": "ghost"}))
    text = _extract_text(result)
    assert text.strip() == "ghost: start not installed"


def test_bus_start_agent_bus_reported_spawn_error(adapter):
    """Bus-side spawn failure: response has id + error (not a transport error)."""
    mcp = adapter.build()
    _stub_async_post(adapter, {"id": "dev-researcher", "error": "command not found"})
    result = asyncio.run(mcp.call_tool("bus_start_agent", {"agent_id": "dev-researcher"}))
    text = _extract_text(result)
    assert text.strip() == "dev-researcher: start error: command not found"


def test_bus_stop_agent(adapter):
    mcp = adapter.build()
    calls = _stub_async_post(adapter, {"id": "dev-researcher", "status": "stopped"})
    result = asyncio.run(mcp.call_tool("bus_stop_agent", {"agent_id": "dev-researcher"}))
    text = _extract_text(result)
    assert text.strip() == "dev-researcher: stop stopped"
    assert calls == [("/v1/install/dev-researcher/stop", {})]


def test_bus_restart_agent(adapter):
    mcp = adapter.build()
    calls = _stub_async_post(
        adapter, {"id": "dev-researcher", "pid": 9999, "status": "started"}
    )
    result = asyncio.run(mcp.call_tool("bus_restart_agent", {"agent_id": "dev-researcher"}))
    text = _extract_text(result)
    assert text.strip() == "dev-researcher: restart started pid=9999"
    assert calls == [("/v1/install/dev-researcher/restart", {})]


def test_bus_lifecycle_transport_error(adapter):
    mcp = adapter.build()
    _stub_async_post(adapter, {"error": "connection refused"})
    result = asyncio.run(mcp.call_tool("bus_start_agent", {"agent_id": "x"}))
    text = _extract_text(result)
    assert text.strip() == "error[start]: connection refused"


# -- dynamic skill refresh --
#
# When an agent reloads with new or different skills, the adapter's
# per-skill tool registry must reconcile automatically. Previously these
# were registered once at build() time, so new skills stayed invisible to
# MCP clients until the bus restarted. refresh_skills() closes that gap.


def test_build_registers_bus_refresh_skills_tool(adapter):
    mcp = adapter.build()
    tools = asyncio.run(mcp.list_tools())
    names = {t.name for t in tools}
    assert "bus_refresh_skills" in names


def test_refresh_skills_adds_new_skill_after_agent_reload(bus_client):
    """An agent registering a new skill after build() becomes callable post-refresh."""
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    a.build()
    # Baseline skill count
    baseline = asyncio.run(a.mcp.list_tools())
    assert "researcher-primary.new_skill" not in {t.name for t in baseline}

    # Agent re-registers with an added skill (simulates an in-place reload)
    bus_client.post("/v1/register", json={
        "id": "researcher-primary",
        "callback": "http://localhost:9001",
        "pid": 1,
        "version": "0.7.0",
        "skills": [
            {"name": "find_papers", "description": "Search for papers"},
            {"name": "synergize", "description": "Classify concepts"},
            {"name": "new_skill", "description": "Added during this test"},
        ],
    })

    diff = a.refresh_skills()
    assert "researcher-primary.new_skill" in diff["added"]
    after = asyncio.run(a.mcp.list_tools())
    assert "researcher-primary.new_skill" in {t.name for t in after}


def test_refresh_skills_removes_tool_when_agent_deregisters(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    a.build()
    assert "developer-primary.read_spec" in {
        t.name for t in asyncio.run(a.mcp.list_tools())
    }

    # Deregister the agent — bus drops its skills from /v1/services
    bus_client.post("/v1/deregister", json={"id": "developer-primary"})

    diff = a.refresh_skills()
    assert "developer-primary.read_spec" in diff["removed"]
    after = {t.name for t in asyncio.run(a.mcp.list_tools())}
    assert "developer-primary.read_spec" not in after


def test_refresh_skills_idempotent_with_no_changes(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    a.build()
    diff = a.refresh_skills()
    # Initial build already registered the live skills; no-op on second pass.
    assert diff == {"added": [], "removed": []}


def test_refresh_skills_preserves_bus_and_flow_tools(bus_client):
    """Reconciliation only touches skill tools, not bus_* or flow tools."""
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    a.build()
    bus_client.post("/v1/deregister", json={"id": "researcher-primary"})
    bus_client.post("/v1/deregister", json={"id": "developer-primary"})

    diff = a.refresh_skills()
    # All per-skill tools should disappear; bus_* and flow tool remain registered.
    names = {t.name for t in asyncio.run(a.mcp.list_tools())}
    assert "bus_services" in names
    assert "bus_start_agent" in names
    assert "bus_refresh_skills" in names
    assert "researcher-primary.find_papers" not in names
    assert "developer-primary.read_spec" not in names
    # Removals should list each deregistered skill
    assert "researcher-primary.find_papers" in diff["removed"]
    assert "developer-primary.read_spec" in diff["removed"]


def test_bus_refresh_skills_tool_reports_no_changes(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_refresh_skills", {}))
    text = _extract_text(result)
    assert text.strip() == "no changes"


def test_bus_refresh_skills_tool_reports_added(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    mcp = a.build()
    bus_client.post("/v1/register", json={
        "id": "researcher-primary",
        "callback": "http://localhost:9001",
        "pid": 1,
        "version": "0.7.0",
        "skills": [
            {"name": "find_papers", "description": "Search for papers"},
            {"name": "synergize", "description": "Classify concepts"},
            {"name": "brand_new", "description": "Freshly added"},
        ],
    })
    result = asyncio.run(mcp.call_tool("bus_refresh_skills", {}))
    text = _extract_text(result)
    assert "brand_new" in text
    assert "+1" in text


class _StubSession:
    """Tracks send_tool_list_changed calls for notification tests."""
    def __init__(self):
        self.notify_calls = 0

    async def send_tool_list_changed(self):
        self.notify_calls += 1


@pytest.mark.asyncio
async def test_notify_list_changed_fires_when_ctx_provided(adapter):
    adapter.build()
    session = _StubSession()

    class _Ctx:
        def __init__(self, sess):
            self.session = sess

    ctx = _Ctx(session)
    await adapter._notify_list_changed(ctx)
    assert session.notify_calls == 1


@pytest.mark.asyncio
async def test_notify_list_changed_tolerates_missing_method(adapter):
    """Older MCP library versions may lack send_tool_list_changed — never crash."""
    adapter.build()

    class _OldSession:
        pass

    class _Ctx:
        def __init__(self):
            self.session = _OldSession()

    # Should not raise
    await adapter._notify_list_changed(_Ctx())


def _extract_text(result) -> str:
    if isinstance(result, tuple) and len(result) == 2:
        meta = result[1]
        if isinstance(meta, dict) and "result" in meta:
            return str(meta["result"])
    if isinstance(result, list) and result:
        first = result[0]
        if hasattr(first, "text"):
            return first.text
    return str(result)
