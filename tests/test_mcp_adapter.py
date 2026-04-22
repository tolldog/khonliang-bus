"""Tests for the bus-MCP adapter: tool generation, routing, bus tools."""

from __future__ import annotations

import asyncio
import json

import httpx
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
        "bus_trace", "bus_skills", "bus_feedback", "bus_artifact_list",
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
    # 21 bus tools (12 existing + 9 artifact tools) + 3 skills + 1 flow = 25
    assert len(tools) == 25


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


def test_bus_feedback_tool(bus_client):
    a = BusMCPAdapter("http://testserver")
    a._http = bus_client
    posted = bus_client.post("/v1/feedback", json={
        "agent_id": "developer-primary",
        "kind": "friction",
        "operation": "next_work_unit",
        "category": "format",
        "severity": "medium",
        "message": "JSON string had to be unwrapped client-side",
        "suggestion": "Return parsed envelope content",
    })
    assert posted.status_code == 200

    mcp = a.build()
    result = asyncio.run(mcp.call_tool(
        "bus_feedback",
        {"agent_id": "developer-primary", "kind": "friction"},
    ))
    payload = json.loads(_extract_text(result))
    assert len(payload) == 1
    assert payload[0]["category"] == "format"
    assert payload[0]["message"] == "JSON string had to be unwrapped client-side"


def test_skill_proxy_returns_response_envelope(adapter):
    async def mock_request(agent_id, operation, args, timeout=None):
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

    async def mock_request(agent_id, operation, args, timeout=None):
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


# -- configurable timeout (FR fr_khonliang_a3dc662d) --
#
# Root cause: the adapter hardcoded httpx timeouts at 30s, silently
# truncating any skill whose local inference exceeded that window even
# though the server-side agent often completed the work. Tests below
# cover the four acceptance behaviors: (1) the library fallback default
# moved above 30s; (2) the fallback is overridable at construction and
# via env/CLI; (3) a per-call ``_mcp_timeout`` arg wins over the default
# and is stripped before forwarding to the skill handler; (4) timeout
# errors surface as non-empty strings carrying ``timed_out=true``.


def test_default_timeout_is_above_30s(adapter):
    """The library fallback must exceed the historic 30s hardcoded cap.

    The exact value may shift with empirical evidence; what matters is
    that 14-16B local reviewers (30-35s typical) aren't truncated.
    """
    assert adapter.default_timeout_s > 30.0


def test_default_timeout_constructor_override():
    a = BusMCPAdapter("http://testserver", default_timeout_s=120.0)
    assert a.default_timeout_s == 120.0


def test_resolve_default_timeout_reads_env(monkeypatch):
    from bus.mcp_adapter import DEFAULT_MCP_TIMEOUT_S, _resolve_default_timeout

    monkeypatch.setenv("KHONLIANG_MCP_DEFAULT_TIMEOUT", "180")
    assert _resolve_default_timeout(None) == 180.0

    monkeypatch.delenv("KHONLIANG_MCP_DEFAULT_TIMEOUT", raising=False)
    assert _resolve_default_timeout(None) == DEFAULT_MCP_TIMEOUT_S

    monkeypatch.setenv("KHONLIANG_MCP_DEFAULT_TIMEOUT", "not-a-number")
    assert _resolve_default_timeout(None) == DEFAULT_MCP_TIMEOUT_S

    monkeypatch.setenv("KHONLIANG_MCP_DEFAULT_TIMEOUT", "-5")
    assert _resolve_default_timeout(None) == DEFAULT_MCP_TIMEOUT_S

    # Explicit CLI wins over env.
    monkeypatch.setenv("KHONLIANG_MCP_DEFAULT_TIMEOUT", "45")
    assert _resolve_default_timeout(90.0) == 90.0


def test_slow_skill_under_cap_returns_payload(adapter):
    """Regression: a 45s-simulated skill must return its payload under a
    120s cap. Uses mocking so we never actually sleep 45s.

    Covers FR acceptance: configuring a default cap > 30s lets long calls
    complete without truncation.
    """
    adapter.default_timeout_s = 120.0
    captured: dict = {}

    async def mock_request(agent_id, operation, args, timeout=None):
        captured["timeout"] = timeout
        # Returns a completed result; by contract the adapter treats this
        # as a non-timeout regardless of wall time.
        return {"result": "findings payload from a long-running reviewer"}

    adapter._async_request = mock_request
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool(
        "researcher-primary.find_papers",
        {"args": json.dumps({"query": "long"})},
    ))
    payload = json.loads(_extract_text(result))
    assert payload["ok"] is True
    assert payload["content"] == "findings payload from a long-running reviewer"
    # The adapter-level default should thread through (None here means
    # "use adapter default" — the skill-proxy doesn't force-pass it).
    assert captured["timeout"] is None


def test_per_call_mcp_timeout_hint_overrides_default(adapter):
    """``_mcp_timeout`` in skill args wins over the adapter default and is
    stripped before reaching the skill handler."""
    captured: dict = {}

    async def mock_request(agent_id, operation, args, timeout=None):
        captured["timeout"] = timeout
        captured["args"] = dict(args)
        return {"result": "ok"}

    adapter._async_request = mock_request
    mcp = adapter.build()
    asyncio.run(mcp.call_tool(
        "researcher-primary.find_papers",
        {"args": json.dumps({"query": "q", "_mcp_timeout": 200})},
    ))
    assert captured["timeout"] == 200.0
    assert "_mcp_timeout" not in captured["args"]
    assert captured["args"] == {"query": "q"}


def test_invalid_mcp_timeout_hint_falls_back_silently(adapter):
    """Non-numeric / non-positive hints must not crash; adapter default
    applies instead."""
    captured: dict = {}

    async def mock_request(agent_id, operation, args, timeout=None):
        captured["timeout"] = timeout
        return {"result": "ok"}

    adapter._async_request = mock_request
    mcp = adapter.build()
    for bad in ("not-a-number", -5, 0):
        asyncio.run(mcp.call_tool(
            "researcher-primary.find_papers",
            {"args": json.dumps({"query": "q", "_mcp_timeout": bad})},
        ))
        assert captured["timeout"] is None


def test_skill_timeout_returns_non_empty_error_with_marker(adapter):
    """Regression: when a call exceeds the cap, the MCP response must be a
    non-empty string including ``timed_out=true`` — not the historical
    silent ``{"error": ""}``. Also preserves trace_id when bus supplied one.
    """
    async def mock_request(agent_id, operation, args, timeout=None):
        # Emulate the shape _async_request returns on httpx.TimeoutException
        return {
            "error": f"mcp transport timeout after {(timeout or 60.0) + 5.0:.1f}s",
            "timed_out": True,
            "timeout_s": timeout or 60.0,
        }

    adapter._async_request = mock_request
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool(
        "researcher-primary.find_papers",
        {"args": json.dumps({"query": "q", "_mcp_timeout": 30})},
    ))
    text = _extract_text(result)
    # Non-empty, informative.
    assert text.strip()
    assert "error:" in text
    assert "timed_out=true" in text
    assert "timeout_s=30" in text


def test_format_error_preserves_trace_id():
    """Trace ids from the bus survive into the rendered error string so
    operators can cross-reference a usage row recorded server-side even
    when the MCP channel truncated."""
    a = BusMCPAdapter("http://testserver")
    out = a._format_error({
        "error": "mcp transport timeout after 65.0s",
        "timed_out": True,
        "timeout_s": 60.0,
        "trace_id": "trace-abc123",
    })
    assert "timed_out=true" in out
    assert "trace_id=trace-abc123" in out
    assert "error: mcp transport timeout" in out


def test_format_error_plain_bus_error_stays_simple():
    """Non-timeout bus errors don't get the timeout marker appended."""
    a = BusMCPAdapter("http://testserver")
    out = a._format_error({"error": "agent unreachable"})
    assert out == "error: agent unreachable"


def test_async_request_passes_resolved_timeout_to_bus(monkeypatch):
    """The ``timeout`` field in the /v1/request body must be the resolved
    value, not the old hardcoded 30. This is what caps the bus→agent call
    server-side; if it's too low, the bus truncates before the adapter.
    """
    a = BusMCPAdapter("http://testserver", default_timeout_s=120.0)
    captured: dict = {}

    class _Response:
        def json(self):
            return {"result": "ok"}

    async def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        captured["transport_timeout"] = timeout
        return _Response()

    monkeypatch.setattr(a._async_http, "post", fake_post)
    result = asyncio.run(a._async_request("agent", "op", {"x": 1}, timeout=200.0))
    assert result == {"result": "ok"}
    assert captured["json"]["timeout"] == 200.0
    # Transport timeout gets a small buffer above the bus-side timeout.
    assert captured["transport_timeout"].read > 200.0


def test_async_request_surfaces_timeout_with_marker(monkeypatch):
    """httpx.TimeoutException must map to a structured error dict carrying
    ``timed_out=true`` — this is the plumbing that makes
    ``test_skill_timeout_returns_non_empty_error_with_marker`` possible
    end-to-end in production."""
    a = BusMCPAdapter("http://testserver", default_timeout_s=60.0)

    async def fake_post(*args, **kwargs):
        raise httpx.ReadTimeout("simulated")

    monkeypatch.setattr(a._async_http, "post", fake_post)
    result = asyncio.run(a._async_request("agent", "op", {}, timeout=45.0))
    assert result["timed_out"] is True
    assert result["timeout_s"] == 45.0
    assert "timeout" in result["error"].lower()


def test_async_post_surfaces_timeout_with_marker(monkeypatch):
    a = BusMCPAdapter("http://testserver", default_timeout_s=60.0)

    async def fake_post(*args, **kwargs):
        raise httpx.ReadTimeout("simulated")

    monkeypatch.setattr(a._async_http, "post", fake_post)
    result = asyncio.run(a._async_post("/v1/flow", {}, http_timeout=30.0))
    assert result["timed_out"] is True
    assert result["timeout_s"] == 30.0
