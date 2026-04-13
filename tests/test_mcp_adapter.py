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
        "bus_trace", "bus_skills",
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
    # 9 bus tools (6 read + 3 lifecycle) + 3 skills (2 researcher + 1 developer) + 1 flow = 13
    assert len(tools) == 13


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


def test_bus_trace_tool_empty(adapter):
    mcp = adapter.build()
    result = asyncio.run(mcp.call_tool("bus_trace", {"trace_id": "nonexistent"}))
    text = _extract_text(result)
    assert "no trace" in text


# -- lifecycle tools --
#
# These mock _async_post so the tests stay focused on tool-layer behavior
# (formatting, error handling) rather than re-covering the HTTP plumbing
# which tests/test_lifecycle.py already verifies.

def _stub_async_post(adapter, response: dict) -> list[tuple[str, dict]]:
    """Replace adapter._async_post with a stub; returns a call-log list."""
    calls: list[tuple[str, dict]] = []

    async def _stub(path: str, body: dict) -> dict:
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
