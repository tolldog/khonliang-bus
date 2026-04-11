"""Tests for the orchestrator — catalog building and plan execution."""

from __future__ import annotations

from bus.orchestrator import Orchestrator
from bus.db import BusDB


def test_build_catalog_empty(db):
    import httpx
    o = Orchestrator(db, httpx.AsyncClient())
    assert o._build_catalog() == ""


def test_build_catalog_with_agents(db):
    import httpx
    db.register_agent("r1", "researcher", "ws://x", 1, "0.6.4",
                       skills=[{"name": "find_papers", "description": "Search"}])
    o = Orchestrator(db, httpx.AsyncClient())
    catalog = o._build_catalog()
    assert "researcher" in catalog
    assert "find_papers" in catalog


def test_resolve_refs():
    import httpx
    o = Orchestrator(BusDB(":memory:"), httpx.AsyncClient())
    outputs = {"papers": ["p1", "p2"]}
    resolved = o._resolve_refs({"evidence": "{{papers}}", "fixed": "hello"}, outputs)
    assert resolved["evidence"] == ["p1", "p2"]
    assert resolved["fixed"] == "hello"


def test_resolve_refs_missing():
    import httpx
    o = Orchestrator(BusDB(":memory:"), httpx.AsyncClient())
    resolved = o._resolve_refs({"x": "{{missing}}"}, {})
    assert resolved["x"] == "{{missing}}"  # unresolved refs pass through


def test_orchestrate_endpoint_exists(client):
    """The /v1/orchestrate endpoint exists and accepts POST."""
    r = client.post("/v1/orchestrate", json={
        "task": "test task",
    }).json()
    # Will fail (no agents registered, or LLM unavailable) but the endpoint works
    assert "status" in r or "error" in r or "trace_id" in r
