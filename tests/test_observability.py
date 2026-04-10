"""Tests for observability: health, status, traces."""

from __future__ import annotations

from tests.conftest import install_test_agent, register_test_agent


def test_health(client):
    r = client.get("/v1/health").json()
    assert r["status"] == "ok"
    assert r["version"] == "0.2.0"


def test_status_empty(client):
    r = client.get("/v1/status").json()
    assert r["installed_agents"] == 0
    assert r["registered_agents"] == 0
    assert r["total_skills"] == 0


def test_status_after_install_and_register(client):
    install_test_agent(client, "a1")
    register_test_agent(client, "a1", skills=[
        {"name": "s1"},
        {"name": "s2"},
        {"name": "s3"},
    ])
    r = client.get("/v1/status").json()
    assert r["installed_agents"] == 1
    assert r["registered_agents"] == 1
    assert r["healthy"] == 1
    assert r["total_skills"] == 3


def test_trace_empty(client):
    r = client.get("/v1/trace/nonexistent").json()
    assert r == []


def test_trace_populated_by_request(client):
    register_test_agent(client)
    client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "timeout": 2,
        "trace_id": "t-trace-test",
    })
    trace = client.get("/v1/trace/t-trace-test").json()
    assert len(trace) >= 1
    assert trace[0]["trace_id"] == "t-trace-test"
    assert trace[0]["operation"] == "do_thing"
    assert trace[0]["status"] is not None  # ok or failed
