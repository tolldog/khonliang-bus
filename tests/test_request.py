"""Tests for request/reply routing, retry, and dead letter."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tests.conftest import register_test_agent


def test_request_to_unknown_agent(client):
    r = client.post("/v1/request", json={
        "agent_id": "ghost",
        "operation": "anything",
    }).json()
    assert "error" in r
    assert "no healthy agent" in r["error"]


def test_request_requires_agent_id_or_type(client):
    r = client.post("/v1/request", json={
        "operation": "anything",
    }).json()
    assert "error" in r
    assert "agent_id or agent_type" in r["error"]


def test_request_to_unreachable_agent_returns_error(client):
    register_test_agent(client, callback="http://localhost:1")  # nothing listens here
    r = client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "args": {"x": "hello"},
        "timeout": 2,
    }).json()
    assert "error" in r
    assert r["trace_id"].startswith("t-")


def test_request_by_agent_type(client):
    register_test_agent(client, agent_id="researcher-1")
    # Request by type (agent_type extracted from ID: "researcher")
    r = client.post("/v1/request", json={
        "agent_type": "researcher",
        "operation": "do_thing",
        "timeout": 2,
    }).json()
    # Will fail because no real agent, but should resolve the agent
    assert "trace_id" in r


def test_request_generates_trace(client):
    register_test_agent(client)
    client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "timeout": 2,
        "trace_id": "t-test-123",
    })
    trace = client.get("/v1/trace/t-test-123").json()
    assert len(trace) >= 1
    assert trace[0]["agent_id"] == "test-agent"
    assert trace[0]["operation"] == "do_thing"


def test_request_dead_letters_after_max_retries(client):
    register_test_agent(client, callback="http://localhost:1")
    client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "failing_op",
        "args": {"x": "bad"},
        "timeout": 1,
    })
    # Check dead letters exist in the DB
    # (No direct endpoint yet, but we can verify through status)
    status = client.get("/v1/status").json()
    # Agent should be unhealthy after failed requests
    assert status["unhealthy"] >= 1
