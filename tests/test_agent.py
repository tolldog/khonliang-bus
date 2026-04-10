"""Tests for BaseAgent: skill registration, handler dispatch, lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from bus.agent import BaseAgent, Collaboration, Skill, handler


# ---------------------------------------------------------------------------
# Test agent subclass
# ---------------------------------------------------------------------------


class EchoAgent(BaseAgent):
    agent_type = "echo"
    module_name = "tests.test_agent"
    version = "0.1.0"

    def register_skills(self):
        return [
            Skill("echo", "Echo the input back", {"text": {"type": "string"}}, since="0.1.0"),
            Skill("fail", "Always fails"),
        ]

    def register_collaborations(self):
        return [
            Collaboration(
                "echo_and_research",
                "Echo then research",
                requires={"researcher": ">=0.5.0"},
                steps=[{"call": "echo.echo"}, {"call": "researcher.find_papers"}],
            ),
        ]

    @handler("echo")
    async def echo(self, args):
        return {"echoed": args.get("text", "")}

    @handler("fail")
    async def fail(self, args):
        raise ValueError("intentional failure")


@pytest.fixture
def echo_agent():
    return EchoAgent(agent_id="echo-test", bus_url="http://localhost:9999")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_skills_registered(echo_agent):
    skills = echo_agent.register_skills()
    assert len(skills) == 2
    assert skills[0].name == "echo"
    assert skills[0].since == "0.1.0"


def test_collaborations_registered(echo_agent):
    collabs = echo_agent.register_collaborations()
    assert len(collabs) == 1
    assert collabs[0].name == "echo_and_research"
    assert collabs[0].requires == {"researcher": ">=0.5.0"}


def test_handlers_collected(echo_agent):
    assert "echo" in echo_agent._handlers
    assert "fail" in echo_agent._handlers


def test_handler_dispatch_success(echo_agent):
    app = echo_agent._build_app()
    from fastapi.testclient import TestClient
    client = TestClient(app)

    r = client.post("/v1/handle", json={
        "operation": "echo",
        "args": {"text": "hello"},
        "correlation_id": "c-1",
    }).json()
    assert r["correlation_id"] == "c-1"
    assert r["result"] == {"echoed": "hello"}


def test_handler_dispatch_failure(echo_agent):
    app = echo_agent._build_app()
    from fastapi.testclient import TestClient
    client = TestClient(app)

    r = client.post("/v1/handle", json={
        "operation": "fail",
        "args": {},
        "correlation_id": "c-2",
    }).json()
    assert r["correlation_id"] == "c-2"
    assert "error" in r
    assert "intentional" in r["error"]
    assert r["retryable"] is True


def test_handler_unknown_operation(echo_agent):
    app = echo_agent._build_app()
    from fastapi.testclient import TestClient
    client = TestClient(app)

    r = client.post("/v1/handle", json={
        "operation": "nonexistent",
        "args": {},
        "correlation_id": "c-3",
    }).json()
    assert "error" in r
    assert "unknown operation" in r["error"]
    assert r["retryable"] is False


def test_health_endpoint(echo_agent):
    app = echo_agent._build_app()
    from fastapi.testclient import TestClient
    client = TestClient(app)

    r = client.get("/v1/health").json()
    assert r["status"] == "ok"
    assert r["agent_id"] == "echo-test"


def test_from_cli_parses_args():
    agent = EchoAgent.from_cli(["--id", "my-echo", "--bus", "http://localhost:8787"])
    assert agent.agent_id == "my-echo"
    assert agent.bus_url == "http://localhost:8787"


def test_agent_type_and_version(echo_agent):
    assert echo_agent.agent_type == "echo"
    assert echo_agent.version == "0.1.0"
