"""Tests for BaseAgent registration — fatal on failure, nack helper."""

from __future__ import annotations

import asyncio

import pytest

from bus.agent import BaseAgent, Skill, handler


class SimpleAgent(BaseAgent):
    agent_type = "simple"
    module_name = "tests.test_agent_registration"
    version = "0.1.0"

    def register_skills(self):
        return [Skill("echo", "echo back")]

    @handler("echo")
    async def echo(self, args):
        return {"echoed": args.get("text", "")}


def test_register_fails_fatally_when_bus_unreachable():
    """Registration failure must raise RuntimeError, not silently log."""
    agent = SimpleAgent(
        agent_id="test-fatal",
        bus_url="http://localhost:1",  # nothing listens here
    )

    with pytest.raises(RuntimeError, match="failed to register"):
        # Build the app (needed for _register to work) but don't serve
        agent._build_app()
        asyncio.run(agent._register())


def test_register_error_message_includes_bus_url():
    """Error message should tell the operator which bus URL failed."""
    agent = SimpleAgent(
        agent_id="test-msg",
        bus_url="http://localhost:1",
    )

    with pytest.raises(RuntimeError, match="localhost:1"):
        agent._build_app()
        asyncio.run(agent._register())


def test_register_error_message_includes_agent_id():
    agent = SimpleAgent(
        agent_id="my-agent-42",
        bus_url="http://localhost:1",
    )

    with pytest.raises(RuntimeError, match="my-agent-42"):
        agent._build_app()
        asyncio.run(agent._register())


def test_nack_helper_exists():
    """BaseAgent should expose a nack() async method."""
    agent = SimpleAgent(agent_id="test", bus_url="http://localhost:1")
    assert hasattr(agent, "nack")
    assert asyncio.iscoroutinefunction(agent.nack)
