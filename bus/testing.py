"""Testing utilities for bus agents.

Provides a mock bus and agent test harness so agent authors can test
handlers, skills, and registration without running the real bus server.

Usage::

    from bus.testing import AgentTestHarness

    class TestMyAgent:
        def setup_method(self):
            self.harness = AgentTestHarness(MyAgent)

        async def test_find_papers(self):
            result = await self.harness.call("find_papers", {"query": "consensus"})
            assert "papers" in result

        def test_skills_registered(self):
            assert "find_papers" in self.harness.skill_names

        def test_collaboration_declared(self):
            assert "evaluate_spec" in self.harness.collaboration_names
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from bus.agent import BaseAgent, Skill, Collaboration


@dataclass
class MockRegistration:
    """Captured registration payload from an agent."""

    agent_id: str
    version: str
    skills: list[dict[str, Any]]
    collaborations: list[dict[str, Any]]
    callback: str = ""
    pid: int = 0


@dataclass
class MockMessage:
    """A message published by an agent during testing."""

    topic: str
    payload: Any
    source: str


class AgentTestHarness:
    """Test harness for bus agents. Exercises handlers without a real bus.

    Creates the agent, collects its skills and collaborations, and
    provides a :meth:`call` method that dispatches directly to the
    agent's ``@handler`` methods — no HTTP, no bus, no FastAPI.

    Args:
        agent_cls: The BaseAgent subclass to test.
        agent_id: Override agent ID (default: "{agent_type}-test").
        config_path: Optional config path to pass to the agent.
        **kwargs: Extra kwargs passed to the agent constructor.
    """

    def __init__(
        self,
        agent_cls: type[BaseAgent],
        agent_id: str = "",
        config_path: str = "",
        **kwargs: Any,
    ):
        self.agent_cls = agent_cls
        agent_id = agent_id or f"{agent_cls.agent_type}-test"
        self.agent = agent_cls(
            agent_id=agent_id,
            bus_url="http://mock-bus:0",
            config_path=config_path,
            **kwargs,
        )
        self._skills = self.agent.register_skills()
        self._collaborations = self.agent.register_collaborations()
        self.published: list[MockMessage] = []
        self.registration: MockRegistration | None = None

        # Capture the registration that would be sent to the bus
        self.registration = MockRegistration(
            agent_id=agent_id,
            version=getattr(self.agent, "version", "0.0.0"),
            skills=[
                {
                    "name": s.name,
                    "description": s.description,
                    "parameters": s.parameters,
                    "since": s.since,
                }
                for s in self._skills
            ],
            collaborations=[
                {
                    "name": c.name,
                    "description": c.description,
                    "requires": c.requires,
                    "steps": c.steps,
                }
                for c in self._collaborations
            ],
        )

    @property
    def skill_names(self) -> set[str]:
        """Set of registered skill names."""
        return {s.name for s in self._skills}

    @property
    def skills(self) -> list[Skill]:
        """List of registered Skill objects."""
        return list(self._skills)

    @property
    def collaboration_names(self) -> set[str]:
        """Set of registered collaboration names."""
        return {c.name for c in self._collaborations}

    @property
    def collaborations(self) -> list[Collaboration]:
        """List of registered Collaboration objects."""
        return list(self._collaborations)

    async def call(
        self,
        operation: str,
        args: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> Any:
        """Call an agent handler directly. Returns the handler's result.

        Dispatches to the ``@handler``-decorated method matching
        ``operation``. Raises ``KeyError`` if no handler exists.
        Raises whatever the handler raises (no error wrapping).
        """
        handler_fn = self.agent._handlers.get(operation)
        if handler_fn is None:
            raise KeyError(
                f"no handler for operation {operation!r}. "
                f"Available: {sorted(self.agent._handlers)}"
            )
        return await handler_fn(args or {})

    def get_skill(self, name: str) -> Skill | None:
        """Look up a skill by name."""
        for s in self._skills:
            if s.name == name:
                return s
        return None

    def get_collaboration(self, name: str) -> Collaboration | None:
        """Look up a collaboration by name."""
        for c in self._collaborations:
            if c.name == name:
                return c
        return None

    def assert_skill_exists(self, name: str, description: str | None = None) -> Skill:
        """Assert a skill is registered. Optionally check description."""
        skill = self.get_skill(name)
        assert skill is not None, (
            f"skill {name!r} not found. Registered: {sorted(self.skill_names)}"
        )
        if description is not None:
            assert description in skill.description, (
                f"skill {name!r} description mismatch: "
                f"expected {description!r} in {skill.description!r}"
            )
        return skill

    def assert_collaboration_exists(
        self,
        name: str,
        requires: dict[str, str] | None = None,
    ) -> Collaboration:
        """Assert a collaboration is declared. Optionally check requirements."""
        collab = self.get_collaboration(name)
        assert collab is not None, (
            f"collaboration {name!r} not found. "
            f"Declared: {sorted(self.collaboration_names)}"
        )
        if requires is not None:
            assert collab.requires == requires, (
                f"collaboration {name!r} requires mismatch: "
                f"expected {requires}, got {collab.requires}"
            )
        return collab
