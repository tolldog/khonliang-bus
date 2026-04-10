"""Semver-style version comparison for skill/collaboration gates.

Supports basic version requirements like ``>=0.5.0``, ``>=1.0``, ``==0.6.4``.
Not a full semver implementation — just enough for agent version gates.
"""

from __future__ import annotations

import re

_VERSION_RE = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?$")
_GATE_RE = re.compile(r"^(>=|<=|==|!=|>|<)\s*(.+)$")


def parse_version(v: str) -> tuple[int, ...]:
    """Parse a version string into a comparable tuple.

    ``"0.6.4"`` → ``(0, 6, 4)``
    ``"1.0"``   → ``(1, 0, 0)``
    ``"2"``     → ``(2, 0, 0)``
    """
    m = _VERSION_RE.match(v.strip())
    if not m:
        return (0, 0, 0)
    return tuple(int(g or 0) for g in m.groups())


def check_version_gate(actual: str, gate: str) -> bool:
    """Check if ``actual`` version satisfies the ``gate`` requirement.

    ``check_version_gate("0.6.4", ">=0.5.0")`` → True
    ``check_version_gate("0.4.2", ">=0.5.0")`` → False
    ``check_version_gate("1.0.0", "==1.0.0")`` → True
    """
    m = _GATE_RE.match(gate.strip())
    if not m:
        # No operator → treat as exact match
        return parse_version(actual) == parse_version(gate)

    op, required = m.group(1), m.group(2)
    a = parse_version(actual)
    r = parse_version(required)

    if op == ">=":
        return a >= r
    if op == "<=":
        return a <= r
    if op == "==":
        return a == r
    if op == "!=":
        return a != r
    if op == ">":
        return a > r
    if op == "<":
        return a < r
    return False


def validate_collaboration_requirements(
    requires: dict[str, str],
    registered_agents: list[dict],
) -> tuple[bool, list[str]]:
    """Check if all version requirements in a collaboration are met.

    Args:
        requires: mapping of agent_type → version gate (e.g. ``{"researcher": ">=0.5.0"}``)
        registered_agents: list of registration dicts with ``agent_type`` and ``version`` fields

    Returns:
        (all_met, list of diagnostic strings for unmet requirements)
    """
    # Build type → best version mapping
    type_versions: dict[str, str] = {}
    for agent in registered_agents:
        agent_type = agent.get("agent_type", "")
        version = agent.get("version", "")
        if agent.get("status") != "healthy":
            continue
        if agent_type not in type_versions or parse_version(version) > parse_version(type_versions[agent_type]):
            type_versions[agent_type] = version

    unmet: list[str] = []
    for agent_type, gate in requires.items():
        if agent_type not in type_versions:
            unmet.append(f"{agent_type}: not registered")
        elif not check_version_gate(type_versions[agent_type], gate):
            unmet.append(f"{agent_type}: {type_versions[agent_type]} does not meet {gate}")

    return len(unmet) == 0, unmet
