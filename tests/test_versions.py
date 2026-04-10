"""Tests for bus.versions — semver parsing, gate checking, collaboration validation."""

from __future__ import annotations

from bus.versions import (
    check_version_gate,
    parse_version,
    validate_collaboration_requirements,
)


def test_parse_version_full():
    assert parse_version("0.6.4") == (0, 6, 4)


def test_parse_version_two_part():
    assert parse_version("1.0") == (1, 0, 0)


def test_parse_version_single():
    assert parse_version("2") == (2, 0, 0)


def test_parse_version_invalid():
    assert parse_version("abc") == (0, 0, 0)


def test_gate_gte():
    assert check_version_gate("0.6.4", ">=0.5.0") is True
    assert check_version_gate("0.5.0", ">=0.5.0") is True
    assert check_version_gate("0.4.9", ">=0.5.0") is False


def test_gate_gt():
    assert check_version_gate("0.6.0", ">0.5.0") is True
    assert check_version_gate("0.5.0", ">0.5.0") is False


def test_gate_eq():
    assert check_version_gate("1.0.0", "==1.0.0") is True
    assert check_version_gate("1.0.1", "==1.0.0") is False


def test_gate_neq():
    assert check_version_gate("1.0.1", "!=1.0.0") is True
    assert check_version_gate("1.0.0", "!=1.0.0") is False


def test_gate_lte():
    assert check_version_gate("0.5.0", "<=0.5.0") is True
    assert check_version_gate("0.6.0", "<=0.5.0") is False


def test_gate_lt():
    assert check_version_gate("0.4.9", "<0.5.0") is True
    assert check_version_gate("0.5.0", "<0.5.0") is False


def test_gate_no_operator_is_exact_match():
    assert check_version_gate("1.0.0", "1.0.0") is True
    assert check_version_gate("1.0.1", "1.0.0") is False


def test_validate_all_met():
    regs = [
        {"agent_type": "researcher", "version": "0.6.4", "status": "healthy"},
        {"agent_type": "developer", "version": "0.1.0", "status": "healthy"},
    ]
    met, unmet = validate_collaboration_requirements(
        {"researcher": ">=0.5.0", "developer": ">=0.1.0"}, regs
    )
    assert met is True
    assert unmet == []


def test_validate_version_too_low():
    regs = [
        {"agent_type": "researcher", "version": "0.4.2", "status": "healthy"},
    ]
    met, unmet = validate_collaboration_requirements(
        {"researcher": ">=0.5.0"}, regs
    )
    assert met is False
    assert "does not meet" in unmet[0]


def test_validate_agent_not_registered():
    met, unmet = validate_collaboration_requirements(
        {"researcher": ">=0.5.0"}, []
    )
    assert met is False
    assert "not registered" in unmet[0]


def test_validate_ignores_unhealthy_agents():
    regs = [
        {"agent_type": "researcher", "version": "0.6.4", "status": "dead"},
    ]
    met, unmet = validate_collaboration_requirements(
        {"researcher": ">=0.5.0"}, regs
    )
    assert met is False
    assert "not registered" in unmet[0]


def test_validate_picks_best_version():
    regs = [
        {"agent_type": "researcher", "version": "0.4.0", "status": "healthy"},
        {"agent_type": "researcher", "version": "0.7.0", "status": "healthy"},
    ]
    met, _ = validate_collaboration_requirements(
        {"researcher": ">=0.5.0"}, regs
    )
    assert met is True
