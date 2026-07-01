"""Tests for ``fr_khonliang-bus_37498850`` — bus_welcome super-skill.

Covers:
- ``BusServer.get_bus_welcome`` shape: platform / agents / suggested_next.
- State derivation from the three sources (registrations + installed_agents
  + welcomes), with the "single source of truth" invariant
  (per-agent state matches ``get_services``).
- detail='brief' vs 'full' shape differences.
- The cold-start use case: caller sees cataloged_dead + start hint.
- REST endpoint ``GET /v1/welcome``.
"""

from __future__ import annotations

import pytest

from bus.server import BusServer


# ---------------------------------------------------------------------------
# Method-level tests
# ---------------------------------------------------------------------------


def test_bus_welcome_shape_includes_platform(bus):
    w = bus.get_bus_welcome()
    assert w["platform"]["name"] == "khonliang"
    # v2 added the top-level ``bus`` self-entry (fr_khonliang-bus_6638f4dc).
    assert w["platform"]["schema_version"] == 2
    # uptime is non-negative int.
    assert isinstance(w["platform"]["bus_uptime_s"], int)
    assert w["platform"]["bus_uptime_s"] >= 0


def test_bus_welcome_empty_fleet_returns_empty_agents(bus):
    """fr_khonliang-bus_37498850 acceptance #2: works correctly when zero
    agents are live. Returns the platform shell + empty agents list,
    not 500 or empty body.
    """
    w = bus.get_bus_welcome()
    assert w["agents"] == []
    assert isinstance(w["suggested_next"], list)


def test_bus_welcome_rejects_bad_detail(bus):
    """detail must be one of brief/full."""
    err = bus.get_bus_welcome(detail="verbose")
    assert "error" in err


def test_bus_welcome_state_matches_get_services(bus, db):
    """Acceptance #3: per-agent state is the single source of truth —
    bus_welcome.agents[].state matches bus_services' status field.
    """
    db.register_agent("alive", "test", "ws://connected", 100, "0.1.0")
    welcome = bus.get_bus_welcome()
    services = bus.get_services()
    alive_w = next(a for a in welcome["agents"] if a["agent_id"] == "alive")
    alive_s = next(s for s in services if s["id"] == "alive")
    assert alive_w["state"] == alive_s["status"]


def test_bus_welcome_includes_cataloged_dead_agents(bus, db):
    """Installed-but-not-running agents appear with state=cataloged_dead
    so cold-start callers know to start them.
    """
    db.install_agent("dead-cool", "test", "/usr/bin/python3", ["-m", "x"], "/opt/x", "/c.yaml")
    w = bus.get_bus_welcome()
    dead = next(a for a in w["agents"] if a["agent_id"] == "dead-cool")
    assert dead["state"] == "cataloged_dead"
    assert dead["pid"] is None


def test_bus_welcome_uses_cached_welcome_role(bus, db):
    """When a welcome blob exists in the catalog, its ``role`` surfaces
    in the per-agent entry — even if the agent is dead (key value of
    persistent welcome: cold-start callers see what each agent is FOR).
    """
    db.install_agent("doc-agent", "researcher", "/usr/bin/python3", ["-m", "x"], "/opt/x", "/c.yaml")
    db.set_agent_welcome("doc-agent", {
        "agent_id": "doc-agent",
        "agent_type": "researcher",
        "role": "documentation guru",
        "skill_count": 7,
    })
    w = bus.get_bus_welcome()
    entry = next(a for a in w["agents"] if a["agent_id"] == "doc-agent")
    assert entry["role"] == "documentation guru"
    assert entry["state"] == "cataloged_dead"
    assert entry["welcome_cached_at"]  # timestamp present


def test_bus_welcome_brief_omits_mission_full_includes(bus, db):
    """Brief mode keeps the payload small (role + state + skill_count);
    full mode adds mission + skill lists + editorial fields.
    """
    db.register_agent("rich", "test", "ws://connected", 1, "0.1.0",
                      skills=[{"name": "a"}, {"name": "b"}])
    db.set_agent_welcome("rich", {
        "agent_id": "rich",
        "agent_type": "test",
        "role": "rich-content fixture",
        "mission": "exercise the detail levels",
        "skill_count": 2,
        "skills_by_category": {"misc": ["a", "b"]},
        "boundaries": {"not_responsible_for": ["nothing"]},
    })

    brief = bus.get_bus_welcome(detail="brief")
    brief_entry = next(a for a in brief["agents"] if a["agent_id"] == "rich")
    assert "mission" not in brief_entry
    assert "boundaries" not in brief_entry

    full = bus.get_bus_welcome(detail="full")
    full_entry = next(a for a in full["agents"] if a["agent_id"] == "rich")
    assert full_entry["mission"] == "exercise the detail levels"
    assert full_entry["boundaries"] == {"not_responsible_for": ["nothing"]}


def test_bus_welcome_suggests_start_for_dead_agents(bus, db):
    """fr_khonliang-bus_37498850 cold-start use case: ``suggested_next``
    tells the caller to ``bus_start_agent(...)`` when there are dead
    agents whose skills it might want.
    """
    db.install_agent("dead-one", "x", "/u/x", ["-m", "x"], "/o", "/c")
    db.install_agent("dead-two", "x", "/u/y", ["-m", "y"], "/o", "/c")
    w = bus.get_bus_welcome()
    hints = "\n".join(w["suggested_next"])
    assert "bus_start_agent" in hints
    assert "dead-one" in hints and "dead-two" in hints


def test_bus_welcome_no_start_hint_when_all_live(bus, db):
    """No dead agents → no bus_start_agent hint cluttering the output."""
    db.register_agent("alive", "test", "ws://connected", 1, "0.1.0")
    w = bus.get_bus_welcome()
    hints = "\n".join(w["suggested_next"])
    assert "bus_start_agent" not in hints


def test_bus_welcome_handles_autostart_failed_state(bus, db):
    """fr_khonliang-bus_fc904c3e (autostart) marks failed agents in
    ``_autostart_failures``; bus_welcome must surface that state so a
    cold-start caller knows the agent isn't usable + sees the error.
    """
    db.install_agent("flaky", "test", "/u/x", ["-m", "x"], "/o", "/c")
    bus._autostart_failures["flaky"] = "boom: bad config"
    w = bus.get_bus_welcome(detail="full")
    entry = next(a for a in w["agents"] if a["agent_id"] == "flaky")
    assert entry["state"] == "autostart_failed"
    assert entry["autostart_error"] == "boom: bad config"


def test_bus_welcome_includes_welcome_only_agent_as_deregistered(bus, db):
    """Edge case: an agent registered, published a welcome, then
    deregistered without an install row (ad-hoc registration per
    feedback_adhoc_agents_are_first_class). Still surfaces in the list
    so the operator can clean up.
    """
    db.set_agent_welcome("ghost", {"agent_id": "ghost", "role": "lingering shade"})
    w = bus.get_bus_welcome()
    ghost = next(a for a in w["agents"] if a["agent_id"] == "ghost")
    assert ghost["state"] == "deregistered"
    assert ghost["role"] == "lingering shade"


def test_bus_welcome_sorts_agents_deterministically(bus, db):
    """Output ordering must be stable across calls — sorted by agent_id
    so consumers can diff responses safely.
    """
    for aid in ("zeta", "alpha", "mu"):
        db.install_agent(aid, "x", "/u/x", ["-m"], "/o", "/c")
    w = bus.get_bus_welcome()
    assert [a["agent_id"] for a in w["agents"]] == ["alpha", "mu", "zeta"]


# ---------------------------------------------------------------------------
# REST surface
# ---------------------------------------------------------------------------


def test_rest_welcome_endpoint_returns_dict(client):
    r = client.get("/v1/welcome")
    assert r.status_code == 200
    body = r.json()
    assert body["platform"]["name"] == "khonliang"
    assert isinstance(body["agents"], list)
    assert isinstance(body["suggested_next"], list)


def test_rest_welcome_endpoint_honors_detail_query(client):
    """detail=full returns richer per-agent entries when populated."""
    client.post("/v1/install", json={
        "agent_type": "test", "id": "test-agent",
        "command": "/u/x", "args": ["-m"], "cwd": "/o", "config": "/c",
    })
    client.post("/v1/agents/test-agent/welcome", json={
        "agent_id": "test-agent",
        "role": "test fixture",
        "mission": "verify full mode",
        "skill_count": 0,
    })
    body = client.get("/v1/welcome?detail=full").json()
    entry = next(a for a in body["agents"] if a["agent_id"] == "test-agent")
    assert entry["mission"] == "verify full mode"


def test_rest_welcome_endpoint_rejects_bad_detail(client):
    r = client.get("/v1/welcome?detail=garbage")
    assert r.status_code == 400
    assert "detail" in r.json()["detail"]
