"""Tests for Step 2: skill registry, validated flows, interaction matrix."""

from __future__ import annotations

from tests.conftest import register_test_agent


def test_list_all_skills(client):
    register_test_agent(client, "a1", skills=[{"name": "s1"}, {"name": "s2"}])
    register_test_agent(client, "a2", skills=[{"name": "s3"}])
    r = client.get("/v1/skills").json()
    assert len(r) == 3
    names = {s["name"] for s in r}
    assert names == {"s1", "s2", "s3"}


def test_list_skills_by_agent(client):
    register_test_agent(client, "a1", skills=[{"name": "s1"}, {"name": "s2"}])
    register_test_agent(client, "a2", skills=[{"name": "s3"}])
    r = client.get("/v1/skills", params={"agent_id": "a1"}).json()
    assert len(r) == 2


def test_flows_empty(client):
    r = client.get("/v1/flows").json()
    assert r == {"available": [], "unavailable": []}


def test_flows_available_when_requirements_met(client):
    # Register researcher with version 0.6.4
    register_test_agent(client, "researcher-primary", skills=[{"name": "find_papers"}])
    # Now register the agent_type — the conftest helper uses the ID minus the suffix
    # Let's register with explicit type by re-registering with a collaboration
    client.post("/v1/register", json={
        "id": "developer-primary",
        "callback": "http://localhost:9999",
        "pid": 22222,
        "version": "0.1.0",
        "skills": [{"name": "read_spec"}],
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

    r = client.get("/v1/flows").json()
    # researcher-primary registered as agent_type "researcher" (extracted from ID)
    # version "0.1.0" (from conftest default) — but we need 0.6.4 for the gate
    # Let me re-register researcher with explicit version
    client.post("/v1/register", json={
        "id": "researcher-primary",
        "callback": "http://localhost:9999",
        "pid": 11111,
        "version": "0.6.4",
        "skills": [{"name": "find_papers"}],
    })

    r = client.get("/v1/flows").json()
    assert len(r["available"]) == 1
    assert r["available"][0]["name"] == "evaluate_spec"
    assert r["available"][0]["valid"] is True


def test_flows_unavailable_when_requirements_not_met(client):
    # Register developer with a collaboration requiring researcher>=0.5
    client.post("/v1/register", json={
        "id": "developer-primary",
        "callback": "http://localhost:9999",
        "pid": 22222,
        "version": "0.1.0",
        "skills": [],
        "collaborations": [{
            "name": "evaluate_spec",
            "requires": {"researcher": ">=0.5.0"},
            "steps": [],
        }],
    })

    # No researcher registered
    r = client.get("/v1/flows").json()
    assert len(r["unavailable"]) == 1
    assert "not registered" in r["unavailable"][0]["unmet"][0]


def test_flows_unavailable_when_version_too_low(client):
    # Register researcher at 0.4.0
    client.post("/v1/register", json={
        "id": "researcher-old",
        "callback": "http://localhost:9999",
        "pid": 11111,
        "version": "0.4.0",
        "skills": [],
    })
    # Register developer requiring >=0.5
    client.post("/v1/register", json={
        "id": "developer-primary",
        "callback": "http://localhost:9999",
        "pid": 22222,
        "version": "0.1.0",
        "skills": [],
        "collaborations": [{
            "name": "evaluate_spec",
            "requires": {"researcher": ">=0.5.0"},
            "steps": [],
        }],
    })

    r = client.get("/v1/flows").json()
    assert len(r["unavailable"]) == 1
    assert "does not meet" in r["unavailable"][0]["unmet"][0]


def test_flows_with_list_requires(client):
    """requires can be a list of agent types (no version gate) — should still validate."""
    client.post("/v1/register", json={
        "id": "dev-1",
        "callback": "http://localhost:9999",
        "pid": 1,
        "version": "0.1.0",
        "skills": [],
        "collaborations": [{
            "name": "needs_researcher",
            "requires": ["researcher"],
            "steps": [],
        }],
    })
    r = client.get("/v1/flows").json()
    # researcher not registered → unavailable
    assert len(r["unavailable"]) == 1

    # Now register researcher
    client.post("/v1/register", json={
        "id": "researcher-1",
        "callback": "http://localhost:9999",
        "pid": 2,
        "version": "0.1.0",
        "skills": [],
    })
    r = client.get("/v1/flows").json()
    assert len(r["available"]) == 1


def test_matrix_empty(client):
    r = client.get("/v1/matrix").json()
    assert r["agents"] == {}
    assert r["collaborations"] == []


def test_matrix_with_agents_and_flows(client):
    client.post("/v1/register", json={
        "id": "researcher-primary",
        "callback": "http://localhost:9999",
        "pid": 1,
        "version": "0.6.4",
        "skills": [{"name": "find_papers"}, {"name": "synergize"}],
    })
    client.post("/v1/register", json={
        "id": "developer-primary",
        "callback": "http://localhost:9998",
        "pid": 2,
        "version": "0.1.0",
        "skills": [{"name": "read_spec"}],
        "collaborations": [{
            "name": "evaluate_spec",
            "requires": {"researcher": ">=0.5.0"},
            "steps": [],
        }],
    })

    r = client.get("/v1/matrix").json()
    assert "researcher-primary" in r["agents"]
    assert r["agents"]["researcher-primary"]["solo_skills"] == 2
    assert "developer-primary" in r["agents"]
    assert r["agents"]["developer-primary"]["solo_skills"] == 1

    assert len(r["collaborations"]) == 1
    assert r["collaborations"][0]["name"] == "evaluate_spec"
    assert r["collaborations"][0]["status"] == "available"


def test_status_includes_flow_counts(client):
    client.post("/v1/register", json={
        "id": "dev-1",
        "callback": "http://localhost:9999",
        "pid": 1,
        "version": "0.1.0",
        "skills": [],
        "collaborations": [
            {"name": "flow_a", "requires": ["researcher"], "steps": []},
            {"name": "flow_b", "requires": [], "steps": []},
        ],
    })
    r = client.get("/v1/status").json()
    assert r["total_flows"] == 2
    # flow_b requires nothing → available; flow_a requires researcher → unavailable
    assert r["available_flows"] == 1
    assert r["unavailable_flows"] == 1
