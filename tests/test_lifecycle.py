"""Tests for agent lifecycle endpoints: install, register, heartbeat, deregister."""

from __future__ import annotations

from tests.conftest import install_test_agent, register_test_agent


def test_install_agent(client):
    r = install_test_agent(client)
    assert r["status"] == "installed"


def test_list_installed(client):
    install_test_agent(client, "a1")
    install_test_agent(client, "a2")
    r = client.get("/v1/install").json()
    assert len(r) == 2
    assert {a["id"] for a in r} == {"a1", "a2"}


def test_uninstall_agent(client):
    install_test_agent(client)
    r = client.delete("/v1/install/test-agent").json()
    assert r["status"] == "uninstalled"
    assert client.get("/v1/install").json() == []


def test_uninstall_unknown_returns_not_found(client):
    r = client.delete("/v1/install/ghost").json()
    assert r["status"] == "not_found"


def test_register_agent(client):
    r = register_test_agent(client)
    assert r["status"] == "registered"


def test_register_shows_in_services(client):
    register_test_agent(client, "a1")
    services = client.get("/v1/services").json()
    assert len(services) == 1
    assert services[0]["id"] == "a1"
    assert services[0]["skill_count"] == 2
    assert "do_thing" in services[0]["skills"]


def test_register_replaces_skills(client):
    register_test_agent(client, skills=[{"name": "old"}])
    register_test_agent(client, skills=[{"name": "new1"}, {"name": "new2"}])
    services = client.get("/v1/services").json()
    assert services[0]["skill_count"] == 2
    assert set(services[0]["skills"]) == {"new1", "new2"}


def test_heartbeat(client):
    register_test_agent(client)
    r = client.post("/v1/heartbeat", json={"id": "test-agent"}).json()
    assert r["status"] == "ok"


def test_heartbeat_unknown_returns_not_registered(client):
    r = client.post("/v1/heartbeat", json={"id": "ghost"}).json()
    assert r["status"] == "not_registered"


def test_deregister(client):
    register_test_agent(client)
    r = client.post("/v1/deregister", json={"id": "test-agent"}).json()
    assert r["status"] == "deregistered"
    assert client.get("/v1/services").json() == []


def test_deregister_unknown_returns_not_found(client):
    r = client.post("/v1/deregister", json={"id": "ghost"}).json()
    assert r["status"] == "not_found"
