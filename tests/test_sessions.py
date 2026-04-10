"""Tests for session management: create, get, suspend, resume, archive."""

from __future__ import annotations

from tests.conftest import register_test_agent


def test_create_session(client):
    register_test_agent(client)
    r = client.post("/v1/session", json={
        "agent_id": "test-agent",
        "task": "research consensus",
    }).json()
    assert "session_id" in r
    assert r["status"] == "active"


def test_get_session(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={
        "agent_id": "test-agent",
    }).json()
    r = client.get(f"/v1/session/{created['session_id']}").json()
    assert r["status"] == "active"
    assert r["agent_id"] == "test-agent"


def test_get_unknown_session(client):
    r = client.get("/v1/session/nonexistent").json()
    assert "error" in r


def test_suspend_and_resume(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    # Suspend
    r = client.post(f"/v1/session/{sid}/suspend").json()
    assert r["status"] == "suspended"

    # Verify
    assert client.get(f"/v1/session/{sid}").json()["status"] == "suspended"

    # Resume
    r = client.post(f"/v1/session/{sid}/resume").json()
    assert r["status"] == "active"

    # Verify
    assert client.get(f"/v1/session/{sid}").json()["status"] == "active"


def test_archive_session(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    r = client.request("DELETE", f"/v1/session/{sid}").json()
    assert r["status"] == "archived"
    assert client.get(f"/v1/session/{sid}").json()["status"] == "archived"


def test_suspend_unknown_session_returns_error(client):
    r = client.post("/v1/session/nonexistent/suspend").json()
    assert "error" in r


def test_resume_unknown_session_returns_error(client):
    r = client.post("/v1/session/nonexistent/resume").json()
    assert "error" in r


def test_archive_unknown_session_returns_error(client):
    r = client.request("DELETE", "/v1/session/nonexistent").json()
    assert "error" in r


def test_session_message_on_suspended_session_fails(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    client.post(f"/v1/session/{sid}/suspend")
    r = client.post(f"/v1/session/{sid}/message", json={
        "message": "hello",
    }).json()
    assert "error" in r
    assert "suspended" in r["error"]
