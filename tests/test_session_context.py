"""Tests for session context: update, get with scope, persistence."""

from __future__ import annotations

from tests.conftest import register_test_agent


def test_update_session_context(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    r = client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"findings": ["paper1", "paper2"], "status": "in_progress"},
        "private_ctx": {"conversation": [{"role": "user", "content": "find papers"}]},
    }).json()
    assert r["status"] == "context_updated"


def test_get_session_context_public_only(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"findings": ["paper1"]},
        "private_ctx": {"secret": "working_memory"},
    })

    r = client.get(f"/v1/session/{sid}/context").json()
    assert "public" in r
    assert r["public"]["findings"] == ["paper1"]
    assert "private" not in r  # default scope is public-only


def test_get_session_context_with_private(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"findings": ["paper1"]},
        "private_ctx": {"secret": "working_memory"},
    })

    r = client.get(f"/v1/session/{sid}/context", params={"scope": "private"}).json()
    assert "public" in r
    assert "private" in r
    assert r["private"]["secret"] == "working_memory"


def test_context_survives_suspend_resume(client):
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    # Write context
    client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"conclusion": "consensus works"},
    })

    # Suspend
    client.post(f"/v1/session/{sid}/suspend")

    # Resume
    client.post(f"/v1/session/{sid}/resume")

    # Context should still be there
    r = client.get(f"/v1/session/{sid}/context").json()
    assert r["public"]["conclusion"] == "consensus works"


def test_context_update_on_unknown_session(client):
    r = client.post("/v1/session/nonexistent/context", json={
        "public_ctx": {"x": 1},
    }).json()
    assert "error" in r


def test_context_get_on_unknown_session(client):
    r = client.get("/v1/session/nonexistent/context").json()
    assert "error" in r


def test_context_partial_update(client):
    """Updating only public_ctx shouldn't overwrite private_ctx."""
    register_test_agent(client)
    created = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    sid = created["session_id"]

    # Set both
    client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"v": 1},
        "private_ctx": {"secret": "keep"},
    })

    # Update only public
    client.post(f"/v1/session/{sid}/context", json={
        "public_ctx": {"v": 2},
    })

    # Private should be unchanged
    r = client.get(f"/v1/session/{sid}/context", params={"scope": "private"}).json()
    assert r["public"]["v"] == 2
    assert r["private"]["secret"] == "keep"
