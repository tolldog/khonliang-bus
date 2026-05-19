"""Tests for ``fr_khonliang-bus_f96722dd`` bus-side: persistent welcome
catalog.

Covers:
- ``BusDB`` upsert / get / list of welcome blobs.
- Welcome survives deregister (separate table from ``registrations``).
- HTTP register path persists welcome alongside the registration.
- WS register path persists welcome too — coverage in
  ``tests/test_agent_ws.py::test_ws_register_persists_welcome``.
- HTTP ``GET /v1/agents/{id}/welcome``, ``POST .../welcome``,
  ``GET /v1/agents/welcomes`` endpoints.
- ``MAX_WELCOME_BYTES`` size cap enforced at the DB layer and surfaced
  as 413 on the POST endpoint / silently logged on register paths.

The bus-lib side (BaseAgent forwarding welcome on register) is tested in
``khonliang-bus-lib/tests/test_agent.py`` + ``test_connector.py``.
"""

from __future__ import annotations

import sqlite3

import pytest

from bus.db import BusDB


# ---------------------------------------------------------------------------
# DB layer
# ---------------------------------------------------------------------------


def _sample_welcome(agent_id: str = "x-primary") -> dict:
    return {
        "agent_id": agent_id,
        "agent_type": "x",
        "version": "0.1.0",
        "role": "test fixture",
        "mission": "exercise welcome persistence",
        "skill_count": 3,
    }


def test_set_and_get_welcome(db):
    welcome = _sample_welcome()
    db.set_agent_welcome("x-primary", welcome)
    record = db.get_agent_welcome("x-primary")
    assert record is not None
    assert record["welcome"] == welcome
    assert "updated_at" in record


def test_get_welcome_returns_none_when_absent(db):
    assert db.get_agent_welcome("ghost") is None


def test_welcome_upsert_overwrites_prior(db):
    """Re-register / late-update path: most-recent-wins, no duplicate rows."""
    db.set_agent_welcome("x-primary", _sample_welcome())
    updated = _sample_welcome()
    updated["mission"] = "REVISED"
    db.set_agent_welcome("x-primary", updated)
    record = db.get_agent_welcome("x-primary")
    assert record["welcome"]["mission"] == "REVISED"
    # Single row — no append.
    assert len(db.list_agent_welcomes()) == 1


def test_welcome_survives_deregister(db):
    """Welcome catalog is intentionally separate from registrations: when an
    agent deregisters (process exit, manual deregister), the welcome
    persists so cold-start LLMs can still consult it.
    """
    db.register_agent("x-primary", "x", "ws://connected", 1, "0.1.0")
    db.set_agent_welcome("x-primary", _sample_welcome())
    assert db.get_registration("x-primary") is not None

    db.deregister_agent("x-primary")
    assert db.get_registration("x-primary") is None  # registration gone
    # Welcome still there — no cascade.
    record = db.get_agent_welcome("x-primary")
    assert record is not None
    assert record["welcome"]["role"] == "test fixture"


def test_list_agent_welcomes_orders_by_agent_id(db):
    for aid in ("zeta", "alpha", "mu"):
        db.set_agent_welcome(aid, _sample_welcome(aid))
    listed = db.list_agent_welcomes()
    assert [r["agent_id"] for r in listed] == ["alpha", "mu", "zeta"]
    # Each carries parsed welcome + updated_at.
    assert listed[0]["welcome"]["agent_id"] == "alpha"
    assert all("updated_at" in r for r in listed)


def test_list_agent_welcomes_empty_when_none_published(db):
    assert db.list_agent_welcomes() == []


def test_welcome_with_corrupt_json_returns_none(db):
    """Defensive: a corrupt row (manually written) is handled — get returns
    ``welcome=None`` but still surfaces ``updated_at`` so callers know
    something exists.
    """
    with db.conn() as c:
        c.execute(
            "INSERT INTO welcomes (agent_id, welcome) VALUES (?, ?)",
            ("corrupt", "not-json"),
        )
    record = db.get_agent_welcome("corrupt")
    assert record is not None
    assert record["welcome"] is None
    assert "updated_at" in record


# ---------------------------------------------------------------------------
# HTTP /v1/register persists welcome (parity with launch_spec/launch_info)
# ---------------------------------------------------------------------------


def test_http_register_persists_welcome(client):
    welcome = _sample_welcome("x-primary")
    r = client.post("/v1/register", json={
        "id": "x-primary",
        "callback": "http://localhost:9000",
        "pid": 4242,
        "version": "0.1.0",
        "skills": [],
        "welcome": welcome,
    })
    assert r.status_code == 200
    # Welcome is retrievable via the read endpoint immediately.
    body = client.get("/v1/agents/x-primary/welcome").json()
    assert body["welcome"] == welcome


def test_http_register_without_welcome_stores_nothing(client):
    """Backward compat: older bus-lib doesn't send welcome → bus stores
    nothing → GET 404 until the agent re-registers with the new lib.
    """
    client.post("/v1/register", json={
        "id": "legacy-agent",
        "callback": "http://localhost:9000",
        "pid": 1, "version": "0.0.0", "skills": [],
    })
    r = client.get("/v1/agents/legacy-agent/welcome")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# GET /v1/agents/{id}/welcome
# ---------------------------------------------------------------------------


def test_get_welcome_endpoint_404_for_unknown_agent(client):
    r = client.get("/v1/agents/nobody/welcome")
    assert r.status_code == 404


def test_get_welcome_endpoint_returns_parsed_dict(client, db):
    db.set_agent_welcome("x-primary", _sample_welcome())
    r = client.get("/v1/agents/x-primary/welcome")
    assert r.status_code == 200
    body = r.json()
    assert body["welcome"]["agent_id"] == "x-primary"
    assert "updated_at" in body


# ---------------------------------------------------------------------------
# POST /v1/agents/{id}/welcome — late update / operator override
# ---------------------------------------------------------------------------


def test_post_welcome_endpoint_persists(client):
    welcome = _sample_welcome("operator-set")
    r = client.post("/v1/agents/operator-set/welcome", json=welcome)
    assert r.status_code == 200
    assert r.json() == {"agent_id": "operator-set", "status": "stored"}
    # Round-trip via GET.
    body = client.get("/v1/agents/operator-set/welcome").json()
    assert body["welcome"] == welcome


def test_post_welcome_endpoint_rejects_non_object(client):
    """The wire shape is a JSON object; lists / strings / numbers rejected.

    The ``payload: dict[str, Any]`` annotation on the handler is the
    schema FastAPI/Pydantic enforces, so 422 is the canonical response —
    we don't need to (and don't) add a redundant isinstance check.
    """
    r = client.post("/v1/agents/x/welcome", json=["not", "an", "object"])
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Size cap (Copilot R1 #4: prevent DB bloat / memory pressure from a
# misconfigured agent publishing a multi-megabyte welcome blob).
# ---------------------------------------------------------------------------


def test_set_agent_welcome_rejects_oversized_blob(db):
    """A welcome blob exceeding MAX_WELCOME_BYTES raises ValueError at the
    DB layer; callers decide how to surface (silent skip on register
    paths, 413 on the POST endpoint).
    """
    # Build a payload whose JSON exceeds the cap by ~1 KB so the test is
    # not flaky against future tweaks to MAX_WELCOME_BYTES.
    oversized = {"agent_id": "x", "padding": "x" * (db.MAX_WELCOME_BYTES + 1024)}
    with pytest.raises(ValueError, match="refusing to persist"):
        db.set_agent_welcome("x-primary", oversized)


def test_post_welcome_endpoint_returns_413_for_oversized(client, db):
    oversized = {"agent_id": "x", "padding": "x" * (db.MAX_WELCOME_BYTES + 1024)}
    r = client.post("/v1/agents/oversized-agent/welcome", json=oversized)
    assert r.status_code == 413
    assert "refusing to persist" in r.json()["detail"]


def test_http_register_with_oversized_welcome_still_registers(client, db):
    """Parity with WS path: oversized welcome from one agent must not block
    the entire registration. The welcome is logged + skipped; the agent
    is still registered and callable.
    """
    oversized = {"agent_id": "x", "padding": "x" * (db.MAX_WELCOME_BYTES + 1024)}
    r = client.post("/v1/register", json={
        "id": "big-welcome-agent",
        "callback": "http://localhost:9000",
        "pid": 1, "version": "0.1.0", "skills": [],
        "welcome": oversized,
    })
    # Register still succeeds.
    assert r.status_code == 200
    # But no welcome got stored.
    assert client.get("/v1/agents/big-welcome-agent/welcome").status_code == 404


def test_post_welcome_endpoint_overwrites_existing(client, db):
    """POST is idempotent; second post replaces."""
    db.set_agent_welcome("x-primary", _sample_welcome())
    client.post("/v1/agents/x-primary/welcome", json={"role": "REPLACED"})
    body = client.get("/v1/agents/x-primary/welcome").json()
    assert body["welcome"] == {"role": "REPLACED"}


# ---------------------------------------------------------------------------
# GET /v1/agents/welcomes — fleet-wide list
# ---------------------------------------------------------------------------


def test_list_welcomes_endpoint(client, db):
    db.set_agent_welcome("alpha", _sample_welcome("alpha"))
    db.set_agent_welcome("beta", _sample_welcome("beta"))
    r = client.get("/v1/agents/welcomes")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    assert {row["agent_id"] for row in rows} == {"alpha", "beta"}
    assert all("welcome" in row and "updated_at" in row for row in rows)


def test_list_welcomes_endpoint_empty(client):
    r = client.get("/v1/agents/welcomes")
    assert r.status_code == 200
    assert r.json() == []
