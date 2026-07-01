"""LearningStore Phase 1 (fr_khonliang-bus_ffd4cf00).

Per-(agent_type, role, model) operational learnings: dedup-merged persistence,
delivery in the register ack, WS save_learning, and the HTTP/MCP surfaces.
Phase 2 (bus-lib BaseAgent inject/save + live learning_update push) is separate.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bus.db import BusDB
from bus.server import BusServer, create_app


@pytest.fixture
def client(tmp_path):
    return TestClient(create_app(db_path=str(tmp_path / "test.db")))


# ---------------------------------------------------------------------------
# DB layer: dedup-merge, grouping, filters
# ---------------------------------------------------------------------------


def _db(tmp_path):
    return BusDB(str(tmp_path / "b.db"))


def test_save_learning_new_then_merges(tmp_path):
    db = _db(tmp_path)
    k = dict(agent_type="researcher", role="summarizer", model="qwen2.5:7b")
    assert db.save_learning(**k, learning="truncate to 12K", confidence=0.8)["status"] == "saved"
    # Independent re-discovery merges: sample_count++, confidence reinforced up.
    assert db.save_learning(**k, learning="truncate to 12K", confidence=0.9)["status"] == "merged"
    rows = db.list_learnings(agent_type="researcher")
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 2
    assert rows[0]["confidence"] == 0.9  # max(0.8, 0.9)


def test_source_precedence_on_merge(tmp_path):
    """Merge keeps the most-authoritative provenance: operator outranks agent,
    and an agent re-discovery doesn't downgrade an operator-confirmed rule."""
    db = _db(tmp_path)
    k = dict(agent_type="a", role="r", model="m", learning="x")
    db.save_learning(**k, source="agent")
    db.save_learning(**k, source="operator")  # operator confirms → upgrade
    assert db.list_learnings()[0]["source"] == "operator"
    db.save_learning(**k, source="agent")     # re-discovery must not downgrade
    assert db.list_learnings()[0]["source"] == "operator"


def test_confidence_reinforced_upward_only(tmp_path):
    db = _db(tmp_path)
    k = dict(agent_type="a", role="r", model="m", learning="x")
    db.save_learning(**k, confidence=0.9)
    db.save_learning(**k, confidence=0.5)  # lower — must not lower stored confidence
    assert db.list_learnings()[0]["confidence"] == 0.9


def test_get_learnings_grouped_by_role(tmp_path):
    db = _db(tmp_path)
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:7b", learning="s1", confidence=0.9)
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:7b", learning="s2", confidence=0.7)
    db.save_learning(agent_type="researcher", role="extractor", model="llama3.2:3b", learning="e1", confidence=0.8)
    grouped = db.get_learnings("researcher", {"summarizer": "qwen2.5:7b", "extractor": "llama3.2:3b"})
    assert set(grouped) == {"summarizer", "extractor"}
    assert grouped["summarizer"]["model"] == "qwen2.5:7b"
    # sorted by confidence desc
    assert [r["learning"] for r in grouped["summarizer"]["rules"]] == ["s1", "s2"]


def test_get_learnings_scoped_to_model(tmp_path):
    """A role with learnings for two models delivers ONLY the requested model's
    rules (model swap = clean slate)."""
    db = _db(tmp_path)
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:7b", learning="7b-rule")
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:32b", learning="32b-rule")
    got = db.get_learnings("researcher", {"summarizer": "qwen2.5:32b"})
    assert [r["learning"] for r in got["summarizer"]["rules"]] == ["32b-rule"]  # no 7b leak


def test_get_learnings_without_model_map_returns_nothing(tmp_path):
    db = _db(tmp_path)
    db.save_learning(agent_type="a", role="r", model="m", learning="x")
    assert db.get_learnings("a") == {}          # can't scope → deliver nothing
    assert db.get_learnings("a", {}) == {}


def test_model_key_isolation(tmp_path):
    """Same role, different model → separate learning sets (a swap = clean slate)."""
    db = _db(tmp_path)
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:7b", learning="7b-only")
    db.save_learning(agent_type="researcher", role="summarizer", model="qwen2.5:32b", learning="32b-only")
    r7 = db.list_learnings(agent_type="researcher", model="qwen2.5:7b")
    assert [r["learning"] for r in r7] == ["7b-only"]


def test_min_confidence_filter(tmp_path):
    db = _db(tmp_path)
    db.save_learning(agent_type="a", role="r", model="m", learning="high", confidence=0.9)
    db.save_learning(agent_type="a", role="r", model="m", learning="low", confidence=0.3)
    grouped = db.get_learnings("a", {"r": "m"}, min_confidence=0.5)
    assert [r["learning"] for r in grouped["r"]["rules"]] == ["high"]


# ---------------------------------------------------------------------------
# Server validation
# ---------------------------------------------------------------------------


def test_server_save_learning_requires_fields(tmp_path):
    bus = BusServer(_db(tmp_path), config={})
    assert "error" in bus.save_learning(agent_type="a", role="", model="m", learning="x")
    ok = bus.save_learning(agent_type="a", role="r", model="m", learning="x")
    assert ok["status"] == "saved"


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


def test_post_and_get_learnings_routes(client):
    r = client.post("/v1/learnings", json={
        "agent_type": "researcher", "role": "summarizer",
        "model": "qwen2.5:7b", "learning": "explicit JSON schema", "confidence": 0.85,
        "source": "operator",
    })
    assert r.status_code == 200 and r.json()["status"] == "saved"

    rows = client.get("/v1/learnings", params={"agent_type": "researcher"}).json()
    assert len(rows) == 1
    assert rows[0]["source"] == "operator"


def test_post_learnings_missing_field_422(client):
    r = client.post("/v1/learnings", json={
        "agent_type": "a", "role": "r", "model": "m", "learning": "",
    })
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# WebSocket: register ack carries learnings; save_learning over WS persists
# ---------------------------------------------------------------------------


def test_register_ack_includes_learnings(client):
    # Seed a learning for the agent_type before it connects.
    client.post("/v1/learnings", json={
        "agent_type": "researcher", "role": "summarizer",
        "model": "qwen2.5:7b", "learning": "truncate to 12K", "confidence": 0.9,
    })
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register", "id": "researcher-1", "agent_type": "researcher",
            "pid": 1, "skills": [], "models": {"summarizer": "qwen2.5:7b"},
        })
        ack = ws.receive_json()
        assert ack["type"] == "registered"
        assert "summarizer" in ack["learnings"]
        assert ack["learnings"]["summarizer"]["rules"][0]["learning"] == "truncate to 12K"


def test_register_ack_scopes_to_agents_model(client):
    """A register with model X must not receive learnings saved for model Y."""
    for m, lrn in (("qwen2.5:7b", "7b-rule"), ("qwen2.5:32b", "32b-rule")):
        client.post("/v1/learnings", json={
            "agent_type": "researcher", "role": "summarizer", "model": m, "learning": lrn,
        })
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({
            "type": "register", "id": "researcher-1", "agent_type": "researcher",
            "pid": 1, "skills": [], "models": {"summarizer": "qwen2.5:32b"},
        })
        ack = ws.receive_json()
        rules = [r["learning"] for r in ack["learnings"]["summarizer"]["rules"]]
        assert rules == ["32b-rule"]  # only the 32B instance's model


def test_http_register_returns_scoped_learnings(client):
    """HTTP-registered agents (POST /v1/register) get the same model-scoped
    learnings as WS agents — no mixed-fleet starvation."""
    client.post("/v1/learnings", json={
        "agent_type": "researcher", "role": "summarizer",
        "model": "qwen2.5:7b", "learning": "truncate to 12K",
    })
    r = client.post("/v1/register", json={
        "id": "researcher-1", "callback": "http://localhost:9", "pid": 1,
        "models": {"summarizer": "qwen2.5:7b"},
    })
    body = r.json()
    assert body["status"] == "registered"
    assert body["learnings"]["summarizer"]["rules"][0]["learning"] == "truncate to 12K"


def test_http_register_without_models_omits_learnings(client):
    client.post("/v1/learnings", json={
        "agent_type": "researcher", "role": "r", "model": "m", "learning": "x",
    })
    r = client.post("/v1/register", json={"id": "researcher-2", "callback": "c", "pid": 1})
    assert "learnings" not in r.json()


def test_register_ack_omits_learnings_when_no_model_map(client):
    # Learnings exist, but an agent that doesn't declare its models can't be
    # scoped safely → nothing delivered.
    client.post("/v1/learnings", json={
        "agent_type": "fresh", "role": "r", "model": "m", "learning": "x",
    })
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "fresh-1", "agent_type": "fresh", "pid": 1, "skills": []})
        ack = ws.receive_json()
        assert ack["type"] == "registered"
        assert "learnings" not in ack


def test_ws_save_learning_persists(client):
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "researcher-1", "agent_type": "researcher", "pid": 1, "skills": []})
        ws.receive_json()  # ack
        ws.send_json({
            "type": "save_learning", "agent_type": "researcher", "role": "extractor",
            "model": "llama3.2:3b", "learning": "constrain predicates", "confidence": 0.8,
        })
        # heartbeat round-trips so the save is processed before we query.
        ws.send_json({"type": "heartbeat"})
        ws.receive_json()

    rows = client.get("/v1/learnings", params={"agent_type": "researcher", "role": "extractor"}).json()
    assert len(rows) == 1
    assert rows[0]["learning"] == "constrain predicates"
    assert rows[0]["source"] == "agent"


def test_ws_save_learning_binds_to_registered_identity(client):
    """A WS save_learning must be bound to the connected socket's agent_type and
    forced source=agent — payload agent_type/source are NOT trusted (else one
    agent could poison another's learnings or forge operator provenance)."""
    with client.websocket_connect("/v1/agent") as ws:
        ws.send_json({"type": "register", "id": "researcher-1", "agent_type": "researcher", "pid": 1, "skills": []})
        ws.receive_json()
        ws.send_json({
            "type": "save_learning", "agent_type": "victim", "source": "operator",  # spoof attempt
            "role": "r", "model": "m", "learning": "poison",
        })
        ws.send_json({"type": "heartbeat"})
        ws.receive_json()

    # Nothing written under the spoofed 'victim' type.
    assert client.get("/v1/learnings", params={"agent_type": "victim"}).json() == []
    # Persisted under the real registered type, forced source=agent.
    rows = client.get("/v1/learnings", params={"agent_type": "researcher"}).json()
    assert len(rows) == 1 and rows[0]["source"] == "agent"
