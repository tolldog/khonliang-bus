"""Tests for the scheduler integration — model selection and distillation thresholds."""

from __future__ import annotations

from bus.scheduler import ModelProfile, SchedulerIntegration


def test_select_smallest_sufficient(db):
    s = SchedulerIntegration(db)
    m = s.select_model(2000)
    assert m.name == "llama3.2:3b"  # 4096 max


def test_select_steps_up(db):
    s = SchedulerIntegration(db)
    m = s.select_model(5000)
    assert m.name == "qwen2.5:7b"  # 16384 max


def test_select_largest_when_exceeds_all(db):
    s = SchedulerIntegration(db)
    m = s.select_model(100000)
    assert m.name == "qwen2.5:32b"


def test_should_distill_at_threshold(db):
    s = SchedulerIntegration(db, distill_threshold=0.75)
    # 3B has max 4096, 75% = 3072
    assert s.should_distill(3072, "llama3.2:3b") is True
    assert s.should_distill(3000, "llama3.2:3b") is False


def test_should_distill_auto_selects_model(db):
    s = SchedulerIntegration(db, distill_threshold=0.75)
    # 2000 tokens → selects 3B (4096 max), 75% = 3072 → not yet
    assert s.should_distill(2000) is False
    # 3500 tokens → selects 3B, above 3072 → distill
    assert s.should_distill(3500) is True


def test_session_recommendation(db):
    db.create_session("s1", "agent1")
    db.update_session("s1", public_ctx='{"data": "x" }', private_ctx='{"notes": "y"}')
    s = SchedulerIntegration(db)
    rec = s.get_session_recommendation("s1")
    assert rec["session_id"] == "s1"
    assert rec["estimated_tokens"] >= 0
    assert "recommended_model" in rec
    assert "should_distill" in rec


def test_session_recommendation_not_found(db):
    s = SchedulerIntegration(db)
    rec = s.get_session_recommendation("nonexistent")
    assert "error" in rec


def test_model_profiles(db):
    s = SchedulerIntegration(db)
    profiles = s.get_model_profiles()
    assert len(profiles) == 3
    assert profiles[0]["name"] == "llama3.2:3b"


def test_custom_profiles(db):
    custom = [
        ModelProfile("tiny:1b", 2048, "instant"),
        ModelProfile("huge:70b", 131072, "glacial"),
    ]
    s = SchedulerIntegration(db, profiles=custom)
    assert s.select_model(1000).name == "tiny:1b"
    assert s.select_model(5000).name == "huge:70b"


def test_models_endpoint(client):
    r = client.get("/v1/models").json()
    assert len(r) == 3
    assert r[0]["name"] == "llama3.2:3b"


def test_session_model_endpoint(client):
    from tests.conftest import register_test_agent
    register_test_agent(client)
    client.post("/v1/session", json={"agent_id": "test-agent"})
    # Get any session
    # We need the session ID — let's create one explicitly
    s = client.post("/v1/session", json={"agent_id": "test-agent"}).json()
    r = client.get(f"/v1/session/{s['session_id']}/model").json()
    assert "recommended_model" in r
    assert "should_distill" in r
