"""Tests for gap reporting and response evaluation."""

from __future__ import annotations

from tests.conftest import register_test_agent


def test_report_gap(client):
    r = client.post("/v1/gap", json={
        "agent_id": "researcher-1",
        "operation": "analyze_video",
        "reason": "Video analysis not implemented",
        "context": {"input_type": "video"},
    }).json()
    assert r["status"] == "open"
    assert "gap_id" in r

    feedback = client.get("/v1/feedback", params={"kind": "gap"}).json()
    assert len(feedback) == 1
    assert feedback[0]["agent_id"] == "researcher-1"
    assert feedback[0]["operation"] == "analyze_video"
    assert feedback[0]["message"] == "Video analysis not implemented"


def test_list_gaps(client):
    client.post("/v1/gap", json={
        "agent_id": "r1", "operation": "op1", "reason": "r1",
    })
    client.post("/v1/gap", json={
        "agent_id": "r2", "operation": "op2", "reason": "r2",
    })
    gaps = client.get("/v1/gaps").json()
    assert len(gaps) == 2
    assert all(g["status"] == "open" for g in gaps)


def test_update_gap_status(client):
    r = client.post("/v1/gap", json={
        "agent_id": "r1", "operation": "op1", "reason": "r1",
    }).json()
    gap_id = r["gap_id"]

    client.patch(f"/v1/gap/{gap_id}", params={"status": "reviewed"})
    gaps = client.get("/v1/gaps").json()  # default: open
    assert len(gaps) == 0

    gaps = client.get("/v1/gaps", params={"status": "reviewed"}).json()
    assert len(gaps) == 1


def test_report_friction_feedback(client):
    r = client.post("/v1/feedback", json={
        "agent_id": "developer-primary",
        "kind": "friction",
        "operation": "next_work_unit",
        "category": "format",
        "severity": "medium",
        "message": "JSON string had to be unwrapped client-side",
        "context": {"tool": "developer_primary_next_work_unit"},
        "suggestion": "Return parsed envelope content",
    })
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "open"
    assert body["count"] == 1

    feedback = client.get("/v1/feedback", params={"kind": "friction"}).json()
    assert len(feedback) == 1
    assert feedback[0]["category"] == "format"
    assert feedback[0]["severity"] == "medium"
    assert feedback[0]["context"]["tool"] == "developer_primary_next_work_unit"


def test_feedback_deduplicates_open_reports(client):
    payload = {
        "agent_id": "developer-primary",
        "kind": "friction",
        "operation": "run_tests",
        "category": "latency",
        "severity": "low",
        "message": "test run exceeded latency threshold",
    }
    first = client.post("/v1/feedback", json=payload).json()
    second = client.post("/v1/feedback", json=payload).json()
    assert first["status"] == "open"
    assert second["status"] == "deduped"
    assert second["feedback_id"] == first["feedback_id"]
    assert second["count"] == 2

    feedback = client.get("/v1/feedback").json()
    assert len(feedback) == 1
    assert feedback[0]["count"] == 2


def test_invalid_feedback_rejected(client):
    r = client.post("/v1/feedback", json={
        "agent_id": "developer-primary",
        "kind": "friction",
        "operation": "run_tests",
        "category": "not-a-kind",
        "message": "bad category",
    })
    assert r.status_code == 422


def test_evaluate_accept(client):
    register_test_agent(client)
    # Make a request to get a trace_id
    r = client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "timeout": 2,
        "trace_id": "t-eval-1",
    }).json()

    # Evaluate: accept
    e = client.post("/v1/evaluate", json={
        "trace_id": "t-eval-1",
        "verdict": "accept",
    }).json()
    assert e["verdict"] == "accept"


def test_evaluate_escalate_creates_gap(client):
    register_test_agent(client)
    client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "timeout": 2,
        "trace_id": "t-eval-2",
    })

    e = client.post("/v1/evaluate", json={
        "trace_id": "t-eval-2",
        "verdict": "escalate",
        "reason": "Response quality too low",
    }).json()
    assert e["escalated"] is True
    assert "gap_id" in e

    # Gap should be created
    gaps = client.get("/v1/gaps").json()
    assert len(gaps) == 1
    assert "escalation" in gaps[0]["operation"]


def test_get_evaluations(client):
    client.post("/v1/evaluate", json={
        "trace_id": "t-eval-3",
        "verdict": "accept",
    })
    evals = client.get("/v1/evaluations/t-eval-3").json()
    assert len(evals) == 1
    assert evals[0]["verdict"] == "accept"


def test_response_mode_in_request(client):
    """response_mode should be passed through to the agent request."""
    register_test_agent(client)
    r = client.post("/v1/request", json={
        "agent_id": "test-agent",
        "operation": "do_thing",
        "timeout": 2,
        "response_mode": "distilled",
        "trace_id": "t-mode-1",
    }).json()
    # Request will fail (no real agent) but the field was accepted
    assert "trace_id" in r
