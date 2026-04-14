"""Tests for the GitHub webhook receiver."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest

from bus.webhooks import build_topic, summarize, verify_signature


@pytest.fixture
def unsigned_client(tmp_path):
    """Client with github_webhook_allow_unsigned=True for webhook endpoint tests."""
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        db_path=str(tmp_path / "webhook-dev.db"),
        config={"github_webhook_allow_unsigned": True},
    )
    return TestClient(app)


# ---------------------------------------------------------------------------
# Unit: signature verification
# ---------------------------------------------------------------------------


def test_verify_signature_valid():
    secret = "s3cr3t"
    body = b'{"action": "opened"}'
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, f"sha256={mac}") is True


def test_verify_signature_invalid():
    assert verify_signature("s3cr3t", b"{}", "sha256=00") is False


def test_verify_signature_missing_header():
    assert verify_signature("s3cr3t", b"{}", "") is False


def test_verify_signature_wrong_scheme():
    assert verify_signature("s3cr3t", b"{}", "sha1=abcd") is False


def test_verify_signature_no_secret_accepts_all():
    """When no secret is configured, accept the request (dev mode)."""
    assert verify_signature("", b"{}", "") is True
    assert verify_signature("", b"{}", "sha256=anything") is True


# ---------------------------------------------------------------------------
# Unit: topic + summary builders
# ---------------------------------------------------------------------------


def test_build_topic_with_action():
    topic = build_topic("pull_request", {"action": "opened"})
    assert topic == "github.pull_request.opened"


def test_build_topic_without_action():
    assert build_topic("push", {"ref": "refs/heads/main"}) == "github.push"


def test_build_topic_ping():
    assert build_topic("ping", {}) == "github.ping"


def test_summarize_pr_event():
    payload = {
        "action": "opened",
        "repository": {"full_name": "tolldog/khonliang-bus"},
        "sender": {"login": "tolldog"},
        "pull_request": {
            "number": 11,
            "title": "bus_wait_for_event",
            "state": "open",
            "html_url": "https://github.com/tolldog/khonliang-bus/pull/11",
            "head": {"ref": "feat/wait-for-event"},
            "base": {"ref": "main"},
        },
    }
    s = summarize("pull_request", payload)
    assert s["repo"] == "tolldog/khonliang-bus"
    assert s["sender"] == "tolldog"
    assert s["pr_number"] == 11
    assert s["pr_title"] == "bus_wait_for_event"
    assert s["branch"] == "feat/wait-for-event"
    assert s["base"] == "main"


def test_summarize_push_event():
    payload = {
        "ref": "refs/heads/main",
        "before": "abc",
        "after": "def",
        "commits": [{"id": "1"}, {"id": "2"}],
        "repository": {"full_name": "tolldog/x"},
        "sender": {"login": "user"},
    }
    s = summarize("push", payload)
    assert s["ref"] == "refs/heads/main"
    assert s["commits"] == 2


def test_summarize_review_event():
    payload = {
        "action": "submitted",
        "pull_request": {"number": 42, "title": "Fix bug"},
        "review": {"state": "approved", "user": {"login": "reviewer"}},
        "repository": {"full_name": "r"},
        "sender": {"login": "reviewer"},
    }
    s = summarize("pull_request_review", payload)
    assert s["review_state"] == "approved"
    assert s["review_author"] == "reviewer"
    assert s["pr_number"] == 42


def test_summarize_check_run():
    payload = {
        "action": "completed",
        "check_run": {
            "name": "pytest",
            "status": "completed",
            "conclusion": "success",
        },
        "repository": {"full_name": "r"},
        "sender": {"login": "github-actions"},
    }
    s = summarize("check_run", payload)
    assert s["check_status"] == "completed"
    assert s["check_conclusion"] == "success"


# ---------------------------------------------------------------------------
# Integration: webhook endpoint
# ---------------------------------------------------------------------------


def test_webhook_rejects_when_no_secret_configured(client):
    """Default behaviour: no secret AND no explicit allow_unsigned → 503.

    Prevents the footgun of a production deployment forgetting the secret
    and accepting any forged event.
    """
    r = client.post(
        "/v1/webhooks/github",
        json={"action": "opened"},
        headers={"X-GitHub-Event": "pull_request"},
    )
    assert r.status_code == 503
    assert "not configured" in r.json()["error"]


def test_webhook_accepts_unsigned_when_opt_in(tmp_path):
    """With ``github_webhook_allow_unsigned=True``, unsigned posts are accepted (dev mode)."""
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        db_path=str(tmp_path / "webhook-unsigned.db"),
        config={"github_webhook_allow_unsigned": True},
    )
    c = TestClient(app)

    payload = {
        "action": "opened",
        "repository": {"full_name": "r"},
        "sender": {"login": "u"},
        "pull_request": {"number": 1, "title": "t"},
    }
    r = c.post(
        "/v1/webhooks/github",
        json=payload,
        headers={"X-GitHub-Event": "pull_request"},
    )
    assert r.status_code == 200
    assert r.json()["topic"] == "github.pull_request.opened"


def test_webhook_publishes_to_bus(unsigned_client):
    """The webhook should publish an event that bus_wait_for_event can see."""
    client = unsigned_client
    payload = {
        "action": "submitted",
        "repository": {"full_name": "r"},
        "sender": {"login": "reviewer"},
        "pull_request": {"number": 7, "title": "t"},
        "review": {"state": "approved", "user": {"login": "reviewer"}},
    }
    client.post(
        "/v1/webhooks/github",
        json=payload,
        headers={"X-GitHub-Event": "pull_request_review"},
    )

    # Long-poll should see the event
    r = client.post("/v1/wait", json={
        "topics": ["github.pull_request_review.submitted"],
        "subscriber_id": "reviewer-listener",
        "timeout": 1.0,
    }).json()
    assert r["status"] == "matched"
    assert r["event"]["topic"] == "github.pull_request_review.submitted"
    payload_out = r["event"]["payload"]
    assert payload_out["summary"]["pr_number"] == 7
    assert payload_out["summary"]["review_state"] == "approved"
    assert payload_out["github"]["action"] == "submitted"


def test_webhook_rejects_invalid_signature(tmp_path):
    """With a secret configured, requests without valid signatures are rejected."""
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        db_path=str(tmp_path / "webhook-sig.db"),
        config={"github_webhook_secret": "topsecret"},
    )
    c = TestClient(app)

    # No signature header
    r = c.post(
        "/v1/webhooks/github",
        json={"action": "opened"},
        headers={"X-GitHub-Event": "pull_request"},
    )
    assert r.status_code == 401

    # Wrong signature
    r = c.post(
        "/v1/webhooks/github",
        json={"action": "opened"},
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": "sha256=bogus",
        },
    )
    assert r.status_code == 401


def test_webhook_accepts_valid_signature(tmp_path):
    from bus.server import create_app
    from fastapi.testclient import TestClient

    secret = "topsecret"
    app = create_app(
        db_path=str(tmp_path / "webhook-ok.db"),
        config={"github_webhook_secret": secret},
    )
    c = TestClient(app)

    body = json.dumps({
        "action": "opened",
        "repository": {"full_name": "r"},
        "sender": {"login": "u"},
        "pull_request": {"number": 1, "title": "t"},
    }).encode()
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    r = c.post(
        "/v1/webhooks/github",
        content=body,
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    assert r.json()["topic"] == "github.pull_request.opened"


def test_webhook_handles_ping(unsigned_client):
    """GitHub sends a ``ping`` event when a webhook is first configured."""
    r = unsigned_client.post(
        "/v1/webhooks/github",
        json={"zen": "Keep it logically awesome."},
        headers={"X-GitHub-Event": "ping"},
    )
    assert r.status_code == 200
    assert r.json()["topic"] == "github.ping"


def test_webhook_handles_invalid_json(unsigned_client):
    """Bad JSON body → 400, not a crash."""
    r = unsigned_client.post(
        "/v1/webhooks/github",
        content=b"not valid json {{",
        headers={
            "X-GitHub-Event": "push",
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 400
    assert "invalid JSON" in r.json()["error"]


def test_webhook_rejects_non_dict_json(unsigned_client):
    """JSON that isn't an object (e.g., a list) → 400."""
    r = unsigned_client.post(
        "/v1/webhooks/github",
        content=b'["not", "an", "object"]',
        headers={
            "X-GitHub-Event": "push",
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 400
    assert "JSON object" in r.json()["error"]
