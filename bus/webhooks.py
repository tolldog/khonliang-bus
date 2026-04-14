"""GitHub webhook → bus event bridge.

Accepts GitHub webhook POSTs, verifies the HMAC signature (if a secret
is configured), and publishes them as bus events on topics like
``github.pull_request.opened``. Any bus subscriber (Claude via
``bus_wait_for_event``, a local agent, whatever) can then react.

Security:
  - If ``GITHUB_WEBHOOK_SECRET`` is set (env var or bus config),
    requests must carry a valid ``X-Hub-Signature-256`` header.
  - If no secret is configured, the endpoint accepts all posts —
    intended for localhost development only.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any


def verify_signature(secret: str, body: bytes, signature_header: str) -> bool:
    """Verify a GitHub webhook ``X-Hub-Signature-256`` header.

    Returns True if the signature matches (or no secret is configured).
    GitHub sends ``sha256=<hex>``; we compute HMAC-SHA256 of the raw body
    with the secret and compare using ``hmac.compare_digest`` (constant-time).
    """
    if not secret:
        return True  # no secret configured = accept all (dev mode)
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected_hex = signature_header.split("=", 1)[1]
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), expected_hex)


def build_topic(event_type: str, payload: dict[str, Any]) -> str:
    """Compute the bus topic for a GitHub event.

    Format: ``github.<event_type>[.<action>]``

    Examples::

        pull_request + opened        → github.pull_request.opened
        pull_request_review + submitted → github.pull_request_review.submitted
        push (no action)            → github.push
        ping                        → github.ping
    """
    topic = f"github.{event_type}"
    action = payload.get("action")
    if isinstance(action, str) and action:
        topic = f"{topic}.{action}"
    return topic


def summarize(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Extract the commonly useful fields so subscribers don't have to
    walk the full GitHub payload for basic identification.

    Always returns a dict with ``repo``, ``sender``, plus event-specific
    keys (``pr_number``, ``pr_title``, ``branch``, etc. when available).
    The full payload is still attached by the caller.
    """
    summary: dict[str, Any] = {
        "event": event_type,
        "action": payload.get("action"),
        "repo": _get_repo(payload),
        "sender": _get_sender(payload),
    }

    # PR events
    pr = payload.get("pull_request") or {}
    if pr:
        summary["pr_number"] = pr.get("number")
        summary["pr_title"] = pr.get("title")
        summary["pr_state"] = pr.get("state")
        summary["pr_url"] = pr.get("html_url")
        summary["branch"] = (pr.get("head") or {}).get("ref")
        summary["base"] = (pr.get("base") or {}).get("ref")

    # Issue events (also triggered by PR comments)
    issue = payload.get("issue") or {}
    if issue and "pr_number" not in summary:
        summary["issue_number"] = issue.get("number")
        summary["issue_title"] = issue.get("title")

    # Review events
    review = payload.get("review") or {}
    if review:
        summary["review_state"] = review.get("state")
        summary["review_author"] = (review.get("user") or {}).get("login")

    # Push events
    if event_type == "push":
        summary["ref"] = payload.get("ref")
        summary["before"] = payload.get("before")
        summary["after"] = payload.get("after")
        summary["commits"] = len(payload.get("commits", []))

    # Check events (CI)
    check_run = payload.get("check_run") or {}
    if check_run:
        summary["check_name"] = check_run.get("name")
        summary["check_status"] = check_run.get("status")
        summary["check_conclusion"] = check_run.get("conclusion")

    return summary


def _get_repo(payload: dict) -> str:
    repo = payload.get("repository") or {}
    return repo.get("full_name", "") or repo.get("name", "")


def _get_sender(payload: dict) -> str:
    sender = payload.get("sender") or {}
    return sender.get("login", "")
