"""Bounded response envelopes for MCP-facing bus tools.

The bus should be a context firewall: helpers may produce large logs, diffs,
or analysis, but MCP callers should receive a small decision-grade envelope
plus artifact references for raw material.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


DEFAULT_INLINE_CHARS = 8000
HARD_INLINE_CHARS = 16000
HIGH_DETAIL_INLINE_CHARS = 64000
SUMMARY_CHARS = 500
MAX_FINDINGS = 12


@dataclass(frozen=True)
class ResponseBudget:
    """Resolved inline response budget."""

    max_chars: int = DEFAULT_INLINE_CHARS
    high_detail: bool = False


def extract_response_budget(args: dict[str, Any]) -> ResponseBudget:
    """Pop bus-only response controls from tool args.

    Agent skills can keep their own ``detail`` semantics. These underscore
    fields are consumed by the bus adapter and are not forwarded to agents.
    """
    high_detail = bool(args.pop("_allow_high_detail", False))
    raw_budget = args.pop("_response_budget_chars", DEFAULT_INLINE_CHARS)
    ceiling = HIGH_DETAIL_INLINE_CHARS if high_detail else HARD_INLINE_CHARS
    try:
        max_chars = int(raw_budget)
    except (TypeError, ValueError):
        max_chars = DEFAULT_INLINE_CHARS
    return ResponseBudget(max_chars=max(1, min(max_chars, ceiling)), high_detail=high_detail)


def serialize_result(value: Any) -> tuple[str, str]:
    """Return ``(text, content_type)`` for an arbitrary agent result."""
    if isinstance(value, str):
        return value, "text/plain"
    return json.dumps(value, indent=2, sort_keys=True), "application/json"


def build_response_envelope(
    *,
    ok: bool,
    status: str,
    producer: str,
    operation: str,
    text: str,
    budget: ResponseBudget,
    artifact: dict[str, Any] | None = None,
    content_type: str = "text/plain",
    value: Any = None,
) -> dict[str, Any]:
    """Build the standard compact envelope returned to MCP clients.

    ``findings`` is a LINE-ORIENTED PREVIEW of text content only. Structured
    (JSON) results are never chopped into per-line fragments — they live whole
    in ``content`` (as the object, so callers don't reassemble) when they fit,
    or in an artifact + bounded excerpt when they don't. Pass ``value`` (the
    original result) so structured content stays structured
    (fr_khonliang-bus_c989e906).
    """
    structured = content_type == "application/json"
    omitted = len(text) > budget.max_chars
    if structured:
        # No line-oriented preview for structured data — reassembling JSON
        # fragments wastes the very inline budget the envelope protects.
        findings: list[str] = []
        summary = _structured_summary(value, producer=producer, operation=operation)
    else:
        findings = _findings(text, budget.max_chars, compact=not omitted)
        summary = _summary(text, producer=producer, operation=operation)
    envelope: dict[str, Any] = {
        "ok": ok,
        "status": status,
        "summary": summary,
        "findings": findings,
        "refs": [],
        "artifact_ids": [],
        "suggested_next_actions": [],
        "truncated": omitted,
        "omitted": omitted,
        "metrics": {
            "raw_chars": len(text),
            "raw_bytes": len(text.encode("utf-8")),
            "inline_budget_chars": budget.max_chars,
            "content_type": content_type,
        },
    }

    if artifact:
        artifact_id = str(artifact.get("id", ""))
        if artifact_id:
            envelope["artifact_ids"].append(artifact_id)
            envelope["refs"].append({
                "type": "artifact",
                "id": artifact_id,
                "kind": artifact.get("kind", ""),
                "title": artifact.get("title", ""),
                "size_bytes": artifact.get("size_bytes", 0),
            })
            # Read-side ``bus_artifact_*`` tools were retired with
            # khonliang-store Phase 4c — point callers at the
            # ``store-primary`` skills that own the read surface
            # now. ``bus_artifact_distill`` stays on the bus until
            # store grows an equivalent (Phase 5 territory).
            envelope["suggested_next_actions"].extend([
                f"store-primary.artifact_tail id={artifact_id} lines=80",
                f"store-primary.artifact_grep id={artifact_id} pattern=<term>",
                f"bus_artifact_distill id={artifact_id}",
            ])

    if omitted:
        # Too big to inline — full payload is in the artifact; give a bounded
        # text excerpt regardless of shape (structured excerpt stays a string).
        envelope["excerpt"] = _bounded_excerpt(text, budget.max_chars)
    elif structured and value is not None:
        envelope["content"] = value  # keep the object/array — no reassembly
    else:
        envelope["content"] = text
    return envelope


def dumps_envelope(envelope: dict[str, Any]) -> str:
    """Stable JSON formatting for MCP text responses."""
    return json.dumps(envelope, indent=2, sort_keys=True)


def _summary(text: str, *, producer: str, operation: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            if len(stripped) > SUMMARY_CHARS:
                stripped = stripped[:SUMMARY_CHARS].rstrip() + "..."
            return f"{producer}.{operation}: {stripped}"
    return f"{producer}.{operation}: empty response"


def _structured_summary(value: Any, *, producer: str, operation: str) -> str:
    """One-line summary for a structured result — a shape description, not a
    line of JSON (the object itself is in ``content``)."""
    prefix = f"{producer}.{operation}:"
    if isinstance(value, dict):
        n = len(value)
        if not n:
            return f"{prefix} empty object"
        keys = ", ".join(str(k) for k in list(value)[:5])
        more = ", ..." if n > 5 else ""
        return f"{prefix} object with {n} field(s) ({keys}{more})"
    if isinstance(value, list):
        return f"{prefix} array of {len(value)} item(s)"
    if value is None:
        return f"{prefix} structured result"
    return f"{prefix} {type(value).__name__}"


def _findings(text: str, max_chars: int, *, compact: bool) -> list[str]:
    limit = 500 if compact else max(200, min(max_chars // 2, 4000))
    max_findings = 3 if compact else MAX_FINDINGS
    findings: list[str] = []
    used = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        remaining = limit - used
        if remaining <= 0 or len(findings) >= max_findings:
            break
        if len(stripped) > remaining:
            stripped = stripped[:remaining].rstrip() + "..."
        findings.append(stripped)
        used += len(stripped)
    return findings


def _bounded_excerpt(text: str, max_chars: int) -> str:
    excerpt_chars = max(200, min(max_chars // 2, 4000))
    if len(text) <= excerpt_chars:
        return text
    return text[:excerpt_chars].rstrip() + "\n... omitted; request artifact excerpt for more ..."
