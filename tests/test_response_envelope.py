"""Response-envelope consistency contract (fr_khonliang-bus_c989e906).

The envelope is the platform's context firewall. ``findings`` is a LINE-ORIENTED
preview of text content only — structured (JSON) results must never be chopped
into per-line fragments; they stay whole in ``content`` (or an artifact). These
tests pin that contract at the single builder every skill flows through.
"""

from __future__ import annotations

import json

from bus.response_envelope import ResponseBudget, build_response_envelope


def _env(value, *, budget_chars=8000, artifact=None):
    text, content_type = (
        (value, "text/plain")
        if isinstance(value, str)
        else (json.dumps(value, indent=2, sort_keys=True), "application/json")
    )
    return build_response_envelope(
        ok=True, status="ok", producer="agent", operation="op",
        text=text, content_type=content_type, value=value,
        budget=ResponseBudget(max_chars=budget_chars), artifact=artifact,
    )


# ---------------------------------------------------------------------------
# Structured content stays structured
# ---------------------------------------------------------------------------


def test_structured_dict_is_not_fragmented():
    obj = {"count": 3, "papers": ["a", "b", "c"], "nested": {"k": "v"}}
    env = _env(obj)
    assert env["findings"] == []           # NOT per-line JSON fragments
    assert env["content"] == obj           # object preserved — no reassembly
    assert "object with 3 field" in env["summary"]


def test_structured_list_is_not_fragmented():
    arr = [{"id": 1}, {"id": 2}]
    env = _env(arr)
    assert env["findings"] == []
    assert env["content"] == arr
    assert "array of 2 item" in env["summary"]


def test_text_result_keeps_line_findings():
    env = _env("line one\nline two\nline three")
    assert env["findings"]                  # line-oriented previews present
    assert env["content"] == "line one\nline two\nline three"
    assert env["summary"].startswith("agent.op: line one")


# ---------------------------------------------------------------------------
# The lint/contract check (AC#3): findings never carry JSON fragments
# ---------------------------------------------------------------------------


def test_findings_never_contain_json_fragments():
    """A structured payload whose pretty-JSON has many lines must not leak those
    lines into findings (the pre-fix bug: a JSON object chopped per line)."""
    obj = {f"key_{i}": {"a": i, "b": [i, i + 1]} for i in range(20)}
    env = _env(obj)
    assert env["findings"] == []
    # And nothing findings-shaped smuggled a JSON fragment.
    for f in env["findings"]:
        assert not f.lstrip().startswith(("{", "}", '"'))


# ---------------------------------------------------------------------------
# Oversized structured content → artifact + bounded excerpt, still no fragments
# ---------------------------------------------------------------------------


def test_oversized_structured_uses_excerpt_not_content():
    big = {"items": list(range(5000))}
    env = _env(big, budget_chars=200, artifact={"id": "art_1", "kind": "tool_result"})
    assert env["omitted"] is True
    assert env["findings"] == []
    assert "content" not in env             # full payload is in the artifact
    assert isinstance(env["excerpt"], str)  # bounded text excerpt
    assert "art_1" in env["artifact_ids"]


def test_empty_object_summary():
    env = _env({})
    assert env["findings"] == []
    assert env["content"] == {}
    assert "empty object" in env["summary"]


def test_json_null_preserved_as_none():
    """A skill returning JSON null must arrive as structured None, not the
    string 'null' (all JSON shapes stay structured)."""
    env = build_response_envelope(
        ok=True, status="ok", producer="agent", operation="op",
        text="null", content_type="application/json", value=None,
        budget=ResponseBudget(max_chars=8000),
    )
    assert env["findings"] == []
    assert env["content"] is None
    assert env["summary"].endswith("null")


def test_structured_summary_is_bounded():
    """A dict with very long keys must not produce an unbounded summary."""
    from bus.response_envelope import SUMMARY_CHARS
    obj = {"k" * 5000: 1, "another_" * 500: 2}
    env = _env(obj)
    assert len(env["summary"]) <= SUMMARY_CHARS + 3  # +len('...')


def test_no_value_passed_falls_back_to_text():
    """Backward-compat: a caller that doesn't pass value keeps text content."""
    env = build_response_envelope(
        ok=True, status="ok", producer="a", operation="op",
        text='{"x": 1}', content_type="application/json",
        budget=ResponseBudget(max_chars=8000),
    )
    assert env["content"] == '{"x": 1}'  # no value → text (sentinel default)
