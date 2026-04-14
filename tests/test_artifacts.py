"""Tests for bus artifact storage and bounded retrieval."""

from __future__ import annotations

import pytest

from bus.artifacts import ArtifactStore


def test_create_artifact_stores_metadata_without_content(db):
    store = ArtifactStore(db)
    meta = store.create(
        kind="pytest_log",
        title="pytest run",
        content="line 1\nline 2",
        producer="developer-primary",
        session_id="sess-1",
        trace_id="trace-1",
        metadata={"repo": "khonliang-bus"},
    )

    assert meta["id"].startswith("art_")
    assert meta["kind"] == "pytest_log"
    assert meta["producer"] == "developer-primary"
    assert meta["metadata"] == {"repo": "khonliang-bus"}
    assert meta["size_bytes"] == len("line 1\nline 2".encode("utf-8"))
    assert "content" not in meta


def test_artifact_head_tail_get_are_bounded(db):
    store = ArtifactStore(db)
    meta = store.create(
        kind="command_output",
        title="large command",
        content="\n".join(f"line {i}" for i in range(1, 101)),
    )

    head = store.head(meta["id"], lines=3, max_chars=1000)
    assert head.text == "line 1\nline 2\nline 3"
    assert head.truncated is True

    tail = store.tail(meta["id"], lines=2, max_chars=1000)
    assert tail.text == "line 99\nline 100"
    assert tail.start_line == 99
    assert tail.end_line == 100
    assert tail.truncated is True

    window = store.get(meta["id"], offset=0, max_chars=8)
    assert window.text == "line 1\nl"
    assert window.truncated is True


def test_artifact_excerpt_and_grep_are_bounded(db):
    store = ArtifactStore(db)
    meta = store.create(
        kind="pytest_log",
        title="pytest failure",
        content="\n".join(
            [
                "setup",
                "tests/test_a.py::test_ok PASSED",
                "tests/test_a.py::test_bad FAILED",
                "AssertionError: created_at must be preserved",
                "teardown",
            ]
        ),
    )

    excerpt = store.excerpt(meta["id"], start_line=3, end_line=4, max_chars=1000)
    assert excerpt.text == (
        "tests/test_a.py::test_bad FAILED\n"
        "AssertionError: created_at must be preserved"
    )
    assert excerpt.start_line == 3
    assert excerpt.end_line == 4

    grep = store.grep(
        meta["id"],
        pattern="created_at",
        context_lines=1,
        max_matches=1,
        max_chars=120,
    )
    assert grep["matches"] == 1
    assert "created_at must be preserved" in grep["text"]
    assert len(grep["text"]) <= 120

    with pytest.raises(ValueError, match="invalid regex pattern"):
        store.grep(meta["id"], pattern="[")


def test_artifact_distill_creates_new_artifact_with_source_ref(db):
    store = ArtifactStore(db)
    source = store.create(
        kind="git_diff",
        title="large diff",
        content="\n".join(f"+ changed line {i}" for i in range(100)),
    )

    result = store.distill(
        source["id"],
        purpose="summarize diff for coding session",
        max_chars=500,
    )

    distilled = result["distilled_artifact"]
    assert distilled["kind"] == "distillation"
    assert distilled["source_artifacts"] == [source["id"]]
    assert "source_lines: 100" in result["digest"]
    assert len(result["digest"]) <= 500


def test_missing_artifact_raises_key_error(db):
    store = ArtifactStore(db)
    with pytest.raises(KeyError):
        store.head("missing")


def test_artifact_http_routes_are_bounded(client):
    content = "\n".join(f"line {i}" for i in range(50))
    created = client.post(
        "/v1/artifacts",
        json={
            "kind": "command_output",
            "title": "cmd",
            "content": content,
            "producer": "tester",
        },
    )
    assert created.status_code == 200
    artifact_id = created.json()["id"]

    listed = client.get("/v1/artifacts", params={"producer": "tester"}).json()
    assert len(listed) == 1
    assert listed[0]["id"] == artifact_id
    assert "content" not in listed[0]

    tail = client.get(
        f"/v1/artifacts/{artifact_id}/tail",
        params={"lines": 5, "max_chars": 30},
    ).json()
    assert tail["truncated"] is True
    assert len(tail["text"]) <= 30

    grep = client.get(
        f"/v1/artifacts/{artifact_id}/grep",
        params={"pattern": "line 42", "context_lines": 1, "max_chars": 100},
    ).json()
    assert "line 42" in grep["text"]
