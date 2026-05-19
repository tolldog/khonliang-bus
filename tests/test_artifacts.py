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


def test_grep_matches_is_total_count_not_capped(db):
    """grep 'matches' field must reflect ALL matching lines, not just returned blocks."""
    store = ArtifactStore(db)
    meta = store.create(
        kind="command_output",
        title="repeated pattern",
        content="\n".join(f"ERROR line {i}" for i in range(10)),
    )

    result = store.grep(meta["id"], pattern="ERROR", max_matches=3, max_chars=10000)
    assert result["matches"] == 10
    assert result["returned_matches"] == 3


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
    assert distilled["metadata"]["max_chars"] == 500
    assert "source_lines: 100" in result["digest"]
    assert len(result["digest"]) <= 500


def test_artifact_distill_cache_hit_returns_same_artifact(db):
    """Same (source, mode, purpose, max_chars) returns the same artifact on re-call."""
    store = ArtifactStore(db)
    source = store.create(
        kind="git_diff",
        title="diff",
        content="\n".join(f"+ line {i}" for i in range(50)),
    )

    first = store.distill(source["id"], purpose="summary", max_chars=400)
    second = store.distill(source["id"], purpose="summary", max_chars=400)

    assert second["distilled_artifact"]["id"] == first["distilled_artifact"]["id"]
    assert second["digest"] == first["digest"]


def test_artifact_distill_cache_miss_on_different_args(db):
    """Different purpose or max_chars must produce distinct distillations."""
    store = ArtifactStore(db)
    source = store.create(
        kind="git_diff",
        title="diff",
        content="\n".join(f"+ line {i}" for i in range(50)),
    )

    a = store.distill(source["id"], purpose="for_review", max_chars=400)
    b = store.distill(source["id"], purpose="for_handoff", max_chars=400)
    c = store.distill(source["id"], purpose="for_review", max_chars=800)

    ids = {a["distilled_artifact"]["id"], b["distilled_artifact"]["id"], c["distilled_artifact"]["id"]}
    assert len(ids) == 3


def test_artifact_distill_cache_false_forces_new_artifact(db):
    """cache=False bypasses lookup even when a matching artifact exists."""
    store = ArtifactStore(db)
    source = store.create(
        kind="git_diff",
        title="diff",
        content="\n".join(f"+ line {i}" for i in range(20)),
    )

    first = store.distill(source["id"], purpose="summary", max_chars=300)
    forced = store.distill(source["id"], purpose="summary", max_chars=300, cache=False)

    assert forced["distilled_artifact"]["id"] != first["distilled_artifact"]["id"]
    assert forced["digest"] == first["digest"]  # deterministic content


def test_artifact_distill_sets_ttl_when_cache_ttl_seconds_given(db):
    """cache_ttl_seconds populates ttl on the new artifact in ISO-8601 UTC format with microsecond precision."""
    import re

    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b\nline c")

    result = store.distill(source["id"], purpose="p", max_chars=200, cache_ttl_seconds=3600)
    ttl = result["distilled_artifact"]["ttl"]
    assert ttl is not None
    # Microsecond precision in the stored value avoids ttl/now both rounding
    # to the same second boundary at the expiry instant (which would flip the
    # entry to "expired" one second early under `ttl > now`).
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", ttl)


def test_artifact_distill_expired_cache_entry_is_bypassed(db):
    """An expired cache entry is skipped; a fresh artifact is produced."""
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b\nline c")

    first = store.distill(source["id"], purpose="p", max_chars=200, cache_ttl_seconds=60)
    # Backdate the cached entry's ttl directly so the next call sees it as expired.
    with db.conn() as c:
        c.execute(
            "UPDATE artifacts SET ttl = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000Z", first["distilled_artifact"]["id"]),
        )
    second = store.distill(source["id"], purpose="p", max_chars=200)

    assert second["distilled_artifact"]["id"] != first["distilled_artifact"]["id"]


def test_artifact_distill_rejects_non_positive_cache_ttl(db):
    """Non-positive cache_ttl_seconds is meaningless as a duration and must be rejected."""
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b")

    for bad in (0, -1, -3600):
        with pytest.raises(ValueError, match="positive integer"):
            store.distill(source["id"], purpose="p", max_chars=200, cache_ttl_seconds=bad)


def test_artifact_distill_cache_ignores_multi_source_distillations(db):
    """distill_many artifacts (multiple sources) must not satisfy single-source cache lookups."""
    store = ArtifactStore(db)
    a = store.create(kind="log", title="a", content="alpha alpha alpha")
    b = store.create(kind="log", title="b", content="beta beta beta")

    store.distill_many([a["id"], b["id"]], purpose="combined", max_chars=400)
    single = store.distill(a["id"], purpose="combined", max_chars=400)

    assert single["distilled_artifact"]["source_artifacts"] == [a["id"]]


def test_artifact_create_rejects_reserved_distillation_kind(db):
    """Public create() must refuse `kind='distillation'` so the cache key cannot be spoofed."""
    store = ArtifactStore(db)
    with pytest.raises(ValueError, match="reserved"):
        store.create(kind="distillation", title="planted", content="x")


def test_artifact_distill_cache_ignores_non_bus_producer(db):
    """Defense-in-depth: even rows planted via the internal path with a foreign producer must be skipped."""
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="real source content")

    # Bypass the public-kind gate to plant a foreign-producer distillation row.
    spoofed = store._create(
        kind="distillation",
        title="Distillation of log",
        content="ATTACKER-CONTROLLED DIGEST",
        producer="not-bus",
        metadata={
            "mode": "brief",
            "purpose": "p",
            "source_kind": "log",
            "max_chars": 200,
            "distiller_version": "v1",
        },
        source_artifacts=[source["id"]],
    )

    result = store.distill(source["id"], purpose="p", max_chars=200)

    assert result["distilled_artifact"]["id"] != spoofed["id"]
    assert "ATTACKER-CONTROLLED DIGEST" not in result["digest"]


def test_artifact_distill_cache_ignores_older_distiller_version(db):
    """A cache entry produced by an older distiller version must not be reused."""
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b")

    first = store.distill(source["id"], purpose="p", max_chars=200)
    # Simulate an algorithm change by retroactively marking the cached entry as v0.
    with db.conn() as c:
        c.execute(
            """
            UPDATE artifacts
            SET metadata = json_set(metadata, '$.distiller_version', 'v0')
            WHERE id = ?
            """,
            (first["distilled_artifact"]["id"],),
        )
    second = store.distill(source["id"], purpose="p", max_chars=200)

    assert second["distilled_artifact"]["id"] != first["distilled_artifact"]["id"]


def test_artifact_distill_cache_hit_does_not_retroactively_apply_caller_ttl(db):
    """Cache hits return the existing entry; caller's cache_ttl_seconds applies on creation only.

    This is documented, intentional behavior: a deterministic distillation
    is just as valid whether stored with no TTL or a 60s TTL. Callers that
    need a guaranteed-fresh artifact (or their own TTL on the stored row)
    must pass cache=False.
    """
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b")

    # First call stores a forever-cached entry (no TTL).
    forever = store.distill(source["id"], purpose="p", max_chars=200)
    assert forever["distilled_artifact"]["ttl"] is None

    # Second call passes a tight TTL but should still return the same forever entry.
    second = store.distill(source["id"], purpose="p", max_chars=200, cache_ttl_seconds=60)
    assert second["distilled_artifact"]["id"] == forever["distilled_artifact"]["id"]
    assert second["distilled_artifact"]["ttl"] is None  # original TTL preserved


def test_artifact_distill_rejects_cache_ttl_above_upper_bound(db):
    """cache_ttl_seconds above the upper bound must be rejected before timedelta overflow."""
    store = ArtifactStore(db)
    source = store.create(kind="log", title="log", content="line a\nline b")

    # One year + 1 second is over the cap.
    too_long = 365 * 24 * 3600 + 1
    with pytest.raises(ValueError, match="1 year"):
        store.distill(source["id"], purpose="p", max_chars=200, cache_ttl_seconds=too_long)


def test_artifact_content_size_limit(db):
    """create() must reject content exceeding MAX_ARTIFACT_BYTES."""
    from bus.artifacts import MAX_ARTIFACT_BYTES

    store = ArtifactStore(db)
    oversized = "x" * (MAX_ARTIFACT_BYTES + 1)
    with pytest.raises(ValueError, match="exceeds maximum size"):
        store.create(kind="log", title="big", content=oversized)


def test_missing_artifact_raises_key_error(db):
    store = ArtifactStore(db)
    with pytest.raises(KeyError):
        store.head("missing")


def test_artifact_distill_http_returns_422_for_invalid_cache_ttl(client):
    """The HTTP route must map distill()'s ValueError to 422, not a 500."""
    created = client.post(
        "/v1/artifacts",
        json={"kind": "log", "title": "t", "content": "x", "producer": "tester"},
    )
    artifact_id = created.json()["id"]

    bad = client.post(
        f"/v1/artifacts/{artifact_id}/distill",
        json={"purpose": "p", "max_chars": 200, "cache_ttl_seconds": 0},
    )
    assert bad.status_code == 422
    assert "positive integer" in bad.json()["detail"]


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
