"""Tests for bus.db — schema, CRUD operations, JSON field handling."""

from __future__ import annotations

from bus.db import BusDB


def test_schema_creates_all_tables(db):
    with db.conn() as c:
        tables = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
    expected = {
        "installed_agents", "registrations", "skills", "flows",
        "messages", "subscriptions", "dead_letters", "sessions", "traces",
        "artifacts", "feedback_reports",
    }
    assert expected.issubset(tables)


def test_install_and_retrieve_agent(db):
    db.install_agent("a1", "researcher", "python", ["-m", "r"], "/tmp", "/tmp/c.yaml")
    agents = db.get_installed_agents()
    assert len(agents) == 1
    assert agents[0]["id"] == "a1"
    assert agents[0]["args"] == ["-m", "r"]  # JSON parsed


def test_install_replaces_on_conflict(db):
    db.install_agent("a1", "researcher", "python", [], "/tmp", "/c1.yaml")
    db.install_agent("a1", "researcher", "python3", [], "/tmp", "/c2.yaml")
    agents = db.get_installed_agents()
    assert len(agents) == 1
    assert agents[0]["command"] == "python3"


def test_uninstall_returns_false_for_unknown(db):
    assert db.uninstall_agent("nonexistent") is False


def test_register_and_retrieve_with_skills(db):
    db.register_agent(
        "a1", "researcher", "http://localhost:9000", 111, "0.1.0",
        skills=[
            {"name": "find", "description": "search", "parameters": {"q": "str"}},
            {"name": "synth", "description": "synthesize"},
        ],
    )
    reg = db.get_registration("a1")
    assert reg is not None
    assert reg["callback_url"] == "http://localhost:9000"

    skills = db.get_skills("a1")
    assert len(skills) == 2
    assert {s["name"] for s in skills} == {"find", "synth"}


def test_register_replaces_skills_on_re_register(db):
    db.register_agent("a1", "t", "http://x", 1, skills=[{"name": "old"}])
    db.register_agent("a1", "t", "http://x", 1, skills=[{"name": "new1"}, {"name": "new2"}])
    skills = db.get_skills("a1")
    assert len(skills) == 2
    assert {s["name"] for s in skills} == {"new1", "new2"}


def test_deregister_removes_skills_and_flows(db):
    db.register_agent(
        "a1", "t", "http://x", 1,
        skills=[{"name": "s1"}],
        collaborations=[{"name": "f1", "steps": []}],
    )
    assert len(db.get_skills("a1")) == 1
    assert len(db.get_flows()) == 1

    db.deregister_agent("a1")
    assert len(db.get_skills("a1")) == 0
    assert len(db.get_flows()) == 0


def test_heartbeat_updates_status_and_timestamp(db):
    db.register_agent("a1", "t", "http://x", 1)
    db.set_agent_status("a1", "unhealthy")
    assert db.get_registration("a1")["status"] == "unhealthy"

    db.heartbeat("a1")
    assert db.get_registration("a1")["status"] == "healthy"


def test_heartbeat_returns_false_for_unknown(db):
    assert db.heartbeat("nonexistent") is False


def test_get_healthy_agent_for_type(db):
    db.register_agent("a1", "researcher", "http://x1", 1)
    db.register_agent("a2", "researcher", "http://x2", 2)
    db.set_agent_status("a1", "dead")

    result = db.get_healthy_agent_for_type("researcher")
    assert result is not None
    assert result["id"] == "a2"


def test_get_healthy_agent_returns_none_when_all_unhealthy(db):
    db.register_agent("a1", "researcher", "http://x", 1)
    db.set_agent_status("a1", "dead")
    assert db.get_healthy_agent_for_type("researcher") is None


def test_publish_and_retrieve_messages(db):
    msg_id = db.publish_message("topic.a", {"key": "val"}, "test")
    msgs = db.get_messages("topic.a")
    assert len(msgs) == 1
    assert msgs[0]["id"] == msg_id
    assert msgs[0]["payload"] == {"key": "val"}


def test_ack_and_get_last_acked(db):
    db.publish_message("t", {}, "s")
    msg_id = db.publish_message("t", {}, "s")
    db.ack_message("sub1", "t", msg_id)
    assert db.get_last_acked("sub1", "t") == msg_id


def test_messages_filtered_by_after_id(db):
    id1 = db.publish_message("t", {"n": 1}, "s")
    id2 = db.publish_message("t", {"n": 2}, "s")
    id3 = db.publish_message("t", {"n": 3}, "s")

    msgs = db.get_messages("t", after_id=id1)
    assert len(msgs) == 2
    assert msgs[0]["id"] == id2


def test_dead_letter_stored(db):
    db.add_dead_letter("t", {"bad": True}, "s", agent_id="a1", operation="op", error="boom", attempts=3)
    with db.conn() as c:
        rows = c.execute("SELECT * FROM dead_letters").fetchall()
    assert len(rows) == 1
    assert rows[0]["error"] == "boom"
    assert rows[0]["attempts"] == 3


def test_session_lifecycle(db):
    db.create_session("s1", "a1")
    s = db.get_session("s1")
    assert s["status"] == "active"

    db.update_session("s1", status="suspended", public_ctx='{"key":"val"}')
    s = db.get_session("s1")
    assert s["status"] == "suspended"
    assert s["public_ctx"] == '{"key":"val"}'

    db.update_session("s1", status="archived")
    assert db.get_session("s1")["status"] == "archived"


def test_trace_lifecycle(db):
    db.record_trace_step("t1", 1, agent_id="a1", operation="op1")
    db.finish_trace_step("t1", 1, status="ok", duration_ms=150)

    trace = db.get_trace("t1")
    assert len(trace) == 1
    assert trace[0]["status"] == "ok"
    assert trace[0]["duration_ms"] == 150


def test_list_topics_empty_returns_empty_list(db):
    assert db.list_topics() == []


def test_list_topics_summarises_per_topic(db):
    db.publish_message("github.pull_request_review.submitted", {"pr": 1}, "github-webhook")
    db.publish_message("github.pull_request_review.submitted", {"pr": 2}, "github-webhook")
    db.publish_message("pr.review", {"pr": 1}, "watch_pr_fleet")
    db.publish_message("bus.registry_changed", {"agent_id": "x"}, "bus")

    rows = db.list_topics()
    by_topic = {r["topic"]: r for r in rows}

    assert set(by_topic) == {
        "github.pull_request_review.submitted",
        "pr.review",
        "bus.registry_changed",
    }
    pr_review = by_topic["github.pull_request_review.submitted"]
    assert pr_review["count"] == 2
    assert pr_review["producers"] == ["github-webhook"]
    # first_fired_at and last_fired_at carry SQLite-formatted timestamps;
    # we only check shape (non-empty, monotonic) — exact value is wall-
    # clock-dependent.
    assert pr_review["first_fired_at"]
    assert pr_review["last_fired_at"]
    assert pr_review["first_fired_at"] <= pr_review["last_fired_at"]


def test_list_topics_distinct_producers(db):
    db.publish_message("multi", {}, "agent-a")
    db.publish_message("multi", {}, "agent-b")
    db.publish_message("multi", {}, "agent-a")

    rows = db.list_topics()
    # Distinct via DISTINCT in GROUP_CONCAT, but order is implementation-
    # defined (usually insertion order in SQLite). Compare as a set so
    # the test isn't ordering-coupled.
    assert set(rows[0]["producers"]) == {"agent-a", "agent-b"}


def test_list_topics_prefix_filter(db):
    db.publish_message("github.push", {}, "github-webhook")
    db.publish_message("github.pull_request.opened", {}, "github-webhook")
    db.publish_message("pr.review", {}, "watch_pr_fleet")
    db.publish_message("bus.registry_changed", {}, "bus")

    rows = db.list_topics(prefix="github.")
    topics = {r["topic"] for r in rows}
    assert topics == {"github.push", "github.pull_request.opened"}


def test_list_topics_orders_by_last_fired_desc(db):
    """Most recently active topic comes first — useful for live debugging
    where the operator wants to know what's currently flowing.

    Avoids ``time.sleep`` (which was 1+ seconds and flaky on busy
    CI) by inserting both rows then back-dating one of them with an
    explicit UPDATE so the timestamp ordering is deterministic."""
    db.publish_message("new.topic", {}, "s")
    db.publish_message("old.topic", {}, "s")
    with db.conn() as c:
        c.execute(
            "UPDATE messages SET created_at = '2020-01-01 00:00:00' "
            "WHERE topic = 'old.topic'"
        )

    rows = db.list_topics()
    assert rows[0]["topic"] == "new.topic"
    assert rows[1]["topic"] == "old.topic"


def test_list_topics_limit_caps_rows(db):
    for i in range(5):
        db.publish_message(f"topic.{i}", {}, "s")
    rows = db.list_topics(limit=2)
    assert len(rows) == 2


def test_list_topics_prefix_is_case_sensitive(db):
    """``GLOB`` is case-sensitive in SQLite (``LIKE`` is not, by
    default — without ``PRAGMA case_sensitive_like=ON``). The
    docstring promises case-sensitive prefix matching, so a lower-
    case prefix must NOT match an upper-case topic."""
    db.publish_message("Github.push", {}, "s")
    db.publish_message("github.push", {}, "s")
    rows = db.list_topics(prefix="github.")
    assert {r["topic"] for r in rows} == {"github.push"}
    rows = db.list_topics(prefix="Github.")
    assert {r["topic"] for r in rows} == {"Github.push"}


def test_list_topics_limit_clamped_against_negative_value(db):
    """``LIMIT -1`` disables the limit in SQLite, so without clamping
    a public endpoint could return the entire ``messages`` table.
    The helper coerces negative values to the floor (1)."""
    for i in range(5):
        db.publish_message(f"topic.{i}", {}, "s")
    rows = db.list_topics(limit=-1)
    assert len(rows) == 1


def test_list_topics_limit_clamped_against_huge_value(db, monkeypatch):
    """A very large ``limit`` is clamped at ``LIST_TOPICS_LIMIT_CAP``
    so the public endpoint can't be coerced into a heavy scan via a
    large operator-supplied value. Verified by lowering the cap to a
    small test value and seeding more topics than the cap, so the
    clamp is the only thing that bounds the returned row count."""
    # Lower the cap for the duration of this test rather than seed
    # 1000+ rows in a unit test. The clamp logic is independent of
    # the cap value, so testing at cap=3 proves the same invariant.
    monkeypatch.setattr(BusDB, "LIST_TOPICS_LIMIT_CAP", 3)
    for i in range(7):
        db.publish_message(f"topic.{i}", {}, "s")
    # Ask for many more rows than the cap; expect exactly 3 back.
    rows = db.list_topics(limit=100_000)
    assert len(rows) == 3


def test_list_topics_limit_invalid_value_falls_back_to_default(db):
    """A non-numeric limit falls back to the default (200) rather
    than raising — defensive against bad operator input on the
    public endpoint."""
    for i in range(3):
        db.publish_message(f"topic.{i}", {}, "s")
    rows = db.list_topics(limit="not-a-number")  # type: ignore[arg-type]
    assert len(rows) == 3  # data-bounded, not limit-bounded


def test_list_topics_prefix_escapes_glob_metacharacters(db):
    """``GLOB`` treats ``*``, ``?``, ``[`` specially. A literal-match
    prefix containing those characters must NOT behave as a glob
    pattern — the helper bracket-escapes them before passing to
    SQL."""
    db.publish_message("foo*bar.x", {}, "s")
    db.publish_message("foo123.x", {}, "s")  # would match foo*. without escape
    db.publish_message("foo?bar.x", {}, "s")
    db.publish_message("foo[bar].x", {}, "s")

    # Without escaping, prefix='foo*' would match all four; with
    # escaping it should match only the literal-asterisk topic.
    rows = db.list_topics(prefix="foo*")
    assert {r["topic"] for r in rows} == {"foo*bar.x"}

    rows = db.list_topics(prefix="foo?")
    assert {r["topic"] for r in rows} == {"foo?bar.x"}

    # ``[`` is bracket-escaped as ``[[]`` so it doesn't open a
    # character class.
    rows = db.list_topics(prefix="foo[")
    assert {r["topic"] for r in rows} == {"foo[bar].x"}


def test_list_topics_producers_handle_comma_in_source(db):
    """``PublishRequest.source`` is unconstrained, so a value
    containing a literal comma must NOT be split across multiple
    producers in the result. The helper aggregates producers as a
    JSON array via ``json_group_array``, which side-steps any
    in-band-separator collision risk."""
    db.publish_message("t", {}, "agent,with,commas")
    db.publish_message("t", {}, "plain-agent")
    rows = db.list_topics(prefix="t")
    assert {r["topic"] for r in rows} == {"t"}
    producers = set(rows[0]["producers"])
    # The comma-bearing source survives as a single producer entry
    # (not three).
    assert "agent,with,commas" in producers
    assert "plain-agent" in producers
    assert len(producers) == 2
