"""The light log agent (fr_khonliang-bus_70862caa PR 2, docs/log-agent-design.md).

Covers: the sqlite substrate (ingest/query/retention/status), the tailer's
correctness rules (last-newline offsets, drain-rotated-before-reset,
copytruncate), the agent's WS protocol dispatch, the bus /v1/logs/query
passthrough + limp verdict, and a real WS E2E through the bus.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import pytest
from fastapi.testclient import TestClient

from bus.log_agent.agent import SKILLS, LogAgent
from bus.log_agent.substrate import LogRecord, SqliteSubstrate
from bus.log_agent.tailer import Tailer, _agent_id_from_filename, _parse_line
from bus.server import _log_file_name, create_app


def _rec(msg: str, agent: str = "a1", ts: float | None = None, level: str | None = "INFO"):
    return LogRecord(ts=ts if ts is not None else time.time(), agent_id=agent, level=level, message=msg)


# ---------------------------------------------------------------------------
# Substrate
# ---------------------------------------------------------------------------


def test_substrate_ingest_and_filters(tmp_path):
    s = SqliteSubstrate(tmp_path / "sub.db")
    now = time.time()
    s.ingest([
        _rec("error in worker", agent="a1", ts=now - 10, level="ERROR"),
        _rec("all fine", agent="a1", ts=now - 5),
        _rec("other agent line", agent="a2", ts=now - 1),
    ])
    assert len(s.query()) == 3
    assert [r["message"] for r in s.query(agent_id="a2")] == ["other agent line"]
    assert [r["message"] for r in s.query(level="error")] == ["error in worker"]
    assert [r["message"] for r in s.query(pattern="worker")] == ["error in worker"]
    assert [r["message"] for r in s.query(since=now - 6)] == ["other agent line", "all fine"]
    assert [r["message"] for r in s.query(until=now - 8)] == ["error in worker"]


def test_substrate_pattern_wildcards_are_literal(tmp_path):
    s = SqliteSubstrate(tmp_path / "sub.db")
    s.ingest([_rec("100% done"), _rec("100x done")])
    assert [r["message"] for r in s.query(pattern="100%")] == ["100% done"]  # % not a wildcard


def test_substrate_retention(tmp_path):
    s = SqliteSubstrate(tmp_path / "sub.db", retention_days=1)
    s.ingest([_rec("old", ts=time.time() - 3 * 86400), _rec("fresh")])
    assert s.enforce_retention() == 1
    assert [r["message"] for r in s.query()] == ["fresh"]


def test_substrate_creates_missing_dir(tmp_path):
    """Fresh deployment: the agent can start before the bus wrote any L0 file —
    the substrate mkdirs its parent rather than failing sqlite connect."""
    s = SqliteSubstrate(tmp_path / "not-yet" / "sub.db")
    assert s.status()["reachable"] is True


def test_substrate_status(tmp_path):
    s = SqliteSubstrate(tmp_path / "sub.db")
    st = s.status()
    assert st["reachable"] is True and st["line_count"] == 0
    s.ingest([_rec("x")])
    st = s.status()
    assert st["line_count"] == 1 and st["last_ingest_ts"] is not None


# ---------------------------------------------------------------------------
# Tailer
# ---------------------------------------------------------------------------


def _tailer(tmp_path):
    d = tmp_path / "logs"
    d.mkdir(exist_ok=True)
    s = SqliteSubstrate(tmp_path / "sub.db")
    return d, s, Tailer(d, s)


def test_tailer_ingests_new_lines_and_persists_offset(tmp_path):
    d, s, t = _tailer(tmp_path)
    (d / "a1.log").write_text("line one\nline two\n")
    assert t.sweep() == 2
    assert t.sweep() == 0  # nothing new
    with open(d / "a1.log", "a") as f:
        f.write("line three\n")
    assert t.sweep() == 1
    assert [r["message"] for r in s.query(agent_id="a1")][0] == "line three"


def test_tailer_offset_survives_restart(tmp_path):
    d, s, t = _tailer(tmp_path)
    (d / "a1.log").write_text("one\n")
    t.sweep()
    t2 = Tailer(d, s)  # fresh tailer, same substrate → offsets persisted
    with open(d / "a1.log", "a") as f:
        f.write("two\n")
    assert t2.sweep() == 1  # only the new line — no re-ingest


def test_tailer_partial_line_carried_not_fragmented(tmp_path):
    d, s, t = _tailer(tmp_path)
    with open(d / "a1.log", "w") as f:
        f.write("complete\npartial-without-newl")
    assert t.sweep() == 1  # only the complete line
    with open(d / "a1.log", "a") as f:
        f.write("ine finished\n")
    assert t.sweep() == 1
    msgs = [r["message"] for r in s.query(agent_id="a1")]
    assert "partial-without-newline finished" in msgs  # ONE record, not two fragments


def test_tailer_drains_rotated_backlog_before_reset(tmp_path):
    """The ▸R1 rule: rotation during a log-agent outage must not lose the
    unforwarded remainder — it's drained from .log.1 before resetting."""
    d, s, t = _tailer(tmp_path)
    p = d / "a1.log"
    p.write_text("forwarded\n")
    t.sweep()
    with open(p, "a") as f:
        f.write("backlog while agent down\n")  # written but NOT swept
    # The bus rotates at agent start: rename + fresh file.
    p.replace(d / "a1.log.1")
    p.write_text("fresh instance line\n")

    assert t.sweep() == 2  # backlog drained + new line
    msgs = {r["message"] for r in s.query(agent_id="a1")}
    assert "backlog while agent down" in msgs
    assert "fresh instance line" in msgs
    assert "forwarded" in msgs and len(msgs) == 3  # no duplicates either


def test_tailer_copytruncate_resets(tmp_path):
    d, s, t = _tailer(tmp_path)
    p = d / "a1.log"
    p.write_text("before truncate\n")
    t.sweep()
    p.write_text("")  # copytruncate: same inode, size < offset
    with open(p, "a") as f:
        f.write("after truncate\n")
    assert t.sweep() == 1
    assert "after truncate" in {r["message"] for r in s.query(agent_id="a1")}


def test_tailer_filename_roundtrip():
    """The tailer inverts the bus's percent-encoded filename mapping exactly."""
    for agent_id in ("researcher-primary", "a/b", "a_b", "unicode-ключ", "a.b"):
        assert _agent_id_from_filename(_log_file_name(agent_id) + ".log") == agent_id


def test_parse_line_standard_format_and_fallback():
    now = time.time()
    r = _parse_line("2026-07-01 10:20:30,123 bus INFO started ok", "a1", now)
    assert r.level == "INFO" and r.ts != now
    r2 = _parse_line("Traceback (most recent call last):", "a1", now)
    assert r2.level is None and r2.ts == now  # unparsed line still stored


# ---------------------------------------------------------------------------
# Agent protocol dispatch
# ---------------------------------------------------------------------------


def _agent(tmp_path) -> LogAgent:
    d = tmp_path / "logs"
    d.mkdir(exist_ok=True)
    return LogAgent("log-agent", "http://localhost:9", str(d),
                    substrate=SqliteSubstrate(tmp_path / "sub.db"))


def test_register_payload_shape(tmp_path):
    p = _agent(tmp_path).register_payload()
    assert p["agent_type"] == "log-agent"       # explicit — id-derived would be "log"
    assert p["pid"] == os.getpid()
    assert {s["name"] for s in p["skills"]} == {
        "log_query", "substrate_status", "self_test", "health_check",
    }
    assert p["welcome"]["agent_type"] == "log-agent"


def test_handle_message_request_response_nesting(tmp_path):
    a = _agent(tmp_path)
    a.substrate.ingest([_rec("hello", agent="x")])
    reply = a.handle_message({
        "type": "request", "operation": "log_query",
        "args": {"agent_id": "x"}, "correlation_id": "c-1",
    })
    assert reply["type"] == "response" and reply["correlation_id"] == "c-1"
    assert reply["result"]["count"] == 1        # payload nested under "result"


def test_handle_message_unknown_op_and_type(tmp_path):
    a = _agent(tmp_path)
    err = a.handle_message({"type": "request", "operation": "nope", "correlation_id": "c"})
    assert err["type"] == "error" and err["retryable"] is False
    assert a.handle_message({"type": "pong"}) is None          # tolerated
    assert a.handle_message({"type": "learning_update"}) is None


def test_self_test_roundtrip(tmp_path):
    a = _agent(tmp_path)
    result = a.handle_message({
        "type": "request", "operation": "self_test", "args": {}, "correlation_id": "c",
    })["result"]
    assert result["ok"] is True and result["probe_found"] is True


def test_ws_target_forms(tmp_path):
    a = _agent(tmp_path)
    a.bus_url = "http://localhost:8787"
    assert a._ws_target() == ("ws://localhost:8787/v1/agent", None)
    a.bus_url = "unix:///home/u/.khonliang/bus.sock"
    assert a._ws_target() == ("ws://localhost/v1/agent", "/home/u/.khonliang/bus.sock")


# ---------------------------------------------------------------------------
# Bus passthrough + limp verdict
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path):
    app = create_app(
        db_path=str(tmp_path / "bus.db"),
        config={"agent_log_dir": str(tmp_path / "logs")},
    )
    return TestClient(app)


def test_logs_query_limp_verdict_when_no_log_agent(client, tmp_path):
    r = client.get("/v1/logs/query", params={"pattern": "x"})
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert detail["error"] == "log agent unreachable"          # clear verdict…
    assert str(tmp_path / "logs") in detail["hint"]            # …with the L0 fallback


async def test_logs_query_e2e_real_agent_over_uds(tmp_path):
    """The strongest end-to-end: a REAL uvicorn bus on a unix socket, the REAL
    LogAgent.run() loop (unix_connect → register → dispatch → response), and a
    REAL tailed file — queried through GET /v1/logs/query."""
    import httpx
    import uvicorn

    sock = tmp_path / "bus.sock"
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "researcher-primary.log").write_text(
        "2026-07-01 10:00:00,000 researcher ERROR researcher exploded\n"
    )

    app = create_app(
        db_path=str(tmp_path / "bus.db"),
        config={"agent_log_dir": str(log_dir)},
    )
    server = uvicorn.Server(uvicorn.Config(app, uds=str(sock), log_level="warning", lifespan="off"))
    server_task = asyncio.create_task(server.serve())

    agent = LogAgent("log-agent", f"unix://{sock}", str(log_dir),
                     substrate=SqliteSubstrate(tmp_path / "sub.db"))
    agent_task = None
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.05)
        agent_task = asyncio.create_task(agent.run())

        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(sock)), base_url="http://localhost"
        ) as http:
            body = None
            for _ in range(100):  # wait for register + first tail sweep
                r = await http.get("/v1/logs/query", params={"pattern": "exploded"})
                if r.status_code == 200 and r.json().get("count"):
                    body = r.json()
                    break
                await asyncio.sleep(0.1)

        assert body is not None, "log agent never became queryable"
        assert body["count"] == 1
        line = body["lines"][0]
        assert line["agent_id"] == "researcher-primary"   # filename → id roundtrip
        assert line["level"] == "ERROR"                    # parsed from the line
    finally:
        if agent_task is not None:
            agent_task.cancel()
            with pytest.raises((asyncio.CancelledError, Exception)):
                await agent_task
        server.should_exit = True
        await server_task
