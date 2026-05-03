"""SQLite database layer for the bus service.

Schema from specs/bus-mcp/bus-agent-interaction.md. Tables split into:
  - Persistent: installed_agents, messages, sessions, traces
  - Runtime: registrations, skills, flows (rebuilt from agent re-registrations)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA = """
-- Persistent: what to start on boot
CREATE TABLE IF NOT EXISTS installed_agents (
    id           TEXT PRIMARY KEY,
    agent_type   TEXT NOT NULL,
    command      TEXT NOT NULL,
    args         TEXT NOT NULL,     -- JSON array
    cwd          TEXT NOT NULL,
    config       TEXT NOT NULL,     -- absolute path to agent config
    installed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Runtime: who's running now (rebuilt on each boot)
CREATE TABLE IF NOT EXISTS registrations (
    id              TEXT PRIMARY KEY,
    agent_type      TEXT NOT NULL,
    callback_url    TEXT NOT NULL,
    pid             INTEGER NOT NULL,
    version         TEXT,
    registered_at   TEXT NOT NULL DEFAULT (datetime('now')),
    last_heartbeat  TEXT NOT NULL DEFAULT (datetime('now')),
    status          TEXT NOT NULL DEFAULT 'healthy'
);

-- Runtime: skills per agent
CREATE TABLE IF NOT EXISTS skills (
    agent_id     TEXT NOT NULL,
    name         TEXT NOT NULL,
    description  TEXT,
    parameters   TEXT,
    since        TEXT,
    PRIMARY KEY (agent_id, name),
    FOREIGN KEY (agent_id) REFERENCES registrations(id) ON DELETE CASCADE
);

-- Runtime: collaborative flows
CREATE TABLE IF NOT EXISTS flows (
    name         TEXT PRIMARY KEY,
    declared_by  TEXT NOT NULL,
    description  TEXT,
    requires     TEXT,
    steps        TEXT NOT NULL,
    FOREIGN KEY (declared_by) REFERENCES registrations(id) ON DELETE CASCADE
);

-- Persistent: pub/sub messages
CREATE TABLE IF NOT EXISTS messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,
    payload      TEXT NOT NULL,
    source       TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    sequence     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic, sequence);

-- Supports list_topics()'s GROUP BY topic + MIN/MAX(created_at) +
-- ORDER BY last_fired_at DESC. Without this, GET /v1/topics scans
-- the entire ``messages`` table and slows down as the bus runs
-- longer (no pruning of old messages exists). Covering both columns
-- lets SQLite satisfy the per-topic min/max from the index alone.
CREATE INDEX IF NOT EXISTS idx_messages_topic_created
    ON messages(topic, created_at);

-- Persistent: subscriber ack tracking
CREATE TABLE IF NOT EXISTS subscriptions (
    subscriber_id TEXT NOT NULL,
    topic         TEXT NOT NULL,
    last_acked_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (subscriber_id, topic)
);

-- Persistent: dead letter queue
CREATE TABLE IF NOT EXISTS dead_letters (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,
    payload      TEXT NOT NULL,
    source       TEXT NOT NULL,
    agent_id     TEXT,
    operation    TEXT,
    error        TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Persistent: sessions
CREATE TABLE IF NOT EXISTS sessions (
    id           TEXT PRIMARY KEY,
    agent_id     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active',
    public_ctx   TEXT,
    private_ctx  TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Persistent: capability gap reports from agents
CREATE TABLE IF NOT EXISTS gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id     TEXT NOT NULL,
    operation    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    context      TEXT,              -- JSON
    status       TEXT NOT NULL DEFAULT 'open',  -- open | reviewed | dismissed
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Persistent: structured agent feedback beyond hard gaps. Gaps remain for
-- backwards compatibility; feedback_reports is queryable by kind, area,
-- operation, severity, and de-duplicated fingerprint.
CREATE TABLE IF NOT EXISTS feedback_reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id    TEXT NOT NULL,
    kind        TEXT NOT NULL,      -- gap | friction | suggestion
    operation   TEXT NOT NULL DEFAULT '',
    area        TEXT NOT NULL DEFAULT '',
    category    TEXT NOT NULL DEFAULT '',
    severity    TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL,
    context     TEXT NOT NULL DEFAULT '{}',
    suggestion  TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'open',
    fingerprint TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    count       INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_feedback_kind ON feedback_reports(kind, status, created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_agent ON feedback_reports(agent_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_fingerprint ON feedback_reports(fingerprint, status);

-- Persistent: response evaluations (push-back, escalation)
CREATE TABLE IF NOT EXISTS evaluations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id     TEXT NOT NULL,
    verdict      TEXT NOT NULL,     -- accept | push_back | escalate
    reason       TEXT,
    retry_with   TEXT,              -- JSON (extra context for push_back)
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Persistent: execution traces
CREATE TABLE IF NOT EXISTS traces (
    trace_id     TEXT NOT NULL,
    step         INTEGER NOT NULL,
    agent_id     TEXT,
    operation    TEXT,
    args         TEXT,              -- JSON: original request args (for push-back replay)
    started_at   TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at  TEXT,
    status       TEXT,
    duration_ms  INTEGER,
    error        TEXT,
    PRIMARY KEY (trace_id, step)
);

-- Persistent: large outputs and durable evidence. Tool calls return artifact
-- IDs and bounded excerpts instead of inlining raw logs/diffs/files.
CREATE TABLE IF NOT EXISTS artifacts (
    id               TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    title            TEXT NOT NULL,
    producer         TEXT NOT NULL DEFAULT '',
    session_id       TEXT NOT NULL DEFAULT '',
    trace_id         TEXT NOT NULL DEFAULT '',
    content_type     TEXT NOT NULL DEFAULT 'text/plain',
    size_bytes       INTEGER NOT NULL,
    sha256           TEXT NOT NULL,
    metadata         TEXT NOT NULL DEFAULT '{}',
    source_artifacts TEXT NOT NULL DEFAULT '[]',
    content          TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    ttl              TEXT
);

CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON artifacts(kind, created_at);
CREATE INDEX IF NOT EXISTS idx_artifacts_producer ON artifacts(producer, created_at);
"""


class BusDB:
    """SQLite database for the bus service."""

    def __init__(self, db_path: str = "data/bus.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def conn(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    # -- installed_agents --

    def install_agent(
        self,
        agent_id: str,
        agent_type: str,
        command: str,
        args: list[str],
        cwd: str,
        config: str,
    ) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO installed_agents (id, agent_type, command, args, cwd, config) VALUES (?, ?, ?, ?, ?, ?)",
                (agent_id, agent_type, command, json.dumps(args), cwd, config),
            )

    def uninstall_agent(self, agent_id: str) -> bool:
        with self.conn() as c:
            r = c.execute("DELETE FROM installed_agents WHERE id = ?", (agent_id,))
            return r.rowcount > 0

    def get_installed_agents(self) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM installed_agents").fetchall()
            return [_row_to_dict(r) for r in rows]

    def get_installed_agent(self, agent_id: str) -> dict[str, Any] | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM installed_agents WHERE id = ?", (agent_id,)).fetchone()
            return _row_to_dict(r) if r else None

    # -- registrations --

    def register_agent(
        self,
        agent_id: str,
        agent_type: str,
        callback_url: str,
        pid: int,
        version: str = "",
        skills: list[dict] | None = None,
        collaborations: list[dict] | None = None,
    ) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO registrations (id, agent_type, callback_url, pid, version) VALUES (?, ?, ?, ?, ?)",
                (agent_id, agent_type, callback_url, pid, version),
            )
            # Rebuild skills for this agent
            c.execute("DELETE FROM skills WHERE agent_id = ?", (agent_id,))
            for skill in (skills or []):
                c.execute(
                    "INSERT INTO skills (agent_id, name, description, parameters, since) VALUES (?, ?, ?, ?, ?)",
                    (
                        agent_id,
                        skill["name"],
                        skill.get("description", ""),
                        json.dumps(skill.get("parameters", {})),
                        skill.get("since", ""),
                    ),
                )
            # Rebuild flows declared by this agent
            c.execute("DELETE FROM flows WHERE declared_by = ?", (agent_id,))
            for collab in (collaborations or []):
                c.execute(
                    "INSERT INTO flows (name, declared_by, description, requires, steps) VALUES (?, ?, ?, ?, ?)",
                    (
                        collab["name"],
                        agent_id,
                        collab.get("description", ""),
                        json.dumps(collab.get("requires", [])),
                        json.dumps(collab.get("steps", [])),
                    ),
                )

    def deregister_agent(self, agent_id: str) -> bool:
        with self.conn() as c:
            c.execute("DELETE FROM skills WHERE agent_id = ?", (agent_id,))
            c.execute("DELETE FROM flows WHERE declared_by = ?", (agent_id,))
            r = c.execute("DELETE FROM registrations WHERE id = ?", (agent_id,))
            return r.rowcount > 0

    def heartbeat(self, agent_id: str) -> bool:
        with self.conn() as c:
            r = c.execute(
                "UPDATE registrations SET last_heartbeat = datetime('now'), status = 'healthy' WHERE id = ?",
                (agent_id,),
            )
            return r.rowcount > 0

    def set_agent_status(self, agent_id: str, status: str) -> None:
        with self.conn() as c:
            c.execute(
                "UPDATE registrations SET status = ? WHERE id = ?",
                (status, agent_id),
            )

    def get_registrations(self) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM registrations").fetchall()
            return [_row_to_dict(r) for r in rows]

    def get_registration(self, agent_id: str) -> dict[str, Any] | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM registrations WHERE id = ?", (agent_id,)).fetchone()
            return _row_to_dict(r) if r else None

    def get_healthy_agent_for_type(self, agent_type: str) -> dict[str, Any] | None:
        """Find a healthy registered agent of the given type."""
        with self.conn() as c:
            r = c.execute(
                "SELECT * FROM registrations WHERE agent_type = ? AND status = 'healthy' ORDER BY last_heartbeat DESC LIMIT 1",
                (agent_type,),
            ).fetchone()
            return _row_to_dict(r) if r else None

    # -- skills --

    def get_skills(self, agent_id: str | None = None) -> list[dict[str, Any]]:
        with self.conn() as c:
            if agent_id:
                rows = c.execute("SELECT * FROM skills WHERE agent_id = ?", (agent_id,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM skills").fetchall()
            return [_row_to_dict(r) for r in rows]

    # -- flows --

    def get_flows(self) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM flows").fetchall()
            return [_row_to_dict(r) for r in rows]

    def get_flow(self, name: str) -> dict[str, Any] | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM flows WHERE name = ?", (name,)).fetchone()
            return _row_to_dict(r) if r else None

    # -- messages (pub/sub) --

    def publish_message(self, topic: str, payload: Any, source: str) -> int:
        with self.conn() as c:
            seq = c.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE topic = ?",
                (topic,),
            ).fetchone()[0]
            c.execute(
                "INSERT INTO messages (topic, payload, source, sequence) VALUES (?, ?, ?, ?)",
                (topic, json.dumps(payload), source, seq),
            )
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_messages(
        self, topic: str, after_id: int = 0, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM messages WHERE topic = ? AND id > ? ORDER BY id LIMIT ?",
                (topic, after_id, limit),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]

    def ack_message(self, subscriber_id: str, topic: str, message_id: int) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO subscriptions (subscriber_id, topic, last_acked_id) VALUES (?, ?, ?)",
                (subscriber_id, topic, message_id),
            )

    LIST_TOPICS_LIMIT_CAP = 1000

    @staticmethod
    def _glob_escape(value: str) -> str:
        """Escape SQLite ``GLOB`` metacharacters in a literal-match
        prefix. ``GLOB`` treats ``*``, ``?``, and ``[`` specially;
        bracket-escape each so the prefix matches verbatim. ``[`` is
        bracket-quoted as ``[[]`` so it doesn't open a character
        class. ``GLOB`` has no ``ESCAPE`` clause (unlike ``LIKE``),
        so this is the standard escape strategy."""
        out = []
        for c in value:
            if c in "*?[":
                out.append(f"[{c}]")
            else:
                out.append(c)
        return "".join(out)

    def list_topics(
        self, prefix: str = "", limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Catalog every topic ever published with summary metadata.

        Mirrors the shape of the bus's other read surfaces
        (:meth:`get_skills` / :meth:`get_flows`, exposed on the
        server as ``GET /v1/skills`` / ``GET /v1/flows``) for the
        event surface so an MCP caller can introspect what's flowing
        on the bus without reading source. Closes the introspection-
        layer gap surfaced in dog_ce53165f and tracked by
        ``fr_bus_7b2d41d2``.

        Args:
            prefix: Optional namespace filter (e.g. ``"github."``).
                Case-sensitive literal-match — implemented with
                SQLite ``GLOB`` (case-sensitive without any global
                pragma) plus bracket-escaping of ``GLOB``
                metacharacters (``*``, ``?``, ``[``) so a prefix
                containing those characters matches verbatim rather
                than as a glob pattern. Empty matches all.
            limit: Cap on rows returned, ordered by ``last_fired_at``
                DESC so the most recently active topics come first.
                Clamped to ``[1, LIST_TOPICS_LIMIT_CAP]`` (1000) so the
                public ``GET /v1/topics`` endpoint can't be coerced
                into an unbounded scan via a very large or negative
                value (``LIMIT -1`` disables the limit in SQLite —
                explicitly defended against here).

        Each row is::

            {
                "topic": str,
                "count": int,                   # total messages on this topic
                "first_fired_at": str,          # earliest created_at
                "last_fired_at": str,           # most-recent created_at
                "producers": list[str],         # distinct source values, deduped
            }
        """
        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 200
        clamped_limit = max(1, min(limit_int, self.LIST_TOPICS_LIMIT_CAP))
        # Aggregate per-topic stats and producers in two stages so the
        # ``producers`` aggregate is built from the DISTINCT source set
        # only — not from every message on the topic. On a high-volume
        # topic (millions of messages, few distinct producers) a flat
        # ``GROUP_CONCAT`` over the full table produced a huge
        # intermediate string. The CTE narrows the inner aggregate to
        # the distinct sources first; the per-topic correlated
        # subquery joins them back. With ``idx_messages_topic_created``
        # each subquery is an indexed lookup.
        #
        # Producers come back as a real JSON array via
        # ``json_group_array`` rather than a separator-joined string.
        # That removes any in-band-separator collision risk —
        # ``PublishRequest.source`` is an unconstrained string, and an
        # earlier 0x1F-separator design could have corrupted the
        # producers list if a source value happened to contain that
        # byte. JSON array encoding sidesteps the entire class of
        # problem at zero readability cost on the consumer side.
        with self.conn() as c:
            sql = """
                WITH topic_stats AS (
                    SELECT topic,
                           COUNT(*) AS count,
                           MIN(created_at) AS first_fired_at,
                           MAX(created_at) AS last_fired_at
                    FROM messages
                    {where}
                    GROUP BY topic
                    ORDER BY last_fired_at DESC
                    LIMIT ?
                )
                SELECT t.topic,
                       t.count,
                       t.first_fired_at,
                       t.last_fired_at,
                       (SELECT json_group_array(s)
                          FROM (SELECT DISTINCT source AS s
                                  FROM messages
                                 WHERE topic = t.topic)) AS producers
                FROM topic_stats t
                ORDER BY t.last_fired_at DESC
            """
            params: list[Any] = []
            where = ""
            if prefix:
                where = "WHERE topic GLOB ? || '*'"
                params.append(self._glob_escape(prefix))
            params.append(clamped_limit)
            rows = c.execute(sql.format(where=where), params).fetchall()
            return [
                {
                    "topic": r["topic"],
                    "count": r["count"],
                    "first_fired_at": r["first_fired_at"],
                    "last_fired_at": r["last_fired_at"],
                    "producers": sorted(
                        json.loads(r["producers"]) if r["producers"] else []
                    ),
                }
                for r in rows
            ]

    def get_last_acked(self, subscriber_id: str, topic: str) -> int:
        with self.conn() as c:
            r = c.execute(
                "SELECT last_acked_id FROM subscriptions WHERE subscriber_id = ? AND topic = ?",
                (subscriber_id, topic),
            ).fetchone()
            return r[0] if r else 0

    def find_earliest_unacked(
        self, subscriber_id: str, topics: list[str] | None = None,
        min_id: int = 0,
    ) -> dict[str, Any] | None:
        """Return the earliest message the subscriber has not acked.

        Single-query replacement for per-topic loops. If ``topics`` is
        non-empty, restricts to those topics. Otherwise matches any.

        ``min_id`` is a per-call floor: the matcher requires
        ``m.id > MAX(last_acked_id, min_id)``. Used by
        ``Bus.wait_for_event`` to implement ``cursor='now'`` semantics
        — a fresh subscriber pinned to the current high-water mark
        won't replay backlog. Defaults to 0 (no floor) so existing
        replay-from-ack behaviour is unchanged.
        """
        with self.conn() as c:
            if topics:
                placeholders = ",".join("?" for _ in topics)
                sql = f"""
                    SELECT m.* FROM messages m
                    LEFT JOIN subscriptions s
                        ON s.subscriber_id = ? AND s.topic = m.topic
                    WHERE m.topic IN ({placeholders})
                      AND m.id > MAX(COALESCE(s.last_acked_id, 0), ?)
                    ORDER BY m.id ASC LIMIT 1
                """
                params: list[Any] = [subscriber_id, *topics, min_id]
            else:
                sql = """
                    SELECT m.* FROM messages m
                    LEFT JOIN subscriptions s
                        ON s.subscriber_id = ? AND s.topic = m.topic
                    WHERE m.id > MAX(COALESCE(s.last_acked_id, 0), ?)
                    ORDER BY m.id ASC LIMIT 1
                """
                params = [subscriber_id, min_id]
            r = c.execute(sql, params).fetchone()
            return _row_to_dict(r) if r else None

    def max_message_id(self) -> int:
        """Current high-water mark across all topics, or 0 when empty.

        Used by ``Bus.wait_for_event`` to snapshot the floor for
        ``cursor='now'`` callers so they skip pre-existing backlog.
        """
        with self.conn() as c:
            r = c.execute("SELECT MAX(id) FROM messages").fetchone()
            return (r[0] or 0) if r else 0

    def add_dead_letter(
        self,
        topic: str,
        payload: Any,
        source: str,
        agent_id: str = "",
        operation: str = "",
        error: str = "",
        attempts: int = 0,
    ) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT INTO dead_letters (topic, payload, source, agent_id, operation, error, attempts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (topic, json.dumps(payload), source, agent_id, operation, error, attempts),
            )

    # -- sessions --

    def create_session(self, session_id: str, agent_id: str) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT INTO sessions (id, agent_id, status) VALUES (?, ?, 'active')",
                (session_id, agent_id),
            )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
            return _row_to_dict(r) if r else None

    def update_session(
        self,
        session_id: str,
        status: str | None = None,
        public_ctx: str | None = None,
        private_ctx: str | None = None,
    ) -> None:
        with self.conn() as c:
            parts = ["updated_at = datetime('now')"]
            params: list[Any] = []
            if status is not None:
                parts.append("status = ?")
                params.append(status)
            if public_ctx is not None:
                parts.append("public_ctx = ?")
                params.append(public_ctx)
            if private_ctx is not None:
                parts.append("private_ctx = ?")
                params.append(private_ctx)
            params.append(session_id)
            c.execute(f"UPDATE sessions SET {', '.join(parts)} WHERE id = ?", params)

    # -- traces --

    def record_trace_step(
        self,
        trace_id: str,
        step: int,
        agent_id: str = "",
        operation: str = "",
        args: dict | None = None,
    ) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO traces (trace_id, step, agent_id, operation, args) VALUES (?, ?, ?, ?, ?)",
                (trace_id, step, agent_id, operation, json.dumps(args) if args else None),
            )

    def finish_trace_step(
        self,
        trace_id: str,
        step: int,
        status: str,
        duration_ms: int,
        error: str = "",
    ) -> None:
        with self.conn() as c:
            c.execute(
                "UPDATE traces SET finished_at = datetime('now'), status = ?, duration_ms = ?, error = ? WHERE trace_id = ? AND step = ?",
                (status, duration_ms, error, trace_id, step),
            )

    def get_trace(self, trace_id: str) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM traces WHERE trace_id = ? ORDER BY step",
                (trace_id,),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]


    # -- gaps --

    def report_gap(
        self,
        agent_id: str,
        operation: str,
        reason: str,
        context: dict | None = None,
    ) -> int:
        with self.conn() as c:
            c.execute(
                "INSERT INTO gaps (agent_id, operation, reason, context) VALUES (?, ?, ?, ?)",
                (agent_id, operation, reason, json.dumps(context or {})),
            )
            gap_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.report_feedback(
            agent_id=agent_id,
            kind="gap",
            operation=operation,
            message=reason,
            context=context or {},
        )
        return gap_id

    def get_gaps(self, status: str = "open") -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM gaps WHERE status = ? ORDER BY created_at DESC",
                (status,),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]

    def update_gap_status(self, gap_id: int, status: str) -> None:
        with self.conn() as c:
            c.execute("UPDATE gaps SET status = ? WHERE id = ?", (status, gap_id))

    # -- structured feedback --

    def report_feedback(
        self,
        *,
        agent_id: str,
        kind: str,
        message: str,
        operation: str = "",
        area: str = "",
        category: str = "",
        severity: str = "",
        context: dict | None = None,
        suggestion: str = "",
        fingerprint: str = "",
    ) -> dict[str, Any]:
        context = context or {}
        fingerprint = fingerprint or _feedback_fingerprint(
            agent_id=agent_id,
            kind=kind,
            operation=operation,
            area=area,
            category=category,
            message=message,
        )
        with self.conn() as c:
            existing = c.execute(
                """
                SELECT id, count FROM feedback_reports
                WHERE fingerprint = ? AND status = 'open'
                ORDER BY created_at DESC LIMIT 1
                """,
                (fingerprint,),
            ).fetchone()
            if existing:
                c.execute(
                    """
                    UPDATE feedback_reports
                    SET count = count + 1,
                        updated_at = datetime('now'),
                        context = ?,
                        suggestion = CASE WHEN ? != '' THEN ? ELSE suggestion END
                    WHERE id = ?
                    """,
                    (json.dumps(context), suggestion, suggestion, existing["id"]),
                )
                return {"feedback_id": existing["id"], "status": "deduped", "count": existing["count"] + 1}

            c.execute(
                """
                INSERT INTO feedback_reports (
                    agent_id, kind, operation, area, category, severity,
                    message, context, suggestion, fingerprint
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    agent_id,
                    kind,
                    operation,
                    area,
                    category,
                    severity,
                    message,
                    json.dumps(context),
                    suggestion,
                    fingerprint,
                ),
            )
            return {"feedback_id": c.execute("SELECT last_insert_rowid()").fetchone()[0], "status": "open", "count": 1}

    def get_feedback(
        self,
        *,
        agent_id: str = "",
        kind: str = "",
        status: str = "open",
        since: str = "",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        clauses = []
        params: list[Any] = []
        if agent_id:
            clauses.append("agent_id = ?")
            params.append(agent_id)
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if since:
            clauses.append("created_at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.conn() as c:
            rows = c.execute(
                f"""
                SELECT * FROM feedback_reports {where}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]

    # -- evaluations --

    def record_evaluation(
        self,
        trace_id: str,
        verdict: str,
        reason: str = "",
        retry_with: dict | None = None,
    ) -> int:
        with self.conn() as c:
            c.execute(
                "INSERT INTO evaluations (trace_id, verdict, reason, retry_with) VALUES (?, ?, ?, ?)",
                (trace_id, verdict, reason, json.dumps(retry_with or {})),
            )
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_evaluations(self, trace_id: str) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM evaluations WHERE trace_id = ? ORDER BY created_at",
                (trace_id,),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    # Parse JSON fields
    for key in (
        "args", "parameters", "requires", "steps", "payload", "context",
        "retry_with", "metadata", "source_artifacts",
    ):
        if key in d and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


def _feedback_fingerprint(
    *,
    agent_id: str,
    kind: str,
    operation: str,
    area: str,
    category: str,
    message: str,
) -> str:
    parts = [
        agent_id.strip(),
        kind.strip(),
        operation.strip(),
        area.strip(),
        category.strip(),
        " ".join(message.lower().split())[:200],
    ]
    return "|".join(parts)
