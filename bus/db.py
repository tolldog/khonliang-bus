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

    def get_last_acked(self, subscriber_id: str, topic: str) -> int:
        with self.conn() as c:
            r = c.execute(
                "SELECT last_acked_id FROM subscriptions WHERE subscriber_id = ? AND topic = ?",
                (subscriber_id, topic),
            ).fetchone()
            return r[0] if r else 0

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
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]

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
    for key in ("args", "parameters", "requires", "steps", "payload", "context", "retry_with"):
        if key in d and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return d
