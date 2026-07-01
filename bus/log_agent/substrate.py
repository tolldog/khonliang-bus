"""Substrate adapter contract + the Phase-1 SQLite implementation.

The contract is deliberately tiny (ingest / query / status) so swapping the
substrate (Loki / Elastic / VictoriaLogs — Phase 2) is mechanical. The SQLite
implementation lives in its OWN db file (never the bus DB) so it survives bus
crashes and can be relocated later. Plain LIKE substring matching is
predictable and fast at single-host fleet volume; FTS5 is a Phase-2
optimization if volume ever demands it.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class LogRecord:
    ts: float
    agent_id: str
    level: str | None
    message: str


class SqliteSubstrate:
    """SQLite-backed log store + tail-offset persistence."""

    substrate_type = "sqlite"

    def __init__(self, db_path: str | Path, retention_days: float = 14.0):
        self.db_path = str(db_path)
        self.retention_days = float(retention_days)
        self._drop_count = 0
        with self._conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS log_lines (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts          REAL NOT NULL,
                    agent_id    TEXT NOT NULL,
                    level       TEXT,
                    message     TEXT NOT NULL,
                    ingested_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_log_lines_lookup
                    ON log_lines(agent_id, ts);
                -- Tail bookkeeping lives WITH the data so offset and content
                -- can't diverge across restarts.
                CREATE TABLE IF NOT EXISTS tail_offsets (
                    path   TEXT PRIMARY KEY,
                    inode  INTEGER NOT NULL,
                    offset INTEGER NOT NULL
                );
                """
            )
        self.enforce_retention()

    @contextmanager
    def _conn(self):
        # sqlite3's own context manager commits but never CLOSES — this one does
        # both (the BusDB idiom), so per-call connections can't accumulate.
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        try:
            yield con
            con.commit()
        finally:
            con.close()

    # -- contract --

    def ingest(self, records: list[LogRecord]) -> int:
        if not records:
            return 0
        now = time.time()
        with self._conn() as c:
            c.executemany(
                "INSERT INTO log_lines (ts, agent_id, level, message, ingested_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [(r.ts, r.agent_id, r.level, r.message, now) for r in records],
            )
        return len(records)

    def query(
        self,
        *,
        agent_id: str | None = None,
        since: float = 0.0,
        until: float | None = None,
        level: str | None = None,
        pattern: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses = ["ts >= ?"]
        params: list[Any] = [float(since)]
        if until is not None:
            clauses.append("ts <= ?")
            params.append(float(until))
        if agent_id:
            clauses.append("agent_id = ?")
            params.append(agent_id)
        if level:
            clauses.append("level = ?")
            params.append(level.upper())
        if pattern:
            # Substring match, escaped so user input can't inject wildcards.
            esc = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("message LIKE ? ESCAPE '\\'")
            params.append(f"%{esc}%")
        params.append(max(1, min(int(limit), 5000)))
        with self._conn() as c:
            rows = c.execute(
                f"SELECT ts, agent_id, level, message FROM log_lines "
                f"WHERE {' AND '.join(clauses)} ORDER BY ts DESC, id DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def status(self) -> dict[str, Any]:
        try:
            with self._conn() as c:
                row = c.execute(
                    "SELECT MAX(ingested_at) AS last_ingest, COUNT(*) AS n FROM log_lines"
                ).fetchone()
            last = row["last_ingest"]
            return {
                "substrate_type": self.substrate_type,
                "reachable": True,
                "line_count": row["n"],
                "last_ingest_ts": last,
                "ingest_lag_s": (time.time() - last) if last else None,
                "drop_count_24h": self._drop_count,
                "retention_window_s": int(self.retention_days * 86400),
            }
        except sqlite3.Error as e:
            return {"substrate_type": self.substrate_type, "reachable": False, "error": str(e)}

    def enforce_retention(self) -> int:
        cutoff = time.time() - self.retention_days * 86400
        with self._conn() as c:
            cur = c.execute("DELETE FROM log_lines WHERE ts < ?", (cutoff,))
            return cur.rowcount

    def record_drop(self, n: int = 1) -> None:
        """Diagnostic counter for lines dropped on persistent ingest failure."""
        self._drop_count += n

    # -- tail offsets (used by the tailer) --

    def get_offset(self, path: str) -> tuple[int, int] | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT inode, offset FROM tail_offsets WHERE path = ?", (path,)
            ).fetchone()
        return (row["inode"], row["offset"]) if row else None

    def set_offset(self, path: str, inode: int, offset: int) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO tail_offsets (path, inode, offset) VALUES (?, ?, ?) "
                "ON CONFLICT(path) DO UPDATE SET inode = excluded.inode, offset = excluded.offset",
                (path, inode, offset),
            )
