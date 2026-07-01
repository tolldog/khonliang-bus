"""File tailer for the L0 agent log files — the forwarding correctness core.

Rules (docs/log-agent-design.md, from the adversarial design review):
- The offset only ever advances to the last ``\\n``; a partial tail line is
  carried to the next sweep, never ingested as a fragment (▸R10).
- On inode change (the bus's rotate-at-start renamed the file), the OLD file's
  ``[offset, EOF)`` remainder is drained from ``<name>.log.1`` BEFORE resetting
  to the new file at 0 — otherwise a supervisor restart during a log-agent
  outage silently drops the whole unforwarded backlog (▸R1).
- Same inode with ``size < offset`` → copytruncate rotation; reset to 0.
- Parse failure never drops a line (fallback ``ts=ingest-time, level=None``).
"""

from __future__ import annotations

import logging
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

from bus.log_agent.substrate import LogRecord, SqliteSubstrate

logger = logging.getLogger(__name__)

#: Matches the fleet-standard ``%(asctime)s %(name)s %(levelname)s ...`` shape.
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s+\S+\s+"
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\b"
)


def _agent_id_from_filename(name: str) -> str:
    """Invert the bus's percent-encoded ``_log_file_name`` mapping."""
    stem = name[:-4] if name.endswith(".log") else name
    return urllib.parse.unquote(stem)


def _parse_line(line: str, agent_id: str, now: float) -> LogRecord:
    m = _LINE_RE.match(line)
    ts = now
    level = None
    if m:
        level = m.group("level")
        try:
            raw_ts = m.group("ts").replace(",", ".").replace("T", " ")
            ts = datetime.fromisoformat(raw_ts).timestamp()
        except ValueError:
            ts = now
    return LogRecord(ts=ts, agent_id=agent_id, level=level, message=line)


class Tailer:
    """Poll-based tailer over ``<log_dir>/*.log`` with persisted offsets."""

    def __init__(self, log_dir: str | Path, substrate: SqliteSubstrate):
        self.log_dir = Path(log_dir)
        self.substrate = substrate

    def sweep(self) -> int:
        """One pass over every log file; returns lines ingested."""
        total = 0
        if not self.log_dir.is_dir():
            return 0
        for path in sorted(self.log_dir.glob("*.log")):
            try:
                total += self._sweep_file(path)
            except OSError as e:
                # One unreadable file must not stall the rest of the fleet.
                logger.warning("tail %s failed: %s", path.name, e)
        return total

    def _sweep_file(self, path: Path) -> int:
        agent_id = _agent_id_from_filename(path.name)
        st = path.stat()
        stored = self.substrate.get_offset(str(path))
        ingested = 0

        if stored is None:
            inode, offset = st.st_ino, 0
        else:
            inode, offset = stored
            if inode != st.st_ino:
                # Rotated (rename): drain the old inode's remainder from .log.1
                # BEFORE resetting, else the unforwarded backlog is lost (▸R1).
                ingested += self._drain_rotated(path, inode, offset, agent_id)
                inode, offset = st.st_ino, 0
            elif st.st_size < offset:
                # Same inode, shrunk: copytruncate-style rotation.
                offset = 0

        new_offset, records = self._read_complete_lines(path, offset, agent_id)
        if records:
            ingested += self.substrate.ingest(records)
        if stored is None or (inode, new_offset) != stored:
            self.substrate.set_offset(str(path), inode, new_offset)
        return ingested

    def _drain_rotated(self, path: Path, old_inode: int, offset: int, agent_id: str) -> int:
        rotated = path.parent / (path.name + ".1")
        try:
            if rotated.stat().st_ino != old_inode:
                # A different (older) rotation generation — the backlog is gone.
                self.substrate.record_drop()
                return 0
        except OSError:
            self.substrate.record_drop()
            return 0
        _, records = self._read_complete_lines(rotated, offset, agent_id, drain_to_eof=True)
        return self.substrate.ingest(records) if records else 0

    def _read_complete_lines(
        self, path: Path, offset: int, agent_id: str, *, drain_to_eof: bool = False
    ) -> tuple[int, list[LogRecord]]:
        """Read from ``offset``, stopping at the LAST newline (▸R10) — unless
        draining a rotated file, where EOF is final (no more writes coming, so
        a trailing fragment is a complete-enough last line)."""
        with open(path, "rb") as f:
            f.seek(offset)
            chunk = f.read()
        if not chunk:
            return offset, []
        if drain_to_eof:
            consumed = len(chunk)
        else:
            last_nl = chunk.rfind(b"\n")
            if last_nl < 0:
                return offset, []  # only a partial line so far — carry it
            consumed = last_nl + 1
        now = time.time()
        records = [
            _parse_line(raw.decode("utf-8", errors="replace"), agent_id, now)
            for raw in chunk[:consumed].splitlines()
            if raw.strip()
        ]
        return offset + consumed, records
