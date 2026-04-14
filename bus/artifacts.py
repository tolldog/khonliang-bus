"""Artifact storage and bounded retrieval helpers.

Artifacts are the bus' context firewall: large logs, diffs, command output,
file snapshots, and session handoffs are stored durably, while MCP callers get
small summaries and explicit references.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from typing import Any

from bus.db import BusDB, _row_to_dict


DEFAULT_MAX_CHARS = 4000
HARD_MAX_CHARS = 20000
MAX_ARTIFACT_BYTES = 10 * 1024 * 1024  # 10 MiB per artifact


@dataclass(frozen=True)
class BoundedText:
    """A bounded text view over an artifact."""

    text: str
    truncated: bool
    start_line: int | None = None
    end_line: int | None = None


class ArtifactStore:
    """Store artifact metadata/content and provide bounded text views."""

    def __init__(self, db: BusDB):
        self.db = db

    def create(
        self,
        *,
        kind: str,
        title: str,
        content: str,
        producer: str = "",
        session_id: str = "",
        trace_id: str = "",
        content_type: str = "text/plain",
        metadata: dict[str, Any] | None = None,
        source_artifacts: list[str] | None = None,
        artifact_id: str = "",
        ttl: str | None = None,
    ) -> dict[str, Any]:
        """Create an artifact and return its metadata.

        Content is currently stored in SQLite. The API intentionally exposes
        only bounded retrieval helpers so the backend can move to filesystem
        blobs later without changing callers.
        """
        if not kind:
            raise ValueError("kind is required")
        if not title:
            raise ValueError("title is required")
        artifact_id = artifact_id or f"art_{uuid.uuid4().hex[:12]}"
        raw = content.encode("utf-8")
        if len(raw) > MAX_ARTIFACT_BYTES:
            raise ValueError(
                f"content exceeds maximum size of {MAX_ARTIFACT_BYTES} bytes"
            )
        with self.db.conn() as c:
            c.execute(
                """
                INSERT INTO artifacts (
                    id, kind, title, producer, session_id, trace_id,
                    content_type, size_bytes, sha256, metadata,
                    source_artifacts, content, ttl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, json(?), json(?), ?, ?)
                """,
                (
                    artifact_id,
                    kind,
                    title,
                    producer,
                    session_id,
                    trace_id,
                    content_type,
                    len(raw),
                    hashlib.sha256(raw).hexdigest(),
                    _json_dumps(metadata or {}),
                    _json_dumps(source_artifacts or []),
                    content,
                    ttl,
                ),
            )
        meta = self.metadata(artifact_id)
        assert meta is not None
        return meta

    def metadata(self, artifact_id: str) -> dict[str, Any] | None:
        """Return artifact metadata without content."""
        with self.db.conn() as c:
            row = c.execute(
                """
                SELECT id, kind, title, producer, session_id, trace_id,
                       content_type, size_bytes, sha256, metadata,
                       source_artifacts, created_at, ttl
                FROM artifacts WHERE id = ?
                """,
                (artifact_id,),
            ).fetchone()
            return _row_to_dict(row) if row else None

    def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """List artifact metadata, newest first."""
        limit = max(1, min(int(limit), 100))
        clauses = []
        params: list[Any] = []
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if producer:
            clauses.append("producer = ?")
            params.append(producer)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.db.conn() as c:
            rows = c.execute(
                f"""
                SELECT id, kind, title, producer, session_id, trace_id,
                       content_type, size_bytes, sha256, metadata,
                       source_artifacts, created_at, ttl
                FROM artifacts {where}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]

    def get(self, artifact_id: str, *, offset: int = 0, max_chars: int = DEFAULT_MAX_CHARS) -> BoundedText:
        """Return a bounded character window."""
        content = self._content(artifact_id)
        offset = max(0, int(offset))
        max_chars = _clamp_max_chars(max_chars)
        text = content[offset:offset + max_chars]
        return BoundedText(text=text, truncated=offset + max_chars < len(content))

    def head(self, artifact_id: str, *, lines: int = 80, max_chars: int = DEFAULT_MAX_CHARS) -> BoundedText:
        """Return a bounded number of lines from the start."""
        content_lines = self._content(artifact_id).splitlines()
        line_count = _clamp_lines(lines)
        selected = content_lines[:line_count]
        return _bound_lines(selected, max_chars, truncated=len(content_lines) > line_count)

    def tail(self, artifact_id: str, *, lines: int = 80, max_chars: int = DEFAULT_MAX_CHARS) -> BoundedText:
        """Return a bounded number of lines from the end."""
        content_lines = self._content(artifact_id).splitlines()
        line_count = _clamp_lines(lines)
        selected = content_lines[-line_count:] if line_count else []
        start = max(1, len(content_lines) - len(selected) + 1)
        result = _bound_lines(selected, max_chars, truncated=len(content_lines) > line_count)
        return BoundedText(result.text, result.truncated, start, len(content_lines))

    def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = DEFAULT_MAX_CHARS,
    ) -> BoundedText:
        """Return an inclusive line range, bounded by max_chars."""
        content_lines = self._content(artifact_id).splitlines()
        start_line = max(1, int(start_line))
        end_line = max(start_line, int(end_line))
        selected = content_lines[start_line - 1:end_line]
        result = _bound_lines(selected, max_chars, truncated=end_line < len(content_lines))
        return BoundedText(result.text, result.truncated, start_line, min(end_line, len(content_lines)))

    def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        """Return bounded context around regex matches."""
        if not pattern:
            raise ValueError("pattern is required")
        try:
            regex = re.compile(pattern)
        except re.error as e:
            raise ValueError(f"invalid regex pattern: {e}") from e
        lines = self._content(artifact_id).splitlines()
        context_lines = max(0, min(int(context_lines), 50))
        max_matches = max(1, min(int(max_matches), 100))

        blocks: list[str] = []
        matches = 0
        for idx, line in enumerate(lines):
            if not regex.search(line):
                continue
            matches += 1
            if len(blocks) < max_matches:
                start = max(0, idx - context_lines)
                end = min(len(lines), idx + context_lines + 1)
                blocks.append(
                    f"--- match {matches} lines {start + 1}-{end} ---\n"
                    + "\n".join(lines[start:end])
                )

        bounded = _bound_text("\n\n".join(blocks), max_chars, truncated=matches > len(blocks))
        return {
            "text": bounded.text,
            "truncated": bounded.truncated,
            "matches": matches,
            "returned_matches": len(blocks),
        }

    def distill(
        self,
        artifact_id: str,
        *,
        mode: str = "brief",
        purpose: str = "",
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        """Create a deterministic bounded distillation artifact.

        This is intentionally simple in the first slice. Later work can route
        by artifact kind to LLM or parser-specific distillers.
        """
        source = self.metadata(artifact_id)
        if source is None:
            raise KeyError(artifact_id)
        content = self._content(artifact_id)
        digest = _distill_text(content, mode=mode, purpose=purpose, max_chars=max_chars)
        distilled = self.create(
            kind="distillation",
            title=f"Distillation of {source['title']}",
            content=digest,
            producer="bus",
            session_id=source.get("session_id", ""),
            trace_id=source.get("trace_id", ""),
            content_type="text/plain",
            metadata={
                "mode": mode,
                "purpose": purpose,
                "source_kind": source.get("kind", ""),
            },
            source_artifacts=[artifact_id],
        )
        return {"source_artifact": artifact_id, "distilled_artifact": distilled, "digest": digest}

    def distill_many(
        self,
        artifact_ids: list[str],
        *,
        purpose: str = "",
        max_chars: int = 8000,
    ) -> dict[str, Any]:
        """Distill several artifacts into one bounded handoff artifact."""
        if not artifact_ids:
            raise ValueError("artifact_ids is required")
        sections = []
        for aid in artifact_ids:
            meta = self.metadata(aid)
            if meta is None:
                raise KeyError(aid)
            text = _distill_text(
                self._content(aid),
                mode="brief",
                purpose=f"{purpose} source={aid}",
                max_chars=max(500, max_chars // len(artifact_ids)),
            )
            sections.append(f"# {meta['title']} ({aid})\n{text}")
        digest = _bound_text("\n\n".join(sections), max_chars, truncated=False).text
        distilled = self.create(
            kind="distillation",
            title="Distillation of multiple artifacts",
            content=digest,
            producer="bus",
            content_type="text/plain",
            metadata={"purpose": purpose, "source_count": len(artifact_ids)},
            source_artifacts=artifact_ids,
        )
        return {"source_artifacts": artifact_ids, "distilled_artifact": distilled, "digest": digest}

    def _content(self, artifact_id: str) -> str:
        with self.db.conn() as c:
            row = c.execute("SELECT content FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()
            if row is None:
                raise KeyError(artifact_id)
            return str(row["content"])


def view_response(meta: dict[str, Any] | None, view: BoundedText | dict[str, Any]) -> dict[str, Any]:
    """Format a bounded artifact view for HTTP/MCP callers."""
    if meta is None:
        raise KeyError("artifact not found")
    if isinstance(view, BoundedText):
        payload: dict[str, Any] = {
            "artifact": meta,
            "text": view.text,
            "truncated": view.truncated,
        }
        if view.start_line is not None:
            payload["start_line"] = view.start_line
        if view.end_line is not None:
            payload["end_line"] = view.end_line
        return payload
    return {"artifact": meta, **view}


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, sort_keys=True)


def _clamp_max_chars(max_chars: int) -> int:
    return max(1, min(int(max_chars), HARD_MAX_CHARS))


def _clamp_lines(lines: int) -> int:
    return max(0, min(int(lines), 1000))


def _bound_lines(lines: list[str], max_chars: int, *, truncated: bool) -> BoundedText:
    return _bound_text("\n".join(lines), max_chars, truncated=truncated)


def _bound_text(text: str, max_chars: int, *, truncated: bool) -> BoundedText:
    max_chars = _clamp_max_chars(max_chars)
    if len(text) > max_chars:
        return BoundedText(text=text[:max_chars], truncated=True)
    return BoundedText(text=text, truncated=truncated)


def _distill_text(content: str, *, mode: str, purpose: str, max_chars: int) -> str:
    lines = [line.rstrip() for line in content.splitlines()]
    nonblank = [line for line in lines if line.strip()]
    header = [
        f"mode: {mode or 'brief'}",
        f"purpose: {purpose}" if purpose else "purpose: artifact distillation",
        f"source_lines: {len(lines)}",
        f"source_chars: {len(content)}",
        "",
    ]
    if not nonblank:
        body = ["(artifact is empty)"]
    else:
        body = nonblank[:20]
    return _bound_text("\n".join(header + body), max_chars, truncated=len(nonblank) > len(body)).text
