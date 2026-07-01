"""The log agent's bus-facing loop.

Speaks the bus WS protocol exactly as verified against ``handle_agent_ws``
(docs/log-agent-design.md): register (with EXPLICIT agent_type — the id-derived
default for ``log-agent`` would be ``log``), heartbeat every ~20s (staleness
threshold is 90s), answer ``{"type": "request"}`` with the payload nested under
``"result"``, tolerate unknown message types. Its own logging is WARNING+ —
its stdout lands in a file inside the very glob it tails, so chatty per-record
logging would self-amplify.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import websockets

from bus.log_agent.substrate import LogRecord, SqliteSubstrate
from bus.log_agent.tailer import Tailer

logger = logging.getLogger("log_agent")

HEARTBEAT_INTERVAL_S = 20.0
SWEEP_INTERVAL_S = 0.5
RETENTION_INTERVAL_S = 86400.0

SKILLS = [
    {
        "name": "log_query",
        "description": (
            "Raw fleet log query against the substrate: filter by agent_id, "
            "time window (since/until, epoch seconds), level, substring "
            "pattern; newest first, limit default 200."
        ),
    },
    {
        "name": "substrate_status",
        "description": "Substrate health: type, reachability, ingest lag, drops, retention.",
    },
    {
        "name": "self_test",
        "description": "Forward+query round-trip: ingest a probe line, query it back.",
    },
    {
        "name": "health_check",
        "description": "Cheap liveness probe (bus_diagnose compatibility).",
    },
]

WELCOME = {
    "agent_type": "log-agent",
    "role": "forwards fleet L0 log files to the substrate; raw log query for debugging",
    "mission": (
        "the debugging path that works when everything else is on fire — "
        "raw before distilled, files before substrate"
    ),
    "boundaries": "no distillation/aggregation (that's the best-effort layer above); single host",
}


class LogAgent:
    def __init__(
        self,
        agent_id: str,
        bus_url: str,
        log_dir: str,
        substrate: SqliteSubstrate | None = None,
    ):
        self.agent_id = agent_id
        self.bus_url = bus_url
        self.log_dir = log_dir
        self.substrate = substrate or SqliteSubstrate(
            os.path.join(log_dir, "substrate.db"),
            retention_days=float(os.environ.get("KHONLIANG_LOG_RETENTION_DAYS", 14)),
        )
        self.tailer = Tailer(log_dir, self.substrate)
        self._stop = asyncio.Event()

    # -- skills (synchronous; sqlite ops are fast) --

    def skill_log_query(self, args: dict[str, Any]) -> dict[str, Any]:
        rows = self.substrate.query(
            agent_id=args.get("agent_id") or None,
            since=float(args.get("since", 0.0)),
            until=float(args["until"]) if args.get("until") is not None else None,
            level=args.get("level") or None,
            pattern=args.get("pattern") or None,
            limit=int(args.get("limit", 200)),
        )
        return {"lines": rows, "count": len(rows)}

    def skill_substrate_status(self, args: dict[str, Any]) -> dict[str, Any]:
        return self.substrate.status()

    def skill_self_test(self, args: dict[str, Any]) -> dict[str, Any]:
        probe = f"self-test probe {time.time():.6f}"
        self.substrate.ingest(
            [LogRecord(ts=time.time(), agent_id=self.agent_id, level=None, message=probe)]
        )
        found = self.substrate.query(agent_id=self.agent_id, pattern=probe, limit=1)
        return {"ok": bool(found), "probe_found": bool(found), **self.substrate.status()}

    def skill_health_check(self, args: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "agent_id": self.agent_id, "substrate": self.substrate.substrate_type}

    def handle_message(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Dispatch one bus message; return the reply to send (or None).

        Kept synchronous and side-effect-contained so the protocol behavior is
        testable without a socket.
        """
        if data.get("type") != "request":
            return None  # pong / learnings / unknown types — tolerate silently
        cid = data.get("correlation_id", "")
        op = data.get("operation", "")
        args = data.get("args") or {}
        handler = getattr(self, f"skill_{op}", None)
        if handler is None:
            return {
                "type": "error", "correlation_id": cid,
                "error": f"unknown operation {op!r}", "retryable": False,
            }
        try:
            # The bus unwraps result.get("result") — nesting is mandatory.
            return {"type": "response", "correlation_id": cid, "result": handler(args)}
        except Exception as e:  # a bad query must not kill the agent loop
            logger.warning("skill %s failed: %s", op, e)
            return {"type": "error", "correlation_id": cid, "error": str(e), "retryable": True}

    def register_payload(self) -> dict[str, Any]:
        return {
            "type": "register",
            "id": self.agent_id,
            # Explicit: the bus's id-derived default for "log-agent" is "log".
            "agent_type": "log-agent",
            "pid": os.getpid(),
            "version": "0.1.0",
            "skills": SKILLS,
            "collaborations": [],
            "welcome": WELCOME,
        }

    # -- connection --

    def _ws_target(self) -> tuple[str, str | None]:
        """(uri, uds_path) for the bus WS per the transport contract."""
        if self.bus_url.startswith("unix://"):
            return "ws://localhost/v1/agent", self.bus_url[len("unix://"):]
        base = self.bus_url.replace("https://", "wss://").replace("http://", "ws://")
        return base.rstrip("/") + "/v1/agent", None

    async def _connect(self):
        uri, uds = self._ws_target()
        if uds is not None:
            return await websockets.unix_connect(uds, uri=uri)
        return await websockets.connect(uri)

    async def _session(self) -> None:
        """One connected session: register, then serve until disconnect."""
        ws = await self._connect()
        try:
            await ws.send(json.dumps(self.register_payload()))
            ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if ack.get("type") != "registered":
                raise RuntimeError(f"register rejected: {ack}")
            logger.warning("log-agent registered (bus=%s, dir=%s)", self.bus_url, self.log_dir)

            async def _heartbeats():
                while True:
                    await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                    await ws.send(json.dumps({"type": "heartbeat"}))

            async def _sweeps():
                last_retention = time.monotonic()
                while True:
                    # to_thread: a large backlog sweep must not block the
                    # heartbeat/request loop past the 90s staleness window.
                    await asyncio.to_thread(self.tailer.sweep)
                    if time.monotonic() - last_retention > RETENTION_INTERVAL_S:
                        await asyncio.to_thread(self.substrate.enforce_retention)
                        last_retention = time.monotonic()
                    await asyncio.sleep(SWEEP_INTERVAL_S)

            async def _messages():
                async for raw in ws:
                    try:
                        reply = self.handle_message(json.loads(raw))
                    except json.JSONDecodeError:
                        continue
                    if reply is not None:
                        await ws.send(json.dumps(reply))

            tasks = [
                asyncio.create_task(coro())
                for coro in (_messages, _heartbeats, _sweeps)
            ]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    t.result()  # surface the terminating exception (disconnect)
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await ws.close()

    async def run(self) -> None:
        """Serve forever, reconnecting with backoff — the log path must outlive
        bus restarts (files keep queueing either way)."""
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._session()
                backoff = 1.0
            except Exception as e:
                logger.warning("bus session ended (%s); reconnecting in %.0fs", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
