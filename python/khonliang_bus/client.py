"""Async-friendly Python client for khonliang-bus.

Auto-registers on construction so callers don't need to remember an
explicit registration step. Provides:

  * sync publish/ack/heartbeat (HTTP)
  * async subscribe iterator (WebSocket)
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, Sequence
from urllib.parse import urlparse, urlunparse

import httpx
import websockets

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A message received from the bus."""

    id: str
    topic: str
    payload: Any  # parsed JSON
    timestamp: str

    @classmethod
    def from_wire(cls, data: dict) -> Message:
        return cls(
            id=data.get("id", ""),
            topic=data.get("topic", ""),
            payload=data.get("payload"),
            timestamp=data.get("timestamp", ""),
        )


class BusClient:
    """Synchronous + async client for khonliang-bus.

    Auto-registers with the bus on construction. Pass ``register=False``
    to defer registration if needed.
    """

    def __init__(
        self,
        base_url: str,
        subscriber_id: str,
        topics: Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        register: bool = True,
        timeout: float = 30.0,
    ) -> None:
        if not base_url or not subscriber_id:
            raise ValueError("base_url and subscriber_id required")
        self.base_url = base_url.rstrip("/")
        self.subscriber_id = subscriber_id
        self.topics = list(topics or [])
        self.metadata = dict(metadata or {})
        self._http = httpx.Client(timeout=timeout)

        if register:
            try:
                self.register(self.topics, self.metadata)
            except Exception as exc:
                logger.warning(
                    "Auto-register failed for %s: %s", subscriber_id, exc
                )

    # ----- lifecycle -----

    def close(self) -> None:
        """Deregister from the bus and close the HTTP client."""
        try:
            self.deregister()
        except Exception:
            pass  # best-effort on shutdown
        self._http.close()

    def __enter__(self) -> BusClient:
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ----- registry -----

    def register(
        self,
        topics: Sequence[str],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Register this service with the bus registry."""
        body = {
            "id": self.subscriber_id,
            "name": self.subscriber_id,
            "topics": list(topics),
            "metadata": metadata or {},
        }
        self._post("/v1/register", body)

    def heartbeat(self) -> None:
        """Refresh the registry's last_seen timestamp."""
        self._post("/v1/heartbeat", {"id": self.subscriber_id})

    def services(self) -> list[dict[str, Any]]:
        """List all registered services."""
        resp = self._http.get(f"{self.base_url}/v1/services")
        resp.raise_for_status()
        return resp.json() or []

    # ----- messaging -----

    def publish(self, topic: str, payload: Any) -> str:
        """Publish a message to a topic. Returns the assigned message ID."""
        body = {"topic": topic, "payload": payload}
        resp = self._post("/v1/publish", body)
        return resp.get("id", "")

    def ack(self, message_id: str) -> None:
        """Acknowledge processing of a message."""
        self._post(
            "/v1/ack",
            {"subscriber_id": self.subscriber_id, "message_id": message_id},
        )

    def nack(self, message_id: int, topic: str, reason: str = "") -> None:
        """Negative-acknowledge: request redelivery of a message.

        Rolls back the ack position so this message (and any after it)
        will be redelivered on next subscribe/poll.
        """
        self._post(
            "/v1/nack",
            {
                "subscriber_id": self.subscriber_id,
                "message_id": message_id,
                "topic": topic,
                "reason": reason,
            },
        )

    def deregister(self) -> None:
        """Remove this service from the bus registry."""
        self._post("/v1/deregister", {"id": self.subscriber_id})

    async def subscribe(
        self,
        topic: str,
        from_id: str = "",
        reconnect: bool = True,
        reconnect_delay: float = 2.0,
        max_reconnect_delay: float = 60.0,
    ) -> AsyncIterator[Message]:
        """Async iterator over messages on a topic.

        Pass ``from_id=""`` to resume from the last acked message
        (or only new messages if none acked yet).

        If ``reconnect=True`` (default), automatically reconnects on
        connection loss with exponential backoff. The bus's durable
        subscription mechanism (resume from last ack) makes reconnection
        safe — the subscriber picks up where it left off with no
        message loss.
        """
        delay = reconnect_delay
        while True:
            try:
                ws_url = self._ws_url("/v1/subscribe")
                async with websockets.connect(ws_url) as ws:
                    await ws.send(
                        json.dumps({
                            "subscriber_id": self.subscriber_id,
                            "topic": topic,
                            "from_id": from_id,
                        })
                    )
                    delay = reconnect_delay  # reset backoff on successful connect
                    try:
                        async for raw in ws:
                            data = json.loads(raw)
                            if data.get("type") == "error":
                                raise RuntimeError(data.get("error", "unknown"))
                            yield Message.from_wire(data)
                    except websockets.ConnectionClosedOK:
                        return  # clean close, don't reconnect
            except (
                websockets.ConnectionClosedError,
                ConnectionRefusedError,
                OSError,
            ) as exc:
                if not reconnect:
                    raise  # caller expects the error when reconnect is disabled
                logger.warning(
                    "Subscribe connection lost for %s on %s, reconnecting in %.1fs",
                    self.subscriber_id, topic, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_reconnect_delay)

    # ----- helpers -----

    def _post(self, path: str, body: dict) -> dict:
        resp = self._http.post(f"{self.base_url}{path}", json=body)
        if resp.status_code >= 400:
            raise RuntimeError(f"{path}: {resp.text}")
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json()
        return {}

    def _ws_url(self, path: str) -> str:
        parsed = urlparse(f"{self.base_url}{path}")
        scheme = "wss" if parsed.scheme == "https" else "ws"
        return urlunparse(parsed._replace(scheme=scheme))
