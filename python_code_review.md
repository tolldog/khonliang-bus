# khonliang-bus Python Client Code Review

**Reviewer:** Claude (researcher session)
**Date:** 2026-04-10
**Scope:** `python/` directory — client library (~170 lines), tests (~135 lines), packaging
**Status:** Clean v0.1, three issues to address

---

## Files reviewed

```
python/
├── khonliang_bus/
│   ├── __init__.py         — re-exports BusClient, Message; version
│   └── client.py           — BusClient class (171 lines)
├── tests/
│   └── test_client.py      — end-to-end tests against real bus binary (135 lines)
├── pyproject.toml           — packaging config
└── README.md                — usage docs
```

---

## What's solid

### BusClient design (client.py)

- **Auto-registration on construction** is a smart default. Most callers want to register immediately. `register=False` escape hatch for cases where the bus isn't up yet or registration should be deferred.
- **Sync/async split** is correct: write operations (publish, ack, heartbeat, register) are sync HTTP via `httpx.Client`; subscribe is async WebSocket via `websockets`. This matches the usage pattern — publish is usually called from sync code, subscribe is an async iterator in an event loop.
- **`Message.from_wire()`** classmethod for deserialization keeps the wire format decoupled from the dataclass. If the bus adds fields, only `from_wire` changes.
- **`_ws_url()` helper** (line 167-170) handles http→ws scheme conversion cleanly. Handles both http→ws and https→wss.
- **Context manager support** (`__enter__`/`__exit__`) for resource cleanup.

### Tests (test_client.py)

- **Real integration tests** — spawns the actual Go bus binary per test. No mocks. This catches real protocol issues that mocked tests would miss.
- **`_free_port()`** pattern — binds to port 0, captures the OS-assigned port, then releases it for the bus to use. Race-window is small.
- **Startup failure diagnostics** — if the bus exits during startup, the fixture captures stdout/stderr and includes them in the pytest failure message. Excellent for debugging CI failures.
- **Publish-and-poll pattern** (test_publish_and_subscribe:113-121) — avoids fixed sleeps by polling with a bounded deadline. Race-free and doesn't flake on slow runners.
- **Task cleanup** (lines 124-130) — explicitly cancels and awaits the consumer task to avoid "Task was destroyed but it is pending!" warnings. Thorough.

### Packaging (pyproject.toml)

- `requires-python = ">=3.10"` — matches the `X | None` syntax used in type hints.
- Dependencies are minimal: `httpx` + `websockets`. No unnecessary bloat.
- Dev deps are optional: `pytest` + `pytest-asyncio`. Clean split.

---

## Issues

### 1. No `nack()` method

The client has `ack()` but no `nack()`. The bus-agent-interaction spec (`POST /v1/nack`) and the queue semantics FR (`fr_khonliang-bus_2c811263`) define NACK as a core operation — "I couldn't process this, redeliver later."

**Impact for v0.1:** None — the Go bus doesn't have a NACK endpoint yet either.

**Impact for the platform:** When NACK is added to the bus, the Python client needs a corresponding method. Agents that fail to process a message need to signal this explicitly, not just skip the ack and wait for a timeout.

**Recommendation:** Note as a TODO. Add `nack()` when the bus gains NACK support. Signature:

```python
def nack(self, message_id: str, delay: float = 0) -> None:
    """Negative-acknowledge: request redelivery after delay seconds."""
    self._post("/v1/nack", {
        "subscriber_id": self.subscriber_id,
        "message_id": message_id,
        "delay": delay,
    })
```

### 2. No `deregister()` method

The client auto-registers but has no way to explicitly deregister. On clean shutdown, the service silently disappears — the bus only notices via TTL expiry (up to 150s with default settings).

**Impact for v0.1:** Minor — the pruning loop handles it. But during development (start/stop/restart cycles), stale registry entries linger and confuse `services()` output.

**Recommendation:** Add `deregister()` and call it from `close()`:

```python
def deregister(self) -> None:
    """Remove this service from the bus registry."""
    self._post("/v1/deregister", {"id": self.subscriber_id})

def close(self) -> None:
    try:
        self.deregister()
    except Exception:
        pass  # best-effort on shutdown
    self._http.close()
```

Depends on the Go bus gaining a `/v1/deregister` endpoint (flagged in the Go code review, issue #2).

### 3. `subscribe()` doesn't reconnect on connection loss

```python
async def subscribe(self, topic: str, from_id: str = "") -> AsyncIterator[Message]:
    ws_url = self._ws_url("/v1/subscribe")
    async with websockets.connect(ws_url) as ws:
        ...
```

If the WebSocket drops (bus restarts, network blip), the async iterator ends silently via `ConnectionClosedOK`. The caller's `async for` loop exits and the subscription is gone.

**Impact for v0.1:** Acceptable — callers can wrap subscribe in a retry loop manually.

**Impact for long-running agents:** A bus restart (or a brief network issue) permanently disconnects every subscriber. The agent continues running but stops receiving events. Silent failure.

**Recommendation:** Add an auto-reconnect wrapper with configurable backoff:

```python
async def subscribe(
    self, topic: str, from_id: str = "", reconnect: bool = True,
) -> AsyncIterator[Message]:
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                await ws.send(json.dumps({...}))
                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("type") == "error":
                        raise RuntimeError(data.get("error"))
                    yield Message.from_wire(data)
        except websockets.ConnectionClosedError:
            if not reconnect:
                return
            await asyncio.sleep(backoff)
            # Reconnect resumes from last acked (fromID="")
```

The bus's durable subscription mechanism (resume from last ack) makes reconnection safe — the subscriber picks up where it left off with no message loss.

---

## Minor observations (not blocking)

### Package naming collision risk

The package is `khonliang-bus` in PyPI terms but `khonliang_bus` as an import. The bus-lib specs envision moving this client into a separate `khonliang-bus-lib` repo with additional modules (BaseAgent, BusService, RemoteRole, BusMCPAdapter). When that happens:

- Either this client code moves into `khonliang-bus-lib` and this package is deprecated
- Or `khonliang-bus-lib` depends on this package and re-exports `BusClient`

**Recommendation:** When `khonliang-bus-lib` is created, absorb `client.py` into it. The `khonliang_bus` import name stays the same; the package just gets bigger. No breaking change for existing callers.

### `httpx.Client` is never closed if `register=False` and close() isn't called

If a caller creates `BusClient(..., register=False)` and doesn't use the context manager or call `close()`, the `httpx.Client` leaks. Standard Python resource-management issue — not a bug, but worth noting.

### Test fixture spawns a memory-backend bus

The test fixture doesn't pass a config file, so the bus defaults to memory backend. This means tests don't exercise SQLite-specific behavior (backfill, ack persistence, trim). Integration tests against the SQLite backend should be added if the client ever needs to verify durable subscription behavior (e.g., reconnection after bus restart).

---

## What's right (keep as-is)

- **`Message` dataclass with `from_wire()` classmethod** — clean separation of wire format from domain object.
- **`_post()` helper** (line 159-165) — unified HTTP error handling. Raises `RuntimeError` with the response body for any 4xx/5xx. Checks content-type before parsing JSON (handles empty responses from non-JSON endpoints).
- **`__init__.py` re-exports** — `from khonliang_bus import BusClient, Message` works. Clean public API.
- **README** is concise with a working code example. Shows auto-registration, publish, and async subscribe.
- **Tests skip gracefully** when the Go binary isn't built (`pytest.skip` with a helpful message telling you how to build it).

---

## Verdict

**Clean v0.1 client.** The three issues (no NACK, no deregister, no reconnect) are all expected gaps that align with the bus service's own v0.1 limitations. The client correctly implements the current bus protocol. When the bus gains NACK, deregister, and the Python rewrite lands, these gaps close naturally.

The test suite is notably good — real integration tests against the actual bus binary, with proper startup diagnostics, race-free assertion patterns, and clean task teardown. This pattern should be preserved when the client moves into `khonliang-bus-lib`.
