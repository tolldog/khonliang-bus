# khonliang-bus Code Review

**Reviewer:** Claude (researcher session)
**Date:** 2026-04-10
**Scope:** Full codebase — Go server (~1150 lines), Python client (~170 lines)
**Status:** Clean v0.1, no blocking issues

---

## Codebase overview

```
cmd/khonliang-bus/main.go          — entry point, config, signal handling, background loops
internal/config/config.go          — YAML config with defaults
internal/registry/registry.go      — in-memory service registry with TTL pruning
internal/server/server.go          — HTTP + WebSocket handlers
internal/storage/storage.go        — Backend interface (Message, Subscription, Backend)
internal/storage/memory/memory.go  — in-memory backend
internal/storage/sqlite/sqlite.go  — SQLite durable backend
pkg/client/client.go               — Go client library
python/khonliang_bus/client.py     — Python client library
```

---

## What's solid

### Storage interface (storage.go)

Clean Backend abstraction with well-documented semantics. Any backend (memory, SQLite, Redis) plugs in without core changes. The contract is explicit:

- Publish appends and returns assigned ID
- Subscribe resumes from last ack (or tail) — no message loss on reconnect
- Ack is per-message, per-subscriber, per-topic
- Backends MUST deliver backfilled messages strictly before live messages

This interface translates directly to whatever language the bus is eventually implemented in.

### SQLite backfill logic (sqlite.go:130-229)

The hardest part of durable subscriptions, done correctly. `backfillLoop` drains pending DB messages via cursor-based pagination, then atomically flips to live delivery under `subsMu` so the Publish path can start delivering to the subscription. The loop re-checks for new messages that arrived during the drain before flipping, preventing a race where a message lands between "drain complete" and "mark live."

Lock ordering is documented and consistent: `subsMu` before `subscription.mu`. Publish snapshots live subscribers under `subsMu` and delivers outside the lock to avoid lock-order inversion with `subscription.Close()`.

### Out-of-order ack protection (sqlite.go:330-344)

```sql
ON CONFLICT(subscriber_id, topic) DO UPDATE SET
    last_acked_id = CASE
        WHEN excluded.last_acked_id > subscriber_acks.last_acked_id
            THEN excluded.last_acked_id
        ELSE subscriber_acks.last_acked_id
    END
```

Prevents ack regression from out-of-order processing. A subscriber that acks message 5 then message 3 keeps the cursor at 5. Correct.

### Registry (registry.go)

Thread-safe via `RWMutex`. TTL-based pruning runs in a background goroutine. `Register` is an upsert — re-registration updates Name/Topics/Metadata and refreshes the heartbeat without losing `RegisteredAt`. Simple, correct, no surprises.

### Main (main.go)

Graceful shutdown on SIGINT/SIGTERM with a 5-second deadline. Background pruning loop runs at the heartbeat interval. Retention loop runs at `ttl/24` (bounded between 1 minute and 1 hour) — frequent enough to bound disk growth without hammering the DB. Environment variable override for listen address (useful for tests). Clean.

### Python client (client.py)

Auto-registers on construction (smart default, skippable via `register=False`). Sync HTTP via `httpx` for write operations (publish, ack, heartbeat). Async WebSocket via `websockets` for subscribe with proper connection cleanup. `_ws_url()` helper converts `http→ws` scheme. Context manager support. Clean separation of concerns.

---

## Issues

### 1. subscription.deliver() silently drops messages on full channel

**File:** `internal/storage/sqlite/sqlite.go:477-479`

```go
func (s *subscription) deliver(msg storage.Message) {
    _ = s.trySend(msg)
}
```

The subscription channel buffer is 64. If a consumer falls behind, `trySend` returns false and the message is silently lost. No error, no log, no signal to the publisher.

**Impact for v0.1:** Acceptable trade-off for non-blocking pub/sub delivery. A slow subscriber misses events but doesn't block publishers.

**Impact for the platform future:** Request/reply and flow orchestration cannot silently drop messages. A flow step response that gets dropped means a flow hangs forever. The NACK/retry FR (`fr_khonliang-bus_2c811263`) addresses this with explicit failure signaling, but the underlying `deliver()` behavior should also be hardened — either with backpressure (block until the channel has space, with a timeout) or with an overflow buffer that persists to DB.

**Recommendation:** Add a TODO comment noting this is a known v0.1 limitation. When request/reply lands, `deliver()` needs a blocking mode with timeout for synchronous responses.

### 2. No /v1/deregister endpoint

**File:** `internal/server/server.go` (missing handler)

The registry has `Deregister(id string)` (registry.go:77-81) but it's not exposed via HTTP. Agents can't announce clean shutdown — they time out via TTL pruning instead.

The `bus-agent-interaction.md` spec expects `POST /v1/deregister` for explicit shutdown signaling. Without it, the bus waits up to `5 * heartbeat_interval` (default: 150 seconds) before noticing an agent is gone.

**Recommendation:** Add a `handleDeregister` handler. ~10 lines:

```go
func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }
    var req struct { ID string `json:"id"` }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
        return
    }
    s.registry.Deregister(req.ID)
    writeJSON(w, http.StatusOK, map[string]string{"status": "deregistered"})
}
```

Plus one line in `routes()`: `s.mux.HandleFunc("/v1/deregister", s.handleDeregister)`.

### 3. Registry Service struct lacks callback_url and pid fields

**File:** `internal/registry/registry.go:14-21`

```go
type Service struct {
    ID            string
    Name          string
    Topics        []string
    Metadata      map[string]string
    RegisteredAt  time.Time
    LastHeartbeat time.Time
}
```

The agent lifecycle spec (`agents.md`) expects agents to register with a `callback_url` (for request routing) and `pid` (for process-level health checks). These aren't in the current struct.

**Impact for v0.1:** None — v0.1 doesn't route requests to agents or check PIDs.

**Impact for Step 1 (request/reply FR):** The bus needs `callback_url` to know where to forward requests. Adding it to the Service struct is straightforward:

```go
type Service struct {
    ID            string            `json:"id"`
    Name          string            `json:"name"`
    CallbackURL   string            `json:"callback_url,omitempty"`
    PID           int               `json:"pid,omitempty"`
    Topics        []string          `json:"topics"`
    Metadata      map[string]string `json:"metadata,omitempty"`
    RegisteredAt  time.Time         `json:"registered_at"`
    LastHeartbeat time.Time         `json:"last_heartbeat"`
}
```

**Recommendation:** Add when request/reply FR is implemented. No change needed for v0.1.

### 4. Python client swallows registration failure silently

**File:** `python/khonliang_bus/client.py:69-74`

```python
if register:
    try:
        self.register(self.topics, self.metadata)
    except Exception as exc:
        logger.warning(
            "Auto-register failed for %s: %s", subscriber_id, exc
        )
```

If the bus isn't running when the client is constructed, registration silently fails. The client appears healthy but isn't registered. Subsequent `publish()` and `ack()` calls will fail individually.

**Impact for v0.1:** Acceptable — manual startup means the operator knows whether the bus is running.

**Impact for bus-managed agents:** When the bus starts an agent and waits for its "call home" registration, a silently-failed registration means the bus thinks the agent never started. The agent thinks it's running. Neither knows the other's state.

**Recommendation:** For v0.1, add a `registered: bool` property so callers can check. For the BaseAgent future (`fr_khonliang-bus_0d34485b`), registration failure in the agent startup sequence should be fatal — not swallowed.

### 5. No CORS headers on HTTP endpoints

**File:** `internal/server/server.go`

WebSocket origins are handled via `websocket.AcceptOptions.OriginPatterns`, but regular HTTP endpoints (publish, register, heartbeat, services, health) have no CORS middleware.

**Impact for v0.1:** None — all callers are localhost Python/Go clients.

**Impact for future web dashboard:** A browser-based dashboard calling `/v1/services` or `/v1/health` would be blocked by CORS. Standard middleware fix.

**Recommendation:** Add when a web frontend is built. No change needed for v0.1.

---

## Not issues (expected gaps addressed by FRs)

These are absent from the codebase but tracked in the FR backlog:

| Missing | FR |
|---|---|
| `/v1/request` (request/reply) | `fr_khonliang-bus_2c811263` |
| `/v1/install` (agent lifecycle) | `fr_khonliang-bus_ad0cd3ee` |
| NACK, dead-letter, shared subscriptions | `fr_khonliang-bus_2c811263` |
| Skill + collaboration registry | `fr_khonliang-bus_5f0c3fc7` |
| `/v1/flow` (flow orchestration) | `fr_khonliang-bus_994dcde5` |
| `/v1/session` (session management) | `fr_khonliang-bus_f3b38f44` |
| `/v1/trace` (observability) | `fr_khonliang-bus_b381250f` |
| MCP transport | `fr_khonliang-bus_7e5f8c1e` |

---

## Code quality observations

- **Test coverage exists** for registry, server, and both storage backends. Tests use `httptest` and exercise the WebSocket subscribe path.
- **Error handling is consistent** — `fmt.Errorf` with `%w` for wrapping, early returns with `http.Error`, context propagation.
- **No panics** — all error paths return errors or log warnings.
- **SQLite uses `SetMaxOpenConns(1)`** — correct for SQLite's single-writer model.
- **`modernc.org/sqlite`** — pure Go SQLite, no CGO dependency. Good for cross-compilation and single-binary deployment.
- **Go module uses `github.com/coder/websocket`** — modern WebSocket library, well-maintained. The older `gorilla/websocket` would also work but `coder/websocket` has a cleaner API.
- **Background loops use `context.WithCancel` for clean shutdown** — no goroutine leaks.

---

## Verdict

**Clean v0.1 that validates the protocol design.** The code does exactly what it set out to do: pub/sub + durable subscriptions + service registry + message retention. The architecture (Backend interface, registry, subscription lifecycle with atomic backfill→live transition) is well-designed and translates directly to whatever language the bus is eventually implemented in.

The five issues above are all minor or expected gaps. Issues #1 and #4 are the most important for the platform evolution — silent message drops and silent registration failures will cause subtle bugs when request/reply and agent lifecycle management land. Both are addressed by their respective FRs.

Whether this codebase gets extended in Go, rewritten in Python, or rewritten in Rust, the *design decisions* it encodes (Backend interface, cursor-based backfill, lock-ordered subscription lifecycle, out-of-order ack protection) are the real value. The implementation language is secondary to the protocol it validates.
