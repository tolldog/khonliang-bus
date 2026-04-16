# khonliang-bus

A cross-app event bus and service registry for the khonliang ecosystem.

Provides push notifications, service discovery, and pub/sub between MCP servers, agents, and applications. Designed so a developer MCP can subscribe to events from the researcher and other apps without each project reinventing its own messaging layer.

## Features

- **HTTP + WebSocket transport** — simple and universal
- **Pluggable backends** — `memory`, `sqlite`, `redis` (planned)
- **Durable subscriptions** — offline subscribers receive missed messages on reconnect
- **Service registry** — TTL-based heartbeat tracking
- **Go and Python clients** — auto-registering, ergonomic APIs

## Quick Start

```bash
# Build the bus
go build -o bin/khonliang-bus ./cmd/khonliang-bus

# Run with default config (memory backend, port 8787)
./bin/khonliang-bus

# Or with a config file
./bin/khonliang-bus -config config/bus.yaml
```

### Python client

```python
from khonliang_bus import BusClient

bus = BusClient(
    base_url="http://localhost:8787",
    subscriber_id="researcher",
    topics=["paper.distilled"],
)

bus.publish("paper.distilled", {"id": "abc123", "title": "..."})
```

### Go client

```go
import "github.com/tolldog/khonliang-bus/pkg/client"

bus, _ := client.New("http://localhost:8787", "developer-mcp")
bus.Register([]string{"fr.completed"}, nil)

sub, _ := bus.Subscribe(ctx, "paper.distilled", "")
for msg := range sub.Messages() {
    handle(msg)
    bus.Ack(msg.ID)
}
```

## Architecture

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  researcher MCP  │    │  developer MCP   │    │   your app       │
└────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
         │                       │                       │
         │ pub/sub               │ pub/sub               │ pub/sub
         │ (WebSocket)           │                       │
         └───────────┬───────────┴───────────┬───────────┘
                     │                       │
                ┌────┴───────────────────────┴────┐
                │       khonliang-bus              │
                │  ┌──────────┐  ┌─────────────┐  │
                │  │ Registry │  │  Pub/Sub    │  │
                │  └──────────┘  └─────┬───────┘  │
                │         ┌────────────┴───────┐  │
                │         │ Storage Backend    │  │
                │         │ memory|sqlite|redis│  │
                │         └────────────────────┘  │
                └──────────────────────────────────┘
```

## Configuration

```yaml
# config/bus.yaml
listen: ":8787"

backend:
  type: sqlite          # memory | sqlite | redis
  sqlite:
    path: data/bus.db
  retention:
    messages_ttl: 168h        # 7d

registry:
  heartbeat_interval: 30s
```

## MCP Response Budgets

The bus MCP adapter is a context firewall. Agent skill and flow calls return a
bounded JSON envelope by default instead of inlining large raw outputs.

Envelope fields include:

- `ok` / `status`
- `summary`
- `findings`
- `refs` and `artifact_ids`
- `suggested_next_actions`
- `truncated` / `omitted`
- `metrics.raw_bytes`, `metrics.raw_chars`, and `metrics.inline_budget_chars`

Default inline budget is 8,000 characters. Non-high-detail calls are capped at
16,000 characters even if a caller requests more with `_response_budget_chars`.
Callers can pass `_allow_high_detail: true` to raise the ceiling for explicit
inspection, but large outputs should still be retrieved through artifact
helpers.

Agent authors should treat `detail` like this:

- `compact`: one-line status, counts, and artifact refs.
- `brief`: actionable summary, top findings, and artifact refs.
- `full`: richer summary, but still no raw logs, full diffs, file bodies, or
  context dumps inline.

Never return raw pytest output, full command stdout/stderr, complete diffs,
large FR lists, or file bodies inline by default. Store raw material as an
artifact and return the artifact id with suggested retrieval commands such as
`bus_artifact_tail`, `bus_artifact_grep`, `bus_artifact_excerpt`, or
`bus_artifact_distill`.

## Agent Feedback

Agents can report structured feedback to the bus when a workflow reveals a
missing capability, costly friction, or improvement opportunity.

Feedback kinds:

- `gap`: expected capability is missing or insufficient.
- `friction`: operation succeeded but was costly or awkward. Categories are
  `token`, `latency`, `round_trip`, `routing`, `format`, `retry`, and
  `fallback`.
- `suggestion`: improvement opportunity not tied to a failing operation.

HTTP agents can `POST /v1/feedback`. WebSocket agents can send
`{"type": "feedback", "kind": "...", ...}`. Reports are queryable with
`GET /v1/feedback` or the `bus_feedback` MCP tool, filtered by `agent_id`,
`kind`, `status`, and `since`. Repeated open reports with the same fingerprint
are de-duplicated and counted.

The older `POST /v1/gap` API remains compatible and also creates a structured
`gap` feedback report.

## Status

**v0.1** — initial release

- [x] HTTP + WebSocket transport
- [x] Memory backend
- [x] SQLite backend
- [x] Service registry with heartbeat
- [x] Go client
- [x] Python client (auto-registers on init)
- [ ] Redis backend (planned v0.2)
- [ ] gRPC transport (planned v0.3)
- [ ] Auth (planned)
- [ ] Topic wildcards (planned)
