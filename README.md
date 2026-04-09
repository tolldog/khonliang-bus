# khonliang-bus

A cross-app event bus and service registry for the khonliang ecosystem.

Provides push notifications, service discovery, and pub/sub between MCP servers, agents, and applications. Designed so a developer MCP can subscribe to events from the researcher, autostock, and other apps without each project reinventing its own messaging layer.

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
│  researcher MCP  │    │  developer MCP   │    │   autostock      │
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
    subscriber_idle_ttl: 720h # 30d

registry:
  heartbeat_interval: 30s
```

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
