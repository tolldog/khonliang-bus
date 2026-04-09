# khonliang-bus (Python client)

Python client for [khonliang-bus](https://github.com/tolldog/khonliang-bus), a cross-app event bus and service registry.

## Install

```bash
pip install khonliang-bus
```

Or from source:

```bash
pip install -e python/
```

## Usage

```python
from khonliang_bus import BusClient

# Auto-registers on init
bus = BusClient(
    base_url="http://localhost:8787",
    subscriber_id="my-service",
    topics=["paper.distilled", "fr.updated"],
    metadata={"version": "0.1.0"},
)

# Publish
bus.publish("paper.distilled", {"id": "abc", "title": "..."})

# Subscribe (async)
import asyncio

async def consume():
    async for msg in bus.subscribe("paper.distilled"):
        print(msg.id, msg.payload)
        bus.ack(msg.id)

asyncio.run(consume())
```

## Auto-registration

`BusClient` registers itself with the bus on construction. Pass `register=False` to defer.

## Backends

The bus supports multiple persistence backends (memory, sqlite, redis). The client doesn't need to know — it talks HTTP/WebSocket regardless.
