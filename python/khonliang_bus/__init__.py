"""Python client for khonliang-bus.

The :class:`BusClient` auto-registers with the bus on init and provides
synchronous publish/ack plus an async subscribe iterator.

Usage::

    from khonliang_bus import BusClient

    bus = BusClient("http://localhost:8787", "my-service",
                    topics=["paper.distilled"])
    # Auto-registers on init.

    bus.publish("paper.distilled", {"id": "abc", "title": "..."})

    async for msg in bus.subscribe("paper.distilled"):
        handle(msg)
        bus.ack(msg.id)
"""

from khonliang_bus.client import BusClient, Message

__all__ = ["BusClient", "Message"]
__version__ = "0.1.0"
