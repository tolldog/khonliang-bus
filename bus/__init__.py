"""khonliang-bus: agent orchestration platform.

Python implementation of the bus service — agent lifecycle management,
request/reply routing, pub/sub messaging, sessions, flow orchestration,
and an MCP adapter for Claude.

Public API::

    from bus import BusClient, Message          # client for non-agent callers
    from bus import BaseAgent, Skill, handler   # base class for agents
    from bus import BusMCPAdapter               # MCP bridge for Claude
"""

__version__ = "0.2.0"

from bus.agent import BaseAgent, Collaboration, Skill, handler
from bus.client import BusClient, Message
from bus.mcp_adapter import BusMCPAdapter

__all__ = [
    "BaseAgent",
    "BusClient",
    "BusMCPAdapter",
    "Collaboration",
    "Message",
    "Skill",
    "handler",
]
