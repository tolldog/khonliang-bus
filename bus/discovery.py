"""Bus discovery — dependency-free client-side helpers.

Shared by every local bus CLIENT (the MCP adapter, the log agent, ad-hoc
tools): resolve where the bus is without port configuration, per
``docs/bus-transport-contract.md``. Lives in its own module (not
``bus.mcp_adapter``) so importing it doesn't drag in the optional ``mcp``
package (fr_khonliang-bus_70862caa PR 2 / _aa5b25cf).
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

#: Well-known bus socket (docs/bus-transport-contract.md, fr_khonliang-bus_aa5b25cf).
#: None when there's no resolvable home (unset $HOME in some containers) — then
#: UDS auto-discovery is simply skipped and we fall back to TCP.
try:
    BUS_SOCK_PATH: Path | None = Path.home() / ".khonliang" / "bus.sock"
except (RuntimeError, OSError):  # pragma: no cover - no resolvable home
    BUS_SOCK_PATH = None


def _socket_live(path: Path) -> bool:
    """Whether a bus is actually listening on the socket — not merely that the
    file exists. A stale socket (crashed UDS run, or the bus now on --no-uds)
    must NOT be auto-selected over a healthy TCP listener."""
    if not path.exists():
        return False
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.3)
    try:
        probe.connect(str(path))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def discover_bus(explicit: str | None = None) -> str:
    """Resolve the bus address per the transport contract, first match wins:

    1. ``explicit`` (a ``--bus`` CLI arg),
    2. ``KHONLIANG_BUS_URL`` env var,
    3. ``~/.khonliang/bus.sock`` if a bus is LIVE on it → ``unix://<abspath>``,
    4. ``http://localhost:8787`` fallback (TCP default, transition-friendly).

    Must stay identical to bus-lib's ``discover_bus`` — see
    ``docs/bus-transport-contract.md``.
    """
    if explicit:
        return explicit
    env = os.environ.get("KHONLIANG_BUS_URL")
    if env and env.strip():
        return env.strip()
    if BUS_SOCK_PATH is not None and _socket_live(BUS_SOCK_PATH):
        return f"unix://{BUS_SOCK_PATH}"
    return "http://localhost:8787"
