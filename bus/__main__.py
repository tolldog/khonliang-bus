"""Entry point: python -m bus [--port 8787] [--db data/bus.db]

Binds a Unix domain socket at ``~/.khonliang/bus.sock`` (local, default) AND a
TCP listener (remote / transition), serving the same bus. See
``docs/bus-transport-contract.md`` (fr_khonliang-bus_aa5b25cf).
"""

import argparse
import asyncio
import logging
import os
import socket
import sys
from pathlib import Path

import uvicorn

from bus.server import create_app

logger = logging.getLogger("bus")

DEFAULT_SOCK_PATH = Path.home() / ".khonliang" / "bus.sock"


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean env var (``1`` / ``true`` / ``yes`` / ``on`` truthy)."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_tcp(args) -> tuple[str, int] | None:
    """(host, port) to bind for TCP, or None if TCP is disabled.

    ``KHONLIANG_BUS_LISTEN`` wins over --host/--port: ``host:port`` overrides,
    ``off``/``none``/empty disables TCP entirely (UDS-only)."""
    listen = os.environ.get("KHONLIANG_BUS_LISTEN")
    if listen is not None:
        if listen.strip().lower() in {"off", "none", ""}:
            return None
        host, _, port = listen.rpartition(":")
        return (host or "0.0.0.0", int(port))
    return (args.host, args.port)


def _clear_stale_socket(sock_path: Path) -> None:
    """Remove a stale socket from a crashed bus, but never clobber a live one.

    If something is already listening on the path, a second bus must NOT start
    (single-bus-per-host). If nothing answers, the file is a leftover from a
    crash and is safe to unlink so the fresh bind succeeds."""
    if not sock_path.exists():
        return
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.5)
    try:
        probe.connect(str(sock_path))
        live = True
    except OSError:
        live = False
    finally:
        probe.close()
    if live:
        raise SystemExit(
            f"bus: another bus is already listening on {sock_path}; refusing to start"
        )
    sock_path.unlink(missing_ok=True)  # stale from a crashed bus


async def _serve(app, tcp: tuple[str, int] | None, uds_path: Path | None) -> None:
    """Run the bus on TCP and/or UDS concurrently (one app, shared state)."""
    servers = []
    if tcp is not None:
        host, port = tcp
        servers.append(uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="info")))
        logger.info("bus: TCP listener on %s:%d", host, port)
    if uds_path is not None:
        servers.append(uvicorn.Server(uvicorn.Config(app, uds=str(uds_path), log_level="info")))
        logger.info("bus: UDS listener on %s", uds_path)
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        # Clean up the socket so "socket exists = bus running" holds.
        if uds_path is not None:
            uds_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="khonliang-bus agent platform")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--db", default="data/bus.db")
    parser.add_argument(
        "--uds",
        default=None,
        help=f"Unix domain socket path (default: {DEFAULT_SOCK_PATH}).",
    )
    parser.add_argument(
        "--no-uds",
        action="store_true",
        help="Disable the UDS listener (TCP only).",
    )
    # fr_khonliang-bus_aa096048: opt-in disclosure for the provenance HTTP route.
    # Default safe (redacted) because the bus binds 0.0.0.0 with no auth.
    # Local-trusted hosts opt in via either flag or env var; the env-var path
    # is operationally cleaner for systemd-managed deployments (drop a line
    # in /etc/khonliang/*.env instead of editing ExecStart).
    parser.add_argument(
        "--provenance-disclose-full",
        action="store_true",
        default=_env_bool("KHONLIANG_BUS_PROVENANCE_DISCLOSE_FULL"),
        help=(
            "Disclose unredacted process metadata "
            "(cwd, executable, config, commit_sha, branch) on "
            "GET /v1/agent/{id}/provenance. Defaults to redacted; "
            "set this flag (or env KHONLIANG_BUS_PROVENANCE_DISCLOSE_FULL=1) "
            "on local-trusted hosts where the bus listener is not "
            "reachable from untrusted networks."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    tcp = _resolve_tcp(args)

    uds_path: Path | None = None
    if not args.no_uds:
        uds_path = Path(args.uds).expanduser() if args.uds else DEFAULT_SOCK_PATH
        uds_path.parent.mkdir(parents=True, exist_ok=True)
        _clear_stale_socket(uds_path)

    if tcp is None and uds_path is None:
        raise SystemExit("bus: both UDS and TCP disabled — nothing to bind")

    # bus_url is the bus's self-reference for internal HTTP; prefer the TCP
    # address (a real URL), else the UDS path is not a usable http URL so fall
    # back to a localhost default (UDS-only is an advanced mode).
    bus_url = f"http://localhost:{tcp[1]}" if tcp is not None else "http://localhost:8787"

    app = create_app(
        db_path=args.db,
        config={
            "bus_url": bus_url,
            "provenance_disclose_full": args.provenance_disclose_full,
        },
    )
    asyncio.run(_serve(app, tcp, uds_path))


if __name__ == "__main__":
    main()
