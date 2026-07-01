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
import stat
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
    """Reclaim a dead bus socket, but never delete anything we can't prove is one.

    Only a path that IS a socket and gives a definitive ``ConnectionRefused``
    (no listener) is unlinked. A non-socket file, a live listener, or an
    ambiguous probe error (permission/timeout) makes the bus refuse to start
    rather than silently deleting something — unlinking a misconfigured ``--uds``
    that points at a real file would be data loss."""
    if not sock_path.exists():
        return
    if not stat.S_ISSOCK(sock_path.stat().st_mode):
        raise SystemExit(
            f"bus: {sock_path} exists and is not a socket; refusing to overwrite it"
        )
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.5)
    try:
        probe.connect(str(sock_path))
    except ConnectionRefusedError:
        sock_path.unlink(missing_ok=True)  # dead socket from a crashed bus
        return
    except OSError as e:
        raise SystemExit(
            f"bus: cannot probe existing socket {sock_path}: {e}; refusing to start"
        )
    else:
        raise SystemExit(
            f"bus: another bus is already listening on {sock_path}; refusing to start"
        )
    finally:
        probe.close()


def _validate_binds(tcp: tuple[str, int] | None, uds_path: Path | None) -> None:
    """Reject bind combinations that can't work yet.

    UDS-only (TCP disabled) is refused until agents are UDS-capable: internal
    callers (``_start_process`` forwards bus_url as ``--bus``; the orchestrator
    builds ``f'{bus_url}/v1/request'``) still assume an HTTP base URL, so a
    ``unix://`` self-URL would break autostart/lazy launches and flow dispatch.
    Lift this once bus-lib Phase 2 (fr_khonliang-bus-lib_042279e2) lands."""
    if tcp is None and uds_path is None:
        raise SystemExit("bus: both UDS and TCP disabled — nothing to bind")
    if tcp is None:
        raise SystemExit(
            "bus: UDS-only mode (TCP disabled) requires UDS-capable agents — "
            "bus-lib Phase 2 (fr_khonliang-bus-lib_042279e2) is not yet available. "
            "Keep TCP enabled (dual-bind) for now."
        )


def _resolve_self_url(tcp: tuple[str, int] | None, uds_path: Path | None) -> str:
    """The bus's own address, handed to spawned agents (as ``--bus``) and used
    for internal self-calls. Prefer TCP (a real URL); in UDS-only mode advertise
    the socket so agents reach a LIVE listener instead of a dead port. (UDS-only
    requires UDS-capable agents — bus-lib Phase 2, fr_khonliang-bus-lib_042279e2.)"""
    if tcp is not None:
        return f"http://localhost:{tcp[1]}"
    if uds_path is not None:
        return f"unix://{uds_path}"
    return "http://localhost:8787"


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
    uds_path: Path | None = None if args.no_uds else (
        Path(args.uds).expanduser() if args.uds else DEFAULT_SOCK_PATH
    )
    _validate_binds(tcp, uds_path)  # refuse unusable combos before touching the fs

    if uds_path is not None:
        uds_path.parent.mkdir(parents=True, exist_ok=True)
        _clear_stale_socket(uds_path)

    bus_url = _resolve_self_url(tcp, uds_path)

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
