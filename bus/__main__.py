"""Entry point: python -m bus --port 8787 --db data/bus.db"""

import argparse
import logging
import os
import sys

import uvicorn

from bus.server import create_app


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean env var (``1`` / ``true`` / ``yes`` / ``on`` truthy)."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main():
    parser = argparse.ArgumentParser(description="khonliang-bus agent platform")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--db", default="data/bus.db")
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

    app = create_app(
        db_path=args.db,
        config={
            "bus_url": f"http://localhost:{args.port}",
            "provenance_disclose_full": args.provenance_disclose_full,
        },
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
