"""Entry point: python -m bus.log_agent [--id log-agent] [--bus <url>] [--log-dir <dir>]

Accepts the bus's spawn CLI contract (``--id --bus --config``); ``--config`` is
accepted for that contract but unused in Phase 1.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from bus.discovery import discover_bus
from bus.log_agent.agent import LogAgent


def main() -> None:
    parser = argparse.ArgumentParser(description="khonliang light log agent")
    parser.add_argument("--id", default="log-agent")
    parser.add_argument("--bus", default=None, help="Bus address; auto-discovered when omitted.")
    parser.add_argument(
        "--log-dir",
        default=os.environ.get("KHONLIANG_BUS_LOG_DIR", "logs/agents"),
        help=(
            "The L0 log dir to tail (must match the bus's --log-dir). When the "
            "bus spawns this agent it exports KHONLIANG_BUS_LOG_DIR with its "
            "actual dir, so bus-spawned instances inherit it automatically."
        ),
    )
    parser.add_argument("--config", default=None, help="Accepted for the bus spawn contract; unused.")
    args = parser.parse_args()

    if not args.log_dir:
        # An empty --log-dir / env is the bus's explicit "L0 file logging
        # disabled" sentinel — fail closed rather than tail a fresh default dir
        # and register healthy with misleading empty results (codex R2).
        raise SystemExit(
            "log-agent: the bus has L0 file logging disabled (empty log dir); "
            "nothing to tail — enable --log-dir on the bus first"
        )

    # WARNING+: this agent's own stdout lands inside the glob it tails —
    # chatty logging would self-amplify (design doc).
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    agent = LogAgent(
        agent_id=args.id,
        bus_url=discover_bus(args.bus),
        log_dir=args.log_dir,
    )
    asyncio.run(agent.run())


if __name__ == "__main__":
    main()
