"""Entry point: python -m bus --port 8787 --db data/bus.db"""

import argparse
import logging
import sys

import uvicorn

from bus.server import create_app


def main():
    parser = argparse.ArgumentParser(description="khonliang-bus agent platform")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--db", default="data/bus.db")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    app = create_app(db_path=args.db, config={"bus_url": f"http://localhost:{args.port}"})
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
