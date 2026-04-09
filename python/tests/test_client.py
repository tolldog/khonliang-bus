"""End-to-end tests for the Python client.

Requires the bus binary to be built. Spawns a real bus instance per test.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

from khonliang_bus import BusClient

REPO_ROOT = Path(__file__).resolve().parents[2]
BUS_BIN = REPO_ROOT / "bin" / "khonliang-bus"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def bus_url():
    if not BUS_BIN.exists():
        pytest.skip(
            "khonliang-bus binary not built; run "
            "`go build -o bin/khonliang-bus ./cmd/khonliang-bus` first"
        )

    port = _free_port()
    # Capture stdout/stderr so a startup failure produces useful diagnostics
    # in the test output instead of being silently skipped.
    proc = subprocess.Popen(
        [str(BUS_BIN)],
        env={**os.environ, "KHONLIANG_BUS_LISTEN": f":{port}"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the listener to come up.
    deadline = time.time() + 5
    started = False
    while time.time() < deadline:
        if proc.poll() is not None:
            # Process exited before we could connect — surface its output.
            stdout, stderr = proc.communicate()
            pytest.fail(
                f"khonliang-bus exited during startup with code {proc.returncode}\n"
                f"stdout: {stdout.decode(errors='replace')}\n"
                f"stderr: {stderr.decode(errors='replace')}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                started = True
                break
        except OSError:
            time.sleep(0.05)
    if not started:
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate(timeout=5)
        pytest.fail(
            "khonliang-bus did not start within 5s\n"
            f"stdout: {stdout.decode(errors='replace')}\n"
            f"stderr: {stderr.decode(errors='replace')}"
        )

    yield f"http://127.0.0.1:{port}"

    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def test_register_and_list(bus_url):
    bus = BusClient(bus_url, "py-test", topics=["events"])
    services = bus.services()
    assert any(s["id"] == "py-test" for s in services)


def test_publish_and_subscribe(bus_url):
    bus = BusClient(bus_url, "py-test", topics=["events"])

    async def run():
        received = asyncio.Event()
        got = []

        async def consume():
            async for msg in bus.subscribe("events"):
                got.append(msg)
                bus.ack(msg.id)
                received.set()
                return

        task = asyncio.create_task(consume())
        try:
            # Publish-and-poll until the subscriber receives. Avoids fixed
            # sleeps that can flake on slow runners. Bounded by an overall
            # deadline.
            deadline = time.monotonic() + 2.0
            while not received.is_set():
                bus.publish("events", {"hello": "world"})
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(received.wait(), timeout=min(0.1, remaining))
                except asyncio.TimeoutError:
                    continue
        finally:
            task.cancel()
            # Await the task so we don't leave it pending and trigger
            # "Task was destroyed but it is pending!" warnings.
            try:
                await task
            except asyncio.CancelledError:
                pass
        return got

    got = asyncio.run(run())
    assert len(got) == 1
    assert got[0].payload == {"hello": "world"}
