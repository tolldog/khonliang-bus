"""L0 fleet logging: per-agent log files written by the bus
(fr_khonliang-bus_70862caa PR 1, docs/log-agent-design.md).

Contract under test: spawned agents' stdout+stderr land in
<agent_log_dir>/<agent_id>.log; a log-layer failure NEVER breaks the spawn
(falls back to DEVNULL); rotation happens at start and is best-effort; the
parent closes its fd copy immediately; children run unbuffered.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from bus.db import BusDB
from bus.server import BusServer


def _bus(tmp_path, **cfg) -> BusServer:
    db = BusDB(str(tmp_path / "bus.db"))
    return BusServer(db, config={"bus_url": "http://localhost:9999", **cfg})


def _install_echo(db: BusDB, agent_id: str, tmp_path) -> None:
    """Install a real tiny agent that prints to stdout AND stderr, then exits."""
    script = tmp_path / "echo_agent.py"
    script.write_text(
        "import sys\n"
        "print('hello-stdout', flush=False)\n"          # unbuffered env must flush this
        "print('hello-stderr', file=sys.stderr)\n"
    )
    db.install_agent(
        agent_id=agent_id, agent_type="test", command=sys.executable,
        args=[str(script)], cwd=str(tmp_path), config=str(tmp_path / "c.yaml"),
    )


def _wait_exit(bus: BusServer, agent_id: str, timeout: float = 10.0) -> None:
    proc = bus._processes[agent_id]
    deadline = time.time() + timeout
    while proc.poll() is None and time.time() < deadline:
        time.sleep(0.05)


def test_spawn_writes_stdout_and_stderr_to_agent_log(tmp_path):
    log_dir = tmp_path / "logs"
    bus = _bus(tmp_path, agent_log_dir=str(log_dir))
    _install_echo(bus.db, "a1", tmp_path)

    result = bus.start_agent("a1")
    assert result["status"] == "started"
    _wait_exit(bus, "a1")

    content = (log_dir / "a1.log").read_text()
    assert "hello-stdout" in content   # PYTHONUNBUFFERED got the block-buffered line out
    assert "hello-stderr" in content   # stderr merged into the same file


def test_log_dir_none_falls_back_to_devnull(tmp_path):
    """No agent_log_dir config → exactly today's behavior (spawn succeeds)."""
    bus = _bus(tmp_path)  # no agent_log_dir
    assert bus._agent_log_dir is None
    _install_echo(bus.db, "a1", tmp_path)
    assert bus.start_agent("a1")["status"] == "started"
    _wait_exit(bus, "a1")


def test_unusable_log_dir_never_breaks_spawn(tmp_path):
    """The invariant's hard edge: a broken log layer must not degrade agent
    launch below DEVNULL. A file where the dir should be → warn + disabled."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("occupied")
    bus = _bus(tmp_path, agent_log_dir=str(blocker / "logs"))  # mkdir will fail
    assert bus._agent_log_dir is None  # disabled at init, not exploded

    _install_echo(bus.db, "a1", tmp_path)
    assert bus.start_agent("a1")["status"] == "started"  # spawn unaffected
    _wait_exit(bus, "a1")


def test_open_failure_falls_back_to_devnull(tmp_path, monkeypatch):
    """Per-agent open failure (dir vanished after init) → DEVNULL, spawn OK."""
    log_dir = tmp_path / "logs"
    bus = _bus(tmp_path, agent_log_dir=str(log_dir))
    import shutil
    shutil.rmtree(log_dir)
    log_dir.write_text("now a file")  # open() inside will now fail

    _install_echo(bus.db, "a1", tmp_path)
    assert bus.start_agent("a1")["status"] == "started"
    _wait_exit(bus, "a1")


def test_rotate_at_start_over_cap(tmp_path):
    log_dir = tmp_path / "logs"
    bus = _bus(tmp_path, agent_log_dir=str(log_dir), agent_log_max_bytes=1_000_000)
    log_dir.mkdir(exist_ok=True)
    big = log_dir / "a1.log"
    big.write_bytes(b"x" * 1_000_001)  # over the (floored) 1MB cap

    _install_echo(bus.db, "a1", tmp_path)
    assert bus.start_agent("a1")["status"] == "started"
    _wait_exit(bus, "a1")

    assert (log_dir / "a1.log.1").stat().st_size == 1_000_001  # rotated aside
    assert "hello-stdout" in (log_dir / "a1.log").read_text()  # fresh file in use


def test_no_rotation_under_cap(tmp_path):
    log_dir = tmp_path / "logs"
    bus = _bus(tmp_path, agent_log_dir=str(log_dir))
    log_dir.mkdir(exist_ok=True)
    (log_dir / "a1.log").write_text("previous run line\n")

    _install_echo(bus.db, "a1", tmp_path)
    bus.start_agent("a1")
    _wait_exit(bus, "a1")

    content = (log_dir / "a1.log").read_text()
    assert content.startswith("previous run line")  # appended, not truncated
    assert "hello-stdout" in content
    assert not (log_dir / "a1.log.1").exists()


def test_parent_does_not_hold_log_fd(tmp_path):
    """The child owns the only fd — the parent's copy is closed right after
    Popen, so supervisor paths that pop _processes directly can't leak."""
    log_dir = tmp_path / "logs"
    bus = _bus(tmp_path, agent_log_dir=str(log_dir))
    _install_echo(bus.db, "a1", tmp_path)
    bus.start_agent("a1")
    _wait_exit(bus, "a1")

    target = str(log_dir / "a1.log")
    held = [
        fd for fd in os.listdir("/proc/self/fd")
        if _readlink(f"/proc/self/fd/{fd}") == target
    ]
    assert held == []  # no parent-side handle to the agent's log


def _readlink(path: str) -> str:
    try:
        return os.readlink(path)
    except OSError:
        return ""


def _cleanup_buslog_handlers():
    """Remove any bus.log handlers _setup_file_logging attached to the root
    logger so tests don't leak handlers into each other."""
    import logging
    root = logging.getLogger()
    for h in list(root.handlers):
        if isinstance(h, logging.handlers.RotatingFileHandler):
            root.removeHandler(h)
            h.close()


def test_setup_file_logging_happy_path(tmp_path):
    import bus.__main__ as busmain
    try:
        assert busmain._setup_file_logging(str(tmp_path / "logs")) == str(tmp_path / "logs")
        assert (tmp_path / "logs").is_dir()
    finally:
        _cleanup_buslog_handlers()


def test_setup_file_logging_empty_disables(tmp_path):
    import bus.__main__ as busmain
    assert busmain._setup_file_logging("") == ""
    assert busmain._setup_file_logging(None) == ""


def test_setup_file_logging_unusable_dir_disables(tmp_path):
    import bus.__main__ as busmain
    blocker = tmp_path / "f"
    blocker.write_text("x")
    assert busmain._setup_file_logging(str(blocker / "logs")) == ""


def test_buslog_only_failure_keeps_agent_logs(tmp_path):
    """A bus.log-only failure (bus.log is a directory) must not disable
    per-agent logging in a still-usable dir — no layer degrades the one below
    it (codex R1)."""
    import bus.__main__ as busmain
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "bus.log").mkdir()  # RotatingFileHandler open fails; dir is fine
    try:
        assert busmain._setup_file_logging(str(log_dir)) == str(log_dir)  # NOT disabled
    finally:
        _cleanup_buslog_handlers()


def test_agent_log_max_bytes_floor(tmp_path):
    bus = _bus(tmp_path, agent_log_dir=str(tmp_path / "logs"), agent_log_max_bytes=5)
    assert bus._agent_log_max_bytes == 1_000_000  # floored, not 5 bytes
    bus2 = _bus(tmp_path, agent_log_dir=str(tmp_path / "logs"), agent_log_max_bytes="junk")
    assert bus2._agent_log_max_bytes == 50_000_000  # bad value → default
