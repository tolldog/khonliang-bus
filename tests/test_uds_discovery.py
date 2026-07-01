"""Unix domain socket discovery + dual-bind (fr_khonliang-bus_aa5b25cf, Phase 1).

Contract: docs/bus-transport-contract.md. Covers discover_bus() resolution, the
adapter's UDS httpx transport, the bus __main__ bind resolution + stale-socket
handling, the once-guarded dual-bind lifespan, and a real UDS bind→connect E2E.
"""

from __future__ import annotations

import asyncio
import socket

import httpx
import pytest

from bus import __main__ as busmain
from bus.mcp_adapter import BusMCPAdapter, discover_bus
from bus.server import create_app


# ---------------------------------------------------------------------------
# discover_bus resolution order
# ---------------------------------------------------------------------------


def test_discover_bus_explicit_wins(monkeypatch):
    monkeypatch.setenv("KHONLIANG_BUS_URL", "http://env:2")
    assert discover_bus("http://explicit:1") == "http://explicit:1"


def test_discover_bus_env(monkeypatch):
    monkeypatch.setenv("KHONLIANG_BUS_URL", "http://env:2")
    assert discover_bus() == "http://env:2"


def test_discover_bus_live_socket(monkeypatch, tmp_path):
    monkeypatch.delenv("KHONLIANG_BUS_URL", raising=False)
    sock = tmp_path / "bus.sock"
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(sock))
    srv.listen(1)  # a live listener
    monkeypatch.setattr("bus.mcp_adapter.BUS_SOCK_PATH", sock)
    try:
        assert discover_bus() == f"unix://{sock}"
    finally:
        srv.close()
        sock.unlink(missing_ok=True)


def test_discover_bus_ignores_dead_socket(monkeypatch, tmp_path):
    """A stale socket (exists but nothing listening — crashed run, or bus now on
    --no-uds) must not be selected over the healthy TCP fallback."""
    monkeypatch.delenv("KHONLIANG_BUS_URL", raising=False)
    sock = tmp_path / "bus.sock"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(str(sock))
    s.close()  # socket file remains, nothing listening
    monkeypatch.setattr("bus.mcp_adapter.BUS_SOCK_PATH", sock)
    assert discover_bus() == "http://localhost:8787"


def test_discover_bus_fallback(monkeypatch, tmp_path):
    monkeypatch.delenv("KHONLIANG_BUS_URL", raising=False)
    monkeypatch.setattr("bus.mcp_adapter.BUS_SOCK_PATH", tmp_path / "absent.sock")
    assert discover_bus() == "http://localhost:8787"


# ---------------------------------------------------------------------------
# adapter UDS transport
# ---------------------------------------------------------------------------


async def test_adapter_uds_uses_uds_transport():
    a = BusMCPAdapter("unix:///tmp/khonliang-test.sock")
    try:
        assert a._uds_path == "/tmp/khonliang-test.sock"
        assert a.bus_url == "http://localhost"  # dummy base for httpx
        # httpx routes the transport to the socket.
        assert a._async_http._transport.__class__.__name__ == "AsyncHTTPTransport"
    finally:
        await a.aclose()


async def test_adapter_tcp_unchanged():
    a = BusMCPAdapter("http://localhost:8787/")
    try:
        assert a._uds_path is None
        assert a.bus_url == "http://localhost:8787"
    finally:
        await a.aclose()


# ---------------------------------------------------------------------------
# __main__ bind resolution
# ---------------------------------------------------------------------------


class _Args:
    host = "0.0.0.0"
    port = 8787


def test_resolve_tcp_default(monkeypatch):
    monkeypatch.delenv("KHONLIANG_BUS_LISTEN", raising=False)
    assert busmain._resolve_tcp(_Args()) == ("0.0.0.0", 8787)


def test_resolve_tcp_off(monkeypatch):
    monkeypatch.setenv("KHONLIANG_BUS_LISTEN", "off")
    assert busmain._resolve_tcp(_Args()) is None


def test_resolve_tcp_override(monkeypatch):
    monkeypatch.setenv("KHONLIANG_BUS_LISTEN", "127.0.0.1:9999")
    assert busmain._resolve_tcp(_Args()) == ("127.0.0.1", 9999)


def test_resolve_self_url_prefers_tcp():
    from pathlib import Path
    assert busmain._resolve_self_url(("0.0.0.0", 8787), Path("/x/bus.sock")) == "http://localhost:8787"


def test_resolve_self_url_uds_only():
    from pathlib import Path
    # TCP disabled → advertise the socket, not a dead port.
    assert busmain._resolve_self_url(None, Path("/x/bus.sock")) == "unix:///x/bus.sock"


def test_clear_stale_socket_removes_dead(tmp_path):
    sock = tmp_path / "bus.sock"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(str(sock))
    s.close()  # socket file remains, nothing listening → stale, ConnectionRefused
    assert sock.exists()
    busmain._clear_stale_socket(sock)
    assert not sock.exists()


def test_clear_stale_socket_refuses_non_socket(tmp_path):
    """A --uds pointing at a real file must NOT be silently deleted."""
    f = tmp_path / "bus.sock"
    f.write_text("important data")
    with pytest.raises(SystemExit):
        busmain._clear_stale_socket(f)
    assert f.exists()  # data preserved


def test_validate_binds_rejects_uds_only(tmp_path):
    from pathlib import Path
    # UDS-only is blocked until bus-lib Phase 2 (internal callers assume HTTP).
    with pytest.raises(SystemExit):
        busmain._validate_binds(None, Path("/x/bus.sock"))


def test_validate_binds_rejects_nothing(tmp_path):
    with pytest.raises(SystemExit):
        busmain._validate_binds(None, None)


def test_validate_binds_allows_dual(tmp_path):
    from pathlib import Path
    busmain._validate_binds(("0.0.0.0", 8787), Path("/x/bus.sock"))  # no raise


def test_clear_stale_socket_refuses_live(tmp_path):
    sock = tmp_path / "bus.sock"
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(sock))
    srv.listen(1)
    try:
        with pytest.raises(SystemExit):
            busmain._clear_stale_socket(sock)  # a live listener → refuse
    finally:
        srv.close()
        sock.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# dual-bind once-guard (two lifespans over one app boot exactly once)
# ---------------------------------------------------------------------------


async def test_lifespan_boots_once_for_dual_bind(tmp_path, monkeypatch):
    import bus.server as srv
    calls = {"boot": 0, "shutdown": 0}
    monkeypatch.setattr(srv.BusServer, "reconcile_on_boot",
                        lambda self: calls.__setitem__("boot", calls["boot"] + 1) or {})
    monkeypatch.setattr(srv.BusServer, "autostart_installed_agents", lambda self: {})
    monkeypatch.setattr(srv.BusServer, "start_supervisor", lambda self: None)

    async def _fake_shutdown(self):
        calls["shutdown"] += 1
    monkeypatch.setattr(srv.BusServer, "shutdown", _fake_shutdown)

    app = create_app(db_path=str(tmp_path / "b.db"), config={})
    # Two concurrent lifespans (simulating the UDS + TCP servers).
    cm1 = app.router.lifespan_context(app)
    cm2 = app.router.lifespan_context(app)
    await cm1.__aenter__()
    await cm2.__aenter__()
    assert calls["boot"] == 1  # booted once despite two lifespan entries
    await cm1.__aexit__(None, None, None)
    assert calls["shutdown"] == 0  # one listener gone, other still serving → NOT torn down
    await cm2.__aexit__(None, None, None)
    assert calls["shutdown"] == 1  # shut down on the LAST listener out


# ---------------------------------------------------------------------------
# E2E: bus binds a real UDS; the adapter's transport reaches it
# ---------------------------------------------------------------------------


async def test_bus_binds_uds_and_client_connects(tmp_path):
    import uvicorn
    sock = tmp_path / "bus.sock"
    app = create_app(db_path=str(tmp_path / "b.db"), config={})
    # lifespan='off' — we only need the HTTP surface, not boot/supervisor.
    server = uvicorn.Server(uvicorn.Config(app, uds=str(sock), log_level="warning", lifespan="off"))
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(200):  # wait up to ~10s for the socket
            if server.started and sock.exists():
                break
            await asyncio.sleep(0.05)
        assert sock.exists(), "bus did not create the UDS"

        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(sock)), base_url="http://localhost"
        ) as client:
            r = await client.get("/v1/status")
            assert r.status_code == 200
            assert "installed_agents" in r.json()
    finally:
        server.should_exit = True
        await task
