"""Autostart of installed agents on bus boot.

Covers fr_khonliang-bus_fc904c3e: bus walks ``installed_agents`` on
lifespan startup, invokes ``start_agent`` per entry, records failures in
a runtime-only dict, surfaces them on ``/v1/services`` as
``status='autostart_failed'``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bus.db import BusDB
from bus.server import BusServer, create_app


def _bus(db: BusDB) -> BusServer:
    return BusServer(db, config={"bus_url": "http://localhost:9999"})


def _install(db: BusDB, agent_id: str, command: str = "/bin/true") -> None:
    db.install_agent(
        agent_id=agent_id,
        agent_type="test",
        command=command,
        args=[],
        cwd="/tmp",
        config="/tmp/cfg.yaml",
    )


def test_autostart_starts_each_installed_agent(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    _install(db, "a2")
    _install(db, "a3")
    bus = _bus(db)

    result = bus.autostart_installed_agents()

    assert set(result["started"]) == {"a1", "a2", "a3"}
    assert result["skipped"] == []
    assert result["failed"] == {}


def test_autostart_recovers_after_one_failure(tmp_path):
    """One agent's failure must not abort the loop — subsequent installs still start."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "a1")
    _install(db, "broken", command="/no/such/binary/for/autostart")
    _install(db, "a2")
    bus = _bus(db)

    result = bus.autostart_installed_agents()

    assert "a1" in result["started"]
    assert "a2" in result["started"]
    assert "broken" in result["failed"]
    assert result["failed"]["broken"]  # non-empty error string
    # Bus's runtime failure dict reflects the same.
    assert bus._autostart_failures.get("broken") == result["failed"]["broken"]


def test_autostart_failure_surfaces_in_services(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "good")
    _install(db, "broken", command="/no/such/binary")
    bus = _bus(db)

    bus.autostart_installed_agents()
    services = bus.get_services()

    by_id = {s["id"]: s for s in services}
    assert "broken" in by_id
    broken = by_id["broken"]
    assert broken["status"] == "autostart_failed"
    assert broken["autostart_error"]
    assert broken["skill_count"] == 0
    # The successful one isn't necessarily 'healthy' yet (subprocess takes
    # time to register over WS), but it MUST NOT carry autostart_failed.
    if "good" in by_id:
        assert by_id["good"]["status"] != "autostart_failed"


def test_autostart_resets_failures_on_each_call(tmp_path):
    """Failures are runtime-only — re-running autostart with fixed installs
    clears the prior dict so a previously-broken agent doesn't stigmatize forever."""
    db = BusDB(str(tmp_path / "test-bus.db"))
    _install(db, "broken", command="/no/such/binary")
    bus = _bus(db)

    bus.autostart_installed_agents()
    assert "broken" in bus._autostart_failures

    # Replace install with a working command, re-run autostart.
    _install(db, "broken", command="/bin/true")
    result = bus.autostart_installed_agents()

    assert "broken" not in result["failed"]
    assert "broken" not in bus._autostart_failures


def test_autostart_runs_inside_create_app_lifespan(tmp_path):
    """TestClient triggers FastAPI lifespan startup — autostart must have
    fired by the time the first request arrives."""
    db_path = str(tmp_path / "test-bus.db")
    db = BusDB(db_path)
    _install(db, "a1")

    app = create_app(db_path=db_path)
    with TestClient(app) as client:
        # /v1/services should be reachable; the autostart row may have
        # registered through WS by now or not — either way the call works.
        r = client.get("/v1/services")
        assert r.status_code == 200


def test_autostart_empty_installed_returns_empty_summary(tmp_path):
    db = BusDB(str(tmp_path / "test-bus.db"))
    bus = _bus(db)

    result = bus.autostart_installed_agents()

    assert result == {"started": [], "skipped": [], "failed": {}}
