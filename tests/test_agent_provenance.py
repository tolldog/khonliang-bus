"""Tests for ``fr_khonliang-bus_aa096048`` — agent provenance primitive.

Covers:
- ``BusDB`` persistence of ``launch_spec`` / ``launch_info`` on register.
- Forward-compat: agents that omit launch fields still register correctly.
- Migration: existing DBs (no launch columns) get the columns added via
  ``ADD COLUMN`` without data loss.
- ``BusServer.get_agent_provenance`` join semantics for each of the four
  ``registration_type`` cases (canonical / adhoc / unknown / none).
- REST surface ``GET /v1/agent/{id}/provenance``.

bus-lib's contribution (the wire-side ``launch_spec`` / ``launch_info``
shape) is unit-tested in ``khonliang-bus-lib/tests/test_launch.py``;
here we test the bus's persistence + join semantics only.
"""

from __future__ import annotations

import sqlite3

import pytest

from bus.db import BusDB


# ---------------------------------------------------------------------------
# DB layer — persistence + backward compatibility
# ---------------------------------------------------------------------------


def test_register_persists_launch_spec_and_info(db):
    spec = {
        "executable": "/opt/x/.venv/bin/python",
        "args": ["-m", "x.agent", "--config", "/etc/x/config.yaml"],
        "cwd": "/opt/x",
        "config": "/etc/x/config.yaml",
    }
    info = {
        "started_at": 1234567.0,
        "commit_sha": "deadbeef" * 5,
        "branch": "main",
        "dirty": False,
    }
    db.register_agent(
        "a1", "x", "ws://connected", 4242, "0.1.0",
        launch_spec=spec, launch_info=info,
    )
    reg = db.get_registration("a1")
    assert reg is not None
    assert reg["launch_spec"] == spec
    assert reg["launch_info"] == info


def test_register_without_launch_fields_stores_null(db):
    """Backward compat: pre-PR#24 bus-lib doesn't send launch_*; stored as None."""
    db.register_agent("a1", "x", "ws://connected", 4242, "0.1.0")
    reg = db.get_registration("a1")
    assert reg["launch_spec"] is None
    assert reg["launch_info"] is None


def test_re_register_overwrites_launch_fields(db):
    """A re-register (e.g. after a code update) replaces prior launch_*."""
    db.register_agent(
        "a1", "x", "ws://connected", 1, "0.1.0",
        launch_spec={"executable": "/old", "args": [], "cwd": "/", "config": None},
    )
    db.register_agent(
        "a1", "x", "ws://connected", 2, "0.1.0",
        launch_spec={"executable": "/new", "args": ["-m", "y"], "cwd": "/new", "config": None},
    )
    reg = db.get_registration("a1")
    assert reg["launch_spec"]["executable"] == "/new"
    assert reg["pid"] == 2


def test_get_registrations_returns_parsed_launch_fields(db):
    """list endpoint also returns inflated dicts, not JSON strings."""
    db.register_agent(
        "a1", "x", "ws://connected", 1, "0.1.0",
        launch_spec={"executable": "/x", "args": [], "cwd": "/", "config": None},
        launch_info={"started_at": 0.0, "commit_sha": None, "branch": None, "dirty": None},
    )
    regs = db.get_registrations()
    assert len(regs) == 1
    assert isinstance(regs[0]["launch_spec"], dict)
    assert isinstance(regs[0]["launch_info"], dict)


def test_migration_adds_launch_columns_to_pre_existing_registrations(tmp_path):
    """A DB created at the old schema (no launch_* columns) gains them via
    ``ALTER TABLE ADD COLUMN`` on the next BusDB init, no data loss.
    """
    db_path = str(tmp_path / "legacy.db")
    # Hand-build the old schema for ``registrations``.
    con = sqlite3.connect(db_path)
    con.execute(
        "CREATE TABLE registrations ("
        " id TEXT PRIMARY KEY, agent_type TEXT NOT NULL, callback_url TEXT NOT NULL,"
        " pid INTEGER NOT NULL, version TEXT,"
        " registered_at TEXT NOT NULL DEFAULT (datetime('now')),"
        " last_heartbeat TEXT NOT NULL DEFAULT (datetime('now')),"
        " status TEXT NOT NULL DEFAULT 'healthy'"
        ")"
    )
    con.execute(
        "INSERT INTO registrations (id, agent_type, callback_url, pid, version) "
        "VALUES ('legacy-agent', 't', 'ws://connected', 100, '0.1.0')"
    )
    con.commit()
    con.close()

    # Now open via BusDB — migration should ADD COLUMN.
    db = BusDB(db_path)
    # The pre-existing row survives.
    reg = db.get_registration("legacy-agent")
    assert reg is not None
    assert reg["pid"] == 100
    # And it carries None for the new columns (no data lost, no spurious values).
    assert reg["launch_spec"] is None
    assert reg["launch_info"] is None
    # New registers work with full launch fields.
    db.register_agent(
        "modern-agent", "t", "ws://connected", 200, "0.2.0",
        launch_spec={"executable": "/x", "args": [], "cwd": "/", "config": None},
    )
    assert db.get_registration("modern-agent")["launch_spec"]["executable"] == "/x"


def test_migration_is_idempotent(tmp_path):
    """Re-opening a BusDB twice doesn't error on the second migration."""
    db_path = str(tmp_path / "idempotent.db")
    BusDB(db_path)
    # Second open should be a no-op for the migration.
    BusDB(db_path)  # would raise if ADD COLUMN tried again unguarded


# ---------------------------------------------------------------------------
# get_agent_provenance — the four registration_type cases
# ---------------------------------------------------------------------------


def _canonical_spec() -> dict:
    return {
        "executable": "/opt/x/.venv/bin/python",
        "args": ["-m", "x.agent", "--config", "/etc/x/config.yaml"],
        "cwd": "/opt/x",
        "config": "/etc/x/config.yaml",
    }


def _install_canonical(db: BusDB, agent_id: str = "x-primary") -> None:
    spec = _canonical_spec()
    db.install_agent(
        agent_id, "x", spec["executable"], spec["args"], spec["cwd"], spec["config"],
    )


def test_provenance_none_when_no_registration_and_no_install(bus):
    p = bus.get_agent_provenance("ghost")
    assert p["registration_type"] == "none"
    assert p["process"] is None
    assert p["canonical_install"] is None
    assert p["match"] is None


def test_provenance_none_when_installed_but_not_running(bus, db):
    """Agent has a canonical install row but no live registration."""
    _install_canonical(db)
    p = bus.get_agent_provenance("x-primary")
    assert p["registration_type"] == "none"
    assert p["canonical_install"] is not None
    assert p["canonical_install"]["command"] == "/opt/x/.venv/bin/python"
    assert p["match"] is None


def test_provenance_canonical_when_launch_spec_matches(bus, db):
    """Runtime launch_spec exactly matches installed_agents → canonical."""
    _install_canonical(db)
    spec = _canonical_spec()
    db.register_agent(
        "x-primary", "x", "ws://connected", 4242, "0.1.0",
        launch_spec=spec,
        launch_info={"started_at": 1.0, "commit_sha": None, "branch": None, "dirty": None},
    )
    p = bus.get_agent_provenance("x-primary")
    assert p["registration_type"] == "canonical"
    assert p["match"] is True
    assert p["process"]["pid"] == 4242
    assert p["process"]["executable"] == spec["executable"]
    assert p["process"]["cwd"] == spec["cwd"]


def test_provenance_adhoc_when_launch_spec_diverges(bus, db):
    """Runtime launch_spec differs from canonical → adhoc."""
    _install_canonical(db)
    spec = _canonical_spec()
    spec["cwd"] = "/home/dev/khonliang-x"  # dev-launched, different cwd
    db.register_agent(
        "x-primary", "x", "ws://connected", 9214, "0.1.0",
        launch_spec=spec,
    )
    p = bus.get_agent_provenance("x-primary")
    assert p["registration_type"] == "adhoc"
    assert p["match"] is False
    assert p["process"]["cwd"] == "/home/dev/khonliang-x"
    assert p["canonical_install"]["cwd"] == "/opt/x"


def test_provenance_adhoc_when_no_canonical_install(bus, db):
    """Agent registered but never installed via bus_start_agent — adhoc."""
    db.register_agent(
        "rogue-primary", "rogue", "ws://connected", 1, "0.1.0",
        launch_spec=_canonical_spec(),
    )
    p = bus.get_agent_provenance("rogue-primary")
    assert p["registration_type"] == "adhoc"
    assert p["match"] is None  # no canonical to compare against
    assert p["canonical_install"] is None
    assert any("no canonical install" in n for n in p["notes"])


def test_provenance_unknown_when_pre_pr24_buslib(bus, db):
    """Older bus-lib doesn't send launch_spec → can't determine match."""
    _install_canonical(db)
    db.register_agent("x-primary", "x", "ws://connected", 7777, "0.1.0")
    p = bus.get_agent_provenance("x-primary")
    assert p["registration_type"] == "unknown"
    assert p["match"] is None
    assert any("did not report launch_spec" in n for n in p["notes"])
    # Pid still reported.
    assert p["process"]["pid"] == 7777


def test_provenance_includes_code_block_from_launch_info(bus, db):
    _install_canonical(db)
    db.register_agent(
        "x-primary", "x", "ws://connected", 4242, "0.1.0",
        launch_spec=_canonical_spec(),
        launch_info={
            "started_at": 1234.0,
            "commit_sha": "abcdef0" * 5 + "1",
            "branch": "main",
            "dirty": True,
        },
    )
    p = bus.get_agent_provenance("x-primary")
    assert p["code"]["commit_sha"].startswith("abcdef0")
    assert p["code"]["branch"] == "main"
    assert p["code"]["dirty"] is True
    # behind_main is bus-side null; downstream consumers compute it.
    assert p["code"]["behind_main"] is None


def test_provenance_args_comparison_is_deep_equality(bus, db):
    """Args list ordering / contents matter for match — element-by-element."""
    _install_canonical(db)
    spec = _canonical_spec()
    # Swap an arg → diverges.
    spec["args"] = ["-m", "x.agent", "--config", "/etc/x/OTHER.yaml"]
    db.register_agent("x-primary", "x", "ws://connected", 1, "0.1.0", launch_spec=spec)
    assert bus.get_agent_provenance("x-primary")["match"] is False


# ---------------------------------------------------------------------------
# REST surface — GET /v1/agent/{id}/provenance
# ---------------------------------------------------------------------------


def test_rest_provenance_endpoint_returns_dict(client):
    """The HTTP route mirrors the method output as JSON."""
    # Install + simulate a registration via the install->start path is heavy;
    # use the install endpoint to create a canonical row and confirm the
    # endpoint returns ``none`` for the not-yet-running case.
    client.post("/v1/install", json={
        "agent_type": "test",
        "id": "test-agent",
        "command": "/usr/bin/python3",
        "args": ["-m", "test"],
        "cwd": "/tmp",
        "config": "/tmp/c.yaml",
    })
    r = client.get("/v1/agent/test-agent/provenance")
    assert r.status_code == 200
    body = r.json()
    assert body["agent_id"] == "test-agent"
    assert body["registration_type"] == "none"
    assert body["canonical_install"]["command"] == "/usr/bin/python3"


def test_rest_provenance_endpoint_for_unknown_agent(client):
    """Endpoint returns the ``none`` shape for an entirely unknown agent_id."""
    r = client.get("/v1/agent/no-such-agent/provenance")
    assert r.status_code == 200
    body = r.json()
    assert body["registration_type"] == "none"
    assert body["canonical_install"] is None
    assert body["match"] is None
