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


# ---------------------------------------------------------------------------
# HTTP /v1/register parity with WebSocket register path (Copilot R1 #2)
# ---------------------------------------------------------------------------


def test_http_register_persists_launch_fields(tmp_path):
    """The HTTP /v1/register endpoint also forwards launch_spec / launch_info
    so HTTP-register callers get the same provenance treatment as WS callers.

    Without this wiring, a new bus-lib client speaking HTTP would have its
    launch fields silently dropped and provenance would always report
    ``unknown``. We opt into ``provenance_disclose_full`` for this test so
    persistence is verifiable via the public read surface; redact-by-default
    behavior is covered by ``test_rest_endpoint_redacts_by_default``.
    """
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        db_path=str(tmp_path / "http-reg.db"),
        config={"provenance_disclose_full": True},
    )
    c = TestClient(app)

    spec = {
        "executable": "/opt/x/.venv/bin/python",
        "args": ["-m", "x.agent"],
        "cwd": "/opt/x",
        "config": "/opt/x/config.yaml",
    }
    info = {"started_at": 1.0, "commit_sha": None, "branch": None, "dirty": None}
    r = c.post("/v1/register", json={
        "id": "http-agent",
        "callback": "http://localhost:9000",
        "pid": 4242,
        "version": "0.1.0",
        "skills": [],
        "launch_spec": spec,
        "launch_info": info,
    })
    assert r.status_code == 200
    prov = c.get("/v1/agent/http-agent/provenance").json()
    # No canonical install for "http-agent" → adhoc (with notes), but launch
    # fields are visible under disclose_full — proving they were persisted.
    assert prov["process"]["executable"] == "/opt/x/.venv/bin/python"
    assert prov["process"]["cwd"] == "/opt/x"
    assert prov["registration_type"] in ("adhoc",)


def test_http_register_without_launch_fields_still_works(client):
    """Forward compat: pre-PR#24 clients still register correctly."""
    r = client.post("/v1/register", json={
        "id": "legacy-http-agent",
        "callback": "http://localhost:9000",
        "pid": 1, "version": "0.0.0", "skills": [],
    })
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Redact-sensitive flag (Copilot R1 #1: information disclosure on
# unauthenticated 0.0.0.0 listener)
# ---------------------------------------------------------------------------


def test_provenance_redacts_sensitive_fields_when_flagged(bus, db):
    """When redact_sensitive=True, executable/cwd/config + the entire code
    block are masked. Callers still see registration_type + match — the
    verdict — without the underlying paths or commit SHA.
    """
    _install_canonical(db)
    spec = _canonical_spec()
    db.register_agent(
        "x-primary", "x", "ws://connected", 4242, "0.1.0",
        launch_spec=spec,
        launch_info={"started_at": 1.0, "commit_sha": "deadbeef" * 5, "branch": "main", "dirty": True},
    )
    p = bus.get_agent_provenance("x-primary", redact_sensitive=True)

    assert p["registration_type"] == "canonical"
    assert p["match"] is True  # verdict still exposed
    assert p["process"]["pid"] == 4242  # pid is not in the sensitive set
    assert p["process"]["executable"] == "<redacted>"
    assert p["process"]["cwd"] == "<redacted>"
    assert p["process"]["config"] == "<redacted>"
    assert p["process"]["args"] == ["<redacted>"]
    # Commit/branch/dirty are dropped entirely under redaction.
    assert p["code"] is None
    # Canonical install paths also masked.
    assert p["canonical_install"]["command"] == "<redacted>"
    assert p["canonical_install"]["cwd"] == "<redacted>"
    assert p["canonical_install"]["config"] == "<redacted>"


def test_provenance_unredacted_by_default(bus, db):
    """The unredacted call site is the default — local-trusted clients
    (MCP adapter, developer-side introspection) want full data.
    """
    _install_canonical(db)
    db.register_agent(
        "x-primary", "x", "ws://connected", 4242, "0.1.0",
        launch_spec=_canonical_spec(),
        launch_info={"started_at": 1.0, "commit_sha": "abc", "branch": "main", "dirty": False},
    )
    p = bus.get_agent_provenance("x-primary")  # no redact flag → default False
    assert p["process"]["executable"] == "/opt/x/.venv/bin/python"
    assert p["code"]["commit_sha"] == "abc"


def _register_canonical_via_http(c) -> None:
    """Helper: install + HTTP-register an agent with matching launch_spec."""
    c.post("/v1/install", json={
        "agent_type": "x", "id": "x-primary",
        "command": "/opt/x/python", "args": ["-m", "x"],
        "cwd": "/opt/x", "config": "/opt/x/config.yaml",
    })
    c.post("/v1/register", json={
        "id": "x-primary", "callback": "http://localhost:9000",
        "pid": 4242, "version": "0.1.0", "skills": [],
        "launch_spec": {
            "executable": "/opt/x/python", "args": ["-m", "x"],
            "cwd": "/opt/x", "config": "/opt/x/config.yaml",
        },
        "launch_info": {"started_at": 1.0, "commit_sha": "abc", "branch": "main", "dirty": False},
    })


def test_rest_endpoint_redacts_by_default(tmp_path):
    """Default HTTP behavior is redacted: an unauthenticated 0.0.0.0
    listener must not leak host/source metadata to network peers.
    Operators on local-trusted hosts opt INTO disclosure (see next test).

    Closes Copilot R2 on PR #34: redact-by-default is the correct stance
    for a control-plane route on an unauthenticated public listener.
    """
    from bus.server import create_app
    from fastapi.testclient import TestClient

    # No provenance_disclose_full in config → defaults to redacted.
    app = create_app(db_path=str(tmp_path / "default.db"))
    c = TestClient(app)
    _register_canonical_via_http(c)

    body = c.get("/v1/agent/x-primary/provenance").json()
    assert body["match"] is True          # verdict still visible
    assert body["registration_type"] == "canonical"
    assert body["process"]["pid"] == 4242  # pid not in sensitive set
    # Sensitive fields masked.
    assert body["process"]["executable"] == "<redacted>"
    assert body["process"]["cwd"] == "<redacted>"
    assert body["code"] is None
    assert body["canonical_install"]["command"] == "<redacted>"


def test_rest_endpoint_discloses_full_when_opted_in(tmp_path):
    """``provenance_disclose_full: true`` opts into unredacted disclosure
    on the HTTP route — typical config for a local-trusted host.
    """
    from bus.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        db_path=str(tmp_path / "disclose.db"),
        config={"provenance_disclose_full": True},
    )
    c = TestClient(app)
    _register_canonical_via_http(c)

    body = c.get("/v1/agent/x-primary/provenance").json()
    assert body["match"] is True
    # Sensitive fields revealed under explicit opt-in.
    assert body["process"]["executable"] == "/opt/x/python"
    assert body["process"]["cwd"] == "/opt/x"
    assert body["code"]["commit_sha"] == "abc"
    assert body["canonical_install"]["command"] == "/opt/x/python"


def test_rest_endpoint_unknown_agent_safe_either_way(tmp_path):
    """The ``none`` shape (no runtime, no install) is safe to expose under
    both flag positions — no host data to redact in the first place.
    """
    from bus.server import create_app
    from fastapi.testclient import TestClient

    for cfg in ({}, {"provenance_disclose_full": True}):
        app = create_app(db_path=str(tmp_path / f"unk-{bool(cfg)}.db"), config=cfg)
        c = TestClient(app)
        body = c.get("/v1/agent/ghost/provenance").json()
        assert body["registration_type"] == "none"
        assert body["canonical_install"] is None
