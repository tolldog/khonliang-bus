"""The bus as a first-class participant in its own catalog.

Covers fr_khonliang-bus_6638f4dc: bus_welcome renders a top-level ``bus``
entry, bus_skills(agent_id='bus') returns the bus tool catalog, and the
declared bus_* skill list (bus/welcome.json) is asserted against the adapter's
actually-registered bus_* tools so it can't silently drift.

The bus is rendered as a synthesized sibling field — deliberately NOT injected
into the agent registry — so it never becomes a routing / liveness target.
"""

from __future__ import annotations

import asyncio

import pytest

from bus.db import BusDB
from bus.mcp_adapter import BusMCPAdapter
from bus.server import BusServer, bus_self_skill_names, load_bus_self_welcome


def _adapter() -> BusMCPAdapter:
    a = BusMCPAdapter(bus_url="http://localhost:8787")
    a._register_bus_tools()
    return a


def _registered_bus_tool_names() -> set[str]:
    a = _adapter()
    tools = asyncio.run(a.mcp.list_tools())
    return {t.name for t in tools if t.name.startswith("bus_")}


# ---------------------------------------------------------------------------
# AC#3 — the sync test (the structural reason this FR matters)
# ---------------------------------------------------------------------------


def test_welcome_skill_list_matches_registered_bus_tools():
    """bus/welcome.json must enumerate EXACTLY the bus_* tools the adapter
    registers — no missing entries (undiscoverable tools) and no stale ones
    (advertised tools that don't exist). Adding a bus_* tool without updating
    welcome.json fails here."""
    declared = set(bus_self_skill_names())
    registered = _registered_bus_tool_names()

    missing = registered - declared      # real tools not advertised
    stale = declared - registered        # advertised tools that don't exist
    assert not missing, f"welcome.json missing bus tools: {sorted(missing)}"
    assert not stale, f"welcome.json lists non-existent bus tools: {sorted(stale)}"


def test_welcome_skill_names_are_unique():
    """No dup across categories (a dup would make the count wrong + is a
    copy-paste smell)."""
    names = bus_self_skill_names()
    assert len(names) == len(set(names)), "duplicate skill names in welcome.json"


# ---------------------------------------------------------------------------
# AC#2 — bus appears in bus_welcome as a top-level entry
# ---------------------------------------------------------------------------


def test_bus_welcome_includes_bus_entry(tmp_path):
    db = BusDB(str(tmp_path / "b.db"))
    bus = BusServer(db, config={})
    w = bus.get_bus_welcome(detail="brief")

    assert "bus" in w
    entry = w["bus"]
    assert entry["kind"] == "bus"
    assert entry["identity"].startswith("khonliang-bus")
    assert entry["role"]
    assert entry["skill_count"] == len(bus_self_skill_names())
    assert entry["state"] == "healthy"
    # It must NOT be smuggled into agents[] (would look like a routable agent).
    assert all(a["agent_id"] != "bus" for a in w["agents"])


def test_bus_welcome_full_does_not_leak_cached_blob(tmp_path):
    """Mutating the returned bus.skills_by_category / suggested_next must not
    corrupt the process-lifetime cached welcome blob."""
    db = BusDB(str(tmp_path / "b.db"))
    bus = BusServer(db, config={})

    w1 = bus.get_bus_welcome(detail="full")
    w1["bus"]["skills_by_category"]["discovery"].append("bus_INJECTED")
    w1["bus"]["suggested_next"].append("INJECTED")

    w2 = bus.get_bus_welcome(detail="full")
    assert "bus_INJECTED" not in w2["bus"]["skills_by_category"]["discovery"]
    assert "INJECTED" not in w2["bus"]["suggested_next"]


def test_bus_welcome_full_has_skill_categories(tmp_path):
    db = BusDB(str(tmp_path / "b.db"))
    bus = BusServer(db, config={})
    w = bus.get_bus_welcome(detail="full")

    cats = w["bus"]["skills_by_category"]
    assert "discovery" in cats and "bus_welcome" in cats["discovery"]
    assert "webhooks" in cats
    assert w["bus"]["boundaries"]  # editorial field surfaced at full detail


# ---------------------------------------------------------------------------
# AC#5 — works with zero agents live
# ---------------------------------------------------------------------------


def test_bus_welcome_bus_entry_present_with_zero_agents(tmp_path):
    db = BusDB(str(tmp_path / "b.db"))
    bus = BusServer(db, config={})
    w = bus.get_bus_welcome(detail="brief")

    assert w["agents"] == []          # nothing registered/installed
    assert w["bus"]["kind"] == "bus"  # bus is still present
    assert w["platform"]["schema_version"] == BusServer.BUS_WELCOME_SCHEMA_VERSION


# ---------------------------------------------------------------------------
# AC#4 — bus_skills(agent_id='bus') returns the bus catalog, not "no skills"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bus_skills_for_bus_returns_catalog():
    a = _adapter()
    result = await a.mcp.call_tool("bus_skills", {"agent_id": "bus"})
    text = result[1]["result"]

    assert "no skills registered" not in text
    # Every declared bus tool shows up in the rendered catalog.
    for name in bus_self_skill_names():
        assert name in text


@pytest.mark.asyncio
async def test_bus_skills_bus_handles_whitespace_only_description():
    """A bus tool with a whitespace-only description must not crash the catalog
    (`.strip().splitlines()[0]` would IndexError on an empty split)."""
    a = _adapter()

    @a.mcp.tool(name="bus_blankdesc", description="   \n  ")
    def _blank() -> str:  # pragma: no cover - never invoked
        return ""

    result = await a.mcp.call_tool("bus_skills", {"agent_id": "bus"})  # must not raise
    assert "bus_blankdesc" in result[1]["result"]


@pytest.mark.asyncio
async def test_bus_is_a_reserved_agent_id(tmp_path):
    """'bus' is reserved for the bus's own catalog — a real agent must not be
    able to install or register under it (that would create ambiguous
    bus_skills(agent_id='bus') semantics)."""
    from bus.server import RegisterRequest, InstallRequest

    db = BusDB(str(tmp_path / "b.db"))
    bus = BusServer(db, config={})

    inst = bus.install_agent(InstallRequest(id="bus", agent_type="x", command="/bin/true", args=[], cwd="/tmp", config="/tmp/c.yaml"))
    assert "reserved" in inst.get("error", "")

    reg = await bus.register_agent(RegisterRequest(id="bus", callback="http://x", pid=1))
    assert "reserved" in reg.get("error", "")
    # And nothing got written.
    assert db.get_registration("bus") is None


def test_boot_purges_preexisting_bus_agent(tmp_path):
    """An upgraded deployment that already has a real 'bus' agent (from before
    the reservation) must have it purged on boot, not left shadowed."""
    db = BusDB(str(tmp_path / "b.db"))
    # Simulate a pre-reservation catalog row written directly at the db layer.
    db.install_agent(agent_id="bus", agent_type="x", command="/bin/true", args=[], cwd="/tmp", config="/tmp/c.yaml")
    db.register_agent(agent_id="bus", agent_type="x", callback_url="cb", pid=0)

    bus = BusServer(db, config={})
    bus.reconcile_on_boot()

    assert db.get_registration("bus") is None
    assert db.get_installed_agent("bus") is None


def test_bus_welcome_skips_residual_bus_row(tmp_path):
    """Even a residual 'bus' catalog row (e.g. a welcome that survived
    deregister) must not appear as a pseudo-agent in agents[]."""
    db = BusDB(str(tmp_path / "b.db"))
    db.set_agent_welcome("bus", {"role": "stale"})  # welcome-only residue

    bus = BusServer(db, config={})
    w = bus.get_bus_welcome(detail="brief")

    assert all(a["agent_id"] != "bus" for a in w["agents"])  # not double-listed
    assert w["bus"]["kind"] == "bus"                          # synthetic entry still present


@pytest.mark.asyncio
async def test_bus_skills_bus_excludes_agent_named_bus_worker():
    """An agent whose id starts with 'bus_' has skill tools like
    'bus_worker.do_thing' — those must NOT pollute the bus's own catalog."""
    a = _adapter()
    # Simulate a registered agent skill tool by adding it to the tracked set +
    # exposing it as an MCP tool.
    @a.mcp.tool(name="bus_worker.do_thing")
    def _bus_worker_skill() -> str:  # pragma: no cover - never invoked
        return ""

    a._registered_tools.add("bus_worker.do_thing")

    result = await a.mcp.call_tool("bus_skills", {"agent_id": "bus"})
    text = result[1]["result"]

    assert "bus_worker.do_thing" not in text
    assert "bus_welcome" in text  # real bus tools still present


# ---------------------------------------------------------------------------
# welcome.json shape sanity
# ---------------------------------------------------------------------------


def test_welcome_json_has_required_fields():
    blob = load_bus_self_welcome()
    for k in ("kind", "identity", "role", "skills_by_category", "suggested_next"):
        assert k in blob, f"welcome.json missing {k!r}"
    assert blob["kind"] == "bus"
