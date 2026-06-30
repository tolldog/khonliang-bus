"""Tests for the outbound webhook-management REST routes + bus_webhook_* MCP tools.

The routes (``/v1/webhooks/manage/*`` in ``bus/server.py``) and the MCP tools
(``bus_webhook_*`` in ``bus/mcp_adapter.py``) compose around the
``bus.webhook_install`` primitives, which are unit-tested separately in
``test_webhook_install.py``. Here we test the *wiring*: config resolution, the
admin opt-in gate, error→HTTP mapping, and the terse tool-response formatting.

GitHub is mocked via ``httpx.MockTransport`` by monkeypatching
``webhook_install.make_client`` so no network I/O occurs.
"""

from __future__ import annotations

from typing import Any, Callable

import httpx
import pytest
from fastapi.testclient import TestClient

from bus import webhook_install as wi
from bus.mcp_adapter import BusMCPAdapter
from bus.server import create_app

CANONICAL_URL = "https://example.test/v1/webhooks/github"
DEFAULT_SECRET = "test-secret-value"

BASE_CONFIG = {
    "github_webhook_admin": True,
    "github_token": "test-token",
    "github_webhook_public_url": CANONICAL_URL,
    "github_webhook_secret": DEFAULT_SECRET,
    "github_owner": "owner",
}


def _hook(
    hook_id: int,
    *,
    url: str = CANONICAL_URL,
    active: bool = True,
    events: tuple[str, ...] = wi.DEFAULT_EVENTS,
    content_type: str = "json",
    insecure_ssl: str = "0",
    last_response_code: int | None = 200,
) -> dict[str, Any]:
    return {
        "id": hook_id,
        "active": active,
        "events": list(events),
        "config": {
            "url": url,
            "content_type": content_type,
            "insecure_ssl": insecure_ssl,
            "secret": "********",
        },
        "last_response": {"code": last_response_code, "status": "active"},
    }


def _build_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=wi.GITHUB_API,
        transport=httpx.MockTransport(handler),
        timeout=5.0,
    )


def _make_client(
    monkeypatch,
    tmp_path,
    handler: Callable[[httpx.Request], httpx.Response] | None = None,
    **config_overrides,
) -> TestClient:
    """Build a TestClient whose webhook routes hit a MockTransport GitHub."""
    if handler is not None:
        monkeypatch.setattr(
            wi, "make_client", lambda token, base_url=wi.GITHUB_API: _build_client(handler)
        )
    cfg = {**BASE_CONFIG, **config_overrides}
    app = create_app(db_path=str(tmp_path / "wh-manage.db"), config=cfg)
    return TestClient(app)


# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------


def test_install_creates_when_missing(monkeypatch, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        if request.method == "POST":
            return httpx.Response(201, json={"id": 999})
        return httpx.Response(500)

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 200
    body = r.json()
    assert body["action"] == "created"
    assert body["hook_id"] == 999
    assert body["orphans"] == []


def test_install_skips_when_canonical_present(monkeypatch, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_hook(42)])

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.json()["action"] == "skipped"
    assert r.json()["hook_id"] == 42


def test_install_dry_run_reports_without_mutating(monkeypatch, tmp_path):
    posted = {"called": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(7, events=("push",))])
        posted["called"] = True
        return httpx.Response(200, json={"id": 7})

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post(
        "/v1/webhooks/manage/install", json={"repo": "owner/repo", "dry_run": True}
    )
    assert r.json()["action"] == "would-repair"
    assert posted["called"] is False


def test_install_requires_repo(monkeypatch, tmp_path):
    client = _make_client(monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]))
    r = client.post("/v1/webhooks/manage/install", json={})
    assert r.status_code == 400
    assert "repo" in r.json()["detail"]


def test_install_maps_github_error_to_502(monkeypatch, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(404, json={"message": "Not Found"})
        return httpx.Response(500)

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/gone"})
    assert r.status_code == 502
    assert "404" in r.json()["detail"]


# ---------------------------------------------------------------------------
# audit / repair
# ---------------------------------------------------------------------------


def test_audit_reports_drift(monkeypatch, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_hook(7, events=("push",))])

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/audit", json={"repo": "owner/repo"})
    body = r.json()
    assert body["kind"] == "drift"
    assert "events" in body["drift_fields"]
    assert body["last_response_code"] == 200


def test_repair_force_patches_clean_hook(monkeypatch, tmp_path):
    patched = {"called": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(42)])  # shape-clean
        if request.method == "PATCH":
            patched["called"] = True
            return httpx.Response(200, json={"id": 42})
        return httpx.Response(500)

    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/repair", json={"repo": "owner/repo"})
    assert r.json()["action"] == "repaired"
    assert r.json()["force_patched"] is True
    assert patched["called"] is True


# ---------------------------------------------------------------------------
# fleet
# ---------------------------------------------------------------------------


def _fleet_handler(repos: list[str], owner_type: str = "User"):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": owner_type})
        if path in ("/user/repos", "/orgs/owner/repos"):
            return httpx.Response(
                200,
                json=[{"name": n.split("/", 1)[1], "owner": {"login": "owner"}} for n in repos],
            )
        if path.endswith("/hooks") and request.method == "GET":
            return httpx.Response(200, json=[])
        if path.endswith("/hooks") and request.method == "POST":
            return httpx.Response(201, json={"id": 1})
        return httpx.Response(404, json={"message": "unexpected " + path})

    return handler


def test_install_fleet_summarizes(monkeypatch, tmp_path):
    handler = _fleet_handler(["owner/khonliang-bus", "owner/khonliang-developer"])
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install_fleet", json={"prefix": "khonliang-"})
    body = r.json()
    assert body["summary"].get("created") == 2
    assert len(body["results"]) == 2


def test_install_fleet_empty_is_400(monkeypatch, tmp_path):
    handler = _fleet_handler([])  # no repos match
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install_fleet", json={"prefix": "nomatch-"})
    assert r.status_code == 400
    assert "no repos" in r.json()["detail"]


def test_install_fleet_empty_prefix_returns_400(monkeypatch, tmp_path):
    # Empty prefix would match EVERY repo (startswith "") → account-wide
    # mutation. Must be refused.
    handler = _fleet_handler(["owner/khonliang-bus"])
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install_fleet", json={"prefix": ""})
    assert r.status_code == 400
    assert "non-empty" in r.json()["detail"]


def test_audit_fleet_empty_prefix_returns_400(monkeypatch, tmp_path):
    handler = _fleet_handler(["owner/khonliang-bus"])
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/audit_fleet", json={"prefix": ""})
    assert r.status_code == 400
    assert "non-empty" in r.json()["detail"]


def test_fleet_requires_owner(monkeypatch, tmp_path):
    handler = _fleet_handler(["owner/khonliang-bus"])
    client = _make_client(monkeypatch, tmp_path, handler, github_owner="")
    r = client.post("/v1/webhooks/manage/install_fleet", json={})
    assert r.status_code == 400
    assert "owner" in r.json()["detail"]


# ---------------------------------------------------------------------------
# admin gate + config preconditions
# ---------------------------------------------------------------------------


def test_admin_disabled_returns_403(monkeypatch, tmp_path):
    monkeypatch.delenv("KHONLIANG_BUS_WEBHOOK_ADMIN", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_webhook_admin=False,
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 403
    assert "admin" in r.json()["detail"]


def test_admin_enabled_via_env(monkeypatch, tmp_path):
    monkeypatch.setenv("KHONLIANG_BUS_WEBHOOK_ADMIN", "1")
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[_hook(42)]),
        github_webhook_admin=False,
    )
    r = client.post("/v1/webhooks/manage/audit", json={"repo": "owner/repo"})
    assert r.status_code == 200
    assert r.json()["kind"] == "ok"


def test_missing_token_returns_400(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_token="",
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 400
    assert "token" in r.json()["detail"]


def test_unset_public_url_returns_400(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_WEBHOOK_PUBLIC_URL", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_webhook_public_url="",
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 400
    assert "public_url" in r.json()["detail"]


def test_non_https_public_url_returns_400(monkeypatch, tmp_path):
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_webhook_public_url="http://example.test/v1/webhooks/github",
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 400
    assert "public_url" in r.json()["detail"]


def test_install_refuses_when_receiver_rejects_all(monkeypatch, tmp_path):
    # No secret + allow_unsigned false (default) → the bus receiver 503s every
    # delivery, so installing a hook configures a permanently-broken webhook.
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_webhook_secret="",
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 400
    assert "refusing" in r.json()["detail"]


def test_install_allows_unsigned_dev_mode(monkeypatch, tmp_path):
    # No secret BUT allow_unsigned true → localhost dev receiver accepts
    # unsigned deliveries, so the install is legitimate.
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        return httpx.Response(201, json={"id": 1})

    client = _make_client(
        monkeypatch, tmp_path, handler,
        github_webhook_secret="", github_webhook_allow_unsigned=True,
    )
    r = client.post("/v1/webhooks/manage/install", json={"repo": "owner/repo"})
    assert r.status_code == 200
    assert r.json()["action"] == "created"


def test_audit_works_without_secret(monkeypatch, tmp_path):
    # Audit is diagnostic — it must stay usable even when the receiver has no
    # secret configured (the require_deliverable guard is mutating-only).
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[_hook(42)]),
        github_webhook_secret="",
    )
    r = client.post("/v1/webhooks/manage/audit", json={"repo": "owner/repo"})
    assert r.status_code == 200
    assert r.json()["kind"] == "ok"


def test_dry_run_install_works_without_secret(monkeypatch, tmp_path):
    # dry_run doesn't mutate, so it isn't blocked by the deliverable guard.
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]),
        github_webhook_secret="",
    )
    r = client.post(
        "/v1/webhooks/manage/install", json={"repo": "owner/repo", "dry_run": True}
    )
    assert r.status_code == 200
    assert r.json()["action"] == "would-create"


def test_malformed_json_body_returns_400(monkeypatch, tmp_path):
    client = _make_client(monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]))
    r = client.post(
        "/v1/webhooks/manage/install",
        content="not json",
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 400
    assert "JSON" in r.json()["detail"]


def test_non_object_body_returns_400(monkeypatch, tmp_path):
    client = _make_client(monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]))
    r = client.post("/v1/webhooks/manage/install", json=["not", "an", "object"])
    assert r.status_code == 400
    assert "object" in r.json()["detail"]


def test_non_boolean_dry_run_returns_400(monkeypatch, tmp_path):
    # {"dry_run": "false"} must NOT silently become a (truthy) dry run that
    # also skips the deliverable guard — reject it as a 400.
    client = _make_client(monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]))
    r = client.post(
        "/v1/webhooks/manage/install", json={"repo": "owner/repo", "dry_run": "false"}
    )
    assert r.status_code == 400
    assert "boolean" in r.json()["detail"]


def test_non_string_owner_returns_400(monkeypatch, tmp_path):
    handler = _fleet_handler(["owner/khonliang-bus"])
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/install_fleet", json={"owner": 123})
    assert r.status_code == 400
    assert "string" in r.json()["detail"]


def test_null_prefix_returns_400(monkeypatch, tmp_path):
    handler = _fleet_handler(["owner/khonliang-bus"])
    client = _make_client(monkeypatch, tmp_path, handler)
    r = client.post("/v1/webhooks/manage/audit_fleet", json={"prefix": None})
    assert r.status_code == 400
    assert "string" in r.json()["detail"]


def test_non_string_repo_returns_400(monkeypatch, tmp_path):
    client = _make_client(monkeypatch, tmp_path, lambda req: httpx.Response(200, json=[]))
    r = client.post("/v1/webhooks/manage/install", json={"repo": 123})
    assert r.status_code == 400
    assert "string" in r.json()["detail"]


# ---------------------------------------------------------------------------
# check_funnel (ungated, token-free)
# ---------------------------------------------------------------------------


def test_check_funnel_returns_probe_result(monkeypatch, tmp_path):
    async def fake_probe(url, **kw):
        return {"reachable": True, "status_code": 400, "url": url}

    monkeypatch.setattr(wi, "check_url_reachable", fake_probe)
    # No admin / token needed for check_funnel.
    client = _make_client(
        monkeypatch, tmp_path, handler=None,
        github_webhook_admin=False, github_token="",
    )
    r = client.get("/v1/webhooks/manage/check_funnel")
    assert r.status_code == 200
    assert r.json()["reachable"] is True
    assert r.json()["url"] == CANONICAL_URL


def test_check_funnel_unset_url_returns_400(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_WEBHOOK_PUBLIC_URL", raising=False)
    client = _make_client(
        monkeypatch, tmp_path, handler=None, github_webhook_public_url=""
    )
    r = client.get("/v1/webhooks/manage/check_funnel")
    assert r.status_code == 400


def test_check_funnel_invalid_url_shape_returns_400(monkeypatch, tmp_path):
    # A bad URL shape is a config error → 400, not a 200 reachability result.
    client = _make_client(
        monkeypatch, tmp_path, handler=None,
        github_webhook_public_url="http://example.test/v1/webhooks/github",
    )
    r = client.get("/v1/webhooks/manage/check_funnel")
    assert r.status_code == 400
    assert "invalid" in r.json()["detail"]


# ---------------------------------------------------------------------------
# bus_webhook_* MCP tool formatting (transport stubbed at _async_post)
# ---------------------------------------------------------------------------


def _adapter() -> BusMCPAdapter:
    a = BusMCPAdapter(bus_url="http://localhost:8787")
    a._register_bus_tools()
    return a


async def _call(adapter: BusMCPAdapter, name: str, args: dict) -> str:
    result = await adapter.mcp.call_tool(name, args)
    # FastMCP returns (content_list, {"result": <tool return>}).
    return result[1]["result"]


def test_all_six_webhook_tools_register():
    import asyncio

    a = _adapter()
    names = asyncio.run(a.mcp.list_tools())
    wh = {t.name for t in names if t.name.startswith("bus_webhook_")}
    assert wh == {
        "bus_webhook_install",
        "bus_webhook_audit",
        "bus_webhook_repair",
        "bus_webhook_install_fleet",
        "bus_webhook_audit_fleet",
        "bus_webhook_check_funnel",
    }


@pytest.mark.asyncio
async def test_tool_install_formats_created(monkeypatch):
    a = _adapter()

    async def fake_post(path, body, **kw):
        assert path == "/v1/webhooks/manage/install"
        return {"action": "created", "repo": body["repo"], "hook_id": 5, "orphans": []}

    a._async_post = fake_post
    out = await _call(a, "bus_webhook_install", {"repo": "owner/x"})
    assert out == "install owner/x: created hook_id=5"


@pytest.mark.asyncio
async def test_tool_install_surfaces_orphans(monkeypatch):
    a = _adapter()

    async def fake_post(path, body, **kw):
        return {
            "action": "skipped",
            "repo": body["repo"],
            "hook_id": 1,
            "orphans": [{"id": 9, "url": "https://old.test/v1/webhooks/github"}],
        }

    a._async_post = fake_post
    out = await _call(a, "bus_webhook_install", {"repo": "owner/x"})
    assert "skipped hook_id=1" in out
    assert "1 orphan" in out


@pytest.mark.asyncio
async def test_tool_install_surfaces_403_detail(monkeypatch):
    a = _adapter()

    async def fake_post(path, body, **kw):
        return {"detail": "webhook admin disabled — set github_webhook_admin: true"}

    a._async_post = fake_post
    out = await _call(a, "bus_webhook_install", {"repo": "owner/x"})
    assert "admin disabled" in out


class _FakeResp:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class _FakeHTTP:
    def __init__(self, resp: _FakeResp):
        self._resp = resp

    async def get(self, url, **kw):
        return self._resp


@pytest.mark.asyncio
async def test_tool_check_funnel_formats(monkeypatch):
    a = _adapter()
    a._async_http = _FakeHTTP(
        _FakeResp(200, {"reachable": True, "status_code": 400, "url": CANONICAL_URL})
    )
    out = await _call(a, "bus_webhook_check_funnel", {})
    assert "reachable=True" in out
    assert "status=400" in out


@pytest.mark.asyncio
async def test_tool_check_funnel_surfaces_config_error(monkeypatch):
    # A 400 config error from the route must reach the user as the actual
    # detail, not a generic "bus unreachable".
    a = _adapter()
    a._async_http = _FakeHTTP(
        _FakeResp(400, {"detail": "github_webhook_public_url is unset"})
    )
    out = await _call(a, "bus_webhook_check_funnel", {})
    assert "public_url is unset" in out
    assert "unreachable" not in out
