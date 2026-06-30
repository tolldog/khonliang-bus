"""Unit tests for ``bus.webhook_install``.

Mocks GitHub via ``httpx.MockTransport`` so every code path runs
without network I/O. Closes the test-coverage gap that left the bash
predecessor at PR #28 with zero unit tests across 19 review passes.
"""

from __future__ import annotations

import json
from typing import Any, Callable

import httpx
import pytest

from bus import webhook_install as wi


# ---------------------------------------------------------------------------
# Fixtures: a builder for httpx.MockTransport and a default canonical config.
# ---------------------------------------------------------------------------


CANONICAL_URL = "https://example.test/v1/webhooks/github"
DEFAULT_SECRET = "test-secret-value"


@pytest.fixture
def canonical_config() -> wi.HookConfig:
    return wi.HookConfig(
        target_url=CANONICAL_URL,
        secret=DEFAULT_SECRET,
        events=wi.DEFAULT_EVENTS,
    )


def _hook(
    hook_id: int,
    *,
    url: str = CANONICAL_URL,
    active: bool = True,
    events: tuple[str, ...] = wi.DEFAULT_EVENTS,
    content_type: str = "json",
    insecure_ssl: str = "0",
    secret_present: bool = True,
    last_response_code: int | None = 200,
    last_response_status: str = "active",
) -> dict[str, Any]:
    return {
        "id": hook_id,
        "active": active,
        "events": list(events),
        "config": {
            "url": url,
            "content_type": content_type,
            "insecure_ssl": insecure_ssl,
            **({"secret": "********"} if secret_present else {}),
        },
        "last_response": {
            "code": last_response_code,
            "status": last_response_status,
        },
    }


def _build_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    """Build a webhook_install-shaped client backed by MockTransport."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(
        base_url=wi.GITHUB_API,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer test-token",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=5.0,
        transport=transport,
    )


# ---------------------------------------------------------------------------
# validate_target_url
# ---------------------------------------------------------------------------


def test_validate_target_url_accepts_canonical():
    wi.validate_target_url(CANONICAL_URL)


def test_validate_target_url_rejects_http():
    with pytest.raises(ValueError, match="HTTPS"):
        wi.validate_target_url("http://example.test/v1/webhooks/github")


def test_validate_target_url_rejects_empty_host():
    # ``https:///v1/webhooks/github`` — the bus#28 rogue-webhook
    # incident shape. Host-empty URLs must not pass.
    with pytest.raises(ValueError, match="no host"):
        wi.validate_target_url("https:///v1/webhooks/github")


def test_validate_target_url_rejects_wrong_path():
    with pytest.raises(ValueError, match="must end in"):
        wi.validate_target_url("https://example.test/wrong-path")


# ---------------------------------------------------------------------------
# find_canonical_match (pure logic, no httpx)
# ---------------------------------------------------------------------------


def test_find_canonical_match_missing(canonical_config):
    match = wi.find_canonical_match([], canonical_config)
    assert match.kind == "missing"


def test_find_canonical_match_ok_active(canonical_config):
    hooks = [_hook(1, active=True)]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "ok"
    assert match.hook_id == 1


def test_find_canonical_match_drift_events(canonical_config):
    hooks = [_hook(1, events=("push",))]  # missing 5 of 6
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "drift"
    assert match.hook_id == 1
    assert "events" in match.drift_fields


def test_find_canonical_match_drift_inactive(canonical_config):
    hooks = [_hook(1, active=False)]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "drift"
    assert "active" in match.drift_fields


def test_find_canonical_match_drift_secret_missing(canonical_config):
    hooks = [_hook(1, secret_present=False)]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "drift"
    assert "secret_missing" in match.drift_fields


def test_find_canonical_match_drift_content_type(canonical_config):
    hooks = [_hook(1, content_type="form")]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "drift"
    assert "content_type" in match.drift_fields


def test_find_canonical_match_duplicate_active(canonical_config):
    """Two active hooks on same URL = duplicate (real double-fire)."""
    hooks = [_hook(1, active=True), _hook(2, active=True)]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "duplicate"
    assert set(match.duplicate_ids) == {1, 2}


def test_find_canonical_match_inactive_plus_active_not_duplicate(canonical_config):
    """Active-only dedup: inactive historical + active canonical is OK."""
    hooks = [_hook(1, active=False), _hook(2, active=True)]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "ok"
    assert match.hook_id == 2  # picks the active one


def test_find_canonical_match_no_active_falls_back_to_inactive_for_drift(canonical_config):
    hooks = [_hook(1, active=False, events=("push",))]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "drift"
    assert match.hook_id == 1
    assert "active" in match.drift_fields
    assert "events" in match.drift_fields


def test_find_canonical_match_ignores_other_url(canonical_config):
    hooks = [_hook(1, url="https://other.test/v1/webhooks/github")]
    match = wi.find_canonical_match(hooks, canonical_config)
    assert match.kind == "missing"


# ---------------------------------------------------------------------------
# find_orphan_hooks
# ---------------------------------------------------------------------------


def test_find_orphan_hooks_detects_old_funnel():
    """Active hook on canonical PATH but stale HOST = orphan."""
    hooks = [
        _hook(1, url=CANONICAL_URL),
        _hook(2, url="https://old-funnel.tailnet.ts.net/v1/webhooks/github"),
    ]
    orphans = wi.find_orphan_hooks(hooks, CANONICAL_URL)
    assert len(orphans) == 1
    assert orphans[0]["id"] == 2


def test_find_orphan_hooks_ignores_inactive():
    hooks = [
        _hook(1, url="https://old-funnel.tailnet.ts.net/v1/webhooks/github", active=False),
    ]
    assert wi.find_orphan_hooks(hooks, CANONICAL_URL) == []


def test_find_orphan_hooks_ignores_other_paths():
    hooks = [
        _hook(1, url="https://example.test/api/different-path"),
    ]
    assert wi.find_orphan_hooks(hooks, CANONICAL_URL) == []


def test_find_orphan_hooks_ignores_canonical_itself():
    hooks = [_hook(1, url=CANONICAL_URL)]
    assert wi.find_orphan_hooks(hooks, CANONICAL_URL) == []


# ---------------------------------------------------------------------------
# Pagination — _paginate via list_hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_hooks_walks_link_header():
    """Two-page response. First page returns Link rel=next; second has no next."""
    page1 = [_hook(i) for i in range(1, 31)]
    page2 = [_hook(i) for i in range(31, 36)]
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(str(request.url))
        if "page=2" in str(request.url):
            return httpx.Response(200, json=page2)
        # First call — return page 1 with rel=next pointing at page=2.
        next_url = f"{wi.GITHUB_API}/repos/o/r/hooks?page=2&per_page=100"
        return httpx.Response(
            200,
            json=page1,
            headers={"Link": f'<{next_url}>; rel="next"'},
        )

    async with _build_client(handler) as client:
        hooks = await wi.list_hooks(client, "o/r")

    assert len(hooks) == 35
    assert {h["id"] for h in hooks} == set(range(1, 36))
    # Two requests fired — multi-page walk verified.
    assert len(seen_paths) == 2


@pytest.mark.asyncio
async def test_list_hooks_single_page_no_link_header():
    page = [_hook(i) for i in range(1, 11)]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=page)

    async with _build_client(handler) as client:
        hooks = await wi.list_hooks(client, "o/r")

    assert len(hooks) == 10


# ---------------------------------------------------------------------------
# install_one — every code path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_install_one_create_path(canonical_config):
    """No matching hook → POST creates."""
    posts: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return httpx.Response(201, json={"id": 999})
        raise AssertionError(f"unexpected {request.method} {request.url}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result["action"] == "created"
    assert result["hook_id"] == 999
    assert len(posts) == 1
    body = posts[0]
    assert body["name"] == "web"
    assert body["active"] is True
    assert set(body["events"]) == set(canonical_config.events)
    assert body["config"]["url"] == CANONICAL_URL
    assert body["config"]["secret"] == DEFAULT_SECRET


@pytest.mark.asyncio
async def test_install_one_skip_when_clean(canonical_config):
    """Single active hook with right config → SKIP, no mutation."""
    mutations: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            mutations.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(42)])
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result == {"action": "skipped", "repo": "o/r", "hook_id": 42, "orphans": []}
    assert mutations == []


@pytest.mark.asyncio
async def test_install_one_repair_path(canonical_config):
    """Drifted hook → PATCH."""
    patches: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(7, events=("push",))])
        if request.method == "PATCH":
            patches.append((str(request.url), json.loads(request.content)))
            return httpx.Response(200, json={"id": 7})
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result["action"] == "repaired"
    assert result["hook_id"] == 7
    assert "events" in result["drift_fields"]
    assert len(patches) == 1
    url, body = patches[0]
    assert "/repos/o/r/hooks/7" in url
    assert "name" not in body  # PATCH must omit immutable name
    assert set(body["events"]) == set(canonical_config.events)


@pytest.mark.asyncio
async def test_install_one_duplicate_refuses(canonical_config):
    """Two active hooks → no mutation, returns duplicate_ids."""
    mutations: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            mutations.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json=[_hook(1, active=True), _hook(2, active=True)],
            )
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result["action"] == "duplicate"
    assert set(result["duplicate_ids"]) == {1, 2}
    assert mutations == []


@pytest.mark.asyncio
async def test_install_one_dry_run_never_mutates(canonical_config):
    mutations: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            mutations.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=[])
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(
            client, "o/r", canonical_config, dry_run=True,
        )

    assert result["action"] == "would-create"
    assert mutations == []


@pytest.mark.asyncio
async def test_install_one_dry_run_drift_reports_would_repair(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(7, events=("push",))])
        raise AssertionError("dry-run must not mutate")

    async with _build_client(handler) as client:
        result = await wi.install_one(
            client, "o/r", canonical_config, dry_run=True,
        )

    assert result["action"] == "would-repair"
    assert result["hook_id"] == 7
    assert "events" in result["drift_fields"]


@pytest.mark.asyncio
async def test_install_one_inactive_plus_active_proceeds_against_active(canonical_config):
    """Real fleet shape: an inactive historical hook + an active
    canonical-shape hook. Active-only dedup means the active match
    proceeds through the normal SKIP/PATCH/CREATE path; the inactive
    one is ignored."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[
                _hook(1, active=False, events=("push",)),
                _hook(2, active=True),  # canonical, clean
            ])
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result == {"action": "skipped", "repo": "o/r", "hook_id": 2, "orphans": []}


@pytest.mark.asyncio
async def test_install_one_surfaces_orphans(canonical_config):
    """A clean canonical hook PLUS an active stale-host orphan: install must
    SKIP but still report the orphan (it's double-delivering)."""
    orphan = _hook(99, url="https://stale.test/v1/webhooks/github")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(1), orphan])
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result["action"] == "skipped"
    assert result["orphans"] == [{"id": 99, "url": "https://stale.test/v1/webhooks/github"}]


@pytest.mark.asyncio
async def test_list_org_repos_user_uses_authenticated_endpoint(canonical_config):
    """A User owner must enumerate via /user/repos (private repos included),
    NOT /users/{owner}/repos (public-only)."""
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": "User"})
        if path == "/user/repos":
            return httpx.Response(200, json=[
                {"name": "khonliang-bus", "owner": {"login": "owner"}, "private": True},
                {"name": "khonliang-x", "owner": {"login": "someone-else"}},  # filtered
                {"name": "unrelated", "owner": {"login": "owner"}},           # prefix-filtered
            ])
        raise AssertionError(f"unexpected {path}")

    async with _build_client(handler) as client:
        repos = await wi.list_org_repos(client, "owner")
    assert repos == ["owner/khonliang-bus"]


@pytest.mark.asyncio
async def test_list_org_repos_org_uses_orgs_endpoint(canonical_config):
    """An Organization owner must enumerate via /orgs/{owner}/repos."""
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/acme":
            return httpx.Response(200, json={"login": "acme", "type": "Organization"})
        if path == "/orgs/acme/repos":
            return httpx.Response(200, json=[
                {"name": "khonliang-bus"}, {"name": "other"},
            ])
        raise AssertionError(f"unexpected {path}")

    async with _build_client(handler) as client:
        repos = await wi.list_org_repos(client, "acme")
    assert repos == ["acme/khonliang-bus"]


# ---------------------------------------------------------------------------
# audit_one
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_one_ok_with_last_response(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[_hook(7, last_response_code=200, last_response_status="active")],
        )

    async with _build_client(handler) as client:
        audit = await wi.audit_one(client, "o/r", canonical_config)

    assert audit.repo == "o/r"
    assert audit.match.kind == "ok"
    assert audit.match.hook_id == 7
    assert audit.last_response_code == 200
    assert audit.last_response_status == "active"
    assert audit.orphans == ()


@pytest.mark.asyncio
async def test_audit_one_drift_includes_drift_fields(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_hook(7, events=("push",))])

    async with _build_client(handler) as client:
        audit = await wi.audit_one(client, "o/r", canonical_config)

    assert audit.match.kind == "drift"
    assert "events" in audit.match.drift_fields


@pytest.mark.asyncio
async def test_audit_one_surfaces_orphans(canonical_config):
    """Active hook on stale Funnel host → orphan, alongside canonical."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[
            _hook(1, url=CANONICAL_URL),
            _hook(99, url="https://old-funnel.tailnet.ts.net/v1/webhooks/github"),
        ])

    async with _build_client(handler) as client:
        audit = await wi.audit_one(client, "o/r", canonical_config)

    assert audit.match.kind == "ok"
    assert len(audit.orphans) == 1
    assert audit.orphans[0]["id"] == 99


@pytest.mark.asyncio
async def test_audit_one_missing_no_last_response(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    async with _build_client(handler) as client:
        audit = await wi.audit_one(client, "o/r", canonical_config)

    assert audit.match.kind == "missing"
    assert audit.last_response_code is None


# ---------------------------------------------------------------------------
# install_fleet / audit_fleet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_install_fleet_skips_existing_creates_new(canonical_config):
    repos_payload = [
        {"name": "khonliang-bus", "owner": {"login": "owner"}},
        {"name": "khonliang-developer", "owner": {"login": "owner"}},
        {"name": "unrelated-project", "owner": {"login": "owner"}},  # filtered by prefix
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": "User"})
        if path == "/user/repos":
            return httpx.Response(200, json=repos_payload)
        if path == "/repos/owner/khonliang-bus/hooks" and request.method == "GET":
            return httpx.Response(200, json=[_hook(1)])  # OK, skip
        if path == "/repos/owner/khonliang-developer/hooks" and request.method == "GET":
            return httpx.Response(200, json=[])  # missing, create
        if path == "/repos/owner/khonliang-developer/hooks" and request.method == "POST":
            return httpx.Response(201, json={"id": 2})
        raise AssertionError(f"unexpected {request.method} {path}")

    async with _build_client(handler) as client:
        out = await wi.install_fleet(client, "owner", canonical_config)

    actions = [r["action"] for r in out["results"]]
    assert "skipped" in actions
    assert "created" in actions
    assert out["summary"].get("skipped") == 1
    assert out["summary"].get("created") == 1
    # Unrelated-project filtered out by prefix.
    assert all("unrelated" not in r["repo"] for r in out["results"])


@pytest.mark.asyncio
async def test_install_fleet_empty_raises(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": "User"})
        if request.url.path == "/user/repos":
            return httpx.Response(200, json=[])
        raise AssertionError("should not query repos when fleet is empty")

    async with _build_client(handler) as client:
        with pytest.raises(ValueError, match="no repos"):
            await wi.install_fleet(client, "owner", canonical_config)


@pytest.mark.asyncio
async def test_install_fleet_per_repo_failure_does_not_abort(canonical_config):
    """One repo's HTTP error must not stop the rest of the fleet."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": "User"})
        if path == "/user/repos":
            return httpx.Response(200, json=[
                {"name": "khonliang-a", "owner": {"login": "owner"}}, {"name": "khonliang-b", "owner": {"login": "owner"}},
            ])
        if path == "/repos/owner/khonliang-a/hooks" and request.method == "GET":
            return httpx.Response(403, json={"message": "forbidden"})
        if path == "/repos/owner/khonliang-b/hooks" and request.method == "GET":
            return httpx.Response(200, json=[_hook(1)])
        raise AssertionError(f"unexpected {request.method} {path}")

    async with _build_client(handler) as client:
        out = await wi.install_fleet(client, "owner", canonical_config)

    actions = {r["action"]: r for r in out["results"]}
    assert "error" in actions
    assert actions["error"]["repo"] == "owner/khonliang-a"
    assert actions["skipped"]["repo"] == "owner/khonliang-b"


@pytest.mark.asyncio
async def test_audit_fleet_summarises_kinds(canonical_config):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/users/owner":
            return httpx.Response(200, json={"login": "owner", "type": "User"})
        if path == "/user/repos":
            return httpx.Response(200, json=[
                {"name": "khonliang-a", "owner": {"login": "owner"}},
                {"name": "khonliang-b", "owner": {"login": "owner"}},
                {"name": "khonliang-c", "owner": {"login": "owner"}},
            ])
        if path == "/repos/owner/khonliang-a/hooks":
            return httpx.Response(200, json=[_hook(1)])  # ok
        if path == "/repos/owner/khonliang-b/hooks":
            return httpx.Response(200, json=[])  # missing
        if path == "/repos/owner/khonliang-c/hooks":
            return httpx.Response(200, json=[_hook(2, events=("push",))])  # drift
        raise AssertionError(f"unexpected {path}")

    async with _build_client(handler) as client:
        out = await wi.audit_fleet(client, "owner", canonical_config)

    assert out["summary"] == {"ok": 1, "missing": 1, "drift": 1}


# ---------------------------------------------------------------------------
# repair_one — force-PATCH for secret rotation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repair_one_force_patches_clean_hook(canonical_config):
    """An OK-shape hook still gets PATCHed when repair is invoked,
    so a rotated secret actually lands. Plain install_one would SKIP."""
    patches: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(42)])
        if request.method == "PATCH":
            patches.append(str(request.url))
            return httpx.Response(200, json={"id": 42})
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.repair_one(client, "o/r", canonical_config)

    assert result["action"] == "repaired"
    assert result["force_patched"] is True
    assert len(patches) == 1
    assert "/repos/o/r/hooks/42" in patches[0]


@pytest.mark.asyncio
async def test_repair_one_missing_falls_through_to_install(canonical_config):
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        if request.method == "POST":
            posts.append(str(request.url))
            return httpx.Response(201, json={"id": 99})
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.repair_one(client, "o/r", canonical_config)

    assert result["action"] == "created"
    assert len(posts) == 1


# ---------------------------------------------------------------------------
# check_url_reachable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_url_reachable_401_is_reachable(monkeypatch):
    """Unsigned POST against a properly-secured bus returns 401 — the
    canonical "yes the receiver is alive and demands signatures"
    response. ``reachable`` must be True."""

    async def fake_post(self, url, **kwargs):
        return httpx.Response(401, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL)
    assert out["reachable"] is True
    assert out["status_code"] == 401


@pytest.mark.asyncio
async def test_check_url_reachable_503_is_reachable(monkeypatch):
    """Bus running but no secret loaded — fresh-deploy intermediate.
    Still reachable for the purposes of "is the URL right"."""

    async def fake_post(self, url, **kwargs):
        return httpx.Response(503, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL)
    assert out["reachable"] is True
    assert out["status_code"] == 503


@pytest.mark.asyncio
async def test_check_url_reachable_404_is_not_reachable(monkeypatch):
    """Wrong path or wrong tunnel target → 404. Not reachable."""

    async def fake_post(self, url, **kwargs):
        return httpx.Response(404, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL)
    assert out["reachable"] is False
    assert out["status_code"] == 404


@pytest.mark.asyncio
async def test_check_url_reachable_connect_error_is_not_reachable(monkeypatch):
    """Funnel disabled / DNS failure / nothing listening → ConnectError."""

    async def fake_post(self, url, **kwargs):
        raise httpx.ConnectError("connection refused", request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL)
    assert out["reachable"] is False
    assert "connect_error" in out["error"]


@pytest.mark.asyncio
async def test_check_url_reachable_timeout(monkeypatch):
    async def fake_post(self, url, **kwargs):
        raise httpx.TimeoutException("timed out", request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL, timeout=0.1)
    assert out["reachable"] is False
    assert out["error"] == "timeout"


@pytest.mark.asyncio
async def test_check_url_reachable_400_reachable_and_body_is_not_json(monkeypatch):
    """The probe must send a NON-JSON body (so the receiver 400s pre-publish in
    unsigned mode, never publishing a real github.ping), and treat 400 as
    reachable."""
    captured: dict[str, Any] = {}

    async def fake_post(self, url, **kwargs):
        captured.update(kwargs)
        return httpx.Response(400, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    out = await wi.check_url_reachable(CANONICAL_URL)
    assert out["reachable"] is True and out["status_code"] == 400
    # Side-effect-free: no JSON payload, and the raw content isn't valid JSON.
    assert "json" not in captured
    with pytest.raises(json.JSONDecodeError):
        json.loads(captured["content"])


# ---------------------------------------------------------------------------
# HookConfig target-URL validation + same-endpoint URL drift
# ---------------------------------------------------------------------------


def test_hookconfig_validates_target_url_at_construction():
    # Every mutating path takes a HookConfig, so an invalid URL must fail at
    # construction, before any GitHub hook is written.
    with pytest.raises(ValueError, match="/v1/webhooks/github"):
        wi.HookConfig(target_url="https://host.test/wrong-path", secret="s")
    with pytest.raises(ValueError, match="HTTPS"):
        wi.HookConfig(target_url="http://host.test/v1/webhooks/github", secret="s")


def test_find_canonical_match_url_drift_same_endpoint(canonical_config):
    # Same host+path but a drifted query string → DRIFT (PATCH to exact URL),
    # not MISSING (which would create a duplicate active hook).
    drifted = _hook(5, url=CANONICAL_URL + "?stale=1")
    match = wi.find_canonical_match([drifted], canonical_config)
    assert match.kind == "drift"
    assert "url" in match.drift_fields


@pytest.mark.asyncio
async def test_install_one_url_drift_patches_not_duplicates(canonical_config):
    """A same-endpoint hook with a drifted URL must be PATCHed, never create a
    second active hook (double-delivery)."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=[_hook(8, url=CANONICAL_URL + "?v=old")])
        if request.method == "PATCH":
            return httpx.Response(200, json={"id": 8})
        raise AssertionError(f"unexpected {request.method}")

    async with _build_client(handler) as client:
        result = await wi.install_one(client, "o/r", canonical_config)

    assert result["action"] == "repaired"
    assert "url" in result["drift_fields"]
    assert "POST" not in calls  # never created a duplicate


# ---------------------------------------------------------------------------
# make_client guard
# ---------------------------------------------------------------------------


def test_make_client_rejects_empty_token():
    with pytest.raises(ValueError, match="token is required"):
        wi.make_client("")


# ---------------------------------------------------------------------------
# HookConfig body shapes
# ---------------------------------------------------------------------------


def test_hookconfig_create_body_has_name():
    cfg = wi.HookConfig(target_url=CANONICAL_URL, secret="s")
    body = cfg.to_create_body()
    assert body["name"] == "web"
    assert body["config"]["secret"] == "s"


def test_hookconfig_patch_body_omits_name():
    cfg = wi.HookConfig(target_url=CANONICAL_URL, secret="s")
    body = cfg.to_patch_body()
    assert "name" not in body
    assert body["active"] is True
