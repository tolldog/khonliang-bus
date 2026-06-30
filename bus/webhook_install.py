"""GitHub webhook outbound management — install / audit / repair.

Symmetric to :mod:`bus.webhooks` (which receives inbound deliveries):
this module owns OUTBOUND GitHub API calls that configure repos to
deliver to the bus's ``/v1/webhooks/github`` endpoint.

Closes ``fr_khonliang-bus_e3b15e88`` (replaces the bash installer
``scripts/install-github-webhook.sh`` that was abandoned at PR #28
after 19 passes of bash-specific defects).

Design notes:

- Pure-async functions taking an injected ``httpx.AsyncClient`` plus
  caller-supplied target URL / secret / event list. No state held in
  this module; the bus's REST endpoints (in ``bus/server.py``) and
  the operator CLI compose around these primitives.
- Hook discovery uses ``GET /repos/{repo}/hooks`` with pagination
  (RFC-5988 ``Link`` headers). Multi-page repos (>30 hooks) just work
  — no per-line JSON parsing, no shell-pipeline SIGPIPE class.
- Active-only duplicate semantics: a repo with one inactive
  historical hook + one active canonical hook is NOT flagged
  (only the active hook delivers, no double-fire). Two-or-more
  ACTIVE hooks on the same URL is the duplicate-error case.
- Drift covers the readable fields the script writes: ``events`` set,
  ``active=True``, ``content_type=json``, ``insecure_ssl="0"``, and the
  URL string. The secret is NOT a drift signal — GitHub's GET response
  doesn't return a usable ``config.secret`` (write-only/redacted), so it
  can't be read back; ``repair_one`` force-PATCHes for secret rotation.
- The orphan-detection (active hook on a NON-canonical host but
  the canonical ``/v1/webhooks/github`` path — left over from a
  Funnel hostname migration) is part of audit; install treats them
  as out-of-scope and surfaces them in the response so an operator
  can clean them up.
- Authentication: caller-supplied bearer token (``GITHUB_ADMIN_TOKEN``
  via env in the typical case). Token requires ``admin:repo_hook``
  scope. The module does NOT touch ``gh auth`` since the bus is a
  long-running service, not an interactive CLI session.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urlparse

import httpx

GITHUB_API = "https://api.github.com"
WEBHOOK_PATH = "/v1/webhooks/github"

# Canonical event set every khonliang-* repo's webhook fires.
# Lock here, not at the call site, so all install / audit paths
# agree on what "drifted events" means.
DEFAULT_EVENTS: tuple[str, ...] = (
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "issue_comment",
    "push",
    "check_run",
)


@dataclass(frozen=True)
class HookConfig:
    """The webhook config the installer writes.

    ``target_url`` is validated at construction (HTTPS, non-empty host, exact
    ``/v1/webhooks/github`` path) — HookConfig is the sole input to the public
    install/audit/repair APIs, so validating here means no mutating path can
    write a hook pointing at an endpoint the bus never serves, without every
    caller having to remember to call :func:`validate_target_url`.
    """

    target_url: str
    secret: str
    events: tuple[str, ...] = DEFAULT_EVENTS

    def __post_init__(self) -> None:
        validate_target_url(self.target_url)

    def to_create_body(self) -> dict[str, Any]:
        return {
            "name": "web",
            "active": True,
            "events": list(self.events),
            "config": {
                "url": self.target_url,
                "content_type": "json",
                "insecure_ssl": "0",
                "secret": self.secret,
            },
        }

    def to_patch_body(self) -> dict[str, Any]:
        # PATCH form omits "name" (immutable per GitHub API).
        body = self.to_create_body()
        del body["name"]
        return body


@dataclass
class HookMatch:
    """Result of comparing existing hooks against a target config.

    Outcomes:
      - ``kind == "missing"``: no hook at this URL → CREATE path
      - ``kind == "ok"``: single active hook, config matches → SKIP
      - ``kind == "drift"``: single active hook, fields drifted → PATCH
      - ``kind == "duplicate"``: two-or-more ACTIVE hooks on the URL → ERROR
    """

    kind: str  # "missing" | "ok" | "drift" | "duplicate"
    hook_id: int | None = None
    drift_fields: tuple[str, ...] = ()
    duplicate_ids: tuple[int, ...] = ()


@dataclass
class AuditResult:
    """Audit shape per repo."""

    repo: str
    match: HookMatch
    last_response_code: int | None = None
    last_response_status: str = ""
    orphans: tuple[dict[str, Any], ...] = ()  # active hooks on non-canonical hosts


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------


def validate_target_url(url: str) -> None:
    """Reject URLs that would silently mis-deliver.

    HTTPS scheme, non-empty host, exact ``/v1/webhooks/github`` path, and a
    parseable port. Empty host (``https:///v1/webhooks/github``) and
    non-canonical path suffixes both trip — operators previously hit
    fleet-wide misroutes when these shapes slipped through. A non-numeric
    port (``https://host:abc/...``) is rejected here too: ``urlparse`` accepts
    it lazily but ``parsed.port`` raises ``ValueError`` on access, which would
    otherwise surface downstream (e.g. URL redaction) as a crash.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"target URL must be HTTPS: {url!r}")
    if not parsed.hostname:
        raise ValueError(f"target URL has no host: {url!r}")
    try:
        parsed.port  # noqa: B018 — access triggers lazy port validation
    except ValueError:
        raise ValueError(f"target URL has an invalid port: {url!r}")
    if parsed.path != WEBHOOK_PATH:
        raise ValueError(
            f"target URL must end in {WEBHOOK_PATH!r}, got {parsed.path!r}: {url!r}"
        )


def find_canonical_match(
    hooks: list[dict[str, Any]],
    config: HookConfig,
) -> HookMatch:
    """Classify the existing-hooks list against the target config.

    Active-only dedup: an inactive historical hook plus an active
    canonical hook is NOT flagged duplicate (only the active hook
    delivers). Two-or-more ACTIVE hooks on the same URL is the real
    double-fire case and is rejected for manual collapse.
    """
    # Match by (host, path) identity — the SAME basis ``find_orphan_hooks``
    # uses (it flags same-path / different-host). Matching on the full URL
    # string instead would classify a same-endpoint hook that differs only by
    # query string or port as ``missing`` → install_one creates a SECOND active
    # hook while the original keeps firing (double-delivery). Same host+path is
    # the canonical hook (PATCH any URL drift to the exact target); different
    # host is an orphan; different path is unrelated.
    target = urlparse(config.target_url)

    def _same_endpoint(url: str) -> bool:
        p = urlparse(url or "")
        return p.hostname == target.hostname and p.path == target.path

    matching = [
        h for h in hooks
        if _same_endpoint((h.get("config") or {}).get("url", ""))
    ]
    if not matching:
        return HookMatch(kind="missing")

    active = [h for h in matching if h.get("active", False)]
    if len(active) > 1:
        return HookMatch(
            kind="duplicate",
            duplicate_ids=tuple(int(h["id"]) for h in active),
        )

    # Pick the active match if any; otherwise fall back to the first
    # inactive so drift detection can still report ``active=False`` as
    # a fixable field.
    h = active[0] if active else matching[0]
    cfg = h.get("config") or {}
    drift: list[str] = []
    if not h.get("active", False):
        drift.append("active")
    # Same endpoint but the stored URL isn't byte-identical (query/port drift) —
    # PATCH rewrites it to the exact canonical URL rather than duplicating.
    if (cfg.get("url") or "") != config.target_url:
        drift.append("url")
    if set(h.get("events") or []) != set(config.events):
        drift.append("events")
    if cfg.get("content_type") != "json":
        drift.append("content_type")
    if cfg.get("insecure_ssl") != "0":
        drift.append("insecure_ssl")
    # NOTE: secret presence is intentionally NOT a drift signal. GitHub's
    # GET /hooks response does not return a usable ``config.secret`` (it's
    # write-only / redacted), so treating a missing/redacted value as drift
    # would flag EVERY real hook forever and PATCH it on every run. Secret
    # rotation can't be detected from a read — that's exactly what
    # ``repair_one`` (force-PATCH) exists for.

    if drift:
        return HookMatch(
            kind="drift",
            hook_id=int(h["id"]),
            drift_fields=tuple(drift),
        )
    return HookMatch(kind="ok", hook_id=int(h["id"]))


def find_orphan_hooks(
    hooks: list[dict[str, Any]],
    canonical_url: str,
) -> list[dict[str, Any]]:
    """Active hooks on the canonical PATH but a different HOST.

    Surfaces hooks left over from a Funnel hostname migration: the path
    matches what the bus is listening on, but the host is stale, so
    every event fires twice (once to the canonical host, once into the
    void). Returned as raw GitHub hook dicts so the caller can delete
    or PATCH them; this module never deletes on its own.
    """
    canonical = urlparse(canonical_url)
    orphans: list[dict[str, Any]] = []
    for h in hooks:
        if not h.get("active", False):
            continue
        url = (h.get("config") or {}).get("url", "")
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.path != canonical.path:
            continue  # different path → not our concern
        if parsed.hostname == canonical.hostname:
            continue  # the canonical hook itself
        orphans.append(h)
    return orphans


# ---------------------------------------------------------------------------
# I/O — async GitHub REST calls via httpx
# ---------------------------------------------------------------------------


def make_client(token: str, base_url: str = GITHUB_API) -> httpx.AsyncClient:
    """Build an authed httpx client for the GitHub REST API.

    Caller is responsible for ``await client.aclose()`` (or use as a
    context manager). Kept as a top-level helper so tests can swap the
    transport without monkey-patching this module.
    """
    if not token:
        raise ValueError("GitHub admin token is required (admin:repo_hook scope)")
    return httpx.AsyncClient(
        base_url=base_url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30.0,
    )


async def _paginate(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Walk RFC-5988 ``Link: rel=next`` pagination.

    GitHub's default page size is 30; an explicit ``per_page=100`` keeps
    the round-trip count down on large fleets without changing the
    eventual result set.
    """
    out: list[dict[str, Any]] = []
    next_url: str | None = path
    next_params: dict[str, Any] | None = dict(params or {})
    next_params.setdefault("per_page", 100)
    while next_url:
        resp = await client.get(next_url, params=next_params)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, list):
            raise ValueError(f"expected list from {path}, got {type(body).__name__}")
        out.extend(body)
        # After the first page, the Link header carries an ABSOLUTE URL with the
        # query (?page=N&per_page=100) already baked in. Pass ``params=None`` —
        # NOT ``{}`` — to follow it: httpx (0.28) drops the URL's existing query
        # string when an empty params dict is supplied, which strips ``page=N``
        # and re-requests page 1 forever (infinite pagination loop).
        link = resp.headers.get("link", "")
        next_url = _parse_link_next(link)
        next_params = None
    return out


def _parse_link_next(link_header: str) -> str | None:
    """Extract the ``rel=next`` URL from an RFC-5988 Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' not in part:
            continue
        # Format: ``<https://api.github.com/...>; rel="next"``
        lt = part.find("<")
        gt = part.find(">", lt + 1)
        if lt == -1 or gt == -1:
            continue
        return part[lt + 1:gt]
    return None


async def list_hooks(
    client: httpx.AsyncClient,
    repo: str,
) -> list[dict[str, Any]]:
    """Return every webhook on ``repo`` (paginated)."""
    return await _paginate(client, f"/repos/{repo}/hooks")


async def list_org_repos(
    client: httpx.AsyncClient,
    owner: str,
    prefix: str = "khonliang-",
) -> list[str]:
    """Return ``owner/<name>`` for every repo under ``owner`` whose
    name starts with ``prefix`` — INCLUDING private repos.

    ``GET /users/{owner}/repos`` returns only PUBLIC repos, so for a
    private fleet it silently lists nothing (or raises empty-fleet). Detect
    the owner type first and pick an endpoint that surfaces private repos:

    - Organization → ``GET /orgs/{owner}/repos`` (private repos visible with
      ``admin:repo_hook`` + ``read:org``).
    - User → ``GET /user/repos`` (the authenticated user's own repos, private
      included), filtered to ``owner`` so a token with broader access doesn't
      pull in other accounts.

    Filtering by prefix happens client-side (the search API needs different
    scopes and returns inconsistent shapes).
    """
    acct = await client.get(f"/users/{owner}")
    acct.raise_for_status()
    acct_type = (acct.json() or {}).get("type", "User")
    if acct_type == "Organization":
        repos = await _paginate(client, f"/orgs/{owner}/repos", {"type": "all"})
    else:
        # Default affiliation (owner + collaborator + organization_member) so an
        # admin/service token that only has COLLABORATOR access to ``owner``'s
        # private repos still enumerates them — then scope by owner login.
        # ``affiliation=owner`` would drop everything when owner != auth user.
        repos = await _paginate(client, "/user/repos")
        # GitHub logins are case-insensitive, so normalize both sides — a caller
        # passing "TToll" must still match repos GitHub returns under "ttoll".
        owner_lc = owner.lower()
        repos = [
            r for r in repos
            if ((r.get("owner") or {}).get("login") or "").lower() == owner_lc
        ]
    return sorted(
        f"{owner}/{r['name']}"
        for r in repos
        if r.get("name", "").startswith(prefix)
    )


async def install_one(
    client: httpx.AsyncClient,
    repo: str,
    config: HookConfig,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Install / repair the canonical hook on ``repo``.

    Returns a dict with at minimum ``{action, repo}`` plus mode-specific
    fields (``hook_id``, ``drift_fields``, ``duplicate_ids``). ``action``
    is one of ``created`` / ``skipped`` / ``repaired`` / ``duplicate``.

    Every result also carries ``orphans`` (active hooks on the canonical PATH
    but a stale HOST). The canonical hook can be perfectly in shape while a
    leftover orphan still double-delivers, so install must surface them for
    cleanup — not just audit (a ``skipped``/``repaired`` repo can still be
    mis-firing).
    """
    hooks = await list_hooks(client, repo)
    match = find_canonical_match(hooks, config)
    orphans = _orphan_summary(find_orphan_hooks(hooks, config.target_url))

    if match.kind == "duplicate":
        return {
            "action": "duplicate",
            "repo": repo,
            "duplicate_ids": list(match.duplicate_ids),
            "orphans": orphans,
        }
    if match.kind == "ok":
        return {"action": "skipped", "repo": repo, "hook_id": match.hook_id,
                "orphans": orphans}
    if dry_run:
        return {
            "action": f"would-{'create' if match.kind == 'missing' else 'repair'}",
            "repo": repo,
            "hook_id": match.hook_id,
            "drift_fields": list(match.drift_fields),
            "orphans": orphans,
        }
    if match.kind == "missing":
        resp = await client.post(
            f"/repos/{repo}/hooks", json=config.to_create_body(),
        )
        resp.raise_for_status()
        new = resp.json()
        return {"action": "created", "repo": repo, "hook_id": int(new["id"]),
                "orphans": orphans}
    # match.kind == "drift"
    resp = await client.patch(
        f"/repos/{repo}/hooks/{match.hook_id}", json=config.to_patch_body(),
    )
    resp.raise_for_status()
    return {
        "action": "repaired",
        "repo": repo,
        "hook_id": match.hook_id,
        "drift_fields": list(match.drift_fields),
        "orphans": orphans,
    }


async def audit_one(
    client: httpx.AsyncClient,
    repo: str,
    config: HookConfig,
) -> AuditResult:
    """Read-only audit of ``repo`` against the canonical config.

    No mutations. Returns the match classification, the most-recent
    delivery's status code (informational; not a pass/fail signal),
    and any orphan hooks for operator cleanup.
    """
    hooks = await list_hooks(client, repo)
    match = find_canonical_match(hooks, config)
    orphans = find_orphan_hooks(hooks, config.target_url)

    last_code: int | None = None
    last_status: str = ""
    if match.kind in {"ok", "drift"}:
        target_id = match.hook_id
        for h in hooks:
            if int(h.get("id", -1)) == target_id:
                lr = h.get("last_response") or {}
                code = lr.get("code")
                if isinstance(code, int):
                    last_code = code
                last_status = str(lr.get("status") or "")
                break

    return AuditResult(
        repo=repo,
        match=match,
        last_response_code=last_code,
        last_response_status=last_status,
        orphans=tuple(orphans),
    )


async def install_fleet(
    client: httpx.AsyncClient,
    owner: str,
    config: HookConfig,
    *,
    prefix: str = "khonliang-",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run :func:`install_one` against every ``owner/<prefix>*`` repo.

    Empty fleet is treated as an actionable failure (auth scoped to
    the wrong account, prefix migration, etc.) — same shape the bash
    installer eventually grew into.
    """
    repos = await list_org_repos(client, owner, prefix=prefix)
    if not repos:
        raise ValueError(
            f"no repos under {owner!r} matching prefix {prefix!r} — "
            f"check token scope / org name / prefix"
        )
    results: list[dict[str, Any]] = []
    for repo in repos:
        try:
            results.append(await install_one(client, repo, config, dry_run=dry_run))
        except httpx.HTTPStatusError as e:
            results.append({
                "action": "error",
                "repo": repo,
                "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}",
            })
        except Exception as e:
            results.append({
                "action": "error",
                "repo": repo,
                "error": f"{type(e).__name__}: {e}",
            })
    by_action: dict[str, int] = {}
    for r in results:
        by_action[r["action"]] = by_action.get(r["action"], 0) + 1
    return {"results": results, "summary": by_action}


async def audit_fleet(
    client: httpx.AsyncClient,
    owner: str,
    config: HookConfig,
    *,
    prefix: str = "khonliang-",
) -> dict[str, Any]:
    """Read-only :func:`audit_one` across the fleet."""
    repos = await list_org_repos(client, owner, prefix=prefix)
    if not repos:
        raise ValueError(
            f"no repos under {owner!r} matching prefix {prefix!r}"
        )
    audits: list[AuditResult] = []
    errors: list[dict[str, Any]] = []
    for repo in repos:
        try:
            audits.append(await audit_one(client, repo, config))
        except Exception as e:
            errors.append({"repo": repo, "error": f"{type(e).__name__}: {e}"})
    by_kind: dict[str, int] = {}
    for a in audits:
        by_kind[a.match.kind] = by_kind.get(a.match.kind, 0) + 1
    return {
        "audits": [_audit_to_dict(a) for a in audits],
        "errors": errors,
        "summary": by_kind,
    }


def _orphan_summary(orphans: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compact {id, url} view of orphan hooks for operator-facing results."""
    return [
        {"id": o.get("id"), "url": (o.get("config") or {}).get("url")}
        for o in orphans
    ]


def _audit_to_dict(a: AuditResult) -> dict[str, Any]:
    return {
        "repo": a.repo,
        "kind": a.match.kind,
        "hook_id": a.match.hook_id,
        "drift_fields": list(a.match.drift_fields),
        "duplicate_ids": list(a.match.duplicate_ids),
        "last_response_code": a.last_response_code,
        "last_response_status": a.last_response_status,
        "orphans": _orphan_summary(a.orphans),
    }


async def repair_one(
    client: httpx.AsyncClient,
    repo: str,
    config: HookConfig,
) -> dict[str, Any]:
    """Force-PATCH the canonical hook on ``repo``.

    Equivalent to ``install_one`` BUT always patches when an active
    hook exists at the target URL — even if the config-shape match is
    clean — so secret rotation lands. The shape-clean case has no way
    to detect a stale secret (GitHub redacts the value) so without an
    explicit force, a re-run after rotation is a no-op.
    """
    hooks = await list_hooks(client, repo)
    match = find_canonical_match(hooks, config)
    orphans = _orphan_summary(find_orphan_hooks(hooks, config.target_url))
    if match.kind == "duplicate":
        return {
            "action": "duplicate",
            "repo": repo,
            "duplicate_ids": list(match.duplicate_ids),
            "orphans": orphans,
        }
    if match.kind == "missing":
        # Repair on a missing hook degrades to install. Could also
        # refuse — current behaviour matches operator intent ("make
        # this repo correct").
        return await install_one(client, repo, config)
    resp = await client.patch(
        f"/repos/{repo}/hooks/{match.hook_id}", json=config.to_patch_body(),
    )
    resp.raise_for_status()
    return {
        "action": "repaired",
        "repo": repo,
        "hook_id": match.hook_id,
        "force_patched": True,
        "orphans": orphans,
    }


def _looks_like_bus_rejection(resp: httpx.Response) -> bool:
    """True iff ``resp`` is a bus webhook-receiver rejection.

    Requires one of the pre-publish rejection statuses AND the bus's JSON
    ``{"error": <str>}`` body, so an unrelated endpoint that merely returns
    400/401/503 for an arbitrary POST doesn't read as reachable.
    """
    if resp.status_code not in {400, 401, 503}:
        return False
    try:
        body = resp.json()
    except Exception:
        return False
    return isinstance(body, dict) and isinstance(body.get("error"), str)


async def check_url_reachable(
    target_url: str,
    *,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Probe the resolved target URL — strictly read-only.

    Sends an INTENTIONALLY INVALID (non-JSON) body so the bus's webhook
    receiver rejects it BEFORE publishing any event. A valid ping payload
    would, in ``github_webhook_allow_unsigned`` dev mode, be accepted and
    published as a real ``github.ping`` bus message (waking subscribers /
    polluting event history) — so the probe must not look like a real event.

    A bus webhook endpoint rejects the bad body pre-publish in every mode,
    each with a JSON ``{"error": ...}`` body:
      - ``400`` — unsigned/dev mode: signature accepted, JSON parse fails.
      - ``401`` — signed mode: signature check fails first.
      - ``503`` — no secret + unsigned not allowed: rejected outright.
    ``reachable`` requires BOTH a rejection status AND the bus's JSON error
    shape — a bare 400/401/503 from some unrelated app behind auth or input
    validation isn't enough, so a mispointed ``target_url`` won't false-positive.
    ``404`` / connection refused / DNS failure / timeout means a wrong URL,
    missing tunnel, or downed bus.

    The target is shape-validated (HTTPS + exact ``/v1/webhooks/github`` path)
    BEFORE any POST, so a mistyped URL can't fire a request at an unrelated
    handler — the probe only ever POSTs to a canonical webhook path.
    """
    try:
        validate_target_url(target_url)
    except ValueError as e:
        return {"reachable": False, "error": f"invalid_url: {e}", "url": target_url}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                target_url,
                content=b"khonliang-webhook-reachability-probe",  # not JSON
                headers={"X-GitHub-Event": "ping"},
            )
        return {
            "reachable": _looks_like_bus_rejection(resp),
            "status_code": resp.status_code,
            "url": target_url,
        }
    except httpx.TimeoutException:
        return {"reachable": False, "error": "timeout", "url": target_url}
    except httpx.ConnectError as e:
        return {
            "reachable": False,
            "error": f"connect_error: {e}",
            "url": target_url,
        }
    except httpx.RequestError as e:
        return {
            "reachable": False,
            "error": f"{type(e).__name__}: {e}",
            "url": target_url,
        }
