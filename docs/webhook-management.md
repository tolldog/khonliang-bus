# Outbound webhook management

The bus can install / audit / repair the GitHub webhooks that point *back* at
its own `/v1/webhooks/github` receiver. This replaces the abandoned bash
installer (`scripts/install-github-webhook.sh`, removed) with Python primitives
(`bus/webhook_install.py`), bus REST routes (`/v1/webhooks/manage/*`), and
`bus_webhook_*` MCP tools — closing `fr_khonliang-bus_e3b15e88`.

This is the **outbound** side (configuring repos). The **inbound** receiver
(`bus/webhooks.py`, `POST /v1/webhooks/github`) is documented inline there.

## Configuration

All resolved from bus config (`config/bus.yaml`), with env fallbacks:

| Config key                  | Env fallback                   | Purpose |
|-----------------------------|--------------------------------|---------|
| `github_webhook_admin`      | `KHONLIANG_BUS_WEBHOOK_ADMIN`  | **Opt-in gate.** Must be truthy to enable the token-backed management routes. Default off. |
| `github_token`              | `GITHUB_TOKEN`, `GH_TOKEN`     | GitHub API token; needs `admin:repo_hook` scope. |
| `github_webhook_public_url` | `GITHUB_WEBHOOK_PUBLIC_URL`    | The canonical HTTPS URL GitHub delivers to — must end in `/v1/webhooks/github` (e.g. a Tailscale Funnel host). Validated at use; non-HTTPS / wrong-path is rejected. |
| `github_webhook_secret`     | `GITHUB_WEBHOOK_SECRET`        | HMAC secret written into the hook config (same secret the receiver verifies). |
| `github_owner`              | —                              | Default owner for fleet operations (e.g. the GitHub account/org). Per-call `owner` overrides. |

### Security posture

The bus binds `0.0.0.0` with no auth, and these routes wield an
`admin:repo_hook` write token. So the whole **token-backed** surface
(install / repair / audit / fleet) is gated behind `github_webhook_admin` —
the same opt-in posture as the provenance disclosure flag. Leave it **off**
unless the listener is on a trusted network. `check_funnel` is the lone
exception: it needs no token and only probes the bus's own public URL, so it
stays ungated.

## REST routes

| Route | Method | Body | Mutates |
|-------|--------|------|---------|
| `/v1/webhooks/manage/install`       | POST | `{repo, dry_run?}`        | yes (unless `dry_run`) |
| `/v1/webhooks/manage/install_fleet` | POST | `{owner?, prefix?, dry_run?}` | yes |
| `/v1/webhooks/manage/audit`         | POST | `{repo}`                  | no |
| `/v1/webhooks/manage/audit_fleet`   | POST | `{owner?, prefix?}`       | no |
| `/v1/webhooks/manage/repair`        | POST | `{repo}`                  | yes (force-PATCH, incl. secret rotation) |
| `/v1/webhooks/manage/check_funnel`  | GET  | —                        | no |

Failure → `HTTPException` (`{"detail": ...}`): `403` admin disabled, `400`
missing token / public URL / bad URL shape / empty fleet, `502` GitHub
rejected the call.

## MCP tools

`bus_webhook_install(repo, dry_run=False)`, `bus_webhook_audit(repo)`,
`bus_webhook_repair(repo)`, `bus_webhook_install_fleet(prefix, owner, dry_run)`,
`bus_webhook_audit_fleet(prefix, owner)`, `bus_webhook_check_funnel()` —
thin formatters over the routes above.

## Behaviour notes

- **Drift** covers `events`, `active`, `content_type`, `insecure_ssl`, and the
  URL string. The secret is **not** a drift signal (GitHub redacts it on GET);
  use `repair` to force a secret rotation.
- **Active-only duplicate semantics**: one inactive historical hook + one active
  canonical hook is *not* flagged. Two-or-more *active* hooks on the URL →
  `duplicate` (manual collapse).
- **Orphans** (active hook on the canonical `/v1/webhooks/github` path but a
  stale host — e.g. after a Funnel hostname change) are surfaced on every
  result, even a clean repo, since they still double-deliver.
- **Empty fleet** is an explicit error (likely a token-scope or prefix mismatch).
- **Dead-on-arrival guard**: a *mutating* call (install/repair, non-dry-run)
  refuses with `400` when the bus receiver has no `github_webhook_secret` and
  `github_webhook_allow_unsigned` is false — otherwise it would install a hook
  the receiver `503`s on every delivery. Read-only audits and dry-runs are
  exempt so they still work for diagnosis.
- **Malformed / non-object** request bodies get a `400`, not a `500`.

## Not yet shipped (follow-up)

A thin operator CLI (`python -m bus.cli.webhook ...`) dispatching to these
routes via the bus client is planned as a follow-up to this layer.
