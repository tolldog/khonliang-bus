# GitHub webhook → bus setup

The bus exposes `POST /v1/webhooks/github` (HMAC-verified) and republishes
each delivery as a `github.<event>[.<action>]` bus event so that any
subscriber — Claude via `bus_wait_for_event`, the developer agent's PR
watcher, an external tool — can react in real time instead of polling.

This runbook walks an operator through the four steps that make the
endpoint actually fire end-to-end:

1. Generate a webhook secret.
2. Install it on the bus (systemd unit drop-in + env file).
3. Make the bus reachable from the public internet.
4. Configure each GitHub repo to point its webhooks at that URL.

The endpoint itself is implemented in `bus/webhooks.py` and registered at
`bus/server.py` (`POST /v1/webhooks/github`). The receiver rejects
unsigned requests with `401` once a secret is set; with no secret AND
no explicit `github_webhook_allow_unsigned: true` in bus config it
returns `503` (preventing a forged-event footgun in production).

> **Port assumption.** All shell snippets below use port **`8788`**,
> which is what the canonical systemd unit
> (`/etc/systemd/system/khonliang-bus.service`) passes to
> `ExecStart=python -m bus --port 8788`. The bare module
> (`bus/__main__.py`) defaults to port `8787`, so a fresh-checkout
> `python -m bus` without the `--port` flag listens on a different
> socket. Either align your systemd unit to use `--port 8788`
> (recommended; matches every example here), or substitute your
> chosen port into each `localhost:8788` reference below.

## 1. Generate a webhook secret

The secret is a shared string between your bus and GitHub. Generate it
once with whichever you prefer:

```sh
python -c 'import secrets; print(secrets.token_urlsafe(48))'
# or
openssl rand -base64 48
```

Treat the result like a credential — do not commit it. Storing it in
your password manager alongside the bus deployment notes is fine.

## 2. Install the secret on the bus

The receiver looks up the secret in this precedence:

1. `github_webhook_secret` in the bus config dict (passed to
   `create_app(config={...})`).
2. `GITHUB_WEBHOOK_SECRET` environment variable.

The recommended layout for a systemd-supervised prod bus is an
environment file plus a unit drop-in, so the secret is owned by the
service user and never lands in committed configuration.

### 2a. Sanity-check the pre-install state (optional)

Before touching anything, confirm the receiver is in the
"no-secret" state. This step is **only meaningful before the
install commands in 2b**: once the secret is installed and the
service has been restarted, the receiver no longer returns 503
for unsigned posts. Skip if you're upgrading an already-configured
bus.

```sh
# Should return HTTP 503 ("webhook receiver not configured") when
# no secret is set AND github_webhook_allow_unsigned is not in the
# bus config. Confirms the receiver itself is reachable on
# localhost:8788 before you start changing anything.
curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8788/v1/webhooks/github \
  -H 'X-GitHub-Event: ping' -H 'Content-Type: application/json' \
  -d '{"zen":"pre-install"}'
```

### 2b. Install the secret + drop-in, then restart

Copy the templates from `etc/khonliang-bus/` into the system locations
and fill them in:

```sh
sudo install -d -o root -g root -m 0755 /etc/khonliang
sudo install -o khonliang -g khonliang -m 0640 \
  etc/khonliang-bus/webhook-secret.env.example \
  /etc/khonliang/webhook-secret.env
sudoedit /etc/khonliang/webhook-secret.env   # paste the real secret

sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/khonliang-bus.service.d
sudo install -o root -g root -m 0644 \
  etc/khonliang-bus/khonliang-bus.service.d/webhook-secret.conf.example \
  /etc/systemd/system/khonliang-bus.service.d/webhook-secret.conf

sudo systemctl daemon-reload
sudo systemctl restart khonliang-bus.service
```

### 2c. Verify post-install state

After restart, the receiver should reject unsigned requests with
`401` and accept properly-signed requests with `200`. Both checks
are runnable in either order; together they prove the secret was
loaded and HMAC verification works end-to-end.

```sh
# Unsigned POST → HTTP 401 ("invalid signature"). If you still see
# 503 here, the EnvironmentFile= path is wrong or the env file is
# unreadable by the khonliang user — re-check the install commands.
curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8788/v1/webhooks/github \
  -H 'X-GitHub-Event: ping' -H 'Content-Type: application/json' \
  -d '{"zen":"unsigned-smoke"}'

# Signed POST → HTTP 200 ("status: published"). Confirms HMAC
# verification AND that the bus publish path fires the event.
#
# The sed pipeline strips surrounding "double" or 'single' quotes so
# the signing matches systemd's EnvironmentFile= unquoting behaviour:
# if the operator wrote GITHUB_WEBHOOK_SECRET="..." in the env file,
# systemd unquotes it before the bus reads it, so the signature must
# also be computed against the unquoted value.
# Use ``tail -n1`` so the smoke command picks the LAST
# ``GITHUB_WEBHOOK_SECRET=`` line, matching systemd's
# ``EnvironmentFile=`` semantics — the bus loads the last
# effective assignment to a duplicated key. ``head -n1`` would
# read a stale earlier value and every signed POST would 401.
SECRET=$(sudo cat /etc/khonliang/webhook-secret.env \
  | grep -E '^GITHUB_WEBHOOK_SECRET=' \
  | tail -n1 \
  | cut -d= -f2- \
  | sed -E 's/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')
BODY='{"zen":"signed-smoke"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^[^=]*= */sha256=/')
curl -s -X POST http://localhost:8788/v1/webhooks/github \
  -H "X-GitHub-Event: ping" \
  -H "X-Hub-Signature-256: $SIG" \
  -H 'Content-Type: application/json' \
  --data-binary "$BODY"
```

## 3. Expose the endpoint to the public internet

GitHub posts webhooks from public IPs over HTTPS, so the bus needs a
reachable URL. By default the bus binds Uvicorn to `0.0.0.0` (see
`bus/__main__.py`'s `--host` default; port is `8788` per the
runbook's port assumption above, or `8787` if you're running the
bare module without `--port`). Binding to `0.0.0.0` means the
unauthenticated control-plane routes — `/v1/register`,
`/v1/request`, `/v1/publish`, `/v1/install/*`, … — are *already*
reachable on every interface the host has, including any LAN /
VPN / tailnet IP. Adding a path-scoped public proxy alone does
NOT make those routes private; it just adds a second public
ingress.

> **Critical — close the host-network exposure first.** Either:
>
> 1. **Bind the bus to `127.0.0.1`** by passing `--host 127.0.0.1`
>    in the systemd `ExecStart`. Recommended for single-host
>    deployments; the unauthenticated routes are then only reachable
>    via the tunnel/proxy you set up below, scoped to the webhook
>    path. After editing the unit, `sudo systemctl daemon-reload &&
>    sudo systemctl restart khonliang-bus`.
> 2. **Or accept LAN-side exposure deliberately** (acceptable for a
>    trusted tailnet where every peer is already an authorized
>    operator) — but document it; the path-scoped Funnel/proxy
>    options below only constrain the *internet*-side surface.
>
> Every Option below additionally scopes the public ingress to
> **only** `/v1/webhooks/github`. The path scoping is necessary
> but not sufficient on its own: it doesn't undo the host-NIC
> binding.

### Option A: Tailscale Funnel (recommended for tailnet-managed hosts)

Funnel terminates TLS at Tailscale's edge and routes a single path
through to your local bus. One-time enable on the tailnet, then a
single command per host:

```sh
# One-time per tailnet (browser action — only the tailnet admin can do this):
#   open the URL printed by:
tailscale funnel status

# Per host, once funnel is enabled tailnet-wide:
tailscale funnel --bg --https=443 \
  --set-path=/v1/webhooks/github \
  http://localhost:8788/v1/webhooks/github
```

Your public webhook URL is then
`https://<host>.<tailnet>.ts.net/v1/webhooks/github`. The rest of
`localhost:8788` stays private; only this exact path is reachable.

### Option B: cloudflared / ngrok / any HTTPS tunnel

The bus doesn't care which tunnel you use — it just needs to receive
the POST on `localhost:8788/v1/webhooks/github`. Scope the tunnel
config to that exact path so the rest of the bus's REST surface
(`/v1/register`, `/v1/request`, `/v1/publish`, etc. — all
unauthenticated) does NOT become public. For cloudflared that's an
`Ingress` block with `path: /v1/webhooks/github`; for a generic
reverse proxy, restrict the upstream `location` block.

### Option C: real public endpoint

If the bus host already has a real public hostname with TLS in front
of it (caddy / nginx / cloud LB), point the webhook URL at that and
skip the tunnel — but configure the proxy to forward **only**
`/v1/webhooks/github` upstream. A blanket forward exposes the
unauthenticated control plane.

## 4. Configure each repo's GitHub webhook

### Option A: scripted (preferred for the khonliang-* fleet)

`scripts/install-github-webhook.sh` reads the secret from
`/etc/khonliang/webhook-secret.env`, derives the public URL from the
local Tailscale hostname (Option A above), and POSTs the canonical
config (events: `pull_request`, `pull_request_review`,
`pull_request_review_comment`, `issue_comment`, `push`, `check_run`)
to one repo or all `tolldog/khonliang-*` repos at once. It is
idempotent — installs that already target the resolved URL are
skipped, not duplicated.

> **Prerequisites for the scripted path:**
>
> - `gh` CLI installed (`https://cli.github.com`).
> - `gh auth login` completed for an account that has admin
>   access to every repo you intend to install on. Webhook
>   create / patch requires the `admin:repo_hook` scope —
>   verify with `gh auth status` (the scope list should
>   include it). A read-only token will return 403/404 from
>   `gh api` calls and the script will surface the gh stderr
>   verbatim.
> - `python3` available (used for JSON encode/decode).
> - When `KHONLIANG_WEBHOOK_URL` is unset, `tailscale` must
>   also be in PATH so the script can resolve your tailnet
>   hostname.
>
> The script's `require_dep` check fires up front and prints
> these requirements explicitly when something is missing.

> **If you used Option B or C** (cloudflared / ngrok / your own
> proxy), the local `*.ts.net` hostname won't resolve to the right
> URL. Set `KHONLIANG_WEBHOOK_URL` to the full public webhook URL
> (e.g. `https://hooks.example.com/v1/webhooks/github`) before
> running the script, otherwise it will install the wrong URL on
> every repo.

```sh
# Install on a single repo (defaults to owner=tolldog if no slash)
scripts/install-github-webhook.sh khonliang-bus
scripts/install-github-webhook.sh someorg/their-repo

# Install on every tolldog/khonliang-* in one pass
scripts/install-github-webhook.sh --all-khonliang

# Verify last_response across one or many repos without making changes
scripts/install-github-webhook.sh --check khonliang-bus khonliang-developer
scripts/install-github-webhook.sh --check --all-khonliang

# Non-Funnel deployments must override the URL explicitly
KHONLIANG_WEBHOOK_URL=https://hooks.example.com/v1/webhooks/github \
  scripts/install-github-webhook.sh --all-khonliang
```

The script does not echo the secret to the terminal, does not log it,
and stages it only in a `mode 0600` tempfile that an `EXIT` trap
unconditionally cleans up — under `set -e` aborts as well as the
normal path. It DOES transmit the secret to GitHub as `config.secret`
on every webhook create / patch (over TLS to the GitHub API) so
GitHub can store it for HMAC-signing future deliveries; that is the
intended credential-sharing operation, not a leak. Treat installation
as a credential-distribution event, not a local-only one. Override
either input via `KHONLIANG_WEBHOOK_URL` or `KHONLIANG_SECRET_FILE`.

### Option B: GitHub UI (one repo at a time)

For repos outside the `tolldog/khonliang-*` fleet, in
`Settings → Webhooks → Add webhook`:

| Field | Value |
| --- | --- |
| Payload URL | `https://<your-public-host>/v1/webhooks/github` |
| Content type | `application/json` |
| Secret | the **unquoted** value from `webhook-secret.env` (see note below) |
| SSL verification | enabled |
| Events | the canonical set the scripted path uses (see below) |

To match the scripted install (``scripts/install-github-webhook.sh``)
and the topic table later in this document, select these six events
under "Let me select individual events":

- `Pull requests`
- `Pull request reviews`
- `Pull request review comments`
- `Issue comments`
- `Pushes`
- `Check runs`

Manually-configured repos that omit any of these will silently never
fire the corresponding bus topics, so a subscriber listening on (say)
`github.issue_comment.created` will get nothing — which looks
identical to "no comment was posted" and is hard to debug. The
script enforces all six; the UI table above mirrors that contract.

> **Quoting:** `EnvironmentFile=` allows `GITHUB_WEBHOOK_SECRET="..."`
> and `GITHUB_WEBHOOK_SECRET='...'` and unquotes the value before the
> bus reads it. Whatever you paste into GitHub's *Secret* field must
> be the **unquoted** value, not the literal characters of the env
> file line. If your env file reads `GITHUB_WEBHOOK_SECRET="abc123"`,
> paste `abc123` into the GitHub UI — pasting `"abc123"` (with
> surrounding quotes) will make the bus 401 every signed delivery.

### Verify the ping arrived on the bus

After saving (either option), GitHub fires a `ping` event. Verify it on the bus:

Two pitfalls to avoid in this verification:

1. **Backlog replay.** ``/v1/wait`` defaults to replaying every
   unacked event for a fresh subscriber, so a ``github.ping`` from a
   previously-configured repo will satisfy the call even if the
   webhook you just saved never reached the bus. Pair the wait with
   a delivery-id cross-check.
2. **No-op extra fields.** FastAPI silently ignores keys that
   ``WaitRequest`` doesn't declare; ``cursor=now`` is a real field
   only after `fr_bus_3db58f0b` ships, so on an older bus build it's
   accepted-and-ignored without error.

Both pitfalls are dodged by giving the wait a never-used
``subscriber_id`` and asserting the returned event's
``payload.summary.delivery_id`` against the *exact* delivery id
GitHub recorded for the save (visible under
``Settings → Webhooks → <hook> → Recent Deliveries``):

```sh
# Use a fresh subscriber_id so the ack-pointer is at zero AND
# verify the returned delivery_id matches THIS save's id. Look up
# the *expected* delivery_id under
# Settings → Webhooks → <hook> → Recent Deliveries → <click row>;
# it's the GUID under the "Headers" tab as ``X-GitHub-Delivery``.
SETUP_SUB="setup-smoke-$(date +%s)"
export EXPECTED_DELIVERY_ID="paste-the-guid-here"   # ``export`` so python3 -c sees it
curl -s -X POST http://localhost:8788/v1/wait \
  -H 'Content-Type: application/json' \
  -d "{\"topics\":[\"github.ping\"],\"subscriber_id\":\"$SETUP_SUB\",\"timeout\":30}" \
  | python3 -c "
import json, os, sys
# /v1/wait returns ``{event: {topic, payload: {...}, ...}, subscriber_id, status}``;
# the matched message lives under the top-level ``event`` key, NOT directly
# at the response root. Reading ``response['payload']`` here would silently
# return None and report a false negative even on a real delivery.
response = json.load(sys.stdin)
event = response.get('event') or {}
summary = ((event.get('payload') or {}).get('summary') or {})
got = summary.get('delivery_id', '')
want = os.environ['EXPECTED_DELIVERY_ID']
if got == want:
    print(f'OK: delivery_id={got} matches the save you just made')
else:
    print(f'FALSE POSITIVE: got delivery_id={got!r}, expected {want!r}')
    print('  → a stale ping from a different repo satisfied this wait,')
    print('  → or the wait timed out before the delivery arrived.')
    print('  → re-run with cursor=now (see below) to skip the backlog.')
    sys.exit(1)
"
```

After the bus running this branch picks up `fr_bus_3db58f0b` (PR #29
in the bus repo), an even cleaner form is available — pass
``"cursor": "now"`` in the body and the wait pins to the current
high-water mark instead of replaying backlog at all:

```sh
# Once the bus is on the cursor=now build:
curl -s -X POST http://localhost:8788/v1/wait \
  -H 'Content-Type: application/json' \
  -d '{"topics":["github.ping"],"subscriber_id":"setup-smoke","timeout":30,"cursor":"now"}' \
  | python -m json.tool
```

A successful ping proves: GitHub reached the endpoint, the signature
verified, and the bus published the event. From here, any subscriber
can react with `bus_wait_for_event(topics="github.<event>.<action>")`.

## Topic shapes the receiver emits

The receiver builds topics as `github.<X-GitHub-Event>[.<action>]`:

| GitHub event | action | bus topic |
| --- | --- | --- |
| `pull_request` | `opened` | `github.pull_request.opened` |
| `pull_request` | `synchronize` | `github.pull_request.synchronize` |
| `pull_request_review` | `submitted` | `github.pull_request_review.submitted` |
| `push` | (none) | `github.push` |
| `check_run` | `completed` | `github.check_run.completed` |
| `ping` | (none) | `github.ping` |

Each event payload is `{"summary": {...}, "github": {...}}`. The
`summary` carries the commonly useful fields (`pr_number`, `pr_url`,
`review_url`, `review_author`, `branch`, etc.) so subscribers don't
have to walk the full GitHub payload for basic identification.

## Rotating the secret

```sh
sudoedit /etc/khonliang/webhook-secret.env   # replace value
sudo systemctl restart khonliang-bus.service
# update the same value in each repo's webhook config under
# Settings → Webhooks → <hook> → Edit → Secret
```

There is no overlap window: as soon as the bus restarts with the new
secret, GitHub posts signed with the old secret will fail HMAC
verification and return 401 until you update each repo.

> **Drift detection limitation:** the script's `--check` and
> install-time drift detection cannot tell that a hook is signing
> with a stale secret. GitHub's hooks API redacts `config.secret`
> in responses, exposing only a "secret is set" presence bit, so
> drift detection covers events / active / content_type /
> insecure_ssl / secret-presence — never the secret value itself.
> After an env-file rotation, every repo's hook still passes
> drift checks even though every signed delivery is failing 401.
> Re-running the install script does NOT force-PATCH a hook whose
> config-shape matches; the only reliable rotation path is the
> manual UI edit (or deleting+reinstalling the hook). Treat the
> per-repo UI step in the box above as required, not optional.
