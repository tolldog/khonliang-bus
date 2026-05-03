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

Verify the bus picked up the secret by walking the three states the
receiver returns:

```sh
# Step 1 — BEFORE installing the secret: should return HTTP 503
# ("webhook receiver not configured"). If you see 503 after restart,
# the EnvironmentFile= path is wrong or the file is missing.
curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8788/v1/webhooks/github \
  -H 'X-GitHub-Event: ping' -H 'Content-Type: application/json' \
  -d '{"zen":"smoke"}'

# Step 2 — AFTER restart with the secret in place: should return
# HTTP 401 ("invalid signature") on an unsigned POST.
curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8788/v1/webhooks/github \
  -H 'X-GitHub-Event: ping' -H 'Content-Type: application/json' \
  -d '{"zen":"smoke"}'

# Step 3 — Signed POST with the actual secret: should return HTTP 200
# ("status: published"). Confirms HMAC verification + bus publish.
SECRET=$(sudo cat /etc/khonliang/webhook-secret.env | grep -E '^GITHUB_WEBHOOK_SECRET=' | cut -d= -f2-)
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
reachable URL. The bus itself stays bound to `localhost:8788`; only the
single webhook path should be exposed to the outside.

> **Critical:** the bus FastAPI app also serves unauthenticated
> control-plane routes (`/v1/register`, `/v1/request`, `/v1/publish`,
> `/v1/install/*`, …) on the same listener. If you point a public
> proxy or load balancer at the entire `localhost:8788`, those routes
> become reachable too. Every option below scopes the public surface
> to **only** `/v1/webhooks/github`. Do not skip that scoping.

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

The script never echoes the secret, never writes it to a persistent
location, and uses a `mode 0600` tempfile for the POST body. Override
either input via `KHONLIANG_WEBHOOK_URL` or `KHONLIANG_SECRET_FILE`.

### Option B: GitHub UI (one repo at a time)

For repos outside the `tolldog/khonliang-*` fleet, in
`Settings → Webhooks → Add webhook`:

| Field | Value |
| --- | --- |
| Payload URL | `https://<your-public-host>/v1/webhooks/github` |
| Content type | `application/json` |
| Secret | (paste the value from `webhook-secret.env`) |
| SSL verification | enabled |
| Events | at minimum `Pull request reviews`; usually also `Pull requests`, `Check runs`, `Pushes` |

### Verify the ping arrived on the bus

After saving (either option), GitHub fires a `ping` event. Verify it on the bus:

```sh
# Long-poll for any github.* event for up to 30s.
curl -s -X POST http://localhost:8788/v1/wait \
  -H 'Content-Type: application/json' \
  -d '{"topics":["github.ping"],"subscriber_id":"setup-smoke","timeout":30}' \
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
