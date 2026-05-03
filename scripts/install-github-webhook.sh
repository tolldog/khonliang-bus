#!/usr/bin/env bash
# Install (or list) the canonical GitHub webhook on a khonliang-* repo
# so its events flow into the bus via Tailscale Funnel.
#
# Usage:
#   scripts/install-github-webhook.sh <repo>           # install on tolldog/<repo>
#   scripts/install-github-webhook.sh <owner/repo>     # install on owner/repo
#   scripts/install-github-webhook.sh --all-khonliang  # install on every tolldog/khonliang-*
#   scripts/install-github-webhook.sh --check <repo>   # verify hook + last_response only
#
# Environment:
#   KHONLIANG_WEBHOOK_URL   public webhook URL (default reads ts.net hostname)
#   KHONLIANG_SECRET_FILE   path to env file holding GITHUB_WEBHOOK_SECRET
#                           (default: /etc/khonliang/webhook-secret.env)
#
# Secret never leaves disk: the script reads it once, builds a temp body
# file with mode 600, posts via gh, then removes the body. No secret ever
# echoes to the terminal or logs.
#
# Idempotency: GitHub's webhook API does NOT dedupe. Running this twice
# creates two hooks. The script auto-paginates the existing-hook check
# so it correctly skips on repos with more than one page of hooks.

set -euo pipefail

WEBHOOK_PATH="/v1/webhooks/github"
DEFAULT_OWNER="tolldog"
DEFAULT_SECRET_FILE="${KHONLIANG_SECRET_FILE:-/etc/khonliang/webhook-secret.env}"
EVENTS=(pull_request pull_request_review pull_request_review_comment issue_comment push check_run)

require_dep() {
    # Surface missing-tool errors up front rather than letting the user
    # hit a confusing failure mid-pipeline. tailscale is only required
    # when KHONLIANG_WEBHOOK_URL is unset (we resolve the public URL
    # from the local tailnet hostname in that case).
    local missing=()
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "error: missing required commands: ${missing[*]}" >&2
        echo "  install gh (https://cli.github.com), python3, and (when" >&2
        echo "  KHONLIANG_WEBHOOK_URL is unset) tailscale before re-running." >&2
        exit 2
    fi
}

require_dep gh python3
[[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]] || require_dep tailscale

resolve_url() {
    if [[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]]; then
        echo "$KHONLIANG_WEBHOOK_URL"
        return
    fi
    # Derive from the local Tailscale hostname so the script stays
    # portable across hosts in the tailnet (e.g. a laptop runs the bus
    # on its own ts.net name during dev).
    local host
    host=$(tailscale status --self --json 2>/dev/null \
        | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["Self"]["DNSName"].rstrip("."))' \
        2>/dev/null || true)
    if [[ -z "$host" ]]; then
        echo "error: could not resolve tailscale hostname; set KHONLIANG_WEBHOOK_URL" >&2
        exit 2
    fi
    echo "https://${host}${WEBHOOK_PATH}"
}

resolve_secret() {
    # Read the env file's body once via whichever path works, then
    # extract the GITHUB_WEBHOOK_SECRET line in pure bash so a missing
    # key surfaces as an actionable error rather than aborting under
    # ``set -euo pipefail`` when ``grep | head | cut`` returns
    # non-zero.
    local body
    if [[ -r "$DEFAULT_SECRET_FILE" ]]; then
        body=$(cat "$DEFAULT_SECRET_FILE")
    elif sudo -n test -r "$DEFAULT_SECRET_FILE" 2>/dev/null; then
        body=$(sudo cat "$DEFAULT_SECRET_FILE")
    else
        echo "error: cannot read $DEFAULT_SECRET_FILE" >&2
        echo "  Either set KHONLIANG_SECRET_FILE to a readable path or" >&2
        echo "  add your account to the file's owning group (typically" >&2
        echo "  'khonliang' on the prod layout — 'sudo usermod -aG" >&2
        echo "  khonliang \"\$USER\"' followed by a fresh login)." >&2
        echo "  As a one-off you can run this script via 'sudo -E' so" >&2
        echo "  the cat fallback can read the 0640 file directly." >&2
        exit 2
    fi
    local line
    line=$(printf '%s\n' "$body" | grep -E '^GITHUB_WEBHOOK_SECRET=' | head -n1 || true)
    if [[ -z "$line" ]]; then
        echo "error: $DEFAULT_SECRET_FILE has no GITHUB_WEBHOOK_SECRET= line" >&2
        echo "  See etc/khonliang-bus/webhook-secret.env.example for the shape." >&2
        exit 2
    fi
    printf '%s\n' "${line#GITHUB_WEBHOOK_SECRET=}"
}

normalize_repo() {
    local repo="$1"
    if [[ "$repo" == */* ]]; then
        echo "$repo"
    else
        echo "${DEFAULT_OWNER}/${repo}"
    fi
}

existing_hook_id() {
    # ``gh api --paginate`` walks all pages of /hooks and emits the
    # concatenated array, so a repo with more than one page of hooks
    # doesn't fool the idempotency check into creating a duplicate.
    # The python sink wraps the multi-page stream into a single list.
    # Errors from gh (auth, missing scope, missing repo) are NOT
    # swallowed — surface them so an operator can diagnose.
    local repo="$1" url="$2" json
    if ! json=$(gh api --paginate "/repos/${repo}/hooks" 2>&1); then
        echo "error: gh api /repos/${repo}/hooks failed:" >&2
        printf '%s\n' "$json" | head -c 300 >&2
        echo >&2
        return 1
    fi
    TARGET="$url" python3 -c '
import json, os, sys
target = os.environ["TARGET"]
text = sys.stdin.read()
# --paginate emits one JSON value per page; collect into a single list.
hooks: list = []
for line in text.splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        chunk = json.loads(line)
    except json.JSONDecodeError:
        # Single-page output is one JSON array spanning multiple lines;
        # fall back to parsing the whole stream as one value.
        chunk = json.loads(text)
        hooks = chunk if isinstance(chunk, list) else []
        break
    if isinstance(chunk, list):
        hooks.extend(chunk)
for h in hooks:
    if (h.get("config") or {}).get("url", "") == target:
        print(h["id"])
        break
' <<<"$json"
}

check_one() {
    local repo url json
    repo=$(normalize_repo "$1")
    url=$(resolve_url)
    # --paginate handles multi-page hook lists. gh errors are surfaced
    # explicitly (auth, missing scope, missing repo, …) so the audit
    # path is actually trustworthy in automation/runbooks.
    if ! json=$(gh api --paginate "/repos/${repo}/hooks" 2>&1); then
        echo "  $repo: error from gh api /repos/${repo}/hooks:" >&2
        printf '%s\n' "$json" | head -c 300 >&2
        echo >&2
        return 1
    fi
    TARGET="$url" REPO="$repo" python3 -c '
import json, os, sys
target = os.environ["TARGET"]
repo = os.environ["REPO"]
text = sys.stdin.read()
hooks: list = []
for line in text.splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        chunk = json.loads(line)
    except json.JSONDecodeError:
        chunk = json.loads(text)
        hooks = chunk if isinstance(chunk, list) else []
        break
    if isinstance(chunk, list):
        hooks.extend(chunk)
matches = [h for h in hooks if (h.get("config") or {}).get("url", "") == target]
if not matches:
    print(f"  {repo}: NO matching hook for {target}")
    sys.exit(0)
for h in matches:
    lr = h.get("last_response") or {}
    hid = h.get("id")
    events = h.get("events")
    code = lr.get("code")
    status = lr.get("status")
    print(f"  {repo}: hook={hid}  events={events}  last_response={code} {status}")
' <<<"$json"
}

install_one() {
    local repo
    repo=$(normalize_repo "$1")
    local url secret
    url=$(resolve_url)
    secret=$(resolve_secret)
    if [[ -z "$secret" ]]; then
        echo "error: empty secret resolved" >&2
        exit 2
    fi

    local existing
    existing=$(existing_hook_id "$repo" "$url")
    if [[ -n "$existing" ]]; then
        echo "skip $repo: hook $existing already targets $url"
        return 0
    fi

    local body
    body=$(mktemp)
    chmod 600 "$body"
    trap 'rm -f "$body"' RETURN
    EVENTS_JSON=$(printf '"%s",' "${EVENTS[@]}"); EVENTS_JSON="[${EVENTS_JSON%,}]"
    EVENTS_JSON="$EVENTS_JSON" URL="$url" SECRET="$secret" python3 -c '
import json, os
print(json.dumps({
    "name": "web",
    "active": True,
    "events": json.loads(os.environ["EVENTS_JSON"]),
    "config": {
        "url": os.environ["URL"],
        "content_type": "json",
        "insecure_ssl": "0",
        "secret": os.environ["SECRET"],
    },
}))' > "$body"

    local result
    if ! result=$(gh api --method POST \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "/repos/${repo}/hooks" --input "$body" 2>&1); then
        echo "ERR $repo: $(printf '%s' "$result" | head -c 300)" >&2
        return 1
    fi
    local hook_id
    hook_id=$(printf '%s' "$result" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("id",""))' 2>/dev/null || true)
    if [[ -z "$hook_id" ]]; then
        echo "ERR $repo: no id in response: $(printf '%s' "$result" | head -c 300)" >&2
        return 1
    fi
    echo "OK $repo → hook $hook_id"
}

list_khonliang_repos() {
    gh repo list "$DEFAULT_OWNER" --limit 200 --json name -q '.[].name' \
        | grep -E '^khonliang' \
        | sort
}

usage() {
    sed -n '2,18p' "$0"
    exit 1
}

case "${1:-}" in
    "" | -h | --help) usage ;;
    --all-khonliang)
        for r in $(list_khonliang_repos); do install_one "$r" || true; done
        ;;
    --check)
        shift
        if [[ "${1:-}" == "--all-khonliang" ]]; then
            for r in $(list_khonliang_repos); do check_one "$r"; done
        else
            for r in "$@"; do check_one "$r"; done
        fi
        ;;
    *)
        for r in "$@"; do install_one "$r"; done
        ;;
esac
