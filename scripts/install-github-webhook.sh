#!/usr/bin/env bash
# Install (or list) the canonical GitHub webhook on a khonliang-* repo
# so its events flow into the bus via Tailscale Funnel.
#
# Usage:
#   scripts/install-github-webhook.sh <repo>           # install on tolldog/<repo>
#   scripts/install-github-webhook.sh <owner/repo>     # install on owner/repo
#   scripts/install-github-webhook.sh --all-khonliang  # install on every tolldog/khonliang-*
#   scripts/install-github-webhook.sh --check <repo>   # verify hook config + drift + last_response
#
# Environment:
#   KHONLIANG_WEBHOOK_URL   public webhook URL (default reads ts.net hostname)
#   KHONLIANG_SECRET_FILE   path to env file holding GITHUB_WEBHOOK_SECRET
#                           (default: /etc/khonliang/webhook-secret.env)
#
# Secret handling: the script reads the secret from the env file
# once, stages it in a mode-0600 tempfile that ``gh api --input``
# consumes. Cleanup is guaranteed by an ``EXIT`` trap that walks
# every tempfile registered via ``register_tmpfile``, so an early
# abort under ``set -e`` (e.g. mid-write to the body file, or a
# failed gh api call) doesn't leave secret-bearing tempfiles on
# disk. The earlier ``RETURN`` trap pattern was removed because
# bash RETURN traps are shell-global, not function-scoped — once
# installed they persisted across every later helper-function
# return and operated on out-of-scope variables. No secret value
# is echoed to the terminal or persisted to logs by this script.
#
# Note: the secret IS transmitted to GitHub as part of the webhook
# create/patch request body (``config.secret``) — that's how GitHub
# stores the value it later uses to HMAC-sign deliveries. The
# transmission goes over TLS to the GitHub API; the secret is not
# logged or copied anywhere else, but it does leave this host. Treat
# this as a credential-sharing operation, not a local-only one.
#
# Idempotency / drift detection: GitHub's webhook API does NOT dedupe.
# The script auto-paginates the existing-hook check (handles repos
# with multi-page hook lists) and compares the full canonical config
# (events, active flag, content_type, insecure_ssl, secret-presence).
# An exact-config match skips; a drifted match is repaired via PATCH;
# the no-match case creates a fresh hook. Multi-hook repos (two hooks
# pointing at the same URL — would deliver every event twice) are
# detected and reported as an error so an operator can collapse them
# manually.
#
# Exit status: ``--all-khonliang`` returns non-zero when any per-repo
# step fails (so automation can detect partial-fleet failures);
# ``--check`` returns non-zero when any audited repo is missing the
# webhook or has a config drift.

set -euo pipefail

WEBHOOK_PATH="/v1/webhooks/github"
DEFAULT_OWNER="tolldog"
DEFAULT_SECRET_FILE="${KHONLIANG_SECRET_FILE:-/etc/khonliang/webhook-secret.env}"
EVENTS=(pull_request pull_request_review pull_request_review_comment issue_comment push check_run)

# Tempfile registry. Every function that creates a secret-bearing
# tempfile appends to this array (via ``register_tmpfile``); the
# EXIT trap below walks the array on any script exit — normal
# termination, ``set -e`` abort mid-write, ^C, or an uncaught
# error — so secret-bearing tempfiles never linger on disk.
TMPFILES=()
register_tmpfile() {
    TMPFILES+=("$1")
}
cleanup_tmpfiles() {
    # Always succeed — this runs in an EXIT trap and its exit code
    # becomes the script's exit code. ``[[ -n "$f" ]] && rm`` would
    # return 1 on the empty-array iteration (when TMPFILES has no
    # entries, ``${TMPFILES[@]:-}`` expands to a single empty
    # string), corrupting normal-success exits to rc=1.
    local f
    for f in "${TMPFILES[@]:-}"; do
        if [[ -n "$f" ]]; then
            rm -f "$f"
        fi
    done
    return 0
}
trap cleanup_tmpfiles EXIT

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

# NOTE: dep checks deliberately deferred until after the help-vs-real-
# command dispatch below. Running them at module-load time aborts
# ``--help`` on a fresh machine that doesn't have ``gh`` / ``python3``
# / ``tailscale`` installed yet — defeating the point of help being
# the first thing operators read.

resolve_url() {
    if [[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]]; then
        # Validate the override looks plausible: HTTPS scheme, host
        # present, path ending in /v1/webhooks/github. A typo like a
        # missing path segment would otherwise be pushed verbatim to
        # every repo — GitHub would happily accept the malformed URL,
        # POST deliveries would 404, and the installer would still
        # report success across the fleet. Loud rejection up front
        # beats a silent fleet-wide misconfiguration.
        if [[ "$KHONLIANG_WEBHOOK_URL" != https://*${WEBHOOK_PATH} ]]; then
            echo "error: KHONLIANG_WEBHOOK_URL=$KHONLIANG_WEBHOOK_URL must be HTTPS and end in '$WEBHOOK_PATH'" >&2
            echo "  example: https://hooks.example.com$WEBHOOK_PATH" >&2
            exit 2
        fi
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
    # Use the LAST matching line, not the first. systemd's
    # ``EnvironmentFile=`` semantics give the last effective
    # assignment to a duplicated key — if an operator leaves an
    # old value earlier in the file and a new value below it, the
    # bus loads the new one. Taking ``head -n1`` here would sign
    # with the stale value and every signed POST would 401. ``tail``
    # matches the bus's loaded value.
    line=$(printf '%s\n' "$body" | grep -E '^GITHUB_WEBHOOK_SECRET=' | tail -n1 || true)
    if [[ -z "$line" ]]; then
        echo "error: $DEFAULT_SECRET_FILE has no GITHUB_WEBHOOK_SECRET= line" >&2
        echo "  See etc/khonliang-bus/webhook-secret.env.example for the shape." >&2
        exit 2
    fi
    local val="${line#GITHUB_WEBHOOK_SECRET=}"
    # ``EnvironmentFile=`` allows quoted values: ``KEY="..."`` and
    # ``KEY='...'`` are both unquoted by systemd before the variable
    # reaches the bus process. Match that semantic here so the script
    # signs requests with the same value the bus is verifying against;
    # otherwise an operator who quoted the secret in the env file
    # would see the bus 401 every signed POST.
    if [[ "${#val}" -ge 2 ]]; then
        local first="${val:0:1}"
        local last="${val: -1}"
        if { [[ "$first" == '"' && "$last" == '"' ]] \
             || [[ "$first" == "'" && "$last" == "'" ]]; }; then
            val="${val:1:${#val}-2}"
        fi
    fi
    if [[ -z "$val" ]]; then
        echo "error: GITHUB_WEBHOOK_SECRET is empty in $DEFAULT_SECRET_FILE" >&2
        echo "  Generate one with 'python -c \"import secrets; print(secrets.token_urlsafe(48))\"'" >&2
        echo "  and write it back to the env file." >&2
        exit 2
    fi
    printf '%s\n' "$val"
}

normalize_repo() {
    local repo="$1"
    if [[ "$repo" == */* ]]; then
        echo "$repo"
    else
        echo "${DEFAULT_OWNER}/${repo}"
    fi
}

existing_hook_match() {
    # Returns one of:
    #   ""                               — no matching hook on this URL
    #   "<id>:ok"                        — single hook on URL, config matches
    #   "<id>:drift:<field1>,<field2>"   — single hook on URL, config drifted
    #   "duplicate:<id1>,<id2>,..."      — TWO OR MORE hooks share this URL
    #
    # Duplicate-URL detection: GitHub doesn't dedupe webhooks, so a
    # repo can end up with two hooks pointing at the same URL — each
    # delivery fires twice, doubling bus event volume and HMAC work.
    # The script reports duplicates as an error; the operator must
    # collapse them manually (the safer path is to delete extras
    # rather than have the installer guess which to keep).
    #
    # Drift detection covers the fields the script writes:
    #   - events: the canonical EVENTS list
    #   - active: must be true
    #   - content_type: must be "json"
    #   - insecure_ssl: must be "0"
    # The hook's stored secret is intentionally NOT compared (GitHub
    # never returns the value, just a "secret is set" indicator), but
    # we DO check the secret-set flag so an operator can spot a hook
    # that lost its secret server-side.
    #
    # ``gh api --paginate`` walks all pages so a repo with many hooks
    # doesn't fool this check. gh errors are NOT swallowed.
    local repo="$1" url="$2" expected_events="$3" json
    if ! json=$(gh api --paginate "/repos/${repo}/hooks" 2>&1); then
        echo "error: gh api /repos/${repo}/hooks failed:" >&2
        printf '%s\n' "$json" | head -c 300 >&2
        echo >&2
        return 1
    fi
    TARGET="$url" EXPECTED_EVENTS="$expected_events" python3 -c '
import json, os, sys
target = os.environ["TARGET"]
expected_events = set(json.loads(os.environ["EXPECTED_EVENTS"]))
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
    sys.exit(0)
# A repo may carry an inactive historical hook plus the current
# active one on the same URL — only the active hook delivers, so
# this is NOT a duplicate-delivery situation. Prefer the single
# active match and ignore inactive ones for the dedupe decision.
# Two-or-more ACTIVE matches still report as duplicate (every
# delivery would fire twice).
active_matches = [h for h in matches if h.get("active", False)]
if len(active_matches) > 1:
    ids = ",".join(str(h["id"]) for h in active_matches)
    print(f"duplicate:{ids}")
    sys.exit(0)
if active_matches:
    h = active_matches[0]
else:
    # No active match — pick the first inactive one so drift
    # detection can still fire on the ``active`` field and the
    # operator gets a single actionable target.
    h = matches[0]
cfg = h.get("config") or {}
drift = []
if not h.get("active", False):
    drift.append("active")
actual_events = set(h.get("events") or [])
if actual_events != expected_events:
    drift.append("events")
if cfg.get("content_type") != "json":
    drift.append("content_type")
if cfg.get("insecure_ssl") != "0":
    drift.append("insecure_ssl")
# GitHub redacts the secret in responses but exposes a presence bit.
# A hook that lost its secret server-side reports ``cfg["secret"]``
# absent or empty.
if not cfg.get("secret"):
    drift.append("secret_missing")
hid = h["id"]
if drift:
    drift_str = ",".join(drift)
    print(f"{hid}:drift:{drift_str}")
else:
    print(f"{hid}:ok")
' <<<"$json"
}

check_one() {
    # Audit a single repo against the canonical config. Exits
    # non-zero on missing, duplicate, or DRIFTED hooks — drift
    # detection mirrors install_one's repair criteria so the audit
    # contract matches the install contract (events, active flag,
    # content_type, insecure_ssl, secret-presence). Earlier
    # behaviour only flagged missing/duplicate, so a repo with the
    # right URL but wrong events still passed audit even though
    # ``install_one`` would have PATCHed it.
    local repo url events_json match
    repo=$(normalize_repo "$1")
    url=$(resolve_url)
    events_json=$(printf '"%s",' "${EVENTS[@]}"); events_json="[${events_json%,}]"
    if ! match=$(existing_hook_match "$repo" "$url" "$events_json"); then
        return 1  # gh api error; existing_hook_match already logged
    fi
    if [[ -z "$match" ]]; then
        echo "  $repo: NO matching hook for $url"
        return 2
    fi
    if [[ "$match" == duplicate:* ]]; then
        local ids="${match#duplicate:}"
        echo "  $repo: DUPLICATE hooks for $url (ids=$ids) — collapse manually"
        return 2
    fi
    local hook_id="${match%%:*}"
    local rest="${match#*:}"
    local kind="${rest%%:*}"
    if [[ "$kind" == "drift" ]]; then
        local fields="${rest#*:}"
        echo "  $repo: hook=$hook_id DRIFT fields=$fields (run install to repair)"
        return 2
    fi
    # OK path — surface last_response / events for human-readable
    # confirmation. The audit succeeds.
    local json hid_re events lr_code lr_status
    if ! json=$(gh api --paginate "/repos/${repo}/hooks" 2>&1); then
        echo "  $repo: error from gh api /repos/${repo}/hooks:" >&2
        printf '%s\n' "$json" | head -c 300 >&2
        echo >&2
        return 1
    fi
    # Treat non-2xx ``last_response.code`` as audit failure: a hook
    # whose deliveries are returning 404/500/timeout is silently
    # broken even though the config-shape match is fine. ``code: null``
    # ("unused") is acceptable — that just means GitHub hasn't
    # delivered yet (e.g. fresh hook with no events fired).
    if ! HOOK_ID="$hook_id" REPO="$repo" python3 -c '
import json, os, sys
target_id = int(os.environ["HOOK_ID"])
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
for h in hooks:
    if h.get("id") == target_id:
        lr = h.get("last_response") or {}
        events = h.get("events")
        code = lr.get("code")
        status = lr.get("status")
        line_out = f"  {repo}: hook={target_id}  events={events}  last_response={code} {status}"
        # ``code is None`` means GitHub has not posted a delivery
        # yet (status is "unused"). Counts as healthy.
        #
        # ``last_response`` carries only the MOST RECENT historical
        # delivery — there is no time bound. A non-2xx code can be:
        #  - a transient outage that has since recovered (false
        #    positive: the hook is configured correctly today),
        #  - a genuine ongoing delivery failure (real defect).
        # Earlier code treated either case as ``sys.exit(2)``, which
        # made automation fail audits forever after a single bad
        # delivery until another delivery happened to land green.
        # Print as a WARNING line; the audit return value is owned by
        # config-shape drift detection, which is the actually-stable
        # signal. Operators can grep for ``DELIVERY-FAILED-RECENT`` if
        # they want to escalate on it manually.
        if isinstance(code, int) and not (200 <= code < 300):
            print(line_out + "  DELIVERY-FAILED-RECENT (warning)")
        else:
            print(line_out)
        break
' <<<"$json"; then
        return 2
    fi
    return 0
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

    local events_json
    events_json=$(printf '"%s",' "${EVENTS[@]}"); events_json="[${events_json%,}]"

    local match
    if ! match=$(existing_hook_match "$repo" "$url" "$events_json"); then
        return 1
    fi
    if [[ -n "$match" ]]; then
        # Duplicate-URL hooks: every event delivers twice. Refuse to
        # patch — collapsing duplicates safely is an operator decision
        # (which to keep, what last_response history matters), not
        # something the script should guess.
        if [[ "$match" == duplicate:* ]]; then
            local ids="${match#duplicate:}"
            echo "ERR $repo: multiple hooks point at $url (ids=$ids); collapse manually before re-running" >&2
            return 1
        fi
        local hook_id="${match%%:*}"
        local rest="${match#*:}"
        local kind="${rest%%:*}"
        if [[ "$kind" == "ok" ]]; then
            echo "skip $repo: hook $hook_id already targets $url with matching config"
            return 0
        fi
        # Drift detected — repair via PATCH so a misconfigured
        # hook (wrong events, inactive, lost secret server-side,
        # …) doesn't stay broken on rerun.
        #
        # NOTE: bash ``trap RETURN`` is shell-global, not
        # function-scoped — installing one here would persist into
        # every subsequent function return in this shell. The
        # script-wide ``cleanup_tmpfiles`` EXIT trap registered at
        # module load time walks ``TMPFILES`` on any termination
        # (success, ``set -e`` abort, ^C), so we register the
        # tempfile up front and cleanup is guaranteed even if the
        # function aborts mid-write. The explicit ``rm -f`` on
        # success paths below just trims the array eagerly so
        # long-running invocations don't accumulate stale entries.
        local drift="${rest#*:}"
        local body
        body=$(mktemp)
        register_tmpfile "$body"
        chmod 600 "$body"
        EVENTS_JSON="$events_json" URL="$url" SECRET="$secret" python3 -c '
import json, os
print(json.dumps({
    "active": True,
    "events": json.loads(os.environ["EVENTS_JSON"]),
    "config": {
        "url": os.environ["URL"],
        "content_type": "json",
        "insecure_ssl": "0",
        "secret": os.environ["SECRET"],
    },
}))' > "$body"
        local patch_result patch_rc=0
        if ! patch_result=$(gh api --method PATCH \
            -H "Accept: application/vnd.github+json" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            "/repos/${repo}/hooks/${hook_id}" --input "$body" 2>&1); then
            patch_rc=1
        fi
        rm -f "$body"
        if [[ $patch_rc -ne 0 ]]; then
            echo "ERR $repo: PATCH hook $hook_id (drift=$drift) failed: $(printf '%s' "$patch_result" | head -c 300)" >&2
            return 1
        fi
        echo "REPAIR $repo: hook $hook_id (drift=$drift) → reconfigured"
        return 0
    fi

    local body
    body=$(mktemp)
    register_tmpfile "$body"
    chmod 600 "$body"
    # See note in the drift branch above: cleanup is guaranteed by
    # the script-wide ``cleanup_tmpfiles`` EXIT trap; the
    # ``rm -f`` on the success path is eager-trim only.
    EVENTS_JSON="$events_json" URL="$url" SECRET="$secret" python3 -c '
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

    local result result_rc=0
    if ! result=$(gh api --method POST \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "/repos/${repo}/hooks" --input "$body" 2>&1); then
        result_rc=1
    fi
    rm -f "$body"
    if [[ $result_rc -ne 0 ]]; then
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
    # Paginate ``gh repo list``: passing a large ``--limit`` makes
    # gh internally walk pages until the cap is hit, so a fleet
    # that grows past 200 repos doesn't get its tail silently
    # truncated. The match prefix below also requires a
    # word-boundary (``khonliang-`` rather than just
    # ``khonliang``) so a future repo named ``khonliang_archive``
    # or ``khonliang2`` doesn't sneak into the install set —
    # automation should target the canonical ``khonliang-*``
    # naming explicitly.
    gh repo list "$DEFAULT_OWNER" --limit 1000 --json name -q '.[].name' \
        | grep -E '^khonliang-' \
        | sort
}

usage() {
    # ``--help`` / ``-h`` is a successful code path. Returning a
    # non-zero exit here would break shell-completion harnesses
    # and CI lint checks that grep ``--help`` for valid
    # invocations and treat non-zero as a missing-tool failure.
    sed -n '2,18p' "$0"
    exit 0
}

case "${1:-}" in
    "" | -h | --help)
        # Don't run dep checks here — operators on a fresh machine
        # need ``--help`` to work even before they've installed
        # ``gh`` / ``python3`` / ``tailscale``. Dep checks fire on
        # the action paths below.
        usage
        ;;
    --all-khonliang)
        require_dep gh python3
        [[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]] || require_dep tailscale
        # Capture the target list up front so an empty fleet — gh
        # auth scoped to the wrong account, the org rename in
        # progress, or genuinely zero matching repos — is treated as
        # an actionable failure rather than silently exiting 0. An
        # earlier shape ran the for-loop directly over the
        # subshell, so ``rollout to 0 repos`` looked identical to
        # ``rollout to 11 repos succeeded``.
        mapfile -t all_targets < <(list_khonliang_repos)
        if [[ ${#all_targets[@]} -eq 0 ]]; then
            echo "error: --all-khonliang matched 0 repos under '$DEFAULT_OWNER'" >&2
            echo "  Check 'gh auth status' — the active account may not see" >&2
            echo "  the canonical fleet, or the prefix may have moved." >&2
            exit 1
        fi
        # Track per-repo outcome. Continue past failures so a single
        # repo's auth/scope/network issue doesn't abort the rest of
        # the fleet, but exit non-zero at the end if anything failed
        # — fleet rollouts must report partial-failure honestly to
        # automation. Earlier ``|| true`` swallowed every error and
        # made the command always look successful.
        all_failed=()
        for r in "${all_targets[@]}"; do
            if ! install_one "$r"; then
                all_failed+=("$r")
            fi
        done
        if [[ ${#all_failed[@]} -gt 0 ]]; then
            echo "FAILED on ${#all_failed[@]} repo(s): ${all_failed[*]}" >&2
            exit 1
        fi
        ;;
    --check)
        require_dep gh python3
        [[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]] || require_dep tailscale
        shift
        # Refuse to run with no targets so a typo in automation
        # (``--check`` with the repo name forgotten, or
        # ``--check`` mistyped after a flag) doesn't silently
        # report success on an empty audit set. Earlier behaviour
        # was an always-success no-op.
        if [[ $# -eq 0 ]]; then
            echo "error: --check requires at least one repo argument or --all-khonliang" >&2
            echo "  examples:" >&2
            echo "    scripts/install-github-webhook.sh --check khonliang-bus" >&2
            echo "    scripts/install-github-webhook.sh --check --all-khonliang" >&2
            exit 2
        fi
        # Track per-repo outcome so the audit path (single repo or
        # whole fleet) returns non-zero when any repo is missing,
        # has duplicate hooks, or has DRIFTED config (events /
        # active / content_type / insecure_ssl / secret-presence
        # diverging from the canonical set).
        check_failed=()
        if [[ "${1:-}" == "--all-khonliang" ]]; then
            # Same empty-fleet-is-a-failure semantics as the install
            # path: a clean audit on zero repos is indistinguishable
            # from a clean audit on the real fleet, which silently
            # masks gh-auth scope errors.
            mapfile -t check_targets < <(list_khonliang_repos)
            if [[ ${#check_targets[@]} -eq 0 ]]; then
                echo "error: --check --all-khonliang matched 0 repos under '$DEFAULT_OWNER'" >&2
                echo "  Check 'gh auth status' — the active account may not see" >&2
                echo "  the canonical fleet, or the prefix may have moved." >&2
                exit 2
            fi
            for r in "${check_targets[@]}"; do
                check_one "$r" || check_failed+=("$r")
            done
        else
            for r in "$@"; do
                check_one "$r" || check_failed+=("$r")
            done
        fi
        if [[ ${#check_failed[@]} -gt 0 ]]; then
            echo "FAILED check on ${#check_failed[@]} repo(s): ${check_failed[*]}" >&2
            exit 2
        fi
        ;;
    *)
        require_dep gh python3
        [[ -n "${KHONLIANG_WEBHOOK_URL:-}" ]] || require_dep tailscale
        for r in "$@"; do install_one "$r"; done
        ;;
esac
