"""Operator CLI for GitHub webhook management.

A thin dispatcher over the bus's ``/v1/webhooks/manage/*`` REST routes
(fr_khonliang-bus_e3b15e88). The routes own every GitHub credential and
config value (token, public URL, secret, owner) resolved from bus config,
so this CLI only forwards ``repo`` / ``prefix`` / ``owner`` / ``dry_run``
and prints the result — it holds no secrets itself.

Usage::

    python -m bus.cli.webhook install owner/repo [--dry-run]
    python -m bus.cli.webhook audit owner/repo
    python -m bus.cli.webhook repair owner/repo
    python -m bus.cli.webhook install-fleet [--prefix khonliang-] [--owner X] [--dry-run]
    python -m bus.cli.webhook audit-fleet  [--prefix khonliang-] [--owner X]
    python -m bus.cli.webhook check-funnel

The bus URL defaults to ``$KHONLIANG_BUS_URL`` or ``http://localhost:8787``;
override with ``--bus``. Pass ``--json`` to print the raw response body.

Exit codes: ``0`` success, ``1`` on a bus-side error (4xx/5xx, or an
unreachable funnel), ``2`` if the bus itself is unreachable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import httpx

DEFAULT_BUS = os.environ.get("KHONLIANG_BUS_URL", "http://localhost:8787")
_MANAGE = "/v1/webhooks/manage"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m bus.cli.webhook",
        description="Manage GitHub webhooks via the bus management routes.",
    )
    p.add_argument("--bus", default=DEFAULT_BUS, help=f"bus URL (default {DEFAULT_BUS})")
    p.add_argument("--json", action="store_true", help="print the raw JSON response")
    sub = p.add_subparsers(dest="command", required=True)

    for name in ("install", "audit", "repair"):
        sp = sub.add_parser(name, help=f"{name} the canonical hook on a repo")
        sp.add_argument("repo", help="owner/name")
        if name == "install":
            sp.add_argument("--dry-run", action="store_true", help="report without mutating")

    inf = sub.add_parser("install-fleet", help="install across owner/<prefix>* repos")
    inf.add_argument("--prefix", default="khonliang-", help="repo-name prefix (default khonliang-)")
    inf.add_argument("--owner", default="", help="owner (defaults to bus github_owner)")
    inf.add_argument("--dry-run", action="store_true", help="report without mutating")

    auf = sub.add_parser("audit-fleet", help="audit across owner/<prefix>* repos")
    auf.add_argument("--prefix", default="khonliang-", help="repo-name prefix (default khonliang-)")
    auf.add_argument("--owner", default="", help="owner (defaults to bus github_owner)")

    sub.add_parser("check-funnel", help="probe the configured public webhook URL")
    return p


def _request(client: httpx.Client, base: str, args: argparse.Namespace) -> httpx.Response:
    base = base.rstrip("/")
    cmd = args.command
    if cmd == "install":
        return client.post(f"{base}{_MANAGE}/install", json={"repo": args.repo, "dry_run": args.dry_run})
    if cmd == "audit":
        return client.post(f"{base}{_MANAGE}/audit", json={"repo": args.repo})
    if cmd == "repair":
        return client.post(f"{base}{_MANAGE}/repair", json={"repo": args.repo})
    if cmd == "install-fleet":
        body: dict[str, Any] = {"prefix": args.prefix, "dry_run": args.dry_run}
        if args.owner:
            body["owner"] = args.owner
        return client.post(f"{base}{_MANAGE}/install_fleet", json=body)
    if cmd == "audit-fleet":
        body = {"prefix": args.prefix}
        if args.owner:
            body["owner"] = args.owner
        return client.post(f"{base}{_MANAGE}/audit_fleet", json=body)
    if cmd == "check-funnel":
        return client.get(f"{base}{_MANAGE}/check_funnel")
    raise ValueError(f"unknown command {cmd!r}")  # pragma: no cover (argparse guards)


# -- formatting (mirrors the bus_webhook_* MCP tools) --


def _fmt_orphans(orphans: list | None) -> str:
    n = len(orphans or [])
    return f"  [!] {n} orphan hook(s)" if n else ""


def _fmt_one(r: dict) -> str:
    line = f"{r.get('repo', '?')}: {r.get('action', '?')}"
    if r.get("hook_id") is not None:
        line += f" hook_id={r['hook_id']}"
    if r.get("drift_fields"):
        line += f" drift={','.join(r['drift_fields'])}"
    if r.get("duplicate_ids"):
        line += f" duplicate_ids={','.join(map(str, r['duplicate_ids']))}"
    if r.get("force_patched"):
        line += " force_patched"
    if r.get("error"):
        line += f" error={r['error']}"
    return line + _fmt_orphans(r.get("orphans"))


def _fmt_audit(a: dict) -> str:
    line = f"{a.get('repo', '?')}: {a.get('kind', '?')}"
    if a.get("hook_id") is not None:
        line += f" hook_id={a['hook_id']}"
    if a.get("drift_fields"):
        line += f" drift={','.join(a['drift_fields'])}"
    if a.get("last_response_code") is not None:
        line += f" last={a['last_response_code']}"
    return line + _fmt_orphans(a.get("orphans"))


def _summary(summary: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in summary.items()) or "(empty)"


def _format(cmd: str, b: dict) -> str:
    if cmd in ("install", "repair"):
        return _fmt_one(b)
    if cmd == "audit":
        return _fmt_audit(b)
    if cmd == "install-fleet":
        lines = [f"summary: {_summary(b.get('summary', {}))}"]
        lines += [f"  {_fmt_one(r)}" for r in b.get("results", [])]
        return "\n".join(lines)
    if cmd == "audit-fleet":
        lines = [f"summary: {_summary(b.get('summary', {}))}"]
        lines += [f"  {_fmt_audit(a)}" for a in b.get("audits", [])]
        lines += [f"  {e.get('repo', '?')}: error={e.get('error', '')}" for e in b.get("errors", [])]
        return "\n".join(lines)
    if cmd == "check-funnel":
        line = f"{b.get('url', '?')}: reachable={b.get('reachable')}"
        if b.get("status_code") is not None:
            line += f" status={b['status_code']}"
        if b.get("error"):
            line += f" error={b['error']}"
        return line
    return json.dumps(b)  # pragma: no cover


def run(argv: list[str] | None = None, *, client: httpx.Client | None = None) -> int:
    args = _build_parser().parse_args(argv)
    owns_client = client is None
    client = client or httpx.Client(timeout=30.0)
    try:
        try:
            resp = _request(client, args.bus, args)
        except httpx.RequestError as e:
            print(f"error: bus unreachable at {args.bus}: {e}", file=sys.stderr)
            return 2
        try:
            body = resp.json()
        except Exception:
            print(f"error: HTTP {resp.status_code} (non-JSON response)", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(body, indent=2))
        if resp.status_code >= 400:
            msg = (body.get("detail") or body.get("error") or f"HTTP {resp.status_code}") \
                if isinstance(body, dict) else f"HTTP {resp.status_code}"
            print(f"error: {msg}", file=sys.stderr)
            return 1
        if not args.json:
            print(_format(args.command, body))
        # A reachable=false funnel is a successful probe but a failed state —
        # give it a non-zero exit so operator scripts can branch on it.
        if args.command == "check-funnel" and not body.get("reachable"):
            return 1
        return 0
    finally:
        if owns_client:
            client.close()


def main() -> None:  # pragma: no cover (thin shell entry)
    sys.exit(run())


if __name__ == "__main__":  # pragma: no cover
    main()
