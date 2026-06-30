"""Tests for the ``python -m bus.cli.webhook`` operator CLI.

The CLI is a thin dispatcher over the ``/v1/webhooks/manage/*`` routes
(themselves covered in ``test_webhook_manage.py``). Here we verify the
dispatch (method/path/body per subcommand), the terse formatting, and the
exit-code contract — driving ``run()`` with an injected MockTransport
client so no bus or network is involved.
"""

from __future__ import annotations

import json
from typing import Callable

import httpx
import pytest

from bus.cli import webhook as cli

BUS = "http://bus.test"


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="")


def _run(argv, handler):
    return cli.run(["--bus", BUS, *argv], client=_client(handler))


# ---------------------------------------------------------------------------
# dispatch: correct method / path / body per subcommand
# ---------------------------------------------------------------------------


def test_install_posts_repo_and_dry_run(capsys):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"action": "created", "repo": "owner/x", "hook_id": 7, "orphans": []})

    code = _run(["install", "owner/x", "--dry-run"], handler)
    assert code == 0
    assert seen["url"] == f"{BUS}/v1/webhooks/manage/install"
    assert seen["body"] == {"repo": "owner/x", "dry_run": True}
    assert "created hook_id=7" in capsys.readouterr().out


def test_install_default_dry_run_false():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"action": "created", "repo": "owner/x", "hook_id": 1, "orphans": []})

    _run(["install", "owner/x"], handler)
    assert seen["body"]["dry_run"] is False


def test_audit_formats_kind_and_orphans(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/webhooks/manage/audit"
        return httpx.Response(200, json={
            "repo": "owner/x", "kind": "drift", "hook_id": 9,
            "drift_fields": ["events"], "last_response_code": 200,
            "orphans": [{"id": 3, "url": "https://old/v1/webhooks/github"}],
        })

    code = _run(["audit", "owner/x"], handler)
    out = capsys.readouterr().out
    assert code == 0
    assert "drift hook_id=9" in out
    assert "drift=events" in out
    assert "1 orphan" in out


def test_repair_path(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/webhooks/manage/repair"
        return httpx.Response(200, json={
            "action": "repaired", "repo": "owner/x", "hook_id": 9,
            "force_patched": True, "orphans": [],
        })

    assert _run(["repair", "owner/x"], handler) == 0
    assert "force_patched" in capsys.readouterr().out


def test_install_fleet_body_and_summary(capsys):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={
            "summary": {"created": 2},
            "results": [
                {"action": "created", "repo": "owner/a", "hook_id": 1, "orphans": []},
                {"action": "created", "repo": "owner/b", "hook_id": 2, "orphans": []},
            ],
        })

    code = _run(["install-fleet", "--prefix", "khonliang-", "--owner", "owner", "--dry-run"], handler)
    out = capsys.readouterr().out
    assert code == 0
    assert seen["url"] == f"{BUS}/v1/webhooks/manage/install_fleet"
    assert seen["body"] == {"prefix": "khonliang-", "dry_run": True, "owner": "owner"}
    assert "summary: created=2" in out
    assert "owner/a: created" in out


def test_fleet_omits_owner_when_not_given():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"summary": {}, "audits": [], "errors": []})

    _run(["audit-fleet"], handler)
    assert "owner" not in seen["body"]
    assert seen["body"]["prefix"] == "khonliang-"


# ---------------------------------------------------------------------------
# check-funnel exit semantics
# ---------------------------------------------------------------------------


def test_check_funnel_reachable_exit_0(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"reachable": True, "status_code": 400, "url": "https://h/v1/webhooks/github"})

    assert _run(["check-funnel"], handler) == 0
    assert "reachable=True" in capsys.readouterr().out


def test_check_funnel_unreachable_exit_1(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"reachable": False, "error": "timeout", "url": "https://h/v1/webhooks/github"})

    # A 200 probe result, but the funnel is down → non-zero for scripts.
    assert _run(["check-funnel"], handler) == 1
    assert "reachable=False" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


def test_route_400_detail_to_stderr_exit_1(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "webhook admin disabled"})

    code = _run(["install", "owner/x"], handler)
    err = capsys.readouterr().err
    assert code == 1
    assert "webhook admin disabled" in err


def test_bus_unreachable_exit_2(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    code = _run(["audit", "owner/x"], handler)
    assert code == 2
    assert "unreachable" in capsys.readouterr().err


def test_json_flag_prints_raw(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"action": "skipped", "repo": "owner/x", "hook_id": 1, "orphans": []})

    code = _run(["--json", "install", "owner/x"], handler)
    out = capsys.readouterr().out
    assert code == 0
    parsed = json.loads(out)
    assert parsed["action"] == "skipped"


def test_json_flag_with_error_still_exit_1(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "'repo' is required"})

    code = _run(["--json", "install", "owner/x"], handler)
    captured = capsys.readouterr()
    assert code == 1
    assert json.loads(captured.out)["detail"] == "'repo' is required"
    assert "required" in captured.err


def test_no_subcommand_is_error():
    with pytest.raises(SystemExit):
        cli.run(["--bus", BUS], client=_client(lambda r: httpx.Response(200, json={})))
