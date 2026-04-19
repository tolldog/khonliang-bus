# khonliang-bus

Python agent orchestration platform for the khonliang ecosystem.

The bus is the single MCP surface Claude connects to. Agents register skills
with the bus, the MCP adapter exposes those skills as tools, and requests are
routed through the bus instead of loading every project MCP directly.

## What It Provides

- **Single MCP adapter** for Claude-facing tools.
- **Agent lifecycle**: install, start, stop, restart, heartbeat, and service
  registry.
- **Request/reply routing** to registered agent callback URLs.
- **Pub/sub and long-poll waits** for cross-agent events.
- **Collaborative flows** built from registered agent skills.
- **Sessions** for long-running agent work.
- **Artifacts** for large outputs, logs, diffs, test output, and distilled
  context.
- **Response envelopes** that keep MCP responses bounded by default.
- **GitHub webhook ingestion** for repository events.

## Quick Start

Install the package in the active environment:

```bash
python -m pip install -e ".[mcp]"
```

Run the bus HTTP service:

```bash
python -m bus --port 8788 --db /tmp/khonliang-bus.db
```

Run the MCP adapter against that bus:

```bash
python -m bus.mcp_adapter --bus http://localhost:8788
```

Example local `.mcp.json` entry:

```json
{
  "mcpServers": {
    "khonliang-bus": {
      "command": "python",
      "args": ["-m", "bus.mcp_adapter", "--bus", "http://localhost:8788"]
    }
  }
}
```

Keep `.mcp.json` local and git-ignored. It contains machine-specific command
paths and live service URLs.

## Configuration And Local State

The bus does not require a checked-in config file. Runtime configuration is
provided by command-line flags and local MCP client settings:

- `python -m bus --port ... --host ... --db ...`
- `python -m bus.mcp_adapter --bus ...`
- local `.mcp.json` entries for Claude/Codex clients

Keep these local-only:

- `.mcp.json`
- SQLite bus databases
- agent working directories
- process logs
- machine-specific command paths

Shared contracts and defaults should be documented in this repo or
`khonliang-bus-lib`, not encoded in local config files.

## Current Architecture

```text
Claude
  |
  | stdio MCP
  v
bus.mcp_adapter
  |
  | HTTP request/reply
  v
khonliang-bus FastAPI service
  |
  +-- service registry and lifecycle
  +-- pub/sub and wait_for_event
  +-- sessions and artifacts
  +-- flow orchestration
  |
  +-- developer agent skills
  +-- researcher agent skills
  +-- domain agents
```

The old Go bus implementation has been retired. The current bus is the Python
service in `bus/`, with shared agent clients supplied by `khonliang-bus-lib`.

## Agent Lifecycle

The bus owns installed agent process lifecycle. Agents register with a callback
URL, heartbeat, and advertise their skills/collaborations.

Common MCP tools:

- `bus_services` lists registered agents and health.
- `bus_status` summarizes registered/installed agents, skills, and flows.
- `bus_start_agent(agent_id)` starts an installed agent.
- `bus_stop_agent(agent_id)` stops an installed agent.
- `bus_restart_agent(agent_id)` restarts an installed agent.
- `bus_refresh_skills()` refreshes MCP tools after agent skill changes.

Skill tools are generated dynamically from agent registrations. Tool names are
namespaced by agent id, for example `developer-primary_next_work_unit`.

## Artifacts

Artifacts are the context firewall for large outputs. Agents should store raw
logs, command output, diffs, file bodies, research extracts, and long summaries
as artifacts, then return compact refs through MCP.

Useful artifact tools:

- `bus_artifact_head`
- `bus_artifact_tail`
- `bus_artifact_grep`
- `bus_artifact_excerpt`
- `bus_artifact_get`
- `bus_artifact_distill`
- `bus_artifact_distill_many`

Use artifacts instead of inlining raw pytest output, full command stdout/stderr,
large diffs, large FR lists, or full file contents.

## MCP Response Budgets

The MCP adapter wraps agent responses in bounded envelopes by default. Envelope
fields include:

- `ok` / `status`
- `summary`
- `findings`
- `refs` and `artifact_ids`
- `suggested_next_actions`
- `truncated` / `omitted`
- `metrics.raw_bytes`, `metrics.raw_chars`, and `metrics.inline_budget_chars`

Default inline budget is 8,000 characters. Non-high-detail calls are capped at
16,000 characters even if a caller requests more with `_response_budget_chars`.
Callers can pass `_allow_high_detail: true` for explicit inspection, but large
outputs should still be retrieved through artifact helpers.

Agent authors should treat `detail` like this:

- `compact`: one-line status, counts, and artifact refs.
- `brief`: actionable summary, top findings, and artifact refs.
- `full`: richer summary, still no raw logs, full diffs, file bodies, or context
  dumps inline.

## Feedback

Agents can report structured feedback when a workflow reveals a missing
capability, costly friction, or improvement opportunity.

Feedback kinds:

- `gap`: expected capability is missing or insufficient.
- `friction`: operation succeeded but was costly or awkward.
- `suggestion`: improvement opportunity not tied to a failing operation.

HTTP agents can `POST /v1/feedback`. WebSocket agents can send
`{"type": "feedback", "kind": "...", ...}`. Reports are queryable with
`GET /v1/feedback` or the `bus_feedback` MCP tool.

## Development

Install development/test dependencies:

```bash
python -m pip install -e ".[test,dev,mcp]"
```

Run tests:

```bash
python -m pytest
```

Run a focused test file:

```bash
python -m pytest tests/test_mcp_adapter.py -q
```

Build the package:

```bash
python -m build
```

## Status

The current bus is the Python orchestration service. Retired Go-era artifacts
have been removed from the active tree.

Implemented:

- HTTP + WebSocket service
- SQLite-backed state
- Service registry and heartbeat
- Agent lifecycle management
- Request/reply routing
- Pub/sub and long-poll waits
- Session management
- Artifact storage and distilled retrieval
- Flow orchestration
- MCP adapter with dynamic skill tools
- Response-envelope budgeting
- GitHub webhook event ingestion

Planned:

- Richer artifact indexing
- More automatic skill routing
- Researcher-librarian taxonomy/index maintenance
- Additional external protocol adapters where useful
