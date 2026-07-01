# Fleet logging design (fr_khonliang-bus_70862caa, _331fadc8)

Design-invariant pass done BEFORE implementation (per
`design-invariant-first-for-statemodel-frs`), then adversarially reviewed;
the review's confirmed flaws are folded in below (marked ▸R).

## The invariant (load-bearing)

**Every debugging layer must work with all layers above it dead — and no layer
may degrade the one below it.** (▸R4: a log-file failure must never break agent
launch; that would make L0 worse than today's DEVNULL.)

| Layer | What | Works when… |
|---|---|---|
| L0 | Per-agent log **files** written by the bus | …log agent, substrate, and distill are all dead. `tail -f` / `grep` is the floor. |
| L1 | **Log agent**: tail → substrate; raw `log_query` | …distill is uninstalled / broken / mid-deploy. |
| L2 | **Distill** (`_331fadc8`): clusters, dedup, anomalies | best-effort acceleration; never required by any debugging path. |

Consumers that must honor "log agent down ≠ broken": `bus_logs_query` (clear
*"log agent unreachable — raw files at `<dir>`"* verdict, synthesized
**bus-side** where `agent_log_dir` is known — ▸R9), `bus_diagnose` (log fields
null + flagged; no change today), the supervisor (ordinary agent, no special
casing — verified).

## Current state (facts, verified)

- `_start_process` spawns agents with `stdout=DEVNULL, stderr=DEVNULL` — agent
  output is **discarded**. L0 does not exist yet.
- Production: bus runs from `/opt/khonliang/bus`; `/opt/khonliang/logs/` exists
  (empty) as the intended log home. Dev uses repo-relative paths.
- `khonliang-bus-lib` is NOT a bus dependency; `websockets` IS.
- The bus config dict is built in `__main__.py` — a new key needs real CLI/env
  plumbing (▸R8), there is no YAML path.

## PR 1 — L0: the bus writes the files

1. **Redirect**: `_start_process` opens `<agent_log_dir>/<agent_id>.log` in
   append mode, passes it as `stdout` with `stderr=subprocess.STDOUT`, and
   **closes the parent's copy immediately after `Popen` returns** (▸R2: the
   child owns the only fd; nothing is stored, so the supervisor's five
   direct-`_processes` mutations can't leak handles, and bus restart is safe).
2. **Never break the spawn** (▸R4): `agent_log_dir` is mkdir'd once at startup;
   any per-agent open/rotate failure logs a warning and falls back to
   `DEVNULL`. No log-file error may propagate out of the redirect block — an
   exception there would feed `autostart_failed` and the supervisor's give-up.
3. **Rotate-at-start** (▸R3): inside `_start_process`, before opening — if the
   file exceeds `agent_log_max_bytes` (default 50MB), rename to `.log.1`.
   Missing file = success; **any rotation error is swallowed** (warn + skip).
   Depth-1 `.log.1` means a crash-loop overwrites earlier evidence — accepted
   trade-off, bounded by the supervisor give-up ceiling (▸R6c).
4. **Unbuffered children** (▸R5): `env={**os.environ, "PYTHONUNBUFFERED": "1"}`
   — Python children block-buffer stdout to files (~8KB), so crash-adjacent
   lines (the ones that matter) would sit in a buffer lost on SIGKILL.
   Documented as the convention for non-Python agents too.
5. **The bus's own log**: `bus.log` in the same dir via `RotatingFileHandler`
   (▸R6b: the bus is long-lived; rotate-at-start doesn't apply to it; the bus
   owns its handle so in-process rotation is safe). Also guarded — unwritable
   dir skips the handler with a warning.
6. **Plumbing** (▸R8): `--log-dir` CLI flag + `KHONLIANG_BUS_LOG_DIR` env in
   `__main__.py` → `config["agent_log_dir"]`; default `logs/agents`
   (cwd-relative, matching `--db data/bus.db`); production sets
   `/opt/khonliang/logs`. `agent_log_max_bytes` likewise (env only).
7. **Mid-flight rotation** is logrotate territory and MUST be `copytruncate`
   (▸R6a: the child holds the fd with no reopen signal — rename-based rotation
   silently diverts writes to the renamed file). The tailer's
   `size < offset → truncation` rule is consistent with copytruncate.

**The file is the local queue**: when the log agent is down the file keeps
growing; forwarding resumes from the persisted offset (see tailer rules).

## PR 2 — L1: the log agent

**Process/module**: `python -m bus.log_agent` — module `bus/log_agent/`, no
imports from `bus.server` (stdlib + `websockets` + `sqlite3`), installed and
supervised like any agent. Must accept the spawn CLI contract
`--id <id> --bus <url> --config <path>` (that's what `_start_process` appends).
`discover_bus()` + `_socket_live` move to a dependency-free `bus/discovery.py`
(shared with the MCP adapter, which currently drags in the optional `mcp`
package). The agent converts `http://host:port` → `ws://host:port/v1/agent`
and supports `unix://<sock>` via `websockets.unix_connect`.

**WS protocol** (verified against `handle_agent_ws`):
- Register with **explicit** `agent_type: "log-agent"` (the id-derived default
  for `log-agent` would be `"log"` — ▸R-secondary), real `pid`, skills incl.
  `health_check` (▸R7: `bus_diagnose` probes `health_check` and flags agents
  without it as unprobeable), and a `welcome`.
- Requests arrive as `{"type": "request", operation, args, correlation_id,...}`;
  reply `{"type": "response", correlation_id, "result": {...}}` — the payload
  MUST nest under `"result"`.
- Heartbeat every ~20s (staleness threshold is 90s; a stale row is excluded
  from type routing). Long queries must not block the heartbeat loop.
- Tolerate unknown message types.

**Skills**: `log_query(agent_id?, since, until?, level?, pattern?, limit=200)`,
`substrate_status()`, `self_test()` (probe line → ingest → query round-trip),
`health_check()` (▸R7).

**Quiet logging** (▸R-secondary): the log agent's own stdout lands in
`log-agent.log` *inside the glob it tails* — per-record INFO logging would
self-amplify. Its own logging is WARNING+ / batched summaries only.

**Tailer rules** (▸R1, ▸R10 — the correctness core):
- Poll 0.5s over `<agent_log_dir>/*.log`; per-file `(path, inode, offset)`
  persisted in the substrate DB.
- **Offset only ever advances to the last `\n`** — a partial tail line is
  carried to the next poll, never ingested as a fragment (▸R10).
- **On inode change (rotation): drain the old file first.** Read
  `[offset, EOF)` from `.log.1` (same inode) and ingest it BEFORE resetting to
  the new file at 0 (▸R1 — without this, a supervisor restart during a
  log-agent outage silently drops the whole unforwarded backlog, defeating the
  file-as-queue guarantee).
- `size < offset` on the same inode → copytruncate; reset to 0.
- Parse: `agent_id` from filename; timestamp+level best-effort from the
  standard `%(asctime)s %(name)s %(levelname)s %(message)s` shape; fallback
  `ts=ingest-time, level=None`. Parse failure never drops a line.

**Substrate contract** (`bus/log_agent/substrate.py`): `ingest(records)`,
`query(...)`, `status()`. Phase 1: SQLite+FTS5 at `<agent_log_dir>/substrate.db`
(own file, never the bus DB). Retention `retention_days` (default 14), enforced
at startup + daily. Loki/Elastic are Phase 2 (mechanical by contract).

**Bus passthrough + limp mode** (▸R9): a thin bus route `GET /v1/logs/query`
forwards to the log agent via normal routing; on no-healthy it synthesizes the
limp verdict *with* `agent_log_dir` (which only the bus knows). The MCP tool
`bus_logs_query` calls that route. The MCP adapter never needs bus config.

## Phasing

- **PR 1 (L0)**: items above — small, immediately valuable alone.
- **PR 2 (L1)**: discovery extraction; `bus/log_agent/`; passthrough + limp.
- **Later, separate FRs**: `_331fadc8` distill (L2); Phase-2 substrate
  adapters; Phase-3 bus log events.

## Out of scope (per FR)

External substrate beyond the sqlite demo adapter; multi-host; substrate auth;
distill/aggregation/anomaly detection.
