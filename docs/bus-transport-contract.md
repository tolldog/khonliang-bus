# Bus transport & discovery contract (bus ⟷ bus-lib)

Authoritative contract for how clients (the MCP adapter, and agents via
`khonliang-bus-lib`) locate and connect to the bus. Both repos MUST implement
`discover_bus()` identically against this document (fr_khonliang-bus_aa5b25cf).

## Well-known socket

```
~/.khonliang/bus.sock
```

A Unix domain socket. `~/.khonliang/` is the existing convention for khonliang
state; `Path.home()` is portable across Linux and macOS with no XDG/TMPDIR
branching. **The path IS the address** — no port to allocate, configure, or
coordinate.

## Bus guarantees (khonliang-bus)

1. On startup the bus binds a UDS listener at `~/.khonliang/bus.sock` (creating
   `~/.khonliang/` if absent), serving **both** HTTP (`/v1/…`) and WebSocket
   (`/v1/agent`, `/v1/subscribe`) on that one socket.
2. The bus **also** binds TCP (default `0.0.0.0:8787`, override via
   `KHONLIANG_BUS_LISTEN=host:port`) so remote callers and not-yet-migrated
   agents keep working during the transition. UDS + TCP serve the same bus (one
   process, one app, shared state). **Disabling TCP (`KHONLIANG_BUS_LISTEN=off`,
   UDS-only) is refused until Phase 2** — the bus's own internal callers
   (`_start_process` forwards `bus_url` as `--bus`; the orchestrator builds
   `f"{bus_url}/v1/request"`) still assume an HTTP base URL, so a `unix://`
   self-URL would break autostart/lazy launches and flow dispatch. The gate
   lifts when bus-lib Phase 2 (fr_khonliang-bus-lib_042279e2) lands.
3. **Socket existence ⟺ a bus is/was listening.** On clean shutdown the bus
   unlinks the socket. A *stale* socket left by a crash is unlinked by the next
   bus start (only if nothing is listening on it — a live bus's socket is never
   clobbered; a second bus refuses to start).

## Discovery contract (adapter AND bus-lib implement identically)

`discover_bus() -> str` — resolution order, first match wins:

1. **Explicit override** — `--bus <url>` CLI arg (highest priority).
2. **Environment** — `KHONLIANG_BUS_URL` env var.
3. **Auto-discovery** — if `~/.khonliang/bus.sock` exists → return
   `unix://<absolute-socket-path>`.
4. **Fallback** — `http://localhost:8787` (TCP default, for backward-compat
   during the transition; a client MAY choose to raise instead once the fleet
   has migrated).

## Address forms

`discover_bus()` returns one of two schemes; client code branches on it:

| Scheme | Meaning | HTTP client | WebSocket client |
|---|---|---|---|
| `http://host:port` | TCP | httpx default transport, base_url = the URL | `ws://host:port/v1/agent` |
| `unix://<abspath>` | UDS | httpx `transport=HTTPTransport(uds=<abspath>)`, base_url = `http://localhost` (dummy — the transport routes to the socket) | `websockets.unix_connect(<abspath>, uri="ws://localhost/v1/agent")` |

Notes:
- The dummy `http://localhost` host for UDS is required because httpx needs a
  scheme+host to build request URLs; the `uds=` transport ignores it and dials
  the socket. Request paths (`/v1/…`) are appended unchanged.
- For UDS WebSocket, `websockets.unix_connect(path, uri=…)` dials the socket and
  performs the WS handshake against `uri`'s path. The host in `uri` is cosmetic.

## What each side implements

**khonliang-bus (this repo, Phase 1 — fr_khonliang-bus_aa5b25cf):**
- Dual-bind UDS + TCP; socket create/stale-unlink/shutdown-unlink lifecycle.
- `BusMCPAdapter`: `discover_bus()` + httpx-UDS transport when the target is
  `unix://…`; `--bus` becomes optional.

**khonliang-bus-lib (Phase 2 — fr_khonliang-bus-lib_042279e2):**
- `discover_bus()` (identical resolution order + address forms above).
- `BaseAgent.from_cli()` defaults to `discover_bus()` — `--bus` optional, not
  required.
- Agent → bus WebSocket connection over UDS via `websockets.unix_connect`
  when the resolved address is `unix://…`; HTTP register/heartbeat/response
  posts over the httpx-UDS transport.
- Any HTTP callback the agent exposes for the bus to POST back is unaffected
  (that's the bus→agent direction and stays as-is).

## Non-goals / out of scope

- Remote agents over UDS (UDS is local-only by definition; remote uses TCP).
- Multi-bus on one host (single-bus assumption; the stale-socket rule enforces
  one owner).
- Authentication on the socket (filesystem permissions on `~/.khonliang/` are
  the trust boundary; unchanged from the TCP-localhost trust model).
