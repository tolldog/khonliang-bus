# Agent lifecycle config

How the bus launches and supervises the agents it spawns. All keys live in the
bus config (`config/bus.yaml`).

## Lazy hot-load (`lazy_eligible`)

Agents listed here are **not** started at boot and **not** supervisor-restarted
— they launch on the **first skill call** to a dead instance, then serve
normally. Idle-heavy agents (embedding workers, large-context reviewers) stay
cold until needed (fr_khonliang-bus_c81f7ab5, Phase 1).

```yaml
lazy_eligible:
  - heavy-reviewer-32b            # short form: agent_id only
  - agent_id: embed-worker        # long form
    idle_shutdown_s: 600          # Phase 2 (idle reaper — not yet active)
lazy_launch_timeout_s: 30         # wait-for-register budget on a cold launch
```

- Each entry's `agent_id` must be **installed** (the launch spec is reused, same
  as autostart) — the config only marks it lazy, it doesn't carry the command.
- **Exactly one mode:** a lazy agent opts out of autostart and out of supervisor
  restart. Don't also put it under autostart.
- A cold skill call **blocks** until the agent registers (up to
  `lazy_launch_timeout_s`), then dispatches with the request's own `timeout`.
  On register-timeout the caller gets a clean error (not a hang), and the
  half-started process is stopped.
- Concurrent cold calls to the same agent share **one** launch (no duplicate
  spawns).
- `bus_welcome` shows a dormant lazy agent as **`lazy_eligible`** (distinct from
  `cataloged_dead`) so callers know its skills are available on demand.

**Not yet shipped (Phase 2):** `idle_shutdown_s` (stop after N seconds idle,
draining in-flight calls) and a `start_only` flag (return once live without
invoking a skill).

## Supervisor (crash restart + backoff)

Bus-spawned agents that crash are restarted with exponential backoff and a
give-up ceiling (fr_khonliang-bus_dc4ef3e9):

```yaml
supervisor_restart_on_crash: true     # global kill-switch
supervisor_backoff_s: [1, 5, 30, 300] # cooldown schedule (capped at last)
supervisor_max_restarts: 5            # consecutive restarts → autostart_failed
supervisor_recovery_window_s: 300     # sustained liveness resets the counter
supervisor_interval_s: 5              # sweep cadence
```

Lazy-eligible agents are exempt (one launch per call, no retry loop).
