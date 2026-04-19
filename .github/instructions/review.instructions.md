---
applyTo: "**"
---

# Review Instructions

Review this repository as part of the khonliang agent ecosystem.

Prioritize findings in this order:

1. Correctness, data loss risks, security issues, and behavioral regressions.
2. Compatibility across the bus, shared libraries, agents, CLI surfaces, MCP
   adapters, and persisted schemas.
3. Context economy. Prefer artifact-backed or distilled outputs for large
   command results, test logs, research context, and long-running work.
4. Dependency discipline. Shared libraries should stay lightweight; app repos
   should avoid pulling in broad dependencies for narrow behavior.
5. Test coverage for validation failures, serialization round-trips, async
   behavior, migrations, and user-facing workflows.
6. Documentation accuracy for the current bus/developer/researcher workflow.

Do not leave actionable correctness issues as vague future work. If a change is
needed for correctness or compatibility, call it out directly with the affected
file and line.

When reviewing bus or agent changes, check that:

- Agents use the bus for communication and do not reintroduce retired MCP or
  callback-server paths.
- Skill registration remains compatible with existing agents and bus servers.
- Large outputs can be returned as artifacts with concise summaries.
- Timeouts, reconnects, cancellations, and failures surface clear errors.
- GitHub and test interactions can be tracked without stuffing raw logs into
  model context.

When reviewing FR, milestone, or developer workflow changes, check that:

- FR ownership lives in developer-facing paths unless a change explicitly says
  otherwise.
- IDs remain stable enough for existing references from papers, specs, and
  prior work units.
- Progress can be resumed after an external LLM session exits and restarts.
