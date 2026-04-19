# khonliang-bus Agent Notes

This repository is the active Python bus service and MCP adapter for the khonliang workspace.

When working here:

- Keep this repo focused on bus runtime behavior: service registry, lifecycle, request routing, pub/sub, sessions, artifacts, response envelopes, flows, and MCP adapter generation.
- Keep reusable client and agent contracts in `khonliang-bus-lib`.
- Keep FR bundles, milestones, specs, repo hygiene, git/GitHub workflow, and developer automation in `khonliang-developer`.
- Keep ingestion and evidence workflows in `khonliang-researcher`.
- Do not restore the retired Go bus path.
- Keep `.mcp.json`, live database files, logs, and machine-specific paths local.
- Prefer artifacts for large outputs; MCP responses should stay compact and return refs.

Validation:

```sh
python -m pytest -q
python -m compileall bus
```

For MCP adapter changes, include focused coverage around dynamic skill registration, response envelopes, and artifact retrieval behavior.
