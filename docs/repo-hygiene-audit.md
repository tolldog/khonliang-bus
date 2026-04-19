# Repo Hygiene Audit

Generated: 1776579546.1046805
Repo: `/mnt/dev/ttoll/dev/khonliang-bus`

## Summary

- 2 docs drift findings, 1 stale/deprecated findings, 3 proposed actions, 0 applied changes
- Python files: 32
- Test files: 20
- Docs files: 2

## Cleanup Plan

- **docs-refresh** [low] Refresh README/CLAUDE/config documentation (`CLAUDE.md`, `README.md`)
  - Docs drift findings indicate setup or architecture guidance is stale or incomplete.
- **review-stale-references** [low] Review stale wording in docs and source comments (`bus/orchestrator.py`)
  - Stale terms may be historical, but current guidance should not point at retired milestones or runtimes.
- **write-hygiene-artifact** [low] Write compact repo hygiene artifact (`docs/repo-hygiene-audit.md`)
  - Persist the audit so future sessions can resume without rereading raw files.

## Docs Drift

- [medium] `README.md`: README does not mention 'config'. Action: document local config/example boundaries
- [medium] `CLAUDE.md`: CLAUDE.md is missing. Action: add repo-specific agent guidance

## Deprecated Or Stale Paths

- [low] `bus/orchestrator.py`: Found stale marker 'MS-02'. Action: review whether this is historical context or current guidance

## Test Plan

- `.venv/bin/python -m pytest -q`
- `.venv/bin/python -m compileall .`
