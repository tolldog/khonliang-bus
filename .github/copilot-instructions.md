# Copilot Review Instructions

These instructions govern how **external reviewers** (GitHub Copilot
and any other cross-vendor reviewer) should respond to pull requests
in this repository. They do **not** override any internal review-event
policies of agents that happen to live in this repo (e.g., the
reviewer agent's own allowed-events list is governed separately by
its FRs, not by this file).

## Finding priority

Weight review attention by impact:

1. **Acceptance-criteria misses** — the PR body states what the code
   must do; if the code doesn't do it, flag immediately. Highest
   priority.
2. **Semantic bugs** — contract violations, silent data loss, missing
   error handling on load-bearing paths, concurrency hazards, fields
   that are populated but always empty.
3. **Test hygiene** — patches that don't restore global state, tests
   reaching into private internals, unused stubs, dead branches.
4. **Documentation / comment drift** — docstrings that don't match
   the code. Worth mentioning but not worth blocking.

A PR can merge with some (4) outstanding; it cannot merge with any
unresolved (1). Focus the first pass on (1) and (2); (3) and (4) are
acceptable but secondary.

## Verify acceptance criteria explicitly

Before submitting the review, re-read the PR description's "Acceptance"
/ "Acceptance Criteria" section (if present) and confirm each bullet
is exercised by code or tests in the diff. If a criterion isn't met
or isn't verifiable from the diff, that's a `CHANGES_REQUESTED`, not
a `COMMENTED`.

## Skip preference-style nits

Don't flag style choices that don't affect correctness or readability:
naming taste, identifier length, comment phrasing that is clear
enough, "could also be written as X" alternatives that are equivalent.
Err on the side of silence for style. A clean review that catches a
real bug beats a verbose review that catches five preferences.

## Always submit a formal review, not a bare comment

When invoked on a pull request (including re-review requests via
`@copilot please re-review`), return the verdict through GitHub's
**formal review** surface (`APPROVED`, `COMMENTED`, or
`CHANGES_REQUESTED`) — not as a plain issue-level comment.

- **If there are findings:** submit as `COMMENTED` with the findings inline.
- **If the previous round's findings are resolved and nothing new surfaces:**
  submit as `APPROVED` with a one-line justification (e.g.
  `` `Verified in {sha}: docstring fix + correctness fix lands as described` ``).
  This gives the maintainer an explicit review record in the PR's review list.
- **If correctness is blocking:** submit as `CHANGES_REQUESTED`.

Plain issue comments are for questions or clarifications only — **not**
for re-review verdicts. Even a one-line "verified" goes through the
formal-review surface so the PR's review history reflects the
merge-decision basis. Approvals and acknowledgements belong in the
review stream.

## One-line re-review acknowledgement is acceptable for LGTM

When re-reviewing fixes that address prior findings, a concise
`` `Verified in {sha}: {what was checked}. No further changes needed.` ``
submitted as an `APPROVED` formal review is sufficient. A verbose
re-review is not required when the fixes are mechanical and verified.

Placeholder convention: `{sha}`, `{what was checked}`, etc. are literal
tokens the reviewer substitutes — they are not HTML tags and must not
be emitted as angle-bracketed text.

## Scope

These instructions apply to PRs in this repo. The same instructions
live at `.github/copilot-instructions.md` in each sibling repo of
the `khonliang-*` ecosystem and are kept in sync.
