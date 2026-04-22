# Copilot Review Instructions

## Always submit a formal review, not a bare comment

When invoked on a pull request (including re-review requests via
`@copilot please re-review`), return the verdict through GitHub's
**formal review** surface (`COMMENTED`, `APPROVED`, or
`REQUEST_CHANGES`) — not as a plain issue-level comment.

- **If there are findings:** submit as `COMMENTED` with the findings inline.
- **If the previous round's findings are resolved and nothing new surfaces:**
  submit as `APPROVED` with a one-line justification ("Verified in <sha>:
  docstring fix + correctness fix land as described"). This gives the
  maintainer an explicit review record in the PR's review list.
- **If correctness is blocking:** submit as `REQUEST_CHANGES`.

Plain issue comments are for questions or clarifications only. Approvals
and acknowledgements belong in the review stream so the merge decision
can cite a specific reviewed commit.

## One-line re-review acknowledgement is acceptable for LGTM

When re-reviewing fixes that address prior findings, a concise
"Verified in `<sha>`: <what was checked>. No further changes needed."
submitted as an `APPROVED` formal review is sufficient. A verbose
re-review is not required when the fixes are mechanical and verified.

## Scope

These instructions apply to PRs in this repo. The same instructions
live at `.github/copilot-instructions.md` in each sibling repo of
the `khonliang-*` ecosystem and are kept in sync.
