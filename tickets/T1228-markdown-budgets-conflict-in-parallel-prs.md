# T1228 — The two markdown line budgets make parallel docs PRs conflict on one line

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1228 | The two markdown line budgets make parallel docs PRs conflict on one line | OPEN | — | build (b): the two line budgets are checked on main only, not inside each PR (owner 2026-11-27) |

Source: repo clean chat, 2026-11-27. Evidence, the night of 2026-11-26: nearly every PR that added a ticket or a README
line conflicted with the next on `readingLines` or `treeLines` in `app/tools/check-size.mjs` (both sat at zero slack;
`readingLines` is still set to the exact measured total); one lane joined a ticket's paragraphs into very long lines
to fit (T1225).

## Work (after the owner's pick)
Options: (a) headroom for both by rule (measured + N%), re-set only by a dedicated PR; (b) a budget checked on main
only; (c) per-area budgets. Whichever: a ticket never counts against a budget its own PR must then edit.

## Acceptance
Two docs PRs that each add a ticket merge in either order with no conflict in `check-size.mjs`; the guard still
refuses real growth.

Depends: none. Size: S. Owner: answered, (b) (2026-11-27).

## Owner

- [x] 2026-11-27 — Which rule keeps the markdown budgets from conflicting: (a) headroom by rule, re-set only by a
  dedicated PR (default), (b) checked on main only, or (c) per-area budgets? → "(b) Checked on main only" (2026-11-27)
