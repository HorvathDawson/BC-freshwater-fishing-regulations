# T1253 — A bbox (small-area) atlas build fails at "areas.json and this build disagree"

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1253 | A bbox (small-area) atlas build fails at "areas.json and this build disagree" | OPEN | — | reproduce on main; say whether the areas check should be scoped to the bbox; after 0.10.0 (hardening) |

Source: the data and release chat, 2026-11-27, while building T1246. A bbox atlas build fails at "areas.json and this
build disagree" on that day's main, so lanes needed a scratch harness for small side builds.

## Known / not known
- Known: only the report above. **Not verified** by this lane.
- Not known: which check raises it, whether a bbox build should be exempt or the check should compare only the areas
  inside the bbox, and whether the scratch harness hides other gaps.

## Work (proposal)
1. Reproduce with a small bbox build on current main; record the command and the message.
2. Say whether the areas check should be scoped to the bbox; if yes, scope it with a test; if no, say why and what a
   small build should do instead. Retire the scratch harness if it is no longer needed.

Target: after 0.10.0 (hardening). Depends: none. Size: S. Owner: no.
