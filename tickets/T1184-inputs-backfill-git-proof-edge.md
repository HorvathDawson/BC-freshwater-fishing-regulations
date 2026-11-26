# T1184 — `python -m pipeline.atlas.inputs --backfill`: the git proof edge

Status: OPEN — a #286 follow-up; scope to confirm

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1184 | `python -m pipeline.atlas.inputs --backfill`: the git proof edge | OPEN — a #286 follow-up; scope to confirm | — | a lane: reproduce the edge in `git_proof` (`pipeline/atlas/inputs.py`), then fix or remove the backfill (scope to confirm) |

Source: the planning chat's follow-ups after #286, 2026-11-26. **Scope to confirm:** the sources give this line and two hints: the third review's M4 (the
backfill's git proof could not succeed on the live atlas: 27 of 345 files committed after its build; it recommended DROPPING the backfill and recording
`git rev-parse HEAD` plus a dirty flag in the atlas meta, done) and the owner's option (e) `--first-parent` in `git_proof`. `inputs.py` still has `backfill`
and `git_proof` on main.

## Work
1. Reproduce: which file or commit shape makes `git_proof` (`inputs.py`, near line 385) answer wrongly or stop.
2. Fix it (for example `--first-parent`), or remove `--backfill` if nothing needs it (the live atlas is rebuilt at the re-cut, no backfill).

## Acceptance
A test with the failing shape; no file is "proven unchanged" that changed. Severity: low. Model: Sonnet. Size: S.
