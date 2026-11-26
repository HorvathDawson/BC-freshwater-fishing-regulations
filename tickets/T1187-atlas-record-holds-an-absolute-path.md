# T1187 — The atlas record keeps an unowned entry's absolute path

Status: OPEN — a #286 follow-up; scope to confirm

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1187 | The atlas record keeps an unowned entry's absolute path | OPEN — a #286 follow-up; scope to confirm | — | a lane: store a path relative to the repo or a content key, never an absolute path; a test that the record is the same on another machine (scope to confirm) |

Source: the planning chat's follow-ups after #286, 2026-11-26. **Scope to confirm:** the sources give this line only. #286 keys a module no distribution owns by its
file's bytes (the third review's M1); the atlas's record of its inputs keeps that entry's ABSOLUTE path, so the same inputs read differently on another
machine (the clean clone `~/dev/personal/canifishthis-fresh` against the main checkout) and `python -m pipeline.atlas.inputs` may say "behind" for nothing.

## Work
1. Find the entry: where the atlas meta writes an unowned module (`pipeline/atlas/inputs.py`, `pipeline/_trace.py`).
2. Record a repo-relative path or the content hash only; the check compares that.

## Acceptance
The atlas record is the same on two machines for the same inputs; the check agrees. Severity: low. Model: Sonnet. Size: S.
