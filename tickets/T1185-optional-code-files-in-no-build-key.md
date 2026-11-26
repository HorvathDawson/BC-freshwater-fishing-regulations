# T1185 — Optional code files that no build key covers

Status: OPEN — a #286 follow-up; scope to confirm

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1185 | Optional code files that no build key covers | OPEN — a #286 follow-up; scope to confirm | — | a lane: list the code files a stage may read without importing them; key each or prove it irrelevant (scope to confirm) |

Source: the planning chat's follow-ups after #286, 2026-11-26. **Scope to confirm:** the sources give this line only. Read as: a stage's key holds only what it
imports ("Only what a stage imports", owner 2026-11-26), so a code file read optionally (a lazy import, a file loaded by path) is in no key.

## Work
1. List the optional reads per stage (the trace in `pipeline/_trace.py` sees imports only).
2. Key each by its file bytes (as an unowned module already is) or record why it cannot change the output.

## Acceptance
A test that edits only such a file and sees the stage rerun. Severity: medium (a stale stage could ship). Model: Opus. Size: S-M.
