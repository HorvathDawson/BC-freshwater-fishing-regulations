# T1183 — The tile assets folder is missing from a build key

Status: OPEN — a #286 follow-up; scope to confirm

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1183 | The tile assets folder is missing from a build key | OPEN — a #286 follow-up; scope to confirm | — | a lane: find the tiles stage's input folder that no key hashes; add it; a test that changes only it and sees the stage rerun (scope to confirm) |

Source: the planning chat's list of follow-ups after PR #286 (T1171), 2026-11-26 (day notes). **Scope to confirm:** the sources give this line only. #286's
third review found the tiles key missed the map style and the `pmtiles` CLI (fixed in #286); this is a further folder of tile assets that no stage key covers.

## Work
1. Name the tile assets folder and which stage reads it (the tiles stage; `pipeline` keys in `pipeline/build.py` and the T1171 key code).
2. Add it to that stage's key; a mutation test that changes only it.

## Acceptance
Editing only that folder makes the tiles stage rerun. Severity: medium (stale tiles pass as fresh). Model: Sonnet. Size: S.
