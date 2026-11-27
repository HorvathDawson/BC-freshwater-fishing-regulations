# T1226 — A side build with `--no-tiles` hands the bundle an empty tiles folder, and the failure reads as no message

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1226 | A side build with `--no-tiles` hands the bundle an empty tiles folder; its error lands above buffered stdout | OPEN | — | refuse up front naming the fix (or point at the live tiles); flush stdout before the error |

Source: the data and release chat, 2026-11-26 (a `python -m pipeline build --side DIR --no-tiles` run).

## What was found
- `--side DIR` points the deliver step at `DIR/tiles` (`pipeline/build.py:563`, `--tiles <side>/tiles`), and
  `--no-tiles` skips the tiles stage (`pipeline/build.py:966`, `:1080`), so that folder is empty.
- The bundle step then exits 1: `access: <side>/tiles/atlas.meta.json not found — section access is measured against
  the tiles' polygons` (`pipeline/deliver/bundle/access.py:300-302`). The message names the file, not the fix.
- The message lands at line 3 of `deliver.log`, above the step's block-buffered stdout, so the step reads as dying
  with no message (stdout and stderr out of order in the log). Seen in the run; the ordering itself is not re-run here.

## Work
1. Up front (the build's preflight, `pipeline/build.py` near `:1019`): a side run with `--no-tiles` and no tiles in
   the side folder refuses before any stage runs, naming the two fixes (build the tiles, or run without `--side`).
   Alternative: point the bundle at the live tiles, as the side run already does for the basemap (`build.py:572`);
   pick this only if the live tiles' section handles can match (the access check at `access.py:304` would refuse
   a mismatch anyway).
2. The deliver step flushes stdout before it raises, so the log reads in order.

## Acceptance
A test: `--side` + `--no-tiles` with an empty side tiles folder stops at preflight with a message naming the fix.
A failing bundle step's log shows its message after its own output, not above it.

Depends: none. Size: S. Owner: no.
