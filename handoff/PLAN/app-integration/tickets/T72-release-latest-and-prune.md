# T72 — Release LATEST pointer and pruning old releases (audit N10, wave 1.15, owner call 8)

Status: OPEN.

Source: `pipeline/tools/publish.py` and `serve-tiles.mjs` pick the newest `data/release/r-*` by mtime; there is no
`data/release/LATEST`; 10 `r-*` folders on disk; implement_progress: "Old releases kept until N10".

## Work
- deliver writes `data/release/LATEST`; publish and serve-tiles read it (or publish requires `--release`).
- A prune command, dry-run by default, keeping: the published release, LATEST, one previous, and anything the dev
  fixture, server or vintage check points at.

## Acceptance
A test shows a touched older release is never picked. The prune dry-run list is shown, approved, then applied by hand
(never pipe `rtk ls` into `rm`). The live release and served tiles are untouched.

Depends: after T56 settles which release is live. Size: S.
Owner: approve the prune list. Default: keep live + LATEST + previous.
