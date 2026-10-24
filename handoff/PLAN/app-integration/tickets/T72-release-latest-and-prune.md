# T72 — Release LATEST pointer and pruning old releases (audit N10, wave 1.15, owner call 8)

Status: DONE — 0eb70144. See Outcome.

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

## Outcome (2026-10-24)
- `pipeline/deliver/release/build.py` writes `data/release/LATEST` after a release (fixtures call `ship()` and do
  not); `manifest.latest()` reads it first (a pointer to a folder with no manifest is refused) and falls back to the
  newest manifest mtime only where no pointer exists; `manifest.releases()` is the one newest-first order.
- `serve-tiles.mjs` reads LATEST before mtime; `publish.py` defaults to `latest()` and, after an upload, writes
  `data/release/PUBLISHED`.
- `python -m pipeline.tools.prune_releases [--keep id] [--yes]`: dry run by default; keeps LATEST, PUBLISHED, the
  newest other release and any `--keep`.
- Pinned by `pipeline/tests/test_release_latest.py` (a touched older release is never picked; prune keeps/removes).
- Owner 2026-10-24 ("we can delete old releases"; Q7 answered): pruned 22 of 24 by hand — kept r-46595c88e76e (live,
  LATEST) and r-9cf1fef180a8. No test or tool read an old release folder (references were comments / labels / a
  frozen fixture JSON). :39217 still serves the live manifest.
- Seen, not this ticket's: `test_fixture_release.py::test_the_bundle_half_only_paired_to_one_derived_handle_table`
  fails on the branch head without this change (expects no `access` file) → T63's test cleanup.
