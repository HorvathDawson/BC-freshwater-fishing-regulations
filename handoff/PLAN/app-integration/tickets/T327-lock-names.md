# T327 — Lock names: owner name decisions out of code into a curated file

Status: DONE (0161c9fc6, 2026-10-30, branch cleanup/t327-t331) — was OPEN (owner 2026-10-30; T324 design approved, plan in
handoff/PLAN/data-flow.md §4–5).

Move `names.OWNER` (pipeline/regs/review/names.py) into `waters/mined_names.json` (or its own curated file);
write Sheldens Creek (accepted, never written). Code reads the file only. Depends: T325. Size: S.

## Done
- Own curated file, not rows in `mined_names.json`: `data/curated/waters/name_calls.json` (`name-calls/1`, AUTHORED),
  declared as `curated.waters.name_calls` (config.yaml + `Waters` model). `mined_names.json` stays the deterministic
  output of `python -m pipeline.tools.mined_names` (candidates + owner calls + name marks), so a rerun cannot drop an
  owner row.
- The 7 owner calls moved verbatim (call / why / name / display, by candidate id). `names.OWNER` is gone; the one
  reader is `names.owner_calls()` (validates format, `call` in accept | hold, a `why`). Callers: `names` itself,
  `tools.mined_names`, the review app's `variants.py`. GOTCHAS' Sheldens entry points at the file.
- Sheldens Creek: already in `mined_names.json` since f0b92de23 (T306 commit, `accepted: owner 2026-10-29 …`).
  It reaches the registry on the next atlas build (the build pass r-51d80e68f056 predates it). OWNER-TODO item updated.
- Proof the registry is unchanged: `python -m pipeline.tools.mined_names --check` → "mined_names.json is current"
  (byte-identical; the atlas reads only that file). Tests: name_sources_t224, mined_names_are_labels, name_variants,
  names, registry, registry_kinds, licence_records, curated_paths — pass (curated_paths' one atlas-needing test
  errors in a worktree with no atlas, as before).
- Not done here: the lock-tool convention (T325) — names has no marks to archive; nothing to close.
