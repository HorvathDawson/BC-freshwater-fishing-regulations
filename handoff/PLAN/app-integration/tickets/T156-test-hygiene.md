# T156 — Test hygiene: the update-hydat sandbox and the _plat.ts race

Status: DONE — 187290e2 (lane-T157)

Source: merges 2026-10-23 (T145 review; batch 6 lanes).

## Work
1. pipeline/tests/test_update_hydat_script.py fails in every checkout: its sandbox has no data/release, so the script
   stops at "no release under data/release; nothing to upload" (3 tests). Give the sandbox a stub release (or make the
   upload step optional in the script's dry run) and keep the order assertion.
2. app check-colours.test scans packages/ui-native/src while check-boundaries / no-localhost tests create and delete a
   temporary _plat.ts there; a parallel run can fail with ENOENT. Write the temporary file outside the scanned tree (or
   serialise those files).
Size: S.

## As built
1. **The sandbox has real releases.** The script's step 5 asks `pipeline.deliver.release.manifest.latest` for THE
   release on disk (T133), and `latest` counts only an `r-*` folder holding a `manifest.json`; the sandbox's folders
   were empty, and the stub python answered the `-c` lookup with nothing. Now `r-new` and `r-old` each hold a
   `manifest.json` (r-old's dated older, as T68 intended), and the stub hands the lookup to the REAL `manifest.latest`
   (`PYTHONPATH=$REPO exec $REAL_PY`), so "the newest by time" is the real rule, not a stub's. The order assertion is
   kept whole; it now names the lookup (`|-c <release>`) between the build and the plan. The script is unchanged.
2. **Fixtures outside the tree.** `tools/check-boundaries.mjs` and `tools/check-platform.mjs` take `CHECK_ROOT`
   (default: the app). `check-boundaries.test.ts` builds a scratch tree per case under the OS temp dir (`inScratch`:
   the app's `layers.json` + the fixture) and runs the real gate on it; nothing is written under `packages/` any more,
   so check-colours / one-speaker / no-localhost walks cannot meet a half-deleted `_plat*` / `_bnd*`. Each gate's
   "passes on a clean tree" still runs on the real tree. (no-localhost keeps its ENOENT tolerance, comment updated.)

## Tests (mutation → test that fails; each run)
- r-new's manifest dated older instead of r-old's → `test_a_new_hydat_reruns_every_consumer…`,
  `test_upload_sends_the_planned_release_once…`.
- `check-platform.mjs` ignoring `CHECK_ROOT` → 8 cases fail, incl. the new "reach the gate: each gate reports a scratch
  tree's violation under its own path" (whole first stderr line pinned for both gates).
- `inScratch` making its tree under the app → "are written outside the app tree, where no other suite walks".

## Checks
- `pytest pipeline/tests/test_update_hydat_script.py` (unfiltered): 8 passed (was 3 failed).
- `vitest tools/check-boundaries.test.ts`: 29 passed. Full `pnpm -C app check`: see T157.

## Rebuild
None.

## Left / follow-ups
- none.
