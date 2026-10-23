# T156 — Test hygiene: the update-hydat sandbox and the _plat.ts race

Status: OPEN

Source: merges 2026-10-23 (T145 review; batch 6 lanes).

## Work
1. pipeline/tests/test_update_hydat_script.py fails in every checkout: its sandbox has no data/release, so the script
   stops at "no release under data/release; nothing to upload" (3 tests). Give the sandbox a stub release (or make the
   upload step optional in the script's dry run) and keep the order assertion.
2. app check-colours.test scans packages/ui-native/src while check-boundaries / no-localhost tests create and delete a
   temporary _plat.ts there; a parallel run can fail with ENOENT. Write the temporary file outside the scanned tree (or
   serialise those files).
Size: S.
