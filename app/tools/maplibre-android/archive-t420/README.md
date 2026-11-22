# ARCHIVE: the T420 MapLibre patches (not used by any build)

Kept for history since T978 (2026-11-22). Every Android build now ships the pinned upstream commit with no patch
(`../pin.env`, `../README.md`). Nothing reads this folder: `../build.sh` applies no patch and refuses a source tree with
local changes, and the builds refuse any AAR that is not the pinned version.

- `README-t420.md`: the patch's own README (the defect, the fix, how it was built on android-v13.6.1).
- `feature-state-deltas.patch` (full, `13.6.1-fsdelta`), `feature-state-deltas-lean.patch` (lean = 0001 + 0002,
  `13.6.1-fslean`: every Android build carried it from the owner's 2026-11-09 rule, production from 0.8.2 (T893),
  until T978).
- `upstream/`: 0001 went upstream as maplibre/maplibre-native#4919 (merged 2026-11-20, 9cf8446d); 0002 measured within
  noise on main (T978) and was not posted. A port of 0002 to main 3a6b955 was made for that measurement only.
- `tests.sh`: the upstream feature-state render tests on this Mac, for the patched 13.6.1 tree.
