# Archive: the retired T420 MapLibre patches (not used by any build)

Every Android build ships one pinned upstream MapLibre commit with no patch of ours (`../pin.env`,
`../README.md`). Nothing reads this folder: `../build.sh` applies no patch and refuses a source tree with local
changes, and the builds refuse any AAR that is not the pinned version. It is kept as the record of a defect
and its fix.

The defect: since upstream PR #3406 MapLibre re-applied every stored feature state to every rendered tile on
every frame (about 6 fps on the owner's Pixel 9). The fix tracks a feature-state version per tile and applies
deltas, a full state only to a tile with new buckets.

- `feature-state-deltas.patch`: the first patch against `android-v13.6.1` (built as `13.6.1-fsdelta`).
- `feature-state-deltas-lean.patch`: the lean one, 0001 + 0002 (built as `13.6.1-fslean`).
- `upstream/0001-*.patch` went upstream as maplibre/maplibre-native#4919 (merged 2026-11-20, 9cf8446d), which is
  why the pin is main. `upstream/0002-*.patch` (re-upload a bucket only when a changed feature is in it)
  measured within noise on main and was not posted.
- `tests.sh`: the upstream feature-state render tests on this Mac, for the patched 13.6.1 tree.
