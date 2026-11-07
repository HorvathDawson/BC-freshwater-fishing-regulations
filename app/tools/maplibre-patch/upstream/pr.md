<!-- DRAFT PR description for maplibre/maplibre-native, not posted. MapLibre's AI policy strongly recommends writing the
     description yourself: use this as the factual base. Replace <issue> with the issue number. -->

Fixes #<issue>.

## What

`SourceFeatureState::coalesceChanges` (called from `RenderTileSource::prepare` every frame) gave every rendered tile the
whole stored feature state on every frame, since #3406. Now a tile gets the whole state once, when it has new buckets,
and after that only the changes of the frames in which something changed.

- `SourceFeatureState` keeps a version, bumped in each frame with changes.
- `Tile::featureStateVersion` is the version a tile's buckets hold; `GeometryTile::onLayout` resets it to 0 (new buckets).
- Per tile: up to date → nothing; one version behind in a frame with changes → this frame's changes; otherwise (new
  buckets, or the tile was not rendered in a frame with changes) → the whole state.
- A removal now merges into the same frame's changes instead of replacing that source layer's changes (#4835 has the same
  one-line change). Before, the per-frame full re-apply hid this a frame later; with changes only it would be lost.

## Why

The per-frame cost grew with every id ever given a state × rendered tiles × layers on the source layer, also in frames
with no change. In our Android app (Vulkan, line layers coloured by feature state) the render thread was CPU-bound
while panning: ~10 fps on a Pixel 9, and 33–44 % of the samples in this path on an emulator profile. Details in the issue.

#3406's case keeps working: a tile laid out after the state was set has version 0, so it gets the whole state.

## Tests

macOS, Metal, Release, main d1b09a89:

- New unit test `SourceFeatureState.CoalesceChanges` (stub tiles that record what they are given): nothing is applied
  again in a frame without changes; a newly rendered tile, and a tile that missed a change, get the whole state; an
  up-to-date tile gets only the change; a removal and a change in one frame both arrive. Fails on main (a frame without
  changes applies the state again), passes with the fix.
- New render test `feature-state/vector-source-relayout`: the state is set and drawn, then a filter change lays the same
  tiles out again; the new buckets must show the state. Passes on main and with the fix; fails if the fix leaves out
  the reset in `GeometryTile::onLayout` (checked).
- Existing: `-f feature-state/` render and query tests, and unit tests `Query.*`, `*FeatureState*`, `GeoJSON*`,
  `Source.*`, `VectorTile*`, `GeometryTile*`, `CustomGeometryTile*`, `*Bucket*`: with the fix (and with the follow-up
  PR on top) 19 render/query tests pass, 6 ignored by the platform's ignore lists, 0 fail; 69 unit tests pass.

Not tested: Android/iOS device tests, other backends (the change is in core, no backend code), the full render suite.
No test measures the frame cost itself; the unit test counts what each tile is given.

## Changelog

Root `CHANGELOG.md`, `main` → Bug fixes:
`- [core] Apply feature state to a tile once, then only its changes, instead of re-applying the whole stored state to every rendered tile on every frame.`
Android and iOS `CHANGELOG.md`, `main`:
`- Feature state no longer costs render-thread time on every frame: a tile gets the stored state once and then only changes.`

## AI disclosure

Claude (Anthropic) in Claude Code read the source, wrote the change, the tests and a first draft of this text, and ran
the tests. I reviewed the code and the results.
