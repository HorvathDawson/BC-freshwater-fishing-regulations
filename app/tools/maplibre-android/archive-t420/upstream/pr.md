<!-- DRAFT PR description for maplibre/maplibre-native, not posted. No separate issue. -->

Since #3406, `SourceFeatureState::coalesceChanges` hands the whole stored feature state to every rendered tile on every
frame, even when nothing changed. With this PR a tile gets the full state once (when it has new buckets) and after that
only the changes.

How it works:

- `SourceFeatureState` keeps a version that goes up in each frame that has changes.
- Each tile remembers the version its buckets hold. `GeometryTile::onLayout` resets it to 0, so a re-laid-out tile gets
  the full state again. That keeps the #3406 case working.
- A tile that is one version behind gets just this frame's changes. Anything else gets the full state.
- A removal now merges into the frame's changes instead of replacing them. #4835 makes the same one-line change. The
  old full re-apply used to cover for this one frame later; with changes only, the removal would get lost.

## Why

I hit this in my app: an Android app on Vulkan with line layers coloured by feature state. Panning got slower the
longer I used it, down to about 10 fps on my Pixel 9. The render thread was the bottleneck, and in profiles a third or
more of it was this path (`coalesceChanges` → `LineBucket::update` → `updateVertexVectors`). The per-frame cost grows
with every id that has ever had a state × rendered tiles × layers on the source layer, and removing states doesn't bring
it back down.

I built the app against this patch and tested it on my phone. Panning is smooth now and stays smooth over a long
session.

## Tests

On macOS (Metal, Release), rebased on main ce1d31a61:

- New unit test `SourceFeatureState.CoalesceChanges` checks what each tile is given. It fails on main and passes with
  the fix.
- New render test `feature-state/vector-source-relayout`: it sets a state, then lays the tiles out again, and the new
  buckets still have to show the state. It passes on main and with the fix, and fails if the `onLayout` reset is left
  out.
- The existing `feature-state/` render and query tests and the related unit tests still pass. pre-commit passes.

I haven't run the full render suite or the device test suites, and I haven't tried other backends. The change is in
core only.

Made with help from Claude Code. I reviewed the change and tested it myself on my Pixel 9, where it helped a lot.
