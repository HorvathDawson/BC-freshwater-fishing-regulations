# DRAFT upstream issue (not filed)

Repo: maplibre/maplibre-native. Owner decides whether and when to file; attach `feature-state-deltas.patch` as a PR.

---

**Title:** Feature state is re-applied to every rendered tile on every frame (cost grows with every id ever set)

**Version:** android-v13.6.1; the same line is on `main` at d1b09a89 (2026-11-06). Introduced by #3406 (merged 2025-06-05, b81de155).

**What happens**

`RenderTileSource::prepare` runs once per frame for each tile source and ends with
`featureState.coalesceChanges(*tiles)` (`src/mln/renderer/sources/render_tile_source.cpp:477`).

`SourceFeatureState::coalesceChanges` (`src/mln/renderer/source_state.cpp:45-109`) builds this frame's `changes` and then,
since #3406, does

```cpp
changes.insert(currentStates.begin(), currentStates.end());   // :100
...
for (auto& tile : tiles) {
    tile.setFeatureState(changes);                              // :107
}
```

so on every frame, even one with no change at all, it copies the whole stored state and hands it to every rendered tile.

`GeometryTile::setFeatureState` (`src/mln/tile/geometry_tile.cpp:594-619`) then calls `bucket->update(...)` for every
layer on that source layer (`:615`). Each data-driven binder looks up every stored id in its `featureMap`
(`src/mln/renderer/paint_property_binder.hpp:319`, `:428`) and re-evaluates the expression for each id it holds.
`PaintPropertyBinders::updateVertexVectors` marks the shared vertex vector modified unconditionally
(`paint_property_binder.hpp:732`, `updateModified(true)`), and the buckets set `uploaded = false`
(`line_bucket.cpp:173-174`, likewise circle, fill, fill-extrusion, symbol). On Vulkan a vertex vector modified after
its buffer was made gets a new buffer resource (`src/mln/vulkan/upload_pass.cpp:78-90`).

The cost per frame is therefore roughly: ids ever given a state × rendered tiles × layers on that source layer ×
feature-state binders, plus a re-upload of every such layer's attribute buffer. It never goes down: removing a state
leaves an empty entry in `currentStates` (`source_state.cpp:76-90`).

**Seen in:** an Android app (React Native, Vulkan) that colours many line features (stream sections) with feature
state; map panning lags on a Pixel 9. Before/after frame timings: TO ADD after our A/B of the attached patch.

**Why #3406 added the merge**

Before #3406 a tile was only ever given the deltas of the frame in which a state changed. A tile that had not been
laid out yet (or was laid out again, or was not rendered that frame) never saw states set earlier, so features in
newly loaded tiles showed no state (the `feature-state/vector-source` render test sets the state before the tiles
load). Re-applying everything every frame fixed that, at the cost above.

**Proposed fix** (patch attached; 12 source files, +75 −24, plus 3 render tests)

1. Each `Tile` records the source's feature-state version its buckets reflect; `GeometryTile::onLayout` clears it
   (new layout result = new buckets with no state).
2. `coalesceChanges` bumps the version only in a frame with changes. Per tile: up to date → nothing; exactly one version
   behind and this frame has changes → the deltas only; otherwise (new buckets, or the tile missed a version while not
   rendered) → the full `currentStates` once. No copy of `currentStates` into `changes`.
3. Deletions are merged into the same frame's `changes` (before, a deletion on a source layer overwrote that layer's
   updates from the same frame; the per-frame full re-apply used to hide that a frame later).
4. `updateVertexVectors` returns whether any vertex was rewritten; binders' buffers are marked modified and buckets
   re-uploaded only then.

The #3406 case (state set before the tile exists) keeps working: a new tile has no version, so it gets the full state on
its first frame. New render tests: `feature-state/update-after-render` (a second change after the first was drawn
reaches the tile as a delta), `feature-state/remove-key-after-render` (removing one key reaches it as a delta),
`feature-state/vector-source-new-tiles` (tiles created after the state was set and drawn, by zooming out of
overscaled tiles, get the full state). Each reuses an existing expected image.
