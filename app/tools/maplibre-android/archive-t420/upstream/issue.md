<!-- DRAFT for maplibre/maplibre-native, not filed. MapLibre's AI policy asks that issues are not AI-generated text posted
     as your own: rewrite this in your own words before filing; the facts and links below are checked against main d1b09a89. -->

**Title:** Feature state is re-applied to every rendered tile on every frame

**Version:** `main` at d1b09a89 (2026-11-07); android-v13.6.1. The behaviour came with #3406 (b81de155).

### What happens

`RenderTileSource::prepare` runs every frame for each tile source and calls `featureState.coalesceChanges(*tiles)`
([render_tile_source.cpp#L477](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/renderer/sources/render_tile_source.cpp#L477)).
`coalesceChanges` merges the whole stored state into this frame's changes and hands it to every rendered tile, also in a
frame in which nothing changed
([source_state.cpp#L100-L108](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/renderer/source_state.cpp#L100-L108)):

```cpp
changes.insert(currentStates.begin(), currentStates.end());
...
for (auto& tile : tiles) {
    tile.setFeatureState(changes);
}
```

Each tile then updates the bucket of every layer on that source layer
([geometry_tile.cpp#L625](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/tile/geometry_tile.cpp#L625)):
each data-driven binder looks up every stored id and re-evaluates its expression for the ids it holds
([paint_property_binder.hpp#L352-L368](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/renderer/paint_property_binder.hpp#L352-L368)),
and the vertex buffer is marked modified even when no id matched
([paint_property_binder.hpp#L768-L773](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/renderer/paint_property_binder.hpp#L768-L773),
[line_bucket.cpp#L184-L189](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/renderer/buckets/line_bucket.cpp#L184-L189),
likewise circle, fill, fill-extrusion, symbol). On Vulkan a vertex vector modified after upload gets a new buffer
([vulkan/upload_pass.cpp#L77-L95](https://github.com/maplibre/maplibre-native/blob/d1b09a891050bf9eadca9e5fc7c71fb70d7b002c/src/mln/vulkan/upload_pass.cpp#L77-L95)).

So the render thread's cost per frame is about: ids ever given a state × rendered tiles × layers on that source layer ×
feature-state binders, plus re-uploads. It does not go down when states are removed: a removal leaves an empty entry in
`currentStates`.

Before #3406 a tile got only the changes of the frame in which they were made, so a tile laid out later never showed
them (#3400). The merge fixed that by re-applying everything every frame.

### Impact (our app)

React Native app on Android (Vulkan), line features coloured with feature state (ids are added as the user pans; 9
data-driven paint properties on the line layers that read it):

- Pixel 9, system trace while panning: the map presented 113 frames in ~11 s (~10 fps); the render thread ran 915–1010
  ms of every second; GPU ~1 ms per frame. CPU-bound on the render thread.
- Stressed Android emulator, `simpleperf`: 33–44 % of the app's samples are this path
  (`coalesceChanges` → `LineBucket::update` → `updateVertexVectors`); frame times grow over a session as more ids hold state
  (one run: p50 99 → 148 ms after 160 pans in one place).

### Proposed fix

A tile gets the whole state once when it has new buckets, and only the changes after. PR: (link)
