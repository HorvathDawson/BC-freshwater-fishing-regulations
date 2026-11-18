# T831 — The route map inside a water's Conditions view draws no gauge dots and no "you are here" pin on the phones

Status: OPEN (app chat; feature-sized).

Source: found by the T826 agent 2026-11-17 while checking the owner's iPhone report (T826 part 1), on both Android
and iOS; the web draws them.

The route map's key names gauge dots and the "you are here" pin, but the native map ignores `trace`
(`app/packages/map/src/Map.native.tsx:115`, `trace: _noTrace`). Build: carry the trace layers into the native map as
the web adapter does, with an adapter-contract test, and prove it on both phones (shots beside the web).

## Done (branch worktree-agent-adb875be91fae4165, 0.8.1 lane B, 2026-11-18)

Cause: `Map.native.tsx` destructured `trace: _noTrace` and never wrote it. The phone's style already carried everything
else (`runtimeStyle` → `traceSources()` + `traceLayers()`; the pin and pill images in `nativeImages`), so the
`trace-pins` source simply stayed the style's empty collection. A second trap behind it: the binding's `GeoJSONSource`
ADOPTS a source the style declares and never pushes the data it was mounted with (`MLRNSource.addToMap`, Android and
iOS) — only a later `data` change reaches it — so a plain `<GeoJSONSource id="trace-pins" data=…>` would still have
shown nothing when a route map opens.

Fix (the `gauges` pattern): `nativeStyle` takes `tracePins` and writes `pinCollection(pins)` (the web `setTrace`'s
collection) into the style's `trace-pins` source; `Map.native.tsx` reads the pins through a ref (no style swap per
horizon change) and mounts `<GeoJSONSource id="trace-pins">` for later changes. Web untouched.

Tests: `adapter-contract.render.test.tsx` "T831 …" (web + native: opens with every dot and the pin, a share change in
place without a style swap, no route map no pins; native fails with the fix reverted), `native-style.test.ts` "(T831) …"
(source, the five pin layers, the themed pin image, the baked collection); `map-props-gate.test.ts` no longer lists
`trace` as a phone gap. Test bed: `route-scroll` step "T831: the route map draws the 'you are here' pin and the gauge
dots" (accent pixels in the map + shot `T831-route-pins.png` to set beside the web) — not run.

## Not done

- The route LINES (`trace.routes`: solid down / dashed up, arrows, confluence rings) are still not drawn on a phone.
  `traceOverlay` reads the atlas by HTTP range through `pmtiles`' `FetchSource`; on a phone the atlas is a `file://`
  archive, and RN's `fetch` of a `file://` URL ignores `Range` and reads the WHOLE file (or fails on Android). Needs a
  file-range `TileSource` (expo-file-system `File.open()` + `offset` + `readBytesSync`, as
  `packages/data/src/release/drivers/install.native.ts` `readAt`) handed to `traceOverlay(url, routes, source)`, and the
  `trace` source baked like the pins. `packages/map` may not import expo-file-system today (`layers.json`), so it is
  either a layers.json/deps decision (owner) or a reader injected from `ui-native`/the app.
- Phone proof (shots beside the web, both platforms).
