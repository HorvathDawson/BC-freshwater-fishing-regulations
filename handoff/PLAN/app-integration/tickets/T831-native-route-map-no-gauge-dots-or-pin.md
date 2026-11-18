# T831 — The route map inside a water's Conditions view draws no gauge dots and no "you are here" pin on the phones

Status: OPEN (app chat; feature-sized).

Source: found by the T826 agent 2026-11-17 while checking the owner's iPhone report (T826 part 1), on both Android
and iOS; the web draws them.

The route map's key names gauge dots and the "you are here" pin, but the native map ignores `trace`
(`app/packages/map/src/Map.native.tsx:115`, `trace: _noTrace`). Build: carry the trace layers into the native map as
the web adapter does, with an adapter-contract test, and prove it on both phones (shots beside the web).

## Done (branch worktree-agent-adb875be91fae4165, 0.8.1 lane B, 2026-11-18)

Cause: `Map.native.tsx` destructured `trace: _noTrace` and never wrote it. The phone's style already carried everything
else (`runtimeStyle` → `traceSources()` + `traceLayers()`; the pin and pill images in `nativeImages`), so the `trace`
and `trace-pins` sources stayed the style's empty collections. Two traps behind it:
- the binding's `GeoJSONSource` ADOPTS a source the style declares and never pushes the data it was mounted with
  (`MLRNSource.addToMap`, Android and iOS) — only a later `data` change reaches it;
- the lines are copied out of the atlas by byte range (`traceOverlay`, `pmtiles`), and a phone's atlas is an installed
  `file://` archive: RN's `fetch` of it ignores `Range` and reads the whole file (or fails on Android).

Fix:
- Pins and lines into each native style (`nativeStyle` `tracePins` / `traceLines`, read through refs: no style swap per
  horizon change), and `<GeoJSONSource id="trace">` / `id="trace-pins"` handed their data only once a style has loaded,
  so every later change is pushed to the adopted source.
- A byte-range reader injected, not imported (coordinator 11-18: `layers.json` unchanged, `packages/map` free of
  expo-file-system): `@app/map` `ReadRange` + `TileEndpoints.readRange`; `trace-geometry` `rangeSource` hands it to the
  real `PMTiles`; `@app/data/release/platform` `atlasRange` (`range.native.ts`: expo-file-system `File.open`, the
  install's `readAt` pattern; `range.web.ts`: null — the web keeps `pmtiles`' fetch); `apps/mobile` `App.tsx` puts it on
  `tiles`. A phone with no reader draws no lines and reports why (never a fetch of the file).
- The native fit frames the drawn lines too, as the web's `useFit`. Web untouched.

Tests: `adapter-contract.render.test.tsx` "T831 …" (web + native pins; native lines through the injected reader over a
real in-memory `.pmtiles` archive, a theme swap keeps lines and pins in the new style, routes cleared, no reader →
reported and no lines; the bake and the source each fail when reverted); `trace-geometry.test.ts` "(T831) …" (a route's
line out of a `.pmtiles` archive through the reader, only ranges ≤ 16 KiB asked, `rangeSource` hands exact bytes);
`native-style.test.ts` "(T831) …" (pin and line sources, layers, themed pin image, baked collections);
`packages/data` `range.native.test.ts` (range, end of file, past it, handle closed; expo-file-system faked);
`map-props-gate.test.ts` no longer lists `trace` as a phone gap. New test fixture: `pmtiles.fixture.ts` (archive writer).

Test bed (added, not run): `route-scroll` step "T831: the route map draws its lines, the gauge dots and the 'you are
here' pin" — fails with no accent-violet pixels in the map (no pin); keeps `T831-route-pins.png` to set beside the web's
route map: a solid line to each gauge downstream, dashed upstream, arrows along them, a dot per gauge with its share.

## Not done

- Phone proof (the test-bed step on Android and iOS, shots beside the web).
