# T831 — The route map inside a water's Conditions view draws no gauge dots and no "you are here" pin on the phones

Status: IN PROGRESS — fix merged: 57d816fdb (gauge dots and the you-are-here pin), d7af5c02c (lines through an injected atlas reader), 278bcfb17; iPhone proof done (lane L6, 3e8189d79: route lines, gauge dots and the pin, light and dark); left (Claude): the Android half, route-scroll's T831 step on the emulator, shots beside the web

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T831 | The route map inside a water's Conditions view draws no gauge dots and no "you are here" pin on the phones | IN PROGRESS — fix merged; iPhone proof done (lane L6: route lines, gauge dots and the pin, light and dark); left (Claude): the Android route-scroll step and shots beside the web | 57d816fdb, d7af5c02c, 278bcfb17, 2f4b492aa | T826 agent 2026-11-17 |

Source: found by the T826 agent 2026-11-17 while checking the owner's iPhone report (T826 part 1), on both Android
and iOS; the web draws them.

## What
The route map's key names gauge dots and the "you are here" pin, but the native map ignored `trace`
(`app/packages/map/src/Map.native.tsx:115`, `trace: _noTrace`). Carry the trace layers into the native map as the web
adapter does, with an adapter-contract test, and prove it on both phones (shots beside the web).

Done (fix merged, commits in the row): pins and lines into each native style (`nativeStyle` `tracePins` / `traceLines`);
`<GeoJSONSource id="trace">` / `id="trace-pins"` handed their data only once a style has loaded (the binding ADOPTS a
source the style declares and never pushes its mount data, `MLRNSource.addToMap`); a byte-range reader injected
(`@app/map` `ReadRange` + `TileEndpoints.readRange`; `@app/data/release/platform` `atlasRange`, `range.native.ts`
expo-file-system `File.open`; RN's `fetch` of a `file://` archive ignores `Range`); a phone with no reader draws no lines and
reports why. Tests: `adapter-contract.render.test.tsx`, `trace-geometry.test.ts`, `native-style.test.ts`,
`range.native.test.ts` ("T831 …"). Proofs:
- Android (L4, 2026-11-20, `route-scroll`, build of 124d79efa; the step now reaches the map under the chart, T920,
  `fromTop`): Coquitlam River PASS, "1269 accent pixels in the map (the pin)", ten fling rounds never blank. Picture:
  `docs/app-integration/L4-android-shots-2026-11-20/T831/T831-route-pins.png`.
- iPhone (lane L6, 3e8189d79, `scenarios-ios/l6-shots.mjs` `t831-ios`, pixels counted inside the `donor-route-map`
  frame): 2 of 2 PASS, light 1,751 / dark 1,619. Shots: `docs/ios/shots/t831/ios-route-light.jpg`,
  `docs/ios/shots/t831/ios-route-dark.jpg`.

## Work (left)
- The shots beside the web's route map (iOS and the web set beside Android were not part of the Android pass), and the
  owner's look below.

## Owner

- [ ] 2026-11-20 — Look at the route map in a water's Conditions view (`docs/app-integration/L4-android-shots-2026-11-20/T831/T831-route-pins.png`): gauge dots and pins.
