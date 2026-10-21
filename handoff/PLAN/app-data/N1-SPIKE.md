# N1 — native map spike: results (Android emulator + web; iOS deferred by the owner)

2026-10-20. Ticket: APP-PREP.md "N1 — native map spike". Branch `worktree-agent-a8261f149d43eead6`; the spike code
is in one commit marked **SPIKE-ONLY** (do not merge). Only this file and `n1/` are meant to merge.

> **Every native number here is from the Android EMULATOR on the laptop. It is PROVISIONAL.** The emulator is an
> arm64 guest on an Apple M5 Pro, drawing through the host GPU. It is not a phone: its CPU is faster than a mid
> phone's, and its GPU path (GLES or Vulkan translated onto Metal) is neither a phone's GPU nor a phone's driver.
> iOS was deferred by the owner (2026-10-20). No physical device was used.

## Verdict

| question | answer |
|---|---|
| Binding | **`@maplibre/maplibre-react-native` 11.5.0** (published 2026-10-04): MapLibre Native **Android 13.6.1**, **iOS 6.31.0** (SPM). Expo config plugin included. Same JS on both platforms. |
| 1. Local `pmtiles://` file, our style | **YES.** `atlas.pmtiles` (531 MB) + `basemap.pmtiles` (918 MB) read from the app's `documentDirectory` as `pmtiles://file:///data/user/0/<app>/files/atlas.pmtiles`, with the self-hosted glyphs and sprites (`map-assets-1`) also from `file://`. Fully offline. `runtimeStyle()` used unchanged; only the URLs differ. |
| 2. Feature-state | **GO, with one tile change.** The binding has `setFeatureState`/`getFeatureState`/`removeFeatureState`, and they colour the map. **But MapLibre Native does not apply the style's `promoteId`** (neither our per-source-layer object nor a plain string): every feature's id is empty, so feature-state addresses nothing. With `section_id` written as the **MVT feature id** in the tiles (`tippecanoe --use-attribute-for-id=section_id`, proven on an extract), feature-state works. **Fallback, also proven:** a data-driven `match` on `["get","section_id"]` set as the layer's colour, applied by a style swap. |
| 3. Tap → `section_id`, 24 px box | **YES.** `queryRenderedFeatures` in a 24 px box: median 0.5–0.8 ms, p95 ≤ 1.8 ms. Real taps (`adb input tap`) through the binding's `onPress`: press event → id in **18–26 ms**. **The id arrives as a STRING on Android** (`"311274"`): the binding serialises `feature.id()` with `putString`. N2 must `Number()` it. |
| 4. Pan frame time z9–14, coloured | **Vulkan renderer: 51–60 fps at every zoom** (frame interval median 16.7 ms, p95 17–20 ms at most zooms). **OpenGL ES renderer (the binding's default): 21–35 fps at z9–11**, 44–57 fps at z12–14. Colouring costs nothing measurable. Choose the Vulkan variant (`app.json` plugin option, below). |
| Fallback: GL JS in a WebView | Works, 60 fps on the emulator (p95 19 ms), and is NOT needed: the native binding works and keeps native gestures. |
| Device factor (emulator ÷ laptop, A1–A11) | **≈ 2.5–3×** for the keyed SQLite reads, **≈ 1×** for the scanning search, **≈ 6×** for the paint decode (JS), and **≈ 55×** for the 5,000-sid JS lookup loop (Hermes bytecode with no JIT, against V8's JIT). Emulator, so provisional; a mid phone will be slower again. |

## 1. Rendering from a local file

- The `<Map>` adapter contract is kept: `Map.native.tsx` builds the same `runtimeStyle(at, theme, modes)` and runs the
  same adapter (`nativeAdapter()` = `baseAdapter("native")`). The adapter's `setPaint`/`setVisibility`/`setZoomRange`
  write into a clone of the style JSON, which is handed to the native `Map` as `mapStyle`. The binding stringifies it.
  Feature-state goes through `<VectorSource id="atlas" ref>`, which attaches to the style's existing `atlas` source by
  id.
- Sources: `atlas` = `pmtiles://file:///data/user/0/ca.canifishthis.app/files/atlas.pmtiles` (tile version
  `d768d6e135e62b99.ffac7c1eb56e`, release `r-6960fe8201e3`); `basemap` = the same pattern; `glyphs` =
  `file://…/map-assets/fonts/{fontstack}/{range}.pbf`; `sprite` = `file://…/map-assets/sprites/v4/light`. Labels,
  road shields and basemap all render. See `n1/android-native-local-pmtiles-z9-uncoloured.jpg`.
- Map loaded (`onDidFinishLoadingMap`) 260–370 ms after mount.
- The binding mounts its children (the `VectorSource`) only once the map is ready, after the parent's first
  effects. Hold refs, not copies.

## 2. Feature-state, `promoteId`, and the fallback

What was measured (Lower Mainland, z9 start, 83 sections visible):

| variant | ids on features | colours? | cost |
|---|---|---|---|
| atlas as built, `promoteId` object (as `runtimeStyle` emits it) | `""` on all 98 rendered features | **no** | — |
| atlas as built, `promoteId: "section_id"` string | `""` | **no** | — |
| extract with MVT ids (`--use-attribute-for-id=section_id`), feature-state | `"302029"`… on 98 of 98 | **yes** (`n1/android-native-vulkan-featurestate-z12.jpg`, the same picture as web) | 10,000 `setFeatureState` calls (5,000 ids × stream + lake): dispatched in 430–446 ms, all resolved in 930–965 ms ≈ **0.1 ms per call**. The ~100–300 sids a phone viewport really shows cost 10–60 ms. |
| atlas as built, `match` on `["get","section_id"]`, style swap | n/a (no id needed) | **yes** (`n1/android-native-match-fallback-z12.jpg`) | 83 ids: colour → fully rendered **225 ms** (a whole style swap) |
| `match` through a `<Layer id="stream" paint>` child | — | **no**: MapLibre logs `Error setting property: line-color [2]: Expected string but found number instead` (the binding's layer-property path mistypes numeric `match` labels) | — |

**Decision:**
- **Primary:** feature-state, as on web, once the tiles carry the section handle as the MVT feature id. That is a
  P18 (tile step) change: write the id-bearing layers (`stream`, `lake`, `lake_flowing`, `wetland`,
  `wetland_flowing`) with `section_id` as the MVT id. MVT ids are unsigned integers, so this works for the section
  layers. The area layers (`park`, `no_access`, `basin`, `region`, `mu`, `contour`) promote STRING or other properties
  (`area_id`, `basin_id`, …), so they need numeric ids or keep property-based styling. Once a layer carries a real id,
  drop its `promoteId` entry (GL JS reads the MVT id directly). Note: tippecanoe REMOVES the attribute it uses as the
  id from the properties, so nothing may read `properties.section_id` afterwards.
- **Fallback if the tile change is refused:** `match` on `["get","section_id"]`, applied by swapping the style (225 ms
  per recolour; acceptable on a day change, too slow per pan). It does NOT go through `<Layer paint>` (above).
- **Size of the problem is smaller than the ticket assumed.** "5,000 visible sids" does not happen with this atlas.
  A phone viewport renders 72 section ids at z9 (264 in the loaded tiles) and 2–83 at z12–14, because sections are
  long reaches. The 5,000 figure was run as a STRESS test: 5,000 feature-states set (real visible ids padded with
  other section ids) before the pan. It did not move the frame time.

## 3. Taps

- `queryRenderedFeatures([[x-12,y-12],[x+12,y+12]], {layers})`, as `Map.web.tsx` (T = 12), stream layers first,
  then lakes: 60 programmatic boxes give median 0.5–0.8 ms, p95 0.8–1.8 ms, and 27 of 60 hit. Viewport query 4–8 ms.
- Real taps: 8 `adb shell input tap` presses on the Stave Falls reach and the lake, through the binding's `onPress`:
  press event received → id ready **18–26 ms** (two async queries, stream then lake). Ids: `"311274"` (stream) and
  `"1566515"` (lake), strings. Touch → press event latency was not measured: MapLibre's own tap detection sits in
  between.
- **Read `e.nativeEvent` before any `await`.** It is `null` afterwards (measured: `Cannot read property 'lngLat' of
  null`). This bit the spike, and N2 needs the same care.
- Coordinates in the press event and in `queryRenderedFeatures` are in **dp**, not pixels (1080 px ÷ 2.625).

## 4. Frame time while panning (scripted: z9…14, two 2 s linear eases per zoom, Lower Mainland)

Measured from **SurfaceFlinger present timestamps of the map's SurfaceView** (`dumpsys SurfaceFlinger --latency`,
polled), not from the binding. The binding's `onDidFinishRenderingFrame` arrives only 3–4 times a second, so it is
useless as a frame clock. 60 Hz display; "fps" counts presented frames in the pan window.

| renderer / mode | z9 | z10 | z11 | z12 | z13 | z14 | pooled interval med / p95 / max (ms) |
|---|---|---|---|---|---|---|---|
| **Vulkan**, feature-state (3 runs) | 51–57 | 54–57 | 59 | 60 | 60 | 59–60 | 16.7 / 18.5–19.6 / 64–68 |
| Vulkan, no colouring | 57 | 58 | 25 (one tile burst) | 60 | 60 | 60 | 16.7 / 28.6 / 218 |
| OpenGL ES, feature-state | 34–36 | 21–27 | 31–36 | 44–48 | 54–57 | 50–53 | 16.9 / 50–51 / 68–101 |
| OpenGL ES, `match` fallback | 30 | 25 | 33 | 47 | 53 | 52 | 16.9 / 50 / 103 |
| OpenGL ES, no colouring | 33 | 21 | 30 | 46–48 | 45–56 | 49–52 | 17.1 / 50–51 / 117–118 |
| WebView (GL JS 5.24), feature-state | 60 (page `render` events, p95 18–19) | 60 | 60 | 60 | 60 | 60 | presents: 16.7 / 19.1 / 33.5 |

- **Colouring is free** in every renderer: none, feature-state and `match` are within run-to-run noise.
- **The renderer variant matters most.** The binding's default `nativeVariant` is `opengl`; `vulkan` doubled the
  low-zoom frame rate here. The emulator's GLES goes through a GLES→Metal translator, so this ratio may not hold on a
  phone. N2 must A/B both variants on a real mid Android phone. Until then: **`vulkan`**, set through the plugin:
  `["@maplibre/maplibre-react-native", { "android": { "nativeVariant": "vulkan" } }]` (in the spike's `app.json`).
  iOS has no such switch: MapLibre iOS 6.x renders with Metal.
- Recordings: `n1/android-native-vulkan-pan.mp4` (native, Vulkan) and `n1/android-webview-pan.mp4` (WebView). Each is
  16 s, 270 px wide.

## 5. Fallback measured: `Map.web.tsx`'s renderer in a WebView

The page is MapLibre GL JS 5.24 + `pmtiles` JS (range reads from a host server through `adb reverse`) with the SAME
`runtimeStyle()` and the SAME `webAdapter()`. It is not the `Map.web.tsx` component itself: the app's React tree does
not run inside the page. Status values cross the bridge once per recolour (`postMessage` → index lookup →
`injectJavaScript`).

- Loaded in 600–970 ms (over HTTP from the host, not a local file). Status round trip over the bridge: 124–131 ms. Then
  feature-state in-page: 0.3 ms for 166 calls, idle in 21–23 ms.
- Pans at a steady 60 fps on the emulator (above). Tap query in-page: median 0.1 ms.
- Ids are numbers (GL JS honours `promoteId`).
- **Not chosen.** A local file needs a custom pmtiles `Source` inside the WebView (WKWebView/Android WebView cannot
  range-read `file://`), so the page would need a bridge or a local HTTP server. Gestures run inside the web view, so
  they do not integrate with RN/RNGH or the system back gesture. The native binding answers all four questions.
- Gesture FEEL was not judged: the emulator was driven only by `adb input` and scripted cameras, never by a finger.

## 6. Web (the real app: `Map.web.tsx` through react-native-web)

The real `App` (P11 release boot: manifest → paint → Shell, `r-6960fe8201e3`) ran through `expo start --web
--no-dev --minify`, against `app/tools/serve-tiles.mjs` serving the promoted tiles and `data/release` (port 39247;
the default 39217/39218 belonged to other workflows). It was measured in headless Chromium 1234 with ANGLE Metal (the
real GPU, an M5 Pro). Viewport 402×874 @3× (canvas 1206×2262). In-page measurements on the map instance (spike hook
`globalThis.__N1map`). Two runs plus a 4× CPU-throttled run:

| | runs 1–2 | 4× CPU throttle |
|---|---|---|
| status coloured by the app itself | yes (`app_status_states_seen` 72 of 72; `n1/web-chromium-release-boot.jpg`) | yes |
| viewport query | 0.8–1.0 ms (86 features, 72 sids at z9) | 2.5 ms |
| feature-state, 10,000 calls (stress) | 2.8–3.1 ms | 7.8 ms |
| pan z9–14 (120 Hz) | 120 fps; interval median 8.3, p95 8.6–9.3, max ≤ 15.9 ms; 0 frames > 16.7 ms | 115–120 fps; p95 9.5–10.5, max 20–50 ms |
| tap query, 24 px box | median 0.0, p95 0.1 ms | p95 0.5 ms |
| real click (Playwright mouse) → id | 10–38 ms (first click slowest); id is a **number** (`1566515`, `286118`) | 24–54 ms |

Web console: no errors in the release-boot runs. (Earlier runs on the dev bundle path showed 404s for absent feeds.)

## 7. A1–A11 through expo-sqlite, files opened in place from `documentDirectory`

The same function (`apps/mobile/n1spike/bench.ts`, after `app/tools/release-perf.test.ts`) ran in the release APK
(Hermes, expo-sqlite sync JSI calls, `openDatabaseSync(name, {}, documentDirectory)`) and on the laptop (node:sqlite).
Both used the same gunzipped `r-6960fe8201e3` files and the same keys. Median / p95 in ms; emulator = runs 2–3 of 3
(run 1 includes a cold first open).

| # | access | emulator | laptop (same script, same files) | factor (median) | budget (mid phone) |
|---|---|---:|---:|---:|---|
| A1 | sid → water → part | 0.029–0.030 / 0.033–0.035 | 0.012 / 0.015 | 2.5× | < 20 ms ✓ |
| A2 | cell + ladder frame (simplified as in release-perf) | 0.022–0.023 / 0.026 | 0.008 / 0.009 | 2.9× | data ≤ 10 ms ✓ |
| A3 | regs-sid block | 0.015 / 0.018–0.023 | 0.005 / 0.007 | 2.9× | < 20 ms ✓ |
| A4 | paint decode (status half) | **105 / 106–107** | 18–21 / 25–27 | 5.4× | < 150 ms at start ✓ (but ≫ the 30 ms laptop gate) |
| A4 | 5,000 `codeOn` lookups | 0.93 / 1.05–1.11 | 0.015–0.020 / 0.018–0.020 | ≈ 55× (Hermes, no JIT) | < 16 ms ✓ |
| A5 | search (substring) | 1.24–1.26 / 5.17–5.21 | 1.20–1.28 / 5.6–6.0 | 1.0× | < 50 ms ✓ |
| A6 | near a town | 0.042 / 0.047 | 0.0065–0.007 / 0.008 | 6.3× | < 50 ms ✓ |
| A7 | gauge for a section | 0.017–0.018 / 0.019–0.022 | 0.006 / 0.007–0.010 | 3.0× | < 50 ms ✓ |
| A8 | trace down ≤ 5,000 steps | 0.041–0.042 / **41.6–43.7** | 0.018 / 17.0–17.8 | 2.3× (p95 2.4×) | < 50 ms: ✓ on the emulator, **at risk on a phone** (P12's cache stays) |
| A9 | part sids | 0.010–0.011 / 0.022–0.033 | 0.004 / 0.005 | 2.7× | < 20 ms ✓ |
| A10 | licence | inside A2 | — | — | — |
| A11 | open a file in place (+ one read) | 0.09–0.33 per file (first ever open of core: 7.1) | 0.06–0.30 (first: 0.68) | ≈ 1.4× | < 50 ms ✓ |

The laptop column matches MINIMAL-PACKAGING §3 (A1 0.012, A3 0.006, A6 0.010, A7 0.020, A8 p95 15.8). **Device factor
(provisional, emulator):** keyed reads ≈ 2.5–3× (6× for A6), scans ≈ 1×, JS decode ≈ 5–6×, tight JS loops ≈ 50×. A
phone is slower than this emulator, so the budgets should not be relaxed on these numbers. The JS-heavy paint decode
(105 ms here) is the one to watch: it fits the 150 ms start budget on the emulator only.

## 8. G1 gestures: how the binding exposes the switches

`Map` props in 11.5.0 are booleans: `dragPan`, `touchZoom` (pinch), `doubleTapZoom`, `doubleTapHoldZoom`,
`touchRotate`, `touchPitch`, plus `compass`. The spike sets `dragPan`, `touchZoom` and `doubleTapZoom` on, and
`touchRotate={false}`, `touchPitch={false}` and `compass={false}`. That is MAP_GESTURES minus wheel, which native lacks.
`doubleTapHoldZoom` is left at the default for G1 to decide. G1's constant can map 1:1 onto these props in
`Map.native.tsx`. **Tap slop:** the binding exposes no slop setting. MapLibre's own detector decides a tap (the
system touch slop on Android), so the "< 8 px" rule cannot be set on native; it can only be checked.
**react-native-gesture-handler:** not on this branch yet, NOT TESTED. The map is a native view that consumes its own
touches; a `GestureHandlerRootView` at the root should not interfere unless a `GestureDetector` wraps the map canvas.
G1 already keeps the part-swipe off the canvas. The left 24 px back zone matters only on iOS.

## 9. What would differ on iOS (deferred), and what to check there

Same JS binding and the same `Map.native.tsx`; the platform work is a build, not a rewrite. To verify on iOS:
- MapLibre iOS 6.31.0 is fetched through **Swift Package Manager** in the plugin's Podfile hook, so `pod install`
  needs the network.
- Renderer: Metal, with no variant switch.
- `promoteId`: same core, so expect the same gap; the MVT-id tiles fix both platforms.
- Feature id type: the iOS binding may hand back numbers; normalise with `Number()` in one place.
- Press event shape and the nativeEvent-after-await behaviour.
- `documentDirectory` path pattern for `file://`.
- Frame timing needs Instruments: SurfaceFlinger does not exist there.
- The edge-back zone (G1) applies.

## 10. Build notes for N2

- Release builds: `expo prebuild --platform android`, then `gradlew app:assembleRelease
  -PreactNativeArchitectures=arm64-v8a`, through `heavy.sh`: 2.5 min clean, ~15 s incremental. JDK = Android Studio's
  JBR. NDK 27.1 installs itself.
- **Gradle's JS bundle task fingerprints `apps/mobile` only.** An edit only in `packages/*` is NOT rebundled; force it
  with `app:createBundleReleaseJsAndAssets --rerun`. Dev builds with Metro do not have this problem.
- Feature-state is one TurboModule call per (feature, source-layer), ≈ 0.1 ms each. Batch by visible set, not by
  province.
- `onDidFinishRenderingFrame` is not a frame clock (3–4/s).
- The emulator console (`adb emu kill`) hangs on this Mac; `adb shell reboot -p` stops it.

## NOT verified

- **Any physical phone**; any iPhone or iOS simulator (deferred by the owner). Every native number is from the
  emulator.
- OpenGL vs Vulkan on real Android GPUs (the variant choice above is emulator-based).
- Gesture feel by hand (no finger on the emulator; WebView gesture integration was judged on reasoning, not use).
- Touch → `onPress` latency (only press event → id was measured).
- `promoteId` on iOS; feature id type on iOS.
- The full atlas rebuilt with MVT ids (only a Lower Mainland extract of `stream` + `lake` was built and rendered); the
  area layers' id story.
- react-native-gesture-handler coexistence (not on this branch).
- A2 as the full regulations-page read (5 frames + ~12 rows inflated): the simplified release-perf form was timed.
- Memory: the app's RSS with a 1.45 GB tile set open was not measured.
- Download/installation of the tiles (P11) — files were pushed with adb.

## Reproduce (spike branch)

Scripts used are in the session scratchpad (not committed): stage the release files and tiles into the app's
`files/` with adb, run the harness by deep link (`canifishthis://n1/<bench|native|webview>?mode=…&tag=…`), and collect
`files/n1-results/*.json`. The harness is `app/apps/mobile/n1spike/` (SPIKE-ONLY commit). Modes: `fs`, `fs-str`,
`fs-mvtid`, `match`, `match-mvtid`, `none`.
