# Dependencies, and why

Every top-level dependency needs a line here. `pnpm deps` fails otherwise. The point is not
to forbid dependencies — it is to make adding one a deliberate act that shows up in review.

v1's webapp reached `chart.js` + `pdf-lib` + `pdfjs-dist` + `fuse.js` + `suncalc` + `pbf` +
`@mapbox/vector-tile` without anyone deciding to. Each was defensible alone.

## Build / test

- `typescript` — the language.
- `vitest` — test runner; also runs the conformance suite against every `WaterSource`.
- `knip` — the unused-code guard in `pnpm check` (N23, T63): an unused file, dependency or export fails the check.
  Dead code regrew between audits while every test stayed green. Allowances, each with its reason, are in
  `knip.jsonc`. Dev-only; pinned exactly.

- `jsdom` — a DOM for tests, so components are actually MOUNTED rather than inspected as
  source. Ninety-four tests passed while nothing in the workspace had ever rendered; a
  component that typechecks and a component that works are different claims.
- `@testing-library/react` — mounts `@app/ui-native` through **react-native-web**, which is
  the mobile-web target, so the assertions run against a real render tree in the same
  renderer a browser uses. Native-only behaviour is still unproven by tests; that is a
  known gap, not a covered one.

## Runtime — the one phone tree, three targets

Expo SDK 57 is a *bundle*: the versions below are the ones its Metro config, its Babel
preset and its native modules are built against. Pinning them independently is how an
Expo project starts failing in ways whose stack traces point nowhere. `npx expo install
--check` is the authority on this set; deviate only with a note here.

- `expo` — the toolchain, pinned to SDK 57. Provides Metro (with the web bundler),
  `babel-preset-expo`, and Continuous Native Generation, so `android/` and `ios/` stay
  derived from `app.json` instead of hand-edited twice.
- `react` — **19.2.3, the single pinned React for the entire workspace.** Not a range.
  See README "One React".
- `react-native` — 0.86.3, the renderer for iOS/Android; the version SDK 57 ships against.
- `react-native-web` — the whole reason mobile web and the native app cannot diverge:
  it renders `@app/ui-native` unchanged in a browser. Without it "mobile web" would be a
  second component tree, which is the v1 failure this workspace exists to prevent.
- `react-dom` — react-native-web's mount target. Never imported by our own source
  (no layer in `layers.json` may import it); it is react-native-web's renderer, not ours.
- `@expo/metro-runtime` — web dev-server client: Fast Refresh and the error overlay.
  Dev-only in effect, a no-op in a native bundle.
- `babel-preset-expo` — declared explicitly because `babel.config.js` names it, and Babel
  resolves presets from the config file's own directory. Inheriting it transitively from
  `expo` works until a hoisting change quietly stops it.
- `@types/react` — types only. Pinned to the 19.2 line to match the pinned React.
- `react-native-svg` — **15.15.4, the version Expo SDK 57 expects** (`expo install --check`
  is the authority; 15.15.5 is what npm hands you and it is the wrong one). The ONE drawing
  primitive, and the reason there is no charting library.

  The hydrograph is the only chart in the app, and an unusual one: a percentile envelope, a
  log axis, two year traces, forecast ribbons. v1 needed `chart.js` + `chartjs-adapter-date-fns`
  + `chartjs-plugin-zoom` — three dependencies — to draw it, and chart.js is canvas-based so it
  cannot render under react-native at all. Every scale, tick and path string is computed in
  `@app/ui/hydrograph` and the component only draws the shapes it is handed, so the same
  arithmetic feeds react-native-svg on the phone and DOM SVG on desktop.

  Also draws the icons (N1, owner 2026-10-22: Lucide, rounded 1.75). Lucide is NOT a dependency: thirteen icons' path
  data are copied from lucide-static@0.460.0 into `packages/ui-native/src/icons.tsx` (ISC; the notice is in
  `LICENSES/lucide.txt` and on the Data sources page; the version is `@app/core` `ICON_SET`). `lucide-react-native`
  would add a package and its own react-native-svg peer for what is a table of strings.

  Also draws the fish loader. Animation is plain `Animated` with `useNativeDriver` rather than
  reanimated: a transform-only loop already runs off the JS thread on native, and a second
  animation library with its own babel plugin is not worth one spinner.
- `react-native-gesture-handler` — **2.32.0, the version Expo SDK 57 names** (`bundledNativeModules.json`:
  `~2.32.0`, of which 2.32.0 is the only release; pinned in the root `overrides` like react-native-svg, because it
  ships native code). G1's gestures: the iOS left-edge back swipe (`<EdgeBack>`), the view swipe (`<ViewSwipe>`) and
  the sheet's drag to dismiss, on the phone AND on mobile web from one recogniser API — RN's own `PanResponder` has
  no direction lock against a scroll view and no web touch-action, which is the whole difficulty here. Callbacks run
  on JS (`runOnJS(true)`): no reanimated, no worklets babel plugin. Cost: ~3.3 MB unpacked, most of it native
  sources; its JS (plus `@egjs/hammerjs`, `hoist-non-react-statics`) is what reaches a bundle. Used by
  `@app/ui-native` (the recognisers, which only measure — every threshold is `@app/core` `gestures.ts`) and by
  `apps/mobile` for `GestureHandlerRootView` at the root. Test renders use its `jest-utils` (`fireGestureHandler`)
  under vitest through the ES-module build (`vitest.config.ts`).
- `expo-font` — registers the three typefaces the design specifies. Loaded in `apps/mobile`
  only: it is an asset loader, and the app shell is the one layer that may register assets.
  `@app/ui-native/type.ts` names the faces and never loads them, which is what keeps that
  package renderable by anything.
- `@expo-google-fonts/bricolage-grotesque` — display face (names, headings). ~90 KB for the
  two weights used; ships with the bundle rather than fetched, so it works offline, which
  the whole app has to.
- `@expo-google-fonts/archivo` — text face. Four weights, ~140 KB.
- `@expo-google-fonts/jetbrains-mono` — figures: readings, percentiles, station ids. One
  weight, ~45 KB. A mono face is not decoration here — flow numbers are read down a column
  and have to align.
- `maplibre-gl` — the web map renderer. **PINNED TO v5.** `pmtiles@4`'s protocol handler is
  written against maplibre 5's `addProtocol` signature; on maplibre 6 it fails SILENTLY —
  the archive header is fetched, no tile is ever requested, no error is raised, and the map
  renders an empty background that looks exactly like water with no regulations. Bump the
  two together, never one alone. Imported by exactly one file
  (`packages/map/src/Map.web.tsx`); `layers.json` bans it everywhere else, because an app
  that can reach the SDK can define its own layers and the two apps drift apart visually.
- `@maplibre/maplibre-react-native` — **11.5.0, exact** (N1 → N2): the native map renderer, MapLibre Native
  Android 13.6.1 / iOS 6.31.0 (fetched by SPM in the plugin's Podfile hook), reading the SAME `atlas.pmtiles` /
  `basemap.pmtiles` from local `pmtiles://file:///` paths with the same `runtimeStyle()`. Native code: it needs a
  development build (`expo prebuild`), not Expo Go, and its Expo config plugin is in `apps/mobile/app.json`, which is
  why `apps/mobile` lists it too (autolinking reads the app's dependencies; `layers.json` still bans importing it
  there). Imported by exactly one file, `packages/map/src/Map.native.tsx`. **Android `nativeVariant: "vulkan"` is
  PROVISIONAL**: N1 measured 51–60 fps against the default OpenGL ES's 21–35 fps at z9–11, on the EMULATOR only (its
  GLES runs through a GLES→Metal translator); it must be A/B'd on a real mid Android phone before it is kept. Cost: the
  native library per ABI plus ~1 MB of JS. Results: `handoff/PLAN/app-data/N1-SPIKE.md`, `n2/README.md`.
  **PATCHED (T74, `app/patches/@maplibre__maplibre-react-native@11.5.0.patch`, `pnpm.patchedDependencies`)**, Android
  only: (1) the atlas `VectorSource` reports each tile it requests (MapLibre Native's `OnTileActionListener`) with the
  tile's MVT feature ids, read off the UI thread from the same local `.pmtiles` (`TileIds.kt`), as the device event
  `MLRNTileIds` — the per-tile event GL JS has as `sourcedata`, which the colouring engine labels from; (2)
  `setFeatureStates`, many feature-states per bridge call; (3) `Images` entries take `stretchX`/`stretchY`/`content`
  (the gauge pill is a stretchable image). Bump the binding only with the patch re-applied and the emulator pan re-run.
- `pmtiles` — registers the `pmtiles://` protocol so maplibre reads a single archive by
  byte range instead of needing 20 million tile files on a server. This is what makes the
  same 835 MB file work from R2 on the web and from local storage offline.
- `@protomaps/basemaps` — generates the OpenStreetMap basemap style for `data/bc.pmtiles`.
  Our water draws on top of it. Taking it from a package rather than hand-writing ~40
  basemap layers means the ground looks like every other OSM map, which is what makes our
  colouring read as the thing that is ours.
- `@maplibre/maplibre-gl-style-spec` — test-only. Validates the generated style against the
  renderer's own schema. Every other style check compares our output to another of our
  artifacts, and all of them passed a style MapLibre refused to load.
- `expo-sqlite` — the app's bundle driver, on all three platforms. It ships a wasm web
  build as well as the native modules, and `deserializeDatabaseSync` opens a database from
  bytes, so "fetch the bundle, open it" is one code path instead of sql.js on web plus a
  native module on the phone. Two drivers would be two places for an answer to differ,
  which is the failure this whole workspace is arranged to prevent.
- `sql.js` — the WEB bundle driver. Plain wasm: no worker, no OPFS, no `SharedArrayBuffer`.
  expo-sqlite's web build needs all three, and `SharedArrayBuffer` requires the page to be
  cross-origin isolated, which breaks every cross-origin resource that does not send CORP
  — starting with the basemap's font CDN. A database must not impose a security posture on
  the rest of the app. `expo-sqlite` remains the native driver; the `Db` seam is four
  methods, which is what makes two drivers cheap rather than a divergence risk.
- `@types/sql.js` — types only, no runtime cost. sql.js ships none of its own.
- `fflate` — gzip (P10: every release file ships gzipped, and `fetchVerified` checks the DECODED bytes) and, at T02,
  the regs blobs' dictionary inflate. Pure JS, ~8 KB, the same on Hermes, web and node; neither Hermes nor every
  browser has a native gunzip that takes a byte array. Pinned 0.8.2. The regs blobs are raw DEFLATE with a per-table
  preset dictionary (`meta.blob_codec = zdict`, `packages/data/src/regs/blob.ts`); zlib preset dictionaries are why
  `DecompressionStream` (no dictionary option, absent on Hermes) cannot do it.
- `expo-file-system` — writes the `.spots` file on iOS and Android. Spots are the only thing
  in this app a person cannot re-download, and the regulation bundle is replaced wholesale
  on every update — so they get a file of their own that no update touches. Web uses browser
  storage instead; nothing above the store can tell which it got. It also holds a spot's
  photographs (`documentDirectory/spots/photos/`, `packages/data/src/spots/photoStore.native.ts`).
- `@react-native-community/netinfo` — **12.0.1, exact: the version Expo SDK 57's `bundledNativeModules.json` names**
  (pinned in the root `overrides` too: native code, like react-native-svg). Owner 2026-10-21 (late): "Add network-status
  detection on phones (netinfo)". Satellite imagery is online-only; without this a phone could not hear itself go
  offline, and the only signal was a one-tile probe every 30-120 s, so satellite lingered as blank tiles for up to two
  minutes and came back as late. With it the switch is instant both ways. The probe stays as the fallback for what
  NetInfo cannot see (Esri down while the phone is online; the phone's map reports no failed tile). Named in ONE file,
  `packages/ui-native/src/connectivity.native.ts`, loaded lazily: NetInfo throws on import where its native module is
  not linked (a development client built before it), and that falls back to the probe instead of a crash. Listed by
  `apps/mobile` for autolinking (it needs a rebuilt development build, like the map). Not used on the web
  (`navigator.onLine` does that job). Cost: a small native module (Android/iOS) and ~25 KB of TypeScript source before minification. Not a data package:
  the owner's "minimal prod packages" rule (2026-10-20) is about the release's data files and is untouched.
- `expo-image-picker` — pinned 57.0.20 (the SDK 57 line; `npx expo install` names `~57.0.14`). Adds a photograph to
  a spot on iOS and Android, from the library or the camera, through the system picker — so the app draws no picker
  of its own. Used ONLY by `apps/mobile/src/pickPhoto.native.ts`; the web side is a plain
  `<input type=file>` and needs nothing. The picked file is copied straight into `documentDirectory/spots/photos/`,
  because the picker's own copy lives in a cache the OS may clear. Its config plugin in `apps/mobile/app.json` carries
  the camera and library permission strings. Photos are the user's own data: never in a release, never uploaded.
- `expo-location` — pinned 57.0.21 (the SDK 57 line; `bundledNativeModules` names `~57.0.14`). (T74, owner 2026-10-27)
  "At my location" in the Spots menu on a phone: where the device is, foreground only, asked for from the reader's own
  tap (`requestForegroundPermissionsAsync`) and never before. Used ONLY by `apps/mobile/src/locate.native.ts`; the web
  side is the browser's geolocation (`locate.web.ts`). Its config plugin in `apps/mobile/app.json` carries the
  when-in-use permission string; background location is off. Native code: it needs a rebuilt development build.
- `react-native-safe-area-context` — **5.7.0, exact: the version Expo SDK 57's `bundledNativeModules.json` names**
  (`~5.7.0`; pinned in the root `overrides`, native code). (T50, owner 2026-10-27) The system bars' real insets: Android
  draws edge-to-edge (API 35), so the status bar sat over the top bar and the gesture handle over the tab bar, and RN's own
  `SafeAreaView` pads iOS only (and is deprecated). ONE place reads them: `apps/mobile` App.tsx (`SafeAreaProvider` +
  its `SafeAreaView` round the whole app); components never read insets. On web it pads by the browser's `env()`.
- `expo-intent-launcher` — **57.0.1** (the SDK 57 line; `bundledNativeModules` names `~57.0.1`). (T50, owner 2026-10-27)
  Android only: hands the highlighted synopsis copy (T215) to the reader's PDF app as a `content://` URI with a read
  grant — `Linking` refuses `file://` and sends no grant, and `Share` drops `url` on Android. Used ONLY by
  `ui-native/src/regs/synopsisPdf.native.ts`; iOS shares the file, web opens a blob. Native code: rebuilt dev build.
- `pdf-lib` — **1.17.1**, pure JS, no native code. (T215, owner 2026-10-27) "open the PDF" shows our copy of the synopsis with the
  entry's row highlighted: it loads the 24 MB release PDF and adds one `/Highlight` annotation per source box
  (`ui-native/src/regs/highlightPdf.ts`), the same on web and phone. Costs ~0.5 MB of JS and 1-2 s to rewrite the PDF at the
  first open; nothing is bundled into the paint path.
