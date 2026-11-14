# Dependencies, and why

Every top-level dependency needs a line here. `pnpm deps` fails otherwise. The point is not
to forbid dependencies — it is to make adding one a deliberate act that shows up in review.

v1's webapp reached `chart.js` + `pdf-lib` + `pdfjs-dist` + `fuse.js` + `suncalc` + `pbf` +
`@mapbox/vector-tile` without anyone deciding to. Each was defensible alone.

## Build / test

- `typescript` — the language. Its compiler API is also the floating-promise gate (`tools/check-promises.mjs`, T458): no
  ESLint for one rule; the gate is typescript-eslint's `no-floating-promises` with its defaults.
- `vitest` — test runner; also runs the conformance suite against every `WaterSource`.
- `knip` — the unused-code guard in `pnpm check` (N23, T63): an unused file, dependency or export fails the check.
  Dead code regrew between audits while every test stayed green. Allowances, each with its reason, are in
  `knip.jsonc`. Dev-only; pinned exactly.
- `pngjs` — (T244) `tools/build-play-graphics.mjs` writes the Play store graphics (icon, feature graphic) as PNGs
  without a browser or a native image library. Dev-only, pure JS, no install script; never in an app bundle.
- `@types/pngjs` — types only, for `pngjs` (pngjs 3 ships none): the feeds worker's `test/cards.test.ts` decodes
  the share cards with it, and the worker's `tsc --noEmit` needs its declarations. Pinned to the 3.x line, exactly.

- `jsdom` — a DOM for tests, so components are actually MOUNTED rather than inspected as
  source. Ninety-four tests passed while nothing in the workspace had ever rendered; a
  component that typechecks and a component that works are different claims.
- `@testing-library/react` — mounts `@app/ui-native` through **react-native-web**, which is
  the mobile-web target, so the assertions run against a real render tree in the same
  renderer a browser uses. Native-only behaviour is still unproven by tests; that is a
  known gap, not a covered one.

## Runtime — the one phone tree, three targets

Expo SDK 58 is a *bundle*: the versions below are the ones its Metro config, its Babel
preset and its native modules are built against. Pinning them independently is how an
Expo project starts failing in ways whose stack traces point nowhere. `npx expo install
--check` is the authority on this set; deviate only with a note here.

**T250 (2026-11-06): SDK 57 → 58.** expo 58.0.19 (2026-10-23) and every Expo-managed module at what its
`bundledNativeModules.json` names, each EXACT and at least two weeks old (owner rule): a `~` range would resolve to a
patch days old, so the SDK set is pinned exactly where it used to be `~`/`^`. The lockfile was resolved with pnpm's
`resolution-mode=time-based` (one install, not kept in `.npmrc`), so the transitive Expo/Metro/RN packages are the ones
published by the newest direct pin's date (2026-10-23) — a plain install had pulled @expo/cli 58.1.19, expo-modules-core
58.0.30 and metro 0.87.3 from 2026-11-03/04. Changes the bump forced: RN 0.88's host components are function components,
so refs are typed with its `ViewInstance` / `ScrollViewInstance` / `TextInstance` / `TextInputInstance`; RN 0.88 removed
`InteractionManager` (`shell/prewarm.native.ts` uses `requestIdleCallback`, its named successor); expo-file-system 58's
`FileHandle.readBytes`/`writeBytes` became async (`release/drivers/install.native.ts` uses the `…Sync` twins, the SDK 57
behaviour); RN 0.87+ dropped `@react-native/assets-registry`, which react-native-svg's web build still imports (Expo's Metro
maps it to `react-native-web/asset-registry`; `vitest.config.ts` and `tools/seo/runner.mjs` alias it the same way). The
rest of the set, unnamed below: react-native-web 0.21.4, @expo/metro-runtime 58.0.15, babel-preset-expo 58.0.20 (what
expo 58.0.19 itself asks for), expo-font 58.0.10, expo-splash-screen 58.0.11, expo-sqlite 58.0.14. SDK 58's prebuild
template also turns R8 on for release builds (`android.enableMinifyInReleaseBuilds=true`; SDK 57 left it off): the first
SDK 58 APK died at start (R8 stripped WorkManager's Room database), so `apps/mobile/plugins/with-no-minify.js` keeps it
off as before; shrinking is a decision of its own (keep rules + a device walk), not part of the bump.

- `expo` — the toolchain, pinned to SDK 58 (58.0.19, exact). Provides Metro (with the web bundler),
  `babel-preset-expo`, and Continuous Native Generation, so `android/` and `ios/` stay
  derived from `app.json` instead of hand-edited twice.
- `react` — **19.3.0, the single pinned React for the entire workspace** (SDK 58's; 19.2.3 under SDK 57). Not a range.
  See README "One React".
- `react-native` — 0.88.0, the renderer for iOS/Android; the version SDK 58 ships against.
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
- `@types/react` — types only. Pinned to the 19.3 line (`~19.3.0`) to match the pinned React.
- `react-native-svg` — **15.16.0, the version Expo SDK 58 expects** (`bundledNativeModules`: `15.16.0`, exact; 15.15.4
  under SDK 57). The ONE drawing
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
- `react-native-gesture-handler` — **3.3.0, the version Expo SDK 58 names** (`bundledNativeModules.json`: `~3.3.0`, of
  which 3.3.0 is the only release; 2.32.0 under SDK 57; pinned in the root `overrides` like react-native-svg, because it
  ships native code). G1's gestures: the iOS left-edge back swipe (`<EdgeBack>`), the view swipe (`<ViewSwipe>`), the
  sheet's drag to dismiss and the chart's pinch/pan/taps (`Hydrograph`), on the phone AND on mobile web from one
  recogniser API — RN's own `PanResponder` has no direction lock against a scroll view and no web touch-action, which is
  the whole difficulty here. **T250: ported to the v3 hook API** (`usePanGesture` / `usePinchGesture` / `useTapGesture` /
  `useSimultaneousGestures` / `useExclusiveGestures`, one memoised config each; `onStart`/`onEnd` → `onActivate`/
  `onDeactivate` with `event.canceled` for the old `success`; the touch callbacks' state manager → the global
  `GestureStateManager` by handler tag; the sheet body's `Gesture.Native()` → RNGH's `ScrollView` with `simultaneousWith`).
  Two v3 behaviours the tests caught: a hook gesture exists from the hook call (so `EdgeBack`'s pan and the sheet's body
  pull live in components drawn only where they apply), and on the web a gesture's own config is applied after its
  detector's `touchAction`/`userSelect`/`enableContextMenu` and resets them (so they ride in the config too:
  `ui-native/src/gestureWeb.ts`). Callbacks run on JS (`runOnJS: true`, `disableReanimated: true`): no reanimated, no
  worklets babel plugin. Used by `@app/ui-native` (the recognisers, which only measure — every threshold is `@app/core`
  `gestures.ts`) and by `apps/mobile` for `GestureHandlerRootView` at the root. Test renders use its `jest-utils`
  (`fireGestureHandler`, `getByGestureTestId` — the v3 hook gestures' `testID`) under vitest through the ES-module build
  (`vitest.config.ts`).
- `expo-font` — registers the three typefaces the design specifies. Loaded in `apps/mobile`
  only: it is an asset loader, and the app shell is the one layer that may register assets.
  `@app/ui-native/type.ts` names the faces and never loads them, which is what keeps that
  package renderable by anything.
- `expo-splash-screen` — the native splash (T244): its config plugin writes the Android/iOS launch screen at prebuild (the
  loading fish's frame 0 on the boot screen's ground, light and dark, from `apps/mobile/app.config.js`), and `holdSplash` /
  `releaseSplash` in `apps/mobile/src/splash.ts` keep it up until the boot screen's own spinner is drawn, so the hand-off is
  seamless. Native only (stubs on web); ~no JS cost, a few KB of native code.
- `@resvg/resvg-js` (dev) — rasterises the favicon SVG into the app icons (`tools/build-icons.mjs`, T244), run by hand, and
  (T406) each water's share card into `share/<slug>.png` in the SEO build (`tools/seo/share.ts`, the app's Archivo files, no
  system font); a prebuilt native binary, never in the app.
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
  **PATCHED (T74 Android, T217 iOS, `app/patches/@maplibre__maplibre-react-native@11.5.0.patch`,
  `pnpm.patchedDependencies`)**: (1) the atlas `VectorSource` reports each tile it requests (MapLibre Native's `OnTileActionListener`) with the
  tile's MVT feature ids, read off the UI thread from the same local `.pmtiles` (`TileIds.kt`), as the device event
  `MLRNTileIds` — the per-tile event GL JS has as `sourcedata`, which the colouring engine labels from; (2)
  `setFeatureStates`, many feature-states per bridge call; (3) `Images` entries take `stretchX`/`stretchY`/`content`
  (the gauge pill is a stretchable image). iOS (T217): the map view's `tileDidTriggerAction` delegate, the same reader
  in Obj-C (`MLRNPmTilesIds.m`), tiles requested before the watch replayed by it, states sent before the source attaches
  kept until it does; the stretch zones become UIImage cap insets, so on iOS the pill's `content` box is its stretch box
  (text sits a few px further in). (T716, Android) `onTapUp`: a still single-finger tap said at its lift, before
  MapLibre's click (which waits out a double tap, 300 ms), so the app can light the tapped water at once (`MLRNMapView.tuneTap`,
  the codegen specs' `onTapUp`; iOS emits nothing). Bump the binding only with the patch re-applied and the emulator AND simulator pans
  re-run.
  **T250 (SDK 58 / RN 0.88): kept at 11.5.0** (11.5.1 is from 2026-10-29, under the two-week rule); the patch applies
  unchanged (same package version) and the Android build on RN 0.88 is the proof (T250 ticket). Not upstreamed.
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
- `expo-file-system` — pinned 58.0.11 (`bundledNativeModules`: `~58.0.11`; also in the root `overrides`, T250: @app/data and
  @app/ui-native take it as a peer and auto-install-peers had kept the SDK 57 copy beside it). Writes the `.spots` file on iOS and Android. Spots are the only thing
  in this app a person cannot re-download, and the regulation bundle is replaced wholesale
  on every update — so they get a file of their own that no update touches. Web uses browser
  storage instead; nothing above the store can tell which it got. It also holds a spot's
  photographs (`documentDirectory/spots/photos/`, `packages/data/src/spots/photoStore.native.ts`).
- `@react-native-community/netinfo` — **12.0.1, exact: the version Expo SDK 57's and 58's `bundledNativeModules.json` name**
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
- **No package — `apps/mobile/modules/bg-download` (local Expo module, Kotlin, Android only; T298).** The first install's
  big files (tiles, mask, basemap, rule book: ~95 % of the ~500 MB) go to **Android's own `DownloadManager`**, so they keep
  downloading with the app swiped away or the screen off, with the system's progress notification, its resume of a
  dropped line, and its "Wi-Fi only" hold (`setAllowedOverMetered(false)`) for the mobile-data setting. Chosen over a
  foreground service / WorkManager (`dataSync`): those need `FOREGROUND_SERVICE_DATA_SYNC` + a Play Console declaration
  (with a video), the Android 13 notification permission, a 6-hour cap on Android 15, and our own resume and Wi-Fi
  wait — DownloadManager needs no permission beyond INTERNET and is maintained by the platform. Chosen over
  `@kesha-antonov/react-native-background-downloader` (a third-party package over the same DownloadManager / iOS
  background URLSession) to add no package for ~150 lines of Kotlin. Cost: one notification per file (the system's,
  not one combined bar); files land in the app's external files folder and are moved into the install root; the
  module also hashes natively (sha256, ~1 s for the basemap where JS took 23 s). JS half: `packages/data/src/release/
  bgDownload.ts` (platform-free, tested), bound in `apps/mobile/src/bgDownload.ts` (`requireOptionalNativeModule`: a
  client built without it downloads in the app, as before). iOS: no module (not built yet) — expo-file-system's `DownloadTask` already runs
  on a background `URLSession` by default (it goes on while the app is suspended; a swipe-away cancels it, iOS's rule)
  and the next open resumes from the `.part`.
- `expo-image-picker` — pinned 58.0.10 (the SDK 58 line; `bundledNativeModules` names `~58.0.10`). Adds a photograph to
  a spot on iOS and Android, from the library or the camera, through the system picker — so the app draws no picker
  of its own. Used ONLY by `apps/mobile/src/pickPhoto.native.ts`; the web side is a plain
  `<input type=file>` and needs nothing. The picked file is copied straight into `documentDirectory/spots/photos/`,
  because the picker's own copy lives in a cache the OS may clear. Its config plugin in `apps/mobile/app.json` carries
  the camera and library permission strings. Photos are the user's own data: never in a release, never uploaded.
- `expo-location` — pinned 58.0.19 (the SDK 58 line; `bundledNativeModules` names `~58.0.19`). (T74, owner 2026-10-27)
  "At my location" in the Spots menu on a phone: where the device is, foreground only, asked for from the reader's own
  tap (`requestForegroundPermissionsAsync`) and never before. Used ONLY by `apps/mobile/src/locate.native.ts`; the web
  side is the browser's geolocation (`locate.web.ts`). Its config plugin in `apps/mobile/app.json` carries the
  when-in-use permission string; background location is off. Native code: it needs a rebuilt development build.
- `expo-notifications` — pinned 58.1.6 (the SDK 58 line; `bundledNativeModules` names `~58.1.6`). (T234, owner
  2026-11-05: phone-only v1) LOCAL notifications only: a gauge alert crossing its line and "new regulations data is
  ready", raised by the phone's own background check (`scheduleNotificationAsync`, trigger now, on one of two Android
  channels), the Android 13+ POST_NOTIFICATIONS permission (asked only when the reader turns something on), and the tap
  that opens the water's Conditions. No push token is ever asked for and no server holds anything. Chosen over
  `@notifee/react-native` (richer local notifications, but a second notification stack beside Expo's, its own config, and
  not in Expo's SDK set) and over a hand-written module: it is Expo's own, versioned with the SDK, has a config plugin
  for the icon and channels, and is the route to FCM push later should local checks prove too late (the ticket's
  measure-first plan). Used ONLY by `apps/mobile/src/notify.native.ts`; `notify.web.ts` imports nothing, so the web bundle
  carries none of it. Its Android library links `firebase-messaging` 25.0.1 (and its
  manifest RECEIVE_BOOT_COMPLETED + POST_NOTIFICATIONS); with no `google-services.json` Firebase never initialises (one
  log line at start) and nothing registers — the emulator checklist in T234 confirms it.
- `expo-background-task` — pinned 58.0.11 (`bundledNativeModules`: `~58.0.11`). (T234) The phone's periodic check:
  Android WorkManager / iOS BGTaskScheduler, ~15 min at best (the system decides; Doze stretches it). Expo's successor to
  `expo-background-fetch` (deprecated in SDK 53: Android's old JobScheduler path and iOS's background fetch API). Chosen
  over `react-native-background-fetch` (Transistorsoft: older, its own headless-task plumbing, a separate licence model
  for some features) and over a foreground service (a permanent notification, battery, Android 15's 6 h/day dataSync
  cap). Used ONLY by `apps/mobile/src/notify.native.ts`.
- `expo-task-manager` — pinned 58.0.12 (`bundledNativeModules`: `~58.0.12`). (T234) `expo-background-task`'s required
  peer: `defineTask` names the JS the system runs (at module scope, so a headless start finds it). Used ONLY by
  `apps/mobile/src/notify.native.ts`.
- `expo-in-app-updates` — pinned 0.13.0 (T390, owner 2026-11-04). The app's own newer version, from inside it: Google
  Play's in-app update (Play Core `app-update` 2.1.0 — flexible download, immediate when new data needs the app) on a
  Play install, and the App Store lookup on iOS. Used ONLY by `apps/mobile/src/appUpdater.native.ts`, loaded on the first
  Play / App Store call (`import()`); `appUpdater.web.ts` imports nothing, so the web bundle carries none of it.
  Chosen over `sp-react-native-in-app-updates` 2.0.0 (the older name, 261k downloads/month vs 472k): that one pulls two
  more native modules (`react-native-device-info` pinned at 10.3.0 from 2022, and `react-native-siren` for iOS) plus
  `underscore`; this one is an Expo module (Expo Modules API: new architecture native, autolinked, no config plugin),
  no runtime dependencies, a web stub of its own, active (0.13.0 2026-10-20). 0.13 is built against SDK 58, which the app
  is on since T250 (under SDK 57 the core it needs was present; the APK builds proved it either way); and a FLEXIBLE download completes itself (`completeUpdate` on
  DOWNLOADED: Play restarts the app once the download is done, after the reader pressed Update) — there is no separate
  "Restart to update" step. Native code: it needs a rebuilt development build.
- `react-native-safe-area-context` — **5.10.1, exact: the version Expo SDK 58's `bundledNativeModules.json` names**
  (`~5.10.1`; 5.7.0 under SDK 57; pinned in the root `overrides`, native code). (T50, owner 2026-10-27) The system bars' real insets: Android
  draws edge-to-edge (API 35), so the status bar sat over the top bar and the gesture handle over the tab bar, and RN's own
  `SafeAreaView` pads iOS only (and is deprecated). ONE place reads them: `apps/mobile` App.tsx (`SafeAreaProvider` +
  its `SafeAreaView` round the whole app); components never read insets — except a surface in its OWN window (a
  `Modal`: the sheet's foot, a menu page), through `ui-native/src/WindowInset` (native only; the web side pads nothing).
  On web the app frame pads by the browser's `env()`.
- `pdf-lib` — **1.17.1**, DEV only (T354): the append tests build small books with it and read the result back. Nothing
  ships: the app highlights by appending (`ui-native/src/regs/appendPdf.ts`), never by parsing the book.
- `react-native-pdf` — **7.0.5** (kept at T250: the newest release; its patch `app/patches/react-native-pdf@7.0.5.patch`,
  T360's one-load-per-file fix, applies unchanged and is not upstreamed), native (Android: a pdfium-based viewer; iOS: PDFKit). (T354, owner 2026-11-03: "We don't
  need minimal if it is better like this.") The rule book opens INSIDE the app at the cited page with the highlight on the
  page in its place; the file is read natively (no 24 MB on the JS heap) and no outside PDF app is needed. Used only by
  `ui-native/src/regs/PdfViewer.native.tsx`; web keeps the browser's viewer. Native code: rebuilt dev build. Replaced
  `expo-intent-launcher` (the hand-off to an outside PDF app; removed, owner 2026-11-03: no silent fallbacks).
- `react-native-blob-util` — **0.25.1**, native; `react-native-pdf`'s required peer (its file/URL loading). Not imported by
  our code.
- `axe-core` — **not installed.** (T243) `tools/a11y-walk.mjs` fetches axe-core 4.10.2 from cdnjs at run time (`AXE_URL`)
  and injects it into the running web app for a WCAG A/AA check; nothing ships, nothing is in a lockfile. A dev tool run
  by hand (needs Chrome and the app on :39218).
