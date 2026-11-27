# Dependencies, and why

Every top-level dependency needs a line here (`pnpm deps`, AGENTS 30, fails otherwise): the gate reads a name in
backticks at the start of a list line. Versions live in the `package.json` files, not here. Expo-managed modules are pinned
exactly to what the SDK's `bundledNativeModules.json` names (`npx expo install --check` is the authority), each at least
two weeks old; the SDK is pinned as a set, never one module alone. Packages with native code are also pinned in the root
`package.json` `pnpm.overrides`, so there is one copy. Two packages carry patches in `patches/`
(`pnpm.patchedDependencies`).

## Build / test (dev only)

- `typescript` — the language; its compiler API is also the floating-promise gate (`tools/check-promises.mjs`).
- `vitest` — test runner, also for the conformance suites.
- `knip` — the unused file / export / dependency guard in `pnpm check`; allowances are in `knip.jsonc`.
- `jsdom` — a DOM for tests, so components are mounted, not read as source.
- `@testing-library/react` — mounts `@app/ui-native` through react-native-web in tests. Native-only behaviour stays a known gap.
- `@commitlint/config-conventional` — the Conventional Commits rules (W48) every commit and PR title is checked
  against, through `tools/check-commits.mjs` and `tools/commitlint.config.mjs`.
- `@commitlint/lint` — the checker `tools/check-commits.mjs` runs (the CLI package is not needed).
- `@commitlint/load` — reads `tools/commitlint.config.mjs` for it.
- `@commitlint/parse` — `tools/check-commits.test.ts` only: the release command's bump (Python) and the check parse alike.
- `@commitlint/is-ignored` — the same test's other half: commitlint's list of skipped messages (merges, reverts).
- `pngjs` — `tools/build-play-graphics.mjs` writes the Play store graphics without a browser; never in a bundle.
- `@types/pngjs` — types for it (the feeds worker's `test/cards.test.ts` decodes share cards with it).
- `esbuild` — bundles the public gauge page's chart (`tools/seo/gauges.ts`); already present under vite, named at that version.
- `@resvg/resvg-js` — rasterises the favicon into app icons (`tools/build-icons.mjs`) and each water's share card
  (`tools/seo/share.ts`); a prebuilt binary, never in the app.
- `@maplibre/maplibre-gl-style-spec` — test only: validates the generated style against the renderer's own schema.
- `pdf-lib` — test only: the append tests (`ui-native/src/regs/appendPdf.ts`) build small books with it.
- `axe-core` — NOT installed: `tools/a11y-walk.mjs` fetches a pinned build from cdnjs at run time; nothing ships.

## Runtime: one phone tree, three targets

- `expo` — the toolchain (Metro, `babel-preset-expo`, Continuous Native Generation: `android/` and `ios/` derive from `app.json`).
- `react` — the single pinned React for the whole workspace, exact, also in `pnpm.overrides` (AGENTS 27; `README.md` "One React").
- `react-native` — the iOS/Android renderer, the version the SDK ships against.
- `react-native-web` — renders `@app/ui-native` unchanged in a browser: why mobile web cannot diverge from the app.
- `react-dom` — react-native-web's mount target; never imported by our source (`layers.json`).
- `@expo/metro-runtime` — the web dev client (Fast Refresh, error overlay).
- `babel-preset-expo` — declared because `babel.config.js` names it and Babel resolves presets from the config's directory.
- `@types/react` — types, on the pinned React's line.
- `react-native-svg` — the one drawing primitive, so no charting library: the hydrograph's scales and paths are computed in
  `@app/ui/hydrograph` and drawn as shapes. Also the icons (Lucide path data copied into `ui-native/src/icons.tsx`, ISC
  notice in `LICENSES/lucide.txt`; Lucide itself is not a dependency) and the fish loader.
- `react-native-gesture-handler` — one recogniser API for the phone and mobile web: the iOS edge-back swipe, the view
  swipe, the sheet drag, the chart's pinch/pan. Callbacks on JS (no reanimated, no worklets plugin); every threshold is
  `@app/core` `gestures.ts`.
- `expo-font` — registers the typefaces; loaded in `apps/mobile` only (`ui-native/type.ts` names faces, never loads them).
- `@expo-google-fonts/bricolage-grotesque` — the display face; bundled, so it works offline.
- `@expo-google-fonts/archivo` — the text face.
- `@expo-google-fonts/jetbrains-mono` — figures (readings, station ids); mono so columns align. The three TTFs are the
  phone's faces and the source of the web's lossless WOFF2 (`tools/seo/fonts/`).
- `expo-splash-screen` — the native splash (config plugin at prebuild); `apps/mobile/src/splash.ts` holds it until the boot spinner draws.
- `maplibre-gl` — the web map renderer (6.x, T1252), only in `packages/map/src/Map.web.tsx` + `src/web/`. Its worker is its own file (`apps/mobile/gl-worker.js`,
  `src/web/glWorker.ts`; without it no tile loads, once blamed on pmtiles). `zoomLevelsToOverscale: undefined` keeps GL 5's overzoom (6 slices z14+ tiles). A bump re-reads `tools/gl-internals.test.ts` and walks the map in a browser.
- `@maplibre/maplibre-react-native` — the native map renderer, exact, imported only by its engine folder
  `packages/map/src/native/maplibre/` (T1154: layers.json and check-boundaries' engine rule; the shell `Map.native.tsx`
  reaches it through `native/engine.ts`) (listed in `apps/mobile` too for autolinking; needs a development build, not Expo Go). PATCHED
  (`patches/@maplibre__maplibre-react-native@11.5.0.patch`): per-tile feature-id events for the colouring engine, batched
  feature-states, stretchable images, a still-tap event, safe rejection of calls before the map is ready. Android builds
  use a local MapLibre Native build of one pinned upstream commit (`tools/maplibre-android/pin.env`). Bump the binding
  only with the patch re-applied and the emulator and simulator pans re-run. Android `vulkan` variant is provisional.
- `pmtiles` — the `pmtiles://` protocol: one archive read by byte range, from R2 on the web and local storage on the phone.
- `@protomaps/basemaps` — the OSM basemap style, so the ground looks like every other OSM map.
- `expo-sqlite` — the native bundle driver (opens a database from bytes).
- `sql.js` — the WEB driver: plain wasm, no worker, OPFS or `SharedArrayBuffer` (which would force cross-origin isolation).
  The `Db` seam is four methods, so two drivers stay cheap.
- `@types/sql.js` — types for it.
- `fflate` — gzip for the release files (`fetchVerified` checks the decoded bytes) and the regs blobs' raw-DEFLATE
  inflate with a preset dictionary, which `DecompressionStream` cannot do and Hermes lacks.
- `expo-file-system` — the phone's files: the spots file no update touches, spot photos, the release install.
- `@react-native-community/netinfo` — the phone hears itself go offline (named in `ui-native/src/connectivity.native.ts`, lazy;
  the one-tile probe stays as the fallback).
- `expo-image-picker` — a spot's photo from the library or camera (`apps/mobile/src/pickPhoto.native.ts` only; the web uses a file input).
- `expo-image-manipulator` — resizes photos for the phone backup (`packages/data/src/spots/backupSet.native.ts` only).
- `expo-location` — "At my location", foreground only, asked for on the reader's tap (`apps/mobile/src/locate.native.ts` only).
- `expo-notifications` — LOCAL notifications (gauge alerts, new data) and, on iPhone only, the push token sent to our own
  feeds worker (`apps/mobile/src/notify.native.ts`, `pushToken.ios.ts`). Android asks for no token.
- `expo-background-task` — the phone's periodic check (WorkManager / BGTaskScheduler), `notify.native.ts` only.
- `expo-task-manager` — its required peer: `defineTask` names the JS the system runs.
- `expo-in-app-updates` — Play's in-app update / the App Store lookup (`apps/mobile/src/appUpdater.native.ts` only; the web stub imports nothing).
- `react-native-safe-area-context` — the system bars' real insets; read in one place (`apps/mobile` App.tsx).
- `react-native-pdf` — opens the rule book inside the app at the cited page, natively (`ui-native/src/regs/PdfViewer.native.tsx`;
  web keeps the browser's viewer). Patched once (`patches/react-native-pdf@7.0.5.patch`: one load per file).
- `react-native-blob-util` — `react-native-pdf`'s required peer; not imported by our code.

No package: `apps/mobile/modules/bg-download` is a local Expo module (Kotlin, Swift) that hands the first install's big
files to Android's `DownloadManager` / one iOS background `URLSession`, so they continue with the app closed; the JS
half is `packages/data/src/release/bgDownload.ts`, bound in `apps/mobile/src/bgDownload.ts`.
