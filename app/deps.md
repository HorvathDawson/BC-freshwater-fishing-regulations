# Dependencies, and why

Every top-level dependency needs a line here. `pnpm deps` fails otherwise. The point is not
to forbid dependencies — it is to make adding one a deliberate act that shows up in review.

v1's webapp reached `chart.js` + `pdf-lib` + `pdfjs-dist` + `fuse.js` + `suncalc` + `pbf` +
`@mapbox/vector-tile` without anyone deciding to. Each was defensible alone.

## Build / test

- `typescript` — the language.
- `vitest` — test runner; also runs the conformance suite against every `RegsSource`.

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
  (`layers.json` bans it outside `apps/web`); it is react-native-web's renderer, not ours.
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

  Also draws the fish loader. Animation is plain `Animated` with `useNativeDriver` rather than
  reanimated: a transform-only loop already runs off the JS thread on native, and a second
  animation library with its own babel plugin is not worth one spinner.
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
- `expo-file-system` — writes the `.spots` file on iOS and Android. Spots are the only thing
  in this app a person cannot re-download, and the regulation bundle is replaced wholesale
  on every update — so they get a file of their own that no update touches. Web uses browser
  storage instead; nothing above the store can tell which it got.
