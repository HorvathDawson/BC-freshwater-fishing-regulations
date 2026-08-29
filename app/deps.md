# Dependencies, and why

Every top-level dependency needs a line here. `pnpm deps` fails otherwise. The point is not
to forbid dependencies — it is to make adding one a deliberate act that shows up in review.

v1's webapp reached `chart.js` + `pdf-lib` + `pdfjs-dist` + `fuse.js` + `suncalc` + `pbf` +
`@mapbox/vector-tile` without anyone deciding to. Each was defensible alone.

## Build / test

- `typescript` — the language.
- `vitest` — test runner; also runs the conformance suite against every `RegsSource`.

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
