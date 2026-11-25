# apps/mobile: the one app shell (iOS, Android, web)

The Expo project that boots the app on all three platforms. It wires `@app/ui-native`'s `Shell` to the data, the map and
the platform (back button, location, notifications, share, fonts, update checks). It holds no regulation logic: that is
`@app/core` and `@app/ui`, and `layers.json` enforces it. The web app (phone, tablet and desktop widths) is served from
here too: Expo web is a platform of this project, not a second app.

## Entry points

- `index.js` -> `src/App.tsx`: the one shell for ios / android / web (`registerRootComponent`).
- `src/*.native.ts` / `*.web.ts` / `*.d.ts`: the platform pairs (`systemBack`, `locate`, `notify`, `share`, `pickPhoto`,
  `appUpdater`, `startLink`, `replaced`, ...), checked by `pnpm platform` (app README "Web and native code").
- `src/config.ts` + `src/env.ts`: the data environment (local / staging / production) and its URLs; `eas.json` the build profiles.
- `app.json` + `app.config.js` (config), `plugins/` (Expo config plugins: R8 rules, signing, backup rules, pinned MapLibre),
  `modules/` (local native modules: `bg-download`, `exit-info`, `memory-trim`), `version-stamp.js` (run by `metro.config.js`).
- `assets/`: fish pictures and thumbnails, icons, splash. Fish thumbs and sprites are generated (`pnpm fish`, `pnpm splash`).

## Run it (from `app/`)

```bash
pnpm dev:web       # expo start --web, the fast loop
pnpm dev:android   # expo run:android (JDK 17+, Android SDK)
pnpm dev:ios       # expo run:ios (Xcode, CocoaPods)
```

## Rules

- `android/` and `ios/` are generated from `app.json` by `expo prebuild` and never committed or edited; change
  `app.json` or add a config plugin, then `pnpm --filter @app/mobile run prebuild`.
- The data is downloaded on first run, never shipped inside the store build (a data fix must not need an app release).
- R8 is on for every release build (`plugins/with-r8.js`); Android release builds use the pinned MapLibre
  (`app/tools/maplibre-android/`). Store builds and uploads are `deploy/` (`deploy/staging/build-apk.sh`).

## Tests

`src/*.test.ts(x)` beside the code, run by `pnpm test` from `app/` (vitest, react-native-web as the renderer). `src/back.test.tsx`
holds the Android back table in [`app/BACK.md`](../../BACK.md).
