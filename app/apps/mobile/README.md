# apps/mobile — the Expo project (iOS, Android, and today's mobile web)

Built **first**, and **offline-first**: the device carries the only hard constraints
(fixed storage, no round trips, no server fallback). Web is a relaxation of this.

Components come from `@app/ui-native` — the same set mobile web renders through
react-native-web, so the two phone experiences match by construction rather than by
discipline. Nothing in `src/` may hold regulation logic; that is `@app/core` and
`@app/ui`, and `layers.json` enforces it.

## Run it

```bash
pnpm dev:web       # from app/  → expo start --web
pnpm dev:android   # from app/  → expo run:android
pnpm dev:ios       # from app/  → expo run:ios
```

One project, three platforms, one `src/App.tsx`. `expo start --web` is the fast loop:
Metro compiles the same React Native tree through react-native-web, so what a browser
shows is the phone UI, not a web imitation of it.

**Mobile web is served from here, not `apps/web`.** Expo's web output is a *platform* of
this project — `layers.json` grants `expo` to `apps/mobile` and withholds it from
`apps/web`. `apps/web` becomes the production web app when desktop exists; see its README.

## Native directories are generated, never committed

`android/` and `ios/` are Continuous Native Generation output, derived from `app.json`.
`expo run:*` creates them on demand; `pnpm --filter @app/mobile run prebuild` regenerates
them explicitly. They are gitignored on purpose: committing them re-creates the v1 problem
where one config change had to be hand-applied in two places.

Do not edit them. Edit `app.json` (or add a config plugin) and prebuild again.

## What still needs a machine

- **Android** needs a JDK (17+) on `PATH` and the Android SDK (`ANDROID_HOME`). The
  generated Gradle project is complete; without a JDK `gradlew` cannot start.
- **iOS** needs Xcode *and* CocoaPods (`brew install cocoapods`). `expo run:ios` runs
  `pod install` first and stops there without it.

Neither affects `pnpm dev:web`, which needs only Node.

## Still to come

The real screens wait on the packaging spike (13-build-plan step 6), which decides what
`data-mobile` reads. `src/App.tsx` renders `PlaceholderScreen` from `@app/ui-native` — a
wiring probe, not product UI. It calls `freshness` (`@app/core`) and `formFactorFor`
(`@app/ui`) and prints the results, so a broken workspace link fails on screen instead of
silently resolving to something stale.

**Non-negotiable:** the bundle is downloaded on first run. It must never ship as an
app-store asset — that is what made a v1 data fix require an app release (issue ㉛).
