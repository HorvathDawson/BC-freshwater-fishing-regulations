# T250 — Expo SDK / dependency upgrade window

Status: DONE — owner's phone walk = the 2026-11-08 phone check on 0.6.9 (built on SDK 58), all 11 items passed. Was: IN REVIEW — SDK 58 done on the dev branch (320785434, 8b4491d6b, 1c47932e9); check green; Android, iOS simulator and web walks pass (2026-11-06); the owner's phone walk left.

Source: owner 2026-10-28 — approved proposal P16 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Before store submission: upgrade Expo SDK + React Native + MapLibre RN to current, re-apply / re-derive
`app/patches/` (MapLibre RN patch), run the full suites, an Android device walk and a web walk; record in app/deps.md.
## Acceptance
Suites green, the patch re-applied (or dropped as upstreamed), device + web walks with shots, deps.md updated.
Depends: none (schedule before T244). Size: M. Owner: no (a phone walk at the end).

## Done (2026-11-06, dev branch: 320785434, 8b4491d6b, 1c47932e9)
**Versions** (all exact, each ≥ 2 weeks old; `expo@58.0.19`'s `bundledNativeModules.json` is the authority): expo 58.0.19,
react / react-dom 19.3.0, react-native 0.88.0, react-native-web 0.21.4, react-native-gesture-handler 3.3.0,
react-native-safe-area-context 5.10.1, react-native-svg 15.16.0, @expo/metro-runtime 58.0.15, babel-preset-expo 58.0.20,
expo-background-task 58.0.11, expo-file-system 58.0.11, expo-font 58.0.10, expo-image-picker 58.0.10, expo-location
58.0.19, expo-notifications 58.1.6, expo-splash-screen 58.0.11, expo-sqlite 58.0.14, expo-task-manager 58.0.12,
@types/react ~19.3.0. Unchanged: @maplibre/maplibre-react-native 11.5.0 (11.5.1 too new), netinfo 12.0.1, react-native-pdf
7.0.5, react-native-blob-util 0.25.1, expo-in-app-updates 0.13.0. Lockfile resolved once with
`--config.resolution-mode=time-based` (transitives capped at 2026-10-23; a plain install pulled 2026-11-03/04 Expo/Metro).
Root `overrides` gained expo-file-system (the peer copy stayed on 57.0.6). Why, per package: `app/deps.md`.

**Patches**: both re-applied unchanged (same package versions), neither upstreamed —
`@maplibre__maplibre-react-native@11.5.0.patch` (the map colours on the emulator: shot 01), `react-native-pdf@7.0.5.patch`.

**gesture-handler 2 → 3**: every recogniser ported to the hook API (`usePanGesture`, `usePinchGesture`, `useTapGesture`,
`useSimultaneousGestures`, `useExclusiveGestures`; memoised configs; `onStart/onEnd(success)` → `onActivate/onDeactivate`
+ `event.canceled`; touch-callback state manager → `GestureStateManager.activate/fail(handlerTag)`; sheet body
`Gesture.Native()` → RNGH `ScrollView simultaneousWith`). Files: EdgeBack, ViewSwipe, swipeActivation.{native,web,d}.ts,
Sheet (body pull now `PullScroll`), Hydrograph, new `gestureWeb.ts`. Two v3 behaviours found by the tests: hook gestures
exist from the hook call (EdgeBack's pan / the sheet body pull moved into components drawn only where they apply), and on
the web a gesture's config reset its detector's touchAction/userSelect/context menu (they now ride in the config).
`GestureHandlerRootView` and jest-utils (`fireGestureHandler`, `getByGestureTestId`) unchanged in use; all G1 tests pass.

**Other forced changes**: RN 0.88 ref types (`ViewInstance`/`ScrollViewInstance`/`TextInstance`/`TextInputInstance`,
annotated forwardRef exports), `InteractionManager` removed → `requestIdleCallback` (prewarm), expo-file-system 58
`readBytes`/`writeBytes` async → the `…Sync` twins (install driver), `@react-native/assets-registry` alias for
react-native-svg web in vitest + SEO runner (as Expo's Metro does), Menu/openBook/RuleBookLink mocks retyped, two
whole-page render walks given 30 s (load timeouts under the full check). **R8**: SDK 58's prebuild turns minify on; the
first APK crashed at start (WorkManager's Room DB stripped) → `apps/mobile/plugins/with-no-minify.js` keeps it off as in
SDK 57 (enabling R8 = its own ticket: keep rules + device walk).

**Proofs**: `pnpm -C app check` green under heavy.sh (532 files, 8376 passed, 977 skipped). Android: release APK (testbed `session.sh --build`, staging, arm64)
on `n1_pixel` — walk 6/6 (`T250-shots/android/walk.json`): 01 app starts + map drawn and coloured, 02 a water page opens
(Vedder → Chilliwack River, Map view), 03 Regulations body swipe → Conditions, 04 Map-view header swipe → Conditions,
05 Layers sheet opens, 06 sheet dragged down by its handle closes. Not done: iOS simulator build/walk, the web walk, the
owner's phone walk.

## iOS + web walks (2026-11-06, chat 4)
- **iOS:** Release simulator build of SDK 58 (Xcode 27.2, iPhone 17 / iOS 26.5, b99268518) via
  `deploy/staging/build-ios-sim.sh --testbed`: BUILD SUCCEEDED, the patched binding's iOS files compile clean;
  `session-ios.sh --fresh … run-ios.mjs first-open perf` both PASS, map coloured after pans
  (`app/tools/testbed/out/t420-ios/`). Details in T420 "iOS half".
- **Web:** staging site on 0.6.6 / r-67b456b5e64b, smoke 18/18 phone + desktop (`app/dist/smoke-staging`).
- **Android:** 0.6.6 fresh-install walk 5/5 (`app/tools/testbed/out/walk-066/`).
Left: the owner's phone walk.
