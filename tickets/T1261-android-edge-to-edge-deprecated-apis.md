# T1261 — Play Console warning: deprecated edge-to-edge APIs (Android 15)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1261 | Play Console warning: deprecated edge-to-edge APIs (Android 15) | OPEN | — | find which library calls the deprecated APIs (build check), then fix or upgrade it; no release assigned |

Source: the owner, 2026-11-27, read in Google Play Console (Android, closed testing), verbatim: "Your app uses deprecated APIs or parameters for edge-to-edge
One or more of the APIs you use or parameters that you set for edge-to-edge and window display have been deprecated in Android 15. To fix this, migrate away from these APIs or parameters."

## What a read-only look found (2026-11-27; guesses are labelled)
- Stack: Expo SDK 58 (`expo` 58.0.19), `react-native` 0.88.0, `@maplibre/maplibre-react-native` 11.5.0,
  `react-native-safe-area-context` 5.10.1, `react-native-gesture-handler` 3.3.0 (`app/apps/mobile/package.json`).
- `app.json` and `app.config.js` set no edge-to-edge key, no status bar colour and no navigation bar colour. The only
  status bar code is `<StatusBar barStyle=...>` in `app/apps/mobile/src/App.tsx` (line 711).
- No `expo-navigation-bar` or `expo-status-bar` dependency was seen. Plugins that touch the Android project:
  `plugins/with-*.js` (signing, R8, gradle memory, backup rules, portrait feature); none sets window flags.
- GUESS (not proven): the deprecated calls come from a library or from React Native's own `StatusBar` colour setters,
  not from our code. Candidates: React Native `StatusBar`, the MapLibre native view, `expo-notifications`,
  `expo-splash-screen`, `react-native-screens` (if present). Play's pre-launch / App Bundle details name the calling class; read that first.

## Work
1. Open Play Console > the warning's "details": it lists the deprecated API and where it is called from. Record it here.
2. If it is a library: check for a newer version that migrated (SDK 58 targets Android 15 edge-to-edge by default); upgrade only
   through the usual dependency PR with its checks. If it is our `StatusBar` use: drop the colour props on Android.
3. Look at the app on an Android 15 device or emulator with gesture and 3-button navigation: top bar, bottom tabs and the map sheet
   must not sit under the system bars. Any look change goes to the owner as whole options (2-4 complete options, one question).

## Acceptance
The warning is gone after the next Play upload (or the details show only third-party code we cannot change, said here); an
Android 15 walk with no content under the system bars; the owner's look answer recorded where one was asked.

Depends: none. Size: S-M. Priority: normal. Owner: yes only if a look changes. Default: fix the code path, keep today's look.
