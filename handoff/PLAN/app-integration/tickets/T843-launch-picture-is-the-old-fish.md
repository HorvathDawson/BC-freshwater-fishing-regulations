# T843 — On opening the app, the old circling fish shows before the new swimming coho

Status: DONE 45fff5e63 (CI green 2026-11-17; checked on the Android emulator: a cold launch shows the coho from the first frame, scratch t843/launch-*-frames.png, sent to the owner; in 0.8.0).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T843 | On opening the app the old circling fish shows before the new swimming coho (the native launch picture was never moved to the coho) | DONE | 45fff5e63 | owner 2026-11-17 (app) |

Source: owner 2026-11-17 ~17:30, in the app chat (Play internal 0.7.12), verbatim: "riught now on current app version
the old loading icon shows up before the new swimming fish one when the app is opened."

## Cause
The phone's native launch picture (expo-splash-screen, shown before any app code runs) is drawn by `pnpm splash`
(`app/tools/build-splash.mjs`) from the loader's first frame, so the hand-off to the boot screen is seamless (T244).
T814 changed the boot screen's loader to the owner's coho but left the launch picture as the circling fish's frame 0
(said so in `App.tsx`: "the hand-off now changes picture"). So a launch shows the old fish, then the coho.

## Fix
- `build-splash.mjs` draws the coho sheet's frame 0 per theme (light: coho-light, dark: coho-dark; the sheets are
  already printed in each theme's ink and red) at the size the boot screen draws it: `COHO_WIDTH` 192 dp, 576 x 240 px
  at 3x, on a transparent ground (the coho's ground is `color.ui.card`, which the splash already paints). The frame is
  read from the committed lossless WebP by `fish-loader/sheet_facts.py --frame0` (an exact decode everywhere).
- `app.config.js` `imageWidth` 110 -> 192.
- Android 12+ clips the launch icon to a 192 dp circle. Frame 0 reaches 93.0 dp from the centre, inside the 96 dp
  radius. `build-splash.mjs` refuses a picture that reaches past it.
- Test `app/tools/splash-is-the-loader.test.ts`: the boot screen draws the coho at `COHO_WIDTH`, `imageWidth` is
  `COHO_WIDTH`, and both PNGs are that size at 3x. Mutation-checked: old width or old PNG fails it; `pnpm splash` fails on
  the old PNG.

Native config: lands in the next app build (0.8.0), not over the air.
