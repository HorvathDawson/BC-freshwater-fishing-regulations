# T814 — iOS: after the app is closed and reopened mid-download, the fish spinner stops moving

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "a-d worked good except after closing app and opening again the fish icon stopped moving....".
The first-install checks (a) lock, (b) switch apps, (c) swipe away and reopen, (d) a steady bar all passed; only the
swimming fish (`fish-sprite.generated.ts`, the download screen's spinner) stood still after the reopen.

## Look at
The spinner's animation start on a cold start that resumes a download (the bar already part-way): an animation started
before the view is laid out, a JS-driven loop paused by the app state, or the T746 "applying" screen's swap. Reproduce on
the iOS simulator (`deploy/staging/build-ios-sim.sh`): start the first download, kill the app, reopen.

## Owner, 2026-11-16 ~23:40 (app chat), verbatim
"the spinner on a second phone jusst never spinned on initial download". So on a second iPhone the fish never moved on the
first download at all, not only after a reopen. Simulator (app chat, iOS worker): the fish moves on first download, after a
mid-download kill and after a background launch, so not reproduced there. Lead: `FishSpinner.native.tsx` stands still by
design under Reduce Motion (`AccessibilityInfo.isReduceMotionEnabled`), and a still fish with no other progress sign reads as
hung. Asked the owner whether Reduce Motion or Low Power Mode is on on that phone.

## Owner, 2026-11-16 ~23:50 (app chat), verbatim
"loading icons seem like they can exist still maybe instead we make a second loading icon i will make it then send it here".
So a still loading icon is fine; the owner designs a second, still icon and sends it in the app chat. The app keeps one slot
for it (used where the fish cannot move, e.g. Reduce Motion); today's still fish frame stands in until it arrives. The
restart of the animation on app-active and first layout goes ahead.

## 2026-11-16 ~23:50 MST, the fix (branch app-fix/ios-1116)
No single cause was guessed; the native fish (`app/packages/ui-native/src/FishSpinner.native.tsx`, `useSwim`) now keeps
both loops alive, still with no per-frame JS:
- **Started again** whenever the app comes to the front (`AppState` "active") and on the spinner view's first layout.
- **A dropped loop is detected**: `Animated.loop` on the native driver sends `iterations: -1` to the platform and DROPS its
  end callback (RN 0.88 `loopImpl` → `_startNativeLoop`), so a loop iOS stopped was invisible to JS. Each loop is now one
  `Animated.timing({ iterations: -1, useNativeDriver: true })` — the same native animation — with its end callback: an
  end nothing here asked for starts both again 1 s later (in front), or at the next "active" (in the background).
- **Still slot** (owner ~23:50): `app/packages/ui-native/src/stillIcon.ts` `STILL_ICON` (null, TODO T814) — a square
  grayscale+alpha picture, tinted like the strip; once set, both spinner halves draw it in the fish's place under Reduce
  Motion. Until then the fish's first frame, as before.
- Tests: `FishSpinner.native.test.tsx` (Animated.timing / AppState stand-ins; first layout, "active", a dropped loop,
  background, unmount, Reduce Motion, the still slot on both halves); each restart path mutation-checked (fails without it).
- Simulator: `t814-bg` gained a first step: first download, the fish, home for 10 s, back, the fish.
- `pnpm check` was red on this branch from `build36.mjs` (T821's ffmpeg, its skip sites): `knip.jsonc` `ignoreBinaries: ["ffmpeg"]`
  and a `no-silent-skips` allowlist line (5 sites), each with its reason.

**Spinner screens with NO visible sign beside the fish** (label aloud only) — under Reduce Motion these show a still
picture and nothing else: the water "updating" carry (`Shell.tsx` ~629), the water sheet / regulations loading
(`WaterScreen.tsx` 224 / 306; words only on a slow line), Conditions view loading (`WaterViews.tsx` 116), search busy
(`SearchSheet.tsx` 210), Conditions "reading the water" / record (`ConditionsPanel.tsx` 208 / 317), the glossary
(`GlossaryPage.tsx` 51), the top bar's sync check (`TopBar.tsx` 235, around the glyph). Every other spinner has words or a
bar beside it; the first download shows the pun, "Downloading the map and regulations for offline use", and once the
sizes are known the bar + "X of Y MB downloaded" (setup: its own bar + "… · N %"). Nothing added there (owner: a still icon
is fine); for the owner's still icon to decide.
