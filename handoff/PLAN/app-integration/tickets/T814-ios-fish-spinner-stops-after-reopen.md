# T814 — iOS: after the app is closed and reopened mid-download, the fish spinner stops moving

Status: DONE (94773621a; restart fix landed in f7b41c208; the coho loader (owner's dark look A) landed 2026-11-17 11:26; owner re-tests on the 0.7.12 build).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T814 | iOS: after closing and reopening mid-download, the fish spinner stops moving | DONE | 94773621a | owner iPhone build 36, 2026-11-16 |

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

## Owner, 2026-11-17 about 00:10 to 00:14 (app chat), verbatim
"it had reduce motion on." — so the second iPhone's still fish was the Reduce Motion design, not a bug in the loop.
"we dont want a full still item for reduce motion on.. i am making a new loader icon that will replace the loading
circle on the splash page or curtain (it will still be used for all other loading." Then he sent the loader: a Python
generator (`fish_loader.py`, Pillow; a linocut spawning coho, 80 frames, 480×200, 8×10 sprite sheet, 20 fps, 4 s loop,
ink #1f2e3b + coho red #cf5a3c, same linocut style as the app icon). Then: "we will ignore reduced motion for this".

So:
- The still-icon slot (`stillIcon.ts`, cdbdfed16) goes; no still image anywhere.
- The new coho loader replaces the circling fish on the splash / first-download / curtain screens only; the circling
  fish stays for every other loading place.
- Both loaders ignore Reduce Motion: they always move. (The generator's still frame and its reduced-motion CSS are not
  used.)
- The restart fix (loops start again on app-active, first layout, a dropped loop) stays.
- The generator is committed under `app/tools/` and run to make the sheet, like `pnpm spinner`.

## 2026-11-17 ~01:50 MST, the coho loader built (branch app-fix/t814-loader, 78d049d40)
Owner 2026-11-17 ~00:10–00:14 (app chat): the still-icon slot goes; his linocut coho loader replaces the circling fish
on the splash / first-download / curtain screens only; both loaders ignore Reduce Motion; the restart fix stays.
- **Generator**: `app/tools/fish-loader/fish_loader.py`, his script as sent (defaults reproduce his output byte for
  byte); app options added (`--format webp`, `--name`, `--sheet-only`). `pnpm coho:build` (`app/tools/build-coho.mjs`)
  bakes 80 frames at 288×120 px (drawn 192×80 dp, 1.5 px/dp), 8×10, lossless WebP (887 KB), one sheet per
  `color.ui.coho.ink` + `.red` pair (new tokens; `color.ui.coho.ground` drawn behind) → `packages/ui-native/src/coho/`,
  `coho.generated.ts`. `pnpm coho` (in `pnpm check`) reads the committed sheets back (`sheet_facts.py`: size + the two
  colours) against the tokens; it never re-draws (Mac vs Linux byte-equality unproven).
- **Component**: `CohoLoader` (native: one platform-driven value, `translateX`/`translateY` staircases on the sheet
  behind a clip, `spriteGridStaircase`; web: two CSS `steps()` keyframes), via `LoadingState coho`, used only by the
  boot screen (`App.tsx` `BootScreen`: first launch, manifest check, Wi-Fi wait, first download, setup, start-time
  apply) and `ApplyCurtain` (T746); `tools/one-spinner.test.ts` holds that. Words unchanged.
- **Restarts**: `usePlatformLoops` (the cdbdfed16 restarts: app-active, first layout, a dropped loop), both loaders.
- **Reduce Motion** stops neither loader; `stillIcon.ts` and its tests removed.
- **OS launch image** (`assets/splash/*`, `tools/build-splash.mjs`) untouched: a launch image cannot animate, so the
  hand-off from the native splash (the circling fish's frame 0) to the coho now changes picture.
- **Memory**: decoded sheet 11.1 MB (his default 480 px sheet: 30.7 MB; the circling fish strip 2.2 MB). Emulator A/B,
  same session, base cdbdfed16 vs coho: first-download peak PSS 170 → 152 MB (Java 36 → 33, native 41 → 40); boot
  screen (dark, 5 s in) PSS 187 → 175, native 40 → 46; `memory` cold start → map PSS 269.7 → 274.2, native
  125.5 → 134.6 (the sheet kept in Fresco's cache). Later `memory` steps: coho PSS +20–40 MB, all in "System" (native
  equal or lower); not attributed to the loader, a re-run would tell.
- **Checks**: `CANIFISHTHIS_R8=1 pnpm check` green (664 files, 6963 tests); CohoLoader native + web tests; restart
  path mutation-checked. Seen moving on the Android emulator, the iOS simulator and the web export.
- **OPEN, owner's pick: the dark theme.** His ink #1f2e3b disappears on the dark card; dark currently wears the light
  look (pending). Options pictured: A paper-coloured ink on the dark card, B his colours on a paper tile. Either is a
  token change in `themes/dark.json` + `pnpm coho:build`.

## Owner, 2026-11-17 ~08:45 (app chat), the dark-mode look, verbatim pick
Shown: light as built, dark as it was (ink lost on the dark screen), A (paper-coloured ink #f2e8d5 on the dark screen,
red kept) and B (his colours on a paper tile). Picked "A: paper-coloured ink". So dark.json `color.ui.coho.ink`
#F2E8D5, ground `@color.ui.card`; light unchanged.
