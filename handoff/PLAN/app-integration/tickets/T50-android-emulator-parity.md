# T50 — Android emulator: the app matches the phone web app, and the system bars never cover the UI

Status: OPEN — ticket 7da6deff, 294dd4d5. Runs after the mass cleanup (T61–T63), heavy, alone.

Phase 4 · After the mass cleanup (owner 2026-10-22) · Depends on T48 (new top/bottom bars) merged · HEAVY (emulator + Gradle): run alone through heavy.sh · Size 1–2 days

## Owner (2026-10-22)
"could you also try to run android simulations on an emulator? to make sure mobile webapp is the same as android. also
it needs to make sure the nav bar and top bar of mobile doesn't affect ui etc."

## Setup that exists
Android SDK at ~/Library/Android/sdk (platform-tools, emulator, build-tools, ndk, cmdline-tools); AVD `n1_pixel` from
the N1 spike (handoff/PLAN/app-data/N1-SPIKE.md: Vulkan vs OpenGL variants, adb-driven taps, emulator ≈ 2.5–3× slower than
the laptop on keyed reads). adb/emulator are not on PATH: use $HOME/Library/Android/sdk/{platform-tools,emulator}.

## Work
1. A development build (`expo prebuild` + `expo run:android`, or the documented `pnpm -C app/apps/mobile android`) on the
   emulator, data from the local release server (the emulator reaches the Mac at 10.0.2.2; never stop :39217/:39218 — run
   a separate server on another port if the live one can't be reached).
2. PARITY WALK: the same screens as the T41 walk and the browse screens — Map tab, Conditions map, a water (Regulations,
   Conditions, Map views), sheets, search, Spots, More, Layers — screenshotted on the emulator and on Expo web at the same
   logical size (412×915 for a Pixel), compared side by side; list every difference (layout, fonts, colours, gestures,
   map colouring — native still uses the pushed colouring path until the native engine ticket).
3. SYSTEM BARS: status bar (top), gesture nav bar and 3-button nav bar (bottom), display cutout, edge-to-edge on Android
   15+: nothing the app draws sits under them or is cut — the top search bar, the bottom tab bar, sheets' bottoms, the side
   rail, the map's attribution and zoom controls, the regs pin bar; the keyboard (search) doesn't cover the field or the
   results. Test both nav modes (`adb shell cmd overlay enable com.android.internal.systemui.navbar.threebutton` /
   gestural), portrait and landscape, light and dark status bar icons readable on the app's colours.
4. Android BACK on the device: walk the back table (app/HANDOFF-ui.md "Back, everywhere") with `adb shell input keyevent 4`.
5. Fix what's wrong (safe-area insets through one place — the frame/bars own them, components never read insets
   directly), test-first where a jsdom/render test can pin it; record what only a real phone can judge (feel, GPU).
## Acceptance
- Side-by-side shots (emulator vs web) for every screen in the walk, in handoff/PLAN/app-integration/T50-shots/, with a
  differences table: fixed / deliberate (cited) / open.
- No UI under a system bar in either nav mode, portrait or landscape; back walk passes on the device.
- The emulator and Gradle daemons are stopped afterwards (memory).

## Found by T172 (Android smoke, 2026-10-23 — shots in handoff/PLAN/app-integration/T172-shots/)
- STATUS BAR SWALLOWS THE SEARCH FIELD: the status bar is 136 px (Pixel, API 35) and the top bar's field sits at y 30–146,
  so only its bottom 10 px take a tap — tapping the field's middle does nothing (`mInputShown=false`). (03, 04)
- STATUS BAR COVERS HEADERS: "‹ Map" / "‹ Spots" / "‹ Back" back links, the Spots page title, and the spot page's Edit /
  Delete sit under the clock and icons (Delete is under the battery icon, untappable). (05, 09, 10, 13)
- GESTURE HANDLE OVER THE TAB BAR: the tab bar ends at the screen bottom (2273–2389 of 2400); the gesture handle is drawn
  across the CONDITIONS / SPOTS labels. Spot-capture full-width buttons (Next, Save spot) touch the handle. (03, 12)
- T145 ON A DEVICE: tapping a spot pin does NOTHING — the `Map.native.tsx` `onPress` guard (`spotUnder`) correctly stops
  the map's tap, but the pin's own press (the `Marker` child's `spots.face` press) never arrives on Android, so the spot
  neither opens nor selects. Tap-off-clears works. (14, 15, 16)
- NATIVE MARKERS ESCAPE THE MAP FRAME: after a drag on a water's Map view the spot pin and its label were drawn at the
  top of the SCREEN, over the header and status bar, outside the map's box (view annotations not clipped). (18)
- STALE HELD RELEASE → HARD ERROR: a device holding an older installed set (the N1 spike's r-6960fe8201e3) opens to "The
  map data could not be loaded. paint: version 1 is not 4" and stays there; no fallback to install the served release.
  An app update over an old install would do the same. Cleared with `pm clear`. (01)
- T175 NOT VERIFIED: zooming out on a small creek needs a pinch; adb `input` is single-pointer and a one-finger
  double-tap-drag only panned. Use the emulator window (or multitouch sendevent) here. (17–19)
- DEV-BUILD WARNINGS: "SafeAreaView has been deprecated" ×5 (RN's SafeAreaView is in use — the insets work here should
  move to react-native-safe-area-context); MapLibre "Source 'atlas' / 'gauges' / 'water-copy' is in use, cannot remove"
  ×6 each (a style swap removing live sources); "Invalid geometry in line layer" ×1. The LogBox toast these raise
  swallows taps on the tab bar under it (dev only).
- DEV SETUP: the debug build reads JS from localhost:8081 (no expo-dev-client), so Metro on another port needs
  `adb reverse tcp:8081 tcp:<port>`; the AVD `n1_pixel` boots in airplane mode (`adb shell cmd connectivity
  airplane-mode disable`); no location fix is set (Spots says "Your location is not available here" — set one with the
  emulator console/window before judging At my location).


## Owner 2026-10-27
Native gestures and navigation must MATCH the web app (swipes on touch only, back behaviour, tab order, part/stretch navigation).

## Gesture/navigation audit 2026-10-27 (no device)
Native and web run the SAME gesture code (ui-native: ViewSwipe, EdgeBack, Sheet pan, useBack via systemBack.native /
.web). The only platform splits are deliberate: `touchDevice` (web swipes on a coarse pointer only; native always),
`escapeBack`/`escapeCloses` (desktop-web only), `searchFocus`. Android back + browser back + sheet/swipe render tests
all pass (114 tests). Nothing found to change without a device. Left for the emulator/phone walk: step 4 (back table
via `adb input keyevent 4` and the edge gesture), view-swipe vs system edge gesture on a real device, sheet pan vs
scroll hand-off, predictive back (manifest has `enableOnBackInvokedCallback=false`; judge before enabling).


## T74 (2026-10-27)
Native now colours through the web's engine (the pushed path is gone), so the parity walk compares like with like.
Emulator dev setup that worked: `scripts/heavy.sh … session.sh` boots `n1_pixel`, `expo run:android --port 8093` with
`adb reverse tcp:8081 tcp:8093`; if adb lists a stale OFFLINE emulator beside the new one ("more than one device"),
`adb kill-server` first; `--device emulator-5556` is refused by expo (it wants the AVD name). After a `pnpm patch-commit`
Metro must be restarted (it served stale JS). Location: `adb emu geo fix <lon> <lat>`. Found: place search "Vedder
Crossing" → "Could not read what is near this town" on native.


## T74 follow-ups on the emulator (2026-10-27; shots `T50-shots/t74f-*`)
Fixed (native bugs):
- BOUNDARY MARK LABELS wrapped to 44 dp ("fish / ing b / ou…"): Android's view annotation lays content out no wider than
  the marker's ROOT. The root is now the 180 px label band with the 44 px button centred; labels shift inside the map as
  the web's `labelShift` does (`markShift`, from `project` where the camera settles). (03 before/after/web)
- RULE BOOK LINK on Android opened an EMPTY share sheet: `Linking` refuses file://, `Share` drops `url` on Android, so the
  T215 highlighted copy never reached a viewer and the fallback never fired. Android now takes the plain link (the PDF in
  Chrome, no highlight). Highlight on Android needs a VIEW intent with a read grant (expo-intent-launcher or similar):
  owner decision (a native package). (04)
- STATUS BAR over the top bar / back links / Close (T172 finding): App pads by `StatusBar.currentHeight` on Android
  (RN's SafeAreaView pads iOS only). Top fixed; the BOTTOM gesture/nav bar over the tab bar is still open. (05)
Confirmed, no change:
- Gauge pill draws on native (Conditions, "p5th" at Vedder Crossing, same as web). (01)
- "Vedder Crossing → Could not read what is near this town": NOT reproduced on release r-f7faa483d2b1 (cold start, Map
  and Conditions tabs, "Vedder" and the full name, after Spots/At my location): 147 within 25 km, as web. The T74 run held
  the previous release; the failure was that set's, not the read path (expo-sqlite opens `near` in place fine). (02)
- Parts list, downstream/upstream, mark tap steps the part, regs tabs (Kootenay Upper West Arm) incl. the stuck pin bar,
  water Conditions, Salmon toggle (same colours as web: all red at the Chilliwack — DFO extent "not yet mapped", data). (06, 07)
Open (listed, not fixed):
- Bottom inset: gesture handle still drawn over the tab bar labels (needs real insets: react-native-safe-area-context).
- Dev: `CI=1` turns Metro's file watcher OFF ("reloads are disabled") — that is the "stale JS after patch-commit" above;
  run Metro without CI for a live session. `openBook`'s console.warn raises a LogBox toast on every Android rule-book tap
  (dev only).
- Not walked this session: a size-tier water (boxed tier + sticky size header, 9849276b), the place row one-line check
  at large text scale.

## Packages approved (owner 2026-10-27) — insets + Android PDF (shots `T50-shots/t50b-*`)
- `react-native-safe-area-context` 5.7.0: App.tsx owns the insets (`SafeAreaProvider` + its `SafeAreaView`, every edge),
  replacing RN's iOS-only SafeAreaView and the interim `StatusBar.currentHeight` pad. Emulator: top bar below the status
  bar, tab bar above the gesture handle AND above the 3-button bar (t50b-insets-gesture / -3button).
- `expo-intent-launcher` 57.0.1: Android opens the HIGHLIGHTED copy (VIEW intent, expo-file-system's content:// URI,
  FLAG_GRANT_READ_URI_PERMISSION) — opened in the Drive PDF viewer; the written file carries the /Highlight on PDF page 24
  (Chilliwack p.22). No PDF app → `openBook` falls back to the plain link. (t50b-pdf-android-viewer)
- Walked: the size box on Chilliwack part 2 (boxed tier, sticky header bounded to the box) and the regs place row at
  font scale 1.3 (two lines only where the name does not fit; Today stays right). (t50b-sizebox*, t50b-fontscale)
Still open:
- Sheets are Modals (their own window): in 3-button mode the sheet's 28 dp bottom padding lies under the nav bar (the
  content clears it; t50b-sheet-3button). Fix inside `Sheet` if a sheet ever ends in a button.
- A font-scale change restarts the activity (`fontScale` not in the manifest's `configChanges`) and the app reopens on the
  Map, losing the open water.
- Landscape and the back table (step 4) still not walked.
