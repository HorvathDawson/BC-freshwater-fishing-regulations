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
