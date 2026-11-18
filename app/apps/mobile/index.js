// Expo entry. Deliberately NOT under src/: this file is bundler plumbing, not app code,
// and src/ is what tools/check-boundaries.mjs polices.

// Web-only dev runtime (Fast Refresh, the error overlay, the dev-server client). It is a
// no-op on native, and must come FIRST — after the app mounts it is too late to install.
import "@expo/metro-runtime";

// registerRootComponent handles all three targets: AppRegistry.registerComponent on
// native, plus mounting into the DOM root on web. One entry for ios / android / web, so
// the three cannot drift.
import { registerRootComponent } from "expo";
import { AppState } from "react-native";

// (T74) The grey-frame probe (@app/map `native/probe.ts`): a development build started with
// EXPO_PUBLIC_COLOUR_PROBE=1 logs, per pan, the frames with a section not yet coloured. Off otherwise.
if (__DEV__ && process.env.EXPO_PUBLIC_COLOUR_PROBE === "1") globalThis.__colourProbe = true;
// (T273) The pan probe (@app/map `native/panProbe.ts`): the test bed's build (EXPO_PUBLIC_PAN_PROBE=1) logs, per busy
// second, the JS thread's lag, the tiles labelled and the feature-state round trips. Off otherwise.
if (process.env.EXPO_PUBLIC_PAN_PROBE === "1") globalThis.__panProbe = true;
// (T286) The open probe (@app/data `openProbe.ts`), on the same test-bed switch: per map tap, when each stage of the
// water's Regulations arrives and the SQLite calls made meanwhile. Off otherwise.
if (process.env.EXPO_PUBLIC_PAN_PROBE === "1") globalThis.__openProbe = true;
// (T287) The stall log, on the same test-bed switch: every time the JS thread is held more than 50 ms (a 16 ms timer's
// overshoot), one line `[stall] {"at":<epoch ms>,"ms":<n>}` — what froze is read off the log lines around it. Off otherwise.
if (process.env.EXPO_PUBLIC_PAN_PROBE === "1") {
  let due = Date.now() + 16;
  // timers stop while the app is in the background: that gap, up to the first tick once active again, is not a stall
  let away = false;
  AppState.addEventListener("change", (s) => { if (s !== "active") away = true; });
  setInterval(() => {
    const now = Date.now(), late = now - due;
    due = now + 16;
    if (away) { away = AppState.currentState !== "active"; return; }
    if (late > 50) console.log(`[stall] ${JSON.stringify({ at: now, ms: late })}`);
  }, 16);
}
// (T360) The heap probe, on the same test-bed switch: every 2 s, the JS engine's own heap (Hermes' instrumented stats,
// bytes), so a `dumpsys meminfo` Native Heap can be split into JS and the rest (map, SQLite). One line `[heap] {…}`.
if (process.env.EXPO_PUBLIC_PAN_PROBE === "1" && globalThis.HermesInternal?.getInstrumentedStats) {
  setInterval(() => {
    const s = globalThis.HermesInternal.getInstrumentedStats();
    console.log(`[heap] ${JSON.stringify({ at: Date.now(), size: s.js_heapSize, used: s.js_allocatedBytes, ext: s.js_externalBytes ?? null })}`);
  }, 2000);
}
// (T716 item 3) An experiment build: a map tap's page shown at the finger's lift, not held for the click (measured for the
// owner's decision, never a store build: EXPO_PUBLIC_TAP_AT_LIFT=1)
if (process.env.EXPO_PUBLIC_TAP_AT_LIFT === "1") globalThis.__tapShowAtLift = true;
// (T420) An experiment build: the map asks for no atlas tile events (`MLRNTileIds`), so the pan's cost without the colouring's
// per-tile pipeline can be measured (the sections are then not coloured). Never a store build.
if (process.env.EXPO_PUBLIC_TB_NO_TILE_EVENTS === "1") globalThis.__tbNoTileEvents = true;
// (T287) The benchmark's "before": release files hashed and inflated in one go (EXPO_PUBLIC_T287_BEFORE=1).
if (process.env.EXPO_PUBLIC_T287_BEFORE === "1") globalThis.__hashOneShot = true;
// (T286) The benchmark's "before": regulations read one row per call and no page held (EXPO_PUBLIC_REGS_ONE_BY_ONE=1).
if (process.env.EXPO_PUBLIC_REGS_ONE_BY_ONE === "1") globalThis.__regsOneByOne = true;

import App from "./src/App";
import { holdSplash } from "./src/splash";
import { withGestureRoot } from "./src/gestureRoot";

// (T244) keep the native splash (the loading fish) up until the boot screen's spinner is drawn: src/splash.ts
holdSplash();

// Gestures (G1) need their root view above everything.
registerRootComponent(withGestureRoot(App));
