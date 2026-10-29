// Expo entry. Deliberately NOT under src/: this file is bundler plumbing, not app code,
// and src/ is what tools/check-boundaries.mjs polices.

// Web-only dev runtime (Fast Refresh, the error overlay, the dev-server client). It is a
// no-op on native, and must come FIRST — after the app mounts it is too late to install.
import "@expo/metro-runtime";

// registerRootComponent handles all three targets: AppRegistry.registerComponent on
// native, plus mounting into the DOM root on web. One entry for ios / android / web, so
// the three cannot drift.
import { registerRootComponent } from "expo";

// (T74) The grey-frame probe (@app/map `native/probe.ts`): a development build started with
// EXPO_PUBLIC_COLOUR_PROBE=1 logs, per pan, the frames with a section not yet coloured. Off otherwise.
if (__DEV__ && process.env.EXPO_PUBLIC_COLOUR_PROBE === "1") globalThis.__colourProbe = true;
// (T273) The pan probe (@app/map `native/panProbe.ts`): the test bed's build (EXPO_PUBLIC_PAN_PROBE=1) logs, per busy
// second, the JS thread's lag, the tiles labelled and the feature-state round trips. Off otherwise.
if (process.env.EXPO_PUBLIC_PAN_PROBE === "1") globalThis.__panProbe = true;

import App from "./src/App";
import { holdSplash } from "./src/splash";
import { withGestureRoot } from "./src/gestureRoot";

// (T244) keep the native splash (the loading fish) up until the boot screen's spinner is drawn: src/splash.ts
holdSplash();

// Gestures (G1) need their root view above everything.
registerRootComponent(withGestureRoot(App));
