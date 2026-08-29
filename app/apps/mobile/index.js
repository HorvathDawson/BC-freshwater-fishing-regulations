// Expo entry. Deliberately NOT under src/: this file is bundler plumbing, not app code,
// and src/ is what tools/check-boundaries.mjs polices.

// Web-only dev runtime (Fast Refresh, the error overlay, the dev-server client). It is a
// no-op on native, and must come FIRST — after the app mounts it is too late to install.
import "@expo/metro-runtime";

// registerRootComponent handles all three targets: AppRegistry.registerComponent on
// native, plus mounting into the DOM root on web. One entry for ios / android / web, so
// the three cannot drift.
import { registerRootComponent } from "expo";

import App from "./src/App";

registerRootComponent(App);
