/**
 * (T244) THE NATIVE SPLASH'S HAND-OFF. The native splash (expo-splash-screen: the loading fish's frame 0 on the boot
 * screen's ground, `pnpm splash`) is held from the first instant (`holdSplash`, called from index.js before the app
 * mounts) and let go by `releaseSplash` once the boot screen's own spinner is on screen — drawn by `App` at the same size
 * and the same centre, its first frame being the picture the native one shows — so there is no jump between them.
 *
 * Idempotent, and never forever: `holdSplash` arms a time limit, so a boot that never reaches the loading screen (a
 * crash in render, a failed screen that draws no spinner) still lets the splash go. On the web both are no-ops (the
 * package's web stubs): a browser has no native splash.
 */
import * as SplashScreen from "expo-splash-screen";

/** The longest the native splash is held without the boot screen being up. */
const HOLD_LIMIT_MS = 8000;

let released = false;

export function holdSplash(): void {
  SplashScreen.preventAutoHideAsync().catch(() => {});
  SplashScreen.setOptions({ fade: true, duration: 150 });
  setTimeout(releaseSplash, HOLD_LIMIT_MS);
}

export function releaseSplash(): void {
  if (released) return;
  released = true;
  SplashScreen.hide();
}
