/**
 * jsdom has no `matchMedia`, and react-native-web reads it AT MODULE LOAD to answer
 * `AccessibilityInfo.isReduceMotionEnabled()`. With it missing that call resolves to
 * `true` — so without this file every component under test would render in its
 * reduced-motion state and the animated path would never be exercised.
 *
 * The stub reads a mutable flag through a getter rather than capturing a boolean, because
 * react-native-web holds one MediaQueryList for the life of the module. That lets a test
 * flip `reduceMotion` and have the next query see it.
 */
declare global {
  var reduceMotion: boolean;
  var setReduceMotion: (on: boolean) => void;
}
globalThis.reduceMotion = false;

const listeners = new Set<(e: { matches: boolean }) => void>();

globalThis.matchMedia = ((query: string) => ({
  get matches() {
    return query.includes("prefers-reduced-motion") ? globalThis.reduceMotion : false;
  },
  media: query,
  onchange: null,
  addEventListener: (_: string, fn: (e: { matches: boolean }) => void) => { listeners.add(fn); },
  removeEventListener: (_: string, fn: (e: { matches: boolean }) => void) => { listeners.delete(fn); },
  addListener: (fn: (e: { matches: boolean }) => void) => { listeners.add(fn); },
  removeListener: (fn: (e: { matches: boolean }) => void) => { listeners.delete(fn); },
  dispatchEvent: () => true,
})) as unknown as typeof globalThis.matchMedia;

/**
 * Flip the preference and tell everyone listening, the way a real browser would.
 *
 * Hung on the global rather than exported, because a test importing it would reach out of
 * its own package into tools/ — which is exactly the boundary violation the checker exists
 * to stop. Tests declare the shape locally instead.
 */
globalThis.setReduceMotion = (on: boolean) => {
  globalThis.reduceMotion = on;
  for (const fn of listeners) fn({ matches: on });
};

/**
 * jsdom has no `URL.createObjectURL`, and maplibre-gl calls it AT MODULE LOAD to spin up
 * its worker. Any test that so much as imports `@app/map` dies during collection with
 * "no tests" — which reads like a broken test file rather than a missing browser API.
 *
 * The worker never runs here (nothing mounts a real map under jsdom; see RENDER-TESTS.md),
 * so the URL only has to exist.
 */
if (typeof URL.createObjectURL !== "function") {
  URL.createObjectURL = () => "blob:jsdom/00000000-0000-0000-0000-000000000000";
  URL.revokeObjectURL = () => {};
}
