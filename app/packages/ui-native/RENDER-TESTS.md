# What renders in tests, and what does not

`vitest` mounts `@app/ui-native` through **react-native-web** (see `vitest.config.ts`), so
every component in this package is really rendered and really asserted — including the ones
drawing SVG. This is the same renderer mobile web ships, so what the tests exercise is what
a phone browser runs.

Two pieces of config make that work, and both are worth knowing about before touching them.

## Platform extensions

`resolve.extensions` puts `.web.js` **first**. That is what Metro and webpack do for
react-native-web, and vite does not do by default.

react-native-svg ships `elements.js` (native) beside `elements.web.js` (DOM) and imports
`'./elements'` with no extension, trusting the bundler to pick. Take the native file and it
imports `react-native/Libraries/Utilities/codegenNativeComponent.js`, which is **Flow**, not
TypeScript — and the parse error surfaces a hundred modules from the actual cause. The
package declares no `browser` field, so its entry point is aliased by hand for the same
reason; everything downstream of it resolves itself.

## `matchMedia`

`tools/render-setup.ts` installs a stub. jsdom has none, and react-native-web reads
`matchMedia` **at module load** to answer `AccessibilityInfo.isReduceMotionEnabled()` —
resolving to `true` when it is missing. Without the stub every component would render in
its reduced-motion state and the animated paths would never be exercised at all, silently.

The stub reads a mutable flag through a getter, because react-native-web keeps one
MediaQueryList for the life of the module. Tests flip it through `globalThis.setReduceMotion`.

## What is still not covered

Native rendering. `expo run:ios` / `run:android` compile these components against the real
platform views, and nothing here proves those behave like react-native-web. That gap is
narrow — no component in this package holds logic — but it is a gap, not coverage.

Closing it means a Metro-based runner (`jest-expo`), which is a second test stack. Worth
revisiting when the native surface is more than views and a chart.

## Known gap: the animation driver on web

`useNativeDriver: true` is honoured on iOS and Android. react-native-web has no native
animated module, so it logs a warning and drives the loop from JS — visible in the browser
console the first time the app was actually loaded.

What the baked sprite buys is unchanged and is the larger half: the fish's spine, fins,
tail and bubbles are not recomputed per frame on any platform. What is still JS on web is
the transform interpolation itself.

The fix is a `FishSpinner.web.tsx` running a CSS `steps(15)` keyframe over the same strip —
the sprite is already laid out for exactly that, and `resolve.extensions` here would pick
the web file up automatically. Not written. Worth doing before the web app ships, not
before the native one does.
