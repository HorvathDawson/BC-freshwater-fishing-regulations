# What renders in tests, and what does not

`vitest` (`app/vitest.config.ts`) mounts `@app/ui-native` through react-native-web under jsdom, so every
component here, the SVG ones included, is really rendered and asserted. It is the renderer mobile web ships.
Two pieces of config make that work:

- Platform extensions: `resolve.extensions` puts `.web.*` first, as Metro and webpack do. react-native-svg
  imports `./elements` with no extension and ships a native `elements.js` (which pulls in Flow-typed
  `react-native/Libraries/...` and fails to parse) beside `elements.web.js` (DOM). Its entry point is aliased by
  hand because the package declares no `browser` field.
- `matchMedia`: `app/tools/render-setup.ts` installs a stub. react-native-web reads it at module load to answer
  `AccessibilityInfo.isReduceMotionEnabled()` and resolves `true` when it is missing, so every component would
  silently render its reduced-motion state. The stub reads a mutable flag through a getter; tests flip it with
  `globalThis.setReduceMotion`.

## Not covered

- Native rendering: `expo run:ios` and `run:android` compile these components against the real platform views,
  and nothing here proves they match react-native-web. No component in this package holds logic, so the gap is
  narrow, but it is a gap. Closing it needs a Metro-based runner (`jest-expo`), a second test stack.
- A map is never mounted under jsdom: maplibre-gl calls `URL.createObjectURL` at module load, which
  `render-setup.ts` stubs so an import of `@app/map` can be collected.

## The loader

`FishSpinner.native.tsx` drives its animation with `useNativeDriver`; react-native-web has none, so the web
build is `FishSpinner.web.tsx`, two CSS animations over the same baked sprite with no JS loop.
`tools/no-per-frame-animation.test.ts` asserts the loader runs no per-frame JS.
