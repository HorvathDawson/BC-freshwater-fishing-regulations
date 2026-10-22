# Layers menu B, as built (2026-10-21)

These are web shots (react-native-web in headless Chromium). The Expo dev server ran from this worktree on port 39872. The main checkout's `serve-tiles.mjs` ran on port 39871 and served release r-6960fe8201e3. The release carries no lake values, so Stocked and Depth show as "No data yet".

| shot | what |
|---|---|
| `390-light-map-closed.png` | The Map tab. The legend strip wraps at 390 px, and every status is visible. Reserve names (Squiaala 7, Skowkale 10, …) are drawn: Indigenous land is always shown. |
| `390-light-layers-top.png` / `-bottom.png` | The sheet: Map · Colour the water by (one choice for all water) · On the map · the "Always shown:" line |
| `390-light-layers-private-on.png`, `390-light-map-badge.png` | The Private land switch is on, and the rail's Layers button shows a badge of 1 |
| `390-light-layers-satellite.png` | Satellite is chosen and online (Esri), and the badge reads 2 |
| `390-light-layers-satellite-offline.png` | Offline: Map is tagged SHOWING. Satellite stays chosen, greyed and dashed, with NEEDS A CONNECTION and the reason line. |
| `390-light-menu-display.png`, `390-dark-menu-display.png` | The palette's new home: the hamburger menu, then Display |
| `390-dark-*.png` | The same sheet and map in the dark theme |
| `1280-map-closed.png`, `1280-layers.png` | Desktop. The sheet spans the window, as every `Sheet` does today. |

These were not shot: native (Android/iOS), which needs an emulator, a heavy job this lane may not run; and a release that carries lake values (none exists). The Stocked and Depth legends are covered by tests (`LegendStrip.test.tsx`, `LayersSheet.test.tsx`).
