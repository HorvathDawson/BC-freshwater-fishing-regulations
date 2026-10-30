# T282 — Web app bundle: 4 MB delays the app taking over (43 s on Fast 3G)

Status: DONE 2026-10-30 — 3144e6fb7, 3c2a23c1d, d5a24fc3d (found by T269 on 2026-10-29). The bundle was not what held the hand-over: see "Outcome".

## Work
Measure the web export's JS by package (source-map-explorer or equivalent). Split what a first screen doesn't need
(conditions charts, runs, menu pages, walkthroughs, review/debug code, large word tables) into lazy chunks; drop dead weight
(dev-only code, duplicate libs); keep the map + regs pane path first. No look change.

## Acceptance
Before/after bundle sizes and Fast 3G time to "app took over" on /water/<slug> and on /; all checks green.

Depends: T269. Size: M. Owner: no.

## Outcome (2026-10-30)
- **What the main bundle held** (source-map attribution, `expo export --source-maps`): maplibre-gl 1,027 KB of ~3.3 MB,
  then core 401, ui-native 294, react-native-web 250, data 211, react-native-gesture-handler 186, react-dom 175.
- **One later chunk** (`ui-native/src/later.ts`, loaded through `part()`): the map (`MapScreen` → maplibre-gl), menu pages,
  More, Spots, an open spot / spot capture, a water's Conditions and Map views, the rarer sheets (obstacle, tidal, not
  fishing, runs, sync errors). ONE module on purpose: per-component `import()`s made Metro add `__common` and
  `__expo-metro-runtime` script tags to index.html, which the stored SEO water pages do not name (relink could not map
  them); with one chunk the page keeps its single script tag.
- `part()` has no Suspense and no fallback: a part draws nothing until its code is in (a closed sheet draws nothing
  anyway), a closed sheet fetches nothing (`when`). A phone asks for every part at once (one bundle, no change). The
  web asks at once on any start but a water's own page (`startLink.web`), else once the line is quiet (Shell).
- Metro does not tree-shake: the ui-native barrel no longer exports the later parts, and `@app/map/colours` (the
  map's colours, layer switches, numbers, endpoints; no `Map`) is what legends, the theme and the zoom handover import.
- **Bytes** (staging export): main `index` 3,538,123 B raw / 982,825 B gzip → **2,195,246 / 634,150 (−38 % / −35 %)**;
  `later` 1,362,489 / 355,271 (the map and the rare parts); `highlightPdf` unchanged.
- **Fast 3G (DevTools preset), staging, `bad-net.mjs --profiles fast-3g --steps takeover`:**

| | `/water/vedder-river` → app took over (pane handed over) | `/` → map chrome |
|---|---|---|
| before (staging 67ba…/1756…) | 39.6 s | 14.3 s |
| after (staging a588…) | 37.7 s / 37.0 s | 13.8 s / 13.8 s |

- **Why the water page barely moves:** its hand-over waits for the release, not the code. On Fast 3G the 647 kB bundle
  is in at ~0 s; then `core` 1,380 kB (@20 s), `water` 2,223 kB (@25 s), `regs-2` 266 kB (@29 s) and the sql.js wasm
  332 kB. A water landing reads the WHOLE core and water files: the T256 region slices are used only when a map region
  is focused, and a landing has no map. Follow-up ticket: a water landing focuses its own region (the page knows its
  water; the region could ride in a `<meta>` like `cift:item`), so it reads `core-<r>` / `water-<r>`.
- Staging deployed by the fast path (export, relink 39,268 water pages, site.sh). Smoke: all pass but the two map
  taps, which T303's locate hint covers (not this change; the test bed now dismisses it, the smoke does not).
