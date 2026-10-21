# G1 — gestures: what landed, what was checked, what is the owner's to feel

Ticket: `../G1-gestures.md`. Library: `react-native-gesture-handler` 2.32.0 (Expo SDK 57's pin), callbacks on JS
(`runOnJS(true)`), no reanimated.

## The layers

| layer | file | what |
|---|---|---|
| core | `app/packages/core/src/gestures.ts` | every threshold and decision: `TAP_SLOP` 8 px, `EDGE_BACK` {zone 24, commit 80, flick 500 px/s}, `SWIPE` {lock 2:1, 60 px, flick 500 px/s after 20 px}, `SHEET_DRAG` {⅓ height, flick 800 px/s}, `edgeBackEnabled(os)` (iOS only), `partSwipeStarts` (never inside either 24 px edge zone), `classifySwipe`, `stepPart`, `sheetDismiss`, `follow` (resistance at an end), `MAP_GESTURES` |
| ui | `app/packages/ui/src/gestures.ts` | `useBack` (the one back; dispatches `{type:"back"}`; reports whether it popped), `backDepth`, `historySync` (browser history ↔ route), `SystemBack` (the platform glue's type), `useEdgeBack`, `useSheetDrag`, `usePartSwipe` / `partSwipeAction` (→ `{type:"part"}`) |
| ui-native | `EdgeBack.tsx`, `PartSwipe.tsx`, `Sheet.tsx` | RNGH `GestureDetector`s that measure and call the hooks; `Sheet` drags from its grabber + title row |
| apps/mobile | `src/systemBack.native.ts` / `.web.ts`, `src/gestureRoot.tsx`, `index.js` | Android `BackHandler`; the History API; `GestureHandlerRootView` at the root |
| map | `app/packages/map/src/gestures.ts` | `maplibreGestures()` applied in `Map.web.tsx`; `nativeMapGestures()` is the typed hook-up point for `Map.native.tsx` |

## Back, one rule
Every input calls `useBack().back`: the ‹ buttons (Shell now passes `back` everywhere it passed `go({type:"back"})`),
Android's system back (button and edge gesture, `BackHandler`; returns false at the browse root so Android leaves the
app), the browser's back and a phone browser's own swipe-back (each back step the route holds is one history entry
carrying `{"canifishthis.back": n}`; ‹ drops its entry with `history.go(-n)`, whose `popstate` lands on the index already
held so nothing pops twice; the browser's forward is refused), and the iOS left-edge swipe (`<EdgeBack>`, off on
Android and the web, where the system owns that strip).

## Part swipe — SUPERSEDED 2026-10-21 by the view swipe (`../view-swipe/README.md`)
Parts are stepped in the water map's order, **mouth first** (the ▼ Downstream / ▲ Upstream buttons):
- **finger moves LEFT → next part UPSTREAM** (▲) — the next part slides in from the right, like turning a page;
- **finger moves RIGHT → next part DOWNSTREAM** (▼);
- at the lowest/highest part the view gives a little (0.3×, at most 32 px) and springs back; no wrap.
Horizontal must beat vertical 2:1 or it is a scroll; it never starts in the 24 px edge zones (left: back; both: the
system's edge gestures on Android and phone browsers).

**Wired, but OFF in the app today:** nothing in the app reads a water's parts yet (P15 `useWaterMap` will). The Shell
wraps the water view in `<PartSwipe>` with `usePartSwipe(route, go, null)`; passing P15's part order (part numbers,
mouth first) turns it on. Views: the **Regs** view exists and is wrapped; the water **Map** and water **Conditions**
views do not exist yet (P15 / T16) — when they land, wrap the Map view's part header/strip (not the canvas) and the
Conditions view whole.

## Map
`Map.web.tsx` is constructed with `...maplibreGestures()`: pan, pinch, double-tap and wheel zoom on; rotate and tilt
off (`dragRotate`, `pitchWithRotate`, `touchPitch` false, `touchZoomRotate.disableRotation()`, and
`keyboard.disableRotation()` so Shift+arrows neither turn nor tilt; the NavigationControl compass, which turns the map
when dragged whatever `dragRotate` says, is hidden while rotation is off — `keyboardRotation()` / `compassShown()`); `clickTolerance`
8 px, so a press that moves 8 px or more is a pan and selects nothing. `Map.native.tsx` (N1's) is untouched;
`nativeMapGestures()` returns `{scrollEnabled, zoomEnabled, rotateEnabled: false, pitchEnabled: false}` for it.

## Checked
- `pnpm -C app check`, unfiltered: exit 0.
- Tests: `core/src/gestures.test.ts` (every threshold at, below and above its edge; `stepPart` at both ends),
  `ui/src/gestures.test.ts` (history model with asynchronous traversal: one pop per input, deep push sequence, ‹ in
  flight, forward refused; the no-double-pop rule mutation-pinned), `ui/src/gestures.render.test.tsx` (`useBack`
  mounted: Android back, ‹ and browser back interleaved), `ui-native/src/gestures.test.tsx` (`fireGestureHandler` on
  EdgeBack, PartSwipe and the Sheet), `map/src/gestures.test.ts`, `tools/map-gestures.test.ts`.
- Web, Chromium at 390×844 with touch emulation (`web-check.mjs`; the Playwright MCP browser was held by another
  session, so the same Playwright drove a headless Chromium directly):
  - open Vedder River from Search → history gains an entry `{"canifishthis.back":1}` (`03-water-open.png`);
  - **browser back** → the water closes, back on the map, history state back to the page's own (`04-after-browser-back.png`);
  - open again, press **‹** → the water closes and history returns to the page's entry (no stranded entry);
  - **Layers sheet**: a slow 40 px drag on the grabber springs back (`05-sheet-open.png` — identical after the drag);
    a 400 px drag closes it (`07-sheet-after-long-drag.png`).
  - **part swipe**: NOT checked in the browser — it is off in the app until parts are read (above); proven only by
    the render tests.
- Android: `expo export --platform android` builds (2.8 MB Hermes bundle).

## Not verified (device only)
- How any of it feels: the edge swipe's follow and spring, the sheet's drag, the part swipe's resistance.
- Android: `BackHandler` on a real device (button and gesture navigation), RNGH inside the Layers `Modal`, the part
  swipe against a vertical `ScrollView` (direction lock), RNGH's native module in a dev build.
- iOS: nothing is built; `<EdgeBack>` is written for it and tested under react-native-web only. Its 24 px strip over
  the map (the map must leave it free) is untested.
- The Sheet's "drag when the content is at its top" path is not built: the drag starts from the grabber and title row
  only, which keeps scrolling content scrolling.
- The spot screen and the add-spot flow are full-screen returns outside the `<EdgeBack>` wrapper (iOS later).

## Review fixes
- **Spot edit vs. system back**: the ‹ on the spot screen ended the edit before popping; Android's and the browser's
  back popped the route directly, so the edit outlived its spot (the next spot opened in edit mode with the old text,
  and Save wrote it there). The cleanup now follows the route: `@app/ui` `useOnLeave(openSpot, …)` in the Shell ends
  the edit (discarding its added photos), the error and a pending tab whenever the open spot changes, by any input.
  `ui/src/leave.render.test.tsx` drives Android's back from an editing spot (and pins the defect without the hook).
  A system back from a dirty edit discards without asking, exactly as the ‹ always has (one back rule).
- **Part swipe acceptance stays OPEN**: no input can step a part in the shipped app until P15 supplies the part order
  and the Shell dispatches `part`; the browser check of the swipe is owed then.

## Follow-ups (review minors), ported onto the view swipe (2026-10-21)
Written first as 9b7287fb against `PartSwipe`; that was replaced by `ViewSwipe` (`../view-swipe/README.md`), so the
items were re-applied to the current code rather than cherry-picked.
- **Back closes an open sheet first, every platform.** `@app/ui` `useBack` keeps back layers: an open sheet registers
  (`useBackLayer(open, onClose)`, through `BackLayerContext`, which the Shell provides around LayersSheet), counts as
  one back step in `depth` (so on the web it holds one history entry) and is closed by back before the route is
  popped. A sheet closed any other way (Done, scrim, drag) unregisters and its entry is dropped (`go(-1)`), so none is
  stranded. Android: `BackHandler` closes it the same way, and the Modal's `onRequestClose` is that same back.
  Tests: `ui/src/gestures.render.test.tsx` "an open sheet: …" (web back closes the sheet, route unchanged, next back
  pops; at the browse root back closes the sheet instead of leaving; Done drops the entry; Android press order;
  onRequestClose; no provider → own close) and `ui-native/src/gestures.test.tsx` (Sheet registers while open, its
  onRequestClose calls the one back). Mutations: dropping the layer from `depth` fails 3; back skipping the layers
  fails 4; `onRequestClose={onClose}` fails the Sheet one.
  Browser (`08-sheet-after-browser-back.png`, web-check "sheet-back"): Layers open → state `{"canifishthis.back":1}`;
  browser back → sheet closed, app still there, state null; open + Done → state null.
- **ViewSwipe on the web** keeps the page's pinch-zoom (touch-action `pan-y pinch-zoom`, WCAG 1.4.4), text selection
  (`userSelect="auto"`) and the context menu (`enableContextMenu`): `WEB_PAGE_PROPS` in `ViewSwipe.tsx`, pinned both
  as the props passed and on the DOM RNGH writes (`gestures.test.tsx` "on the web the body keeps …"). Not checked in
  a browser on a water (the web-check's path into a water predates the search eye; see below).
- **Cancelled gestures are not releases**: EdgeBack, Sheet and ViewSwipe `onEnd` commit only on RNGH's `success`;
  tests fire a CANCELLED end for each, and removing each `success` check fails its test.
- **STACK_CAP**: already true at HEAD (route.ts `push` slices to the cap); `ui/src/gestures.test.ts` "at the stack
  cap …" pushes 10 frames — the depth holds at 8, no history entry is added, back pops one. Removing the slice fails it.
- **`web-check.mjs` runs from the repo**, no machine paths. From the repo root, servers up:
  `node handoff/PLAN/app-data/g1/web-check.mjs <out-dir> [url]` (url default `http://localhost:39418/`). Playwright
  comes from `app/node_modules` if present, else the repo `.venv`'s Python Playwright driver (`playwright-core`); the
  browser is Playwright's own Chromium (`.venv/bin/python -m playwright install chromium` if missing);
  `PLAYWRIGHT_CORE=<dir>` overrides. Servers: `TILE_PORT=<p> node app/tools/serve-tiles.mjs` (or an existing tile
  server) and, in `app/apps/mobile`, `EXPO_PUBLIC_DATA_BASE=http://localhost:<p> npx expo start --web --port <q>`.
  Each check runs on its own from a fresh load ("water", "sheet-drag", "sheet-back"). The "water" check still waits
  for the old "Regulations are coming" water page reached from a search row; search's eye now shows the water on the
  main map instead, so that check fails until its path is rewritten — the sheet checks pass.
