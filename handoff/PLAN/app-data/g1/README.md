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

## Part swipe — which way is which (owner "maybe")
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
off (`dragRotate`, `pitchWithRotate`, `touchPitch` false, and `touchZoomRotate.disableRotation()`); `clickTolerance`
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
