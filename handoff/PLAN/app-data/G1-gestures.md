# G1 — gestures, one clean layer (web + Android now, iOS-portable)

Owner, 2026-10-20: "a nice clean way to have gesture support for mobile on web and mobile app". Answers:
- **Back everywhere**, **map pan/pinch/tap tuned**, **sheets drag to dismiss**: yes.
- **Swipe sideways**: not between the header tabs. Instead, a sideways swipe on a water's Regs, Conditions or Map view
  steps to the next or previous part (section) of the river ("maybe": owner reviews the feel before it is final).
- **Library**: `react-native-gesture-handler` (RNGH), callbacks on JS (`runOnJS(true)`), no reanimated.

## Shape (layers, AGENTS 22–25)

| layer | what | rule |
|---|---|---|
| `core/src/gestures.ts` | pure decisions: `EDGE_BACK` {zone 24 px, commit 80 px, or a flick}, `TAP_SLOP` 8 px, `classifySwipe(dx, dy, vx)` → `"next" \| "prev" \| null` (horizontal-dominant, distance or velocity threshold), `sheetDismiss(dy, vy, height)`, `stepPart(part, count, dir)` → part or null at the ends, `MAP_GESTURES` (pan, pinch, double-tap zoom, wheel on web; no rotate, no tilt) | no React, unit-tested with whole tables |
| `ui/src/gestures.ts` | hooks that turn a decision into a route action: `useBack()` (one function every back input calls → `reduce(r, {type:"back"})`; reports whether anything was popped), `usePartSwipe()` (→ `{type:"part"}`) | no react-native imports |
| `ui-native` | `<EdgeBack>`, `<PartSwipe>`, the `Sheet` drag: RNGH `GestureDetector`s that only measure and call the hook | draws, decides nothing |
| `apps/mobile` | `GestureHandlerRootView` at the root; `BackHandler` (native) and the History API (web) wired to `useBack` | platform glue behind `.native`/`.web` |
| `packages/map` | `MAP_GESTURES` applied in `Map.web.tsx` (maplibre-gl options); `Map.native.tsx` takes the same constant (the N1 spike owns that file: apply the constant there only if N1 has landed, else leave a typed hook-up point and note it) | one policy, two bindings |

## Back, one rule (APP-PREP "One back rule")
Every input pops the same route stack: the ‹ button, the **Android system back** (button and gesture: `BackHandler`;
at the browse root return false so Android leaves the app as usual), the **browser back** button and a phone browser's
own swipe-back (web: each pushed route frame gets a `history.pushState`; `popstate` → back; never double-pop when the
‹ button itself pops — keep history and the stack in step), and the prototype's **left-edge swipe** (24 px zone, the
view follows the finger, release past 80 px or a flick commits) **only where the platform has no system back**: iOS
native (later) — off on Android (the system edge gesture owns that strip) and off on web (the browser owns it).

## Map
The browse map and the water map use `MAP_GESTURES` on both bindings. The left 24 px stays free for back where
`<EdgeBack>` is on. A tap is a press that moved < 8 px (the prototype's rule), so a small pan never selects a section.

## Sheets
`Sheet` (Layers, later the presenter sheets T36/T37) follows the finger downward only; it closes past a third of its
height or on a downward flick, and springs back otherwise. Content that scrolls keeps scrolling: the drag starts only
from the handle/header, or when the content is at its top (RNGH `simultaneousWithExternalGesture` / a native scroll
ref). Accessibility: the close button stays; the gesture is an addition, never the only way.

## Part swipe (owner "maybe")
On a water's Regs, Conditions and Map views: a horizontal-dominant swipe steps to the next/previous part, in the same
order as the water map's step buttons (match the prototype's "▼ Downstream / ▲ Upstream" order and say which way is
which in the README). At the ends nothing moves (a short resistance, no wrap). Vertical scrolling must never trigger it
(direction lock); on the Map view the swipe is taken from the part header/strip, not the canvas (the canvas pans). It
must not fire inside the 24 px back zone. Where a view does not exist yet, build `<PartSwipe>` + the hook and wire it
into the views that do; list the rest.

## Acceptance
- `pnpm -C app check` (unfiltered) exits 0; check-platform and layers pass; deps.md has the RNGH line (why, size).
- core tables: every threshold decision tested at, below and above its edge; `stepPart` at both ends.
- ui hook tests: one back from each input source pops exactly one frame; web history and the stack stay in step across
  ‹, browser back and a deep push sequence (no double pop, no stranded entry).
- Render tests for EdgeBack/PartSwipe/Sheet with RNGH's jest/test utilities (`fireGestureHandler`) where they work under
  vitest; otherwise test the hooks and say what is unproven.
- Web: Playwright in a mobile viewport (touch emulation): browser back pops the stack; a sheet drag closes it; a
  part swipe steps the part. Screenshots/notes in handoff/PLAN/app-data/g1/README.md for the owner.
- Android: an `expo export --platform android` bundle builds. On-device feel is for the owner/N1 later (no emulator
  while heavy jobs run).

## Owner, 2026-10-20 (later)
Feel uses the defaults in `core/src/gestures.ts` (edge 24 px / 80 px or flick, tap slop 8 px, swipe distance and
velocity, sheet third-of-height). They are tuned during real-device testing; nothing waits on a feel review. The part
swipe turns on when P15 supplies the part order.
