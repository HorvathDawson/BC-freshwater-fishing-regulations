# T508 — A water opened into its Map or Conditions view appears in one piece

Status: IN REVIEW — f2428537b, 02364791a (+ testbed 49dd973d3), in build 22 (3d807829d); owner: OWNER-TODO "Phone check with build 22" (Map / Conditions views open whole).
Earlier: IN REVIEW — f2428537b, 02364791a (+ testbed 49dd973d3); owner sees it on the next APK. Follow-up of T498. Part of T495 (smooth map).

T498 holds a water's Regulations pane hidden over the frozen browse map until it is drawn whole. The Conditions and Map
views carry their own live MapLibre map and were left out: Search → Open on the Fraser lands on its Map view and shows
~8 short screens (blank beige, grey, unlit map, lit, marks); a Conditions-map tap shows ~2. A build that held a Map view
crashed MapLibre's render thread once (SIGSEGV), before T498's `collapsable={false}` fix — maybe the same bug, unverified.

## Do
- The same one open path (`shell/paneReveal.ts`) for every view: a Map or Conditions view is held until it is drawn
  whole. "Ready" said honestly: the pane's map has drawn its lit part (style, highlight written, fit at rest, a full
  render after), the Conditions content is final — not at first render.
- Emulator, test-bed `pane-open` (extended): Fraser Search → Open (Map view), Fraser and Norrish Conditions taps, the
  Regulations cases still 2 screens. Hold a Map view ≥ 20 times; logcat for SIGSEGV. If holding a live map is unsafe,
  say so with evidence and propose the alternative instead.
- Report: does a feed update rebuild the water map's view model and reset an open mark, chooser or parts list?
- A contract test for the new ready signal(s) that fails without the fix. Shots `shots/T508-*.png`.

## Done (2026-11-08)
- **Cause** (emulator, testbed `pane-open`, before = the T498 APK): only the Regulations view was held. Fraser search →
  Open (its Map view): 18–20 screens — blank beige, grey, the map unlit, lit, the marks, the fit gliding. Conditions-map
  taps: Norrish 3, Fraser 6 (the gauge card empty, its reading, its chart). Norrish search → Open: 6 (the search was
  consumed before the water opened: the emptied field and recent searches drawn first).
- **Fix** — the one hold for every view (`shell/paneReveal.ts`): each part of a held pane says it is in — the view's
  content (`usePaneReady`) and every live map in it (`usePaneMap` → `MapProps.onDrawn`: style loaded, the lit sections
  written, the fit's glide over, then a frame with every tile in). Conditions waits for its reach, place, access notices,
  watershed days, the panel's first answers (`ConditionsVM.settled`) and chart; its route map for itself. The held pane
  is sized to its final height (the frozen tab bar's row: the map re-framed itself after reveal, 334 → 389 dp). A searched
  water consumes the search once it has opened. Phone only for Conditions/Map (`shell/holdViews.*`): the web builds a
  map only once seen, so it still holds Regulations only.
- **After**: every case 2 screens (Fraser search → Open 20 → 2, Fraser Conditions 6 → 2, Norrish Conditions 3 → 2,
  Norrish search 6 → 2 seen ≥ 40 ms; Regulations map taps still 2). All 8 holds ended "whole", none by the 1.2 s
  timeout. Shots `shots/T508-{fraser-search,fraser-conditions,norrish-conditions,norrish-search}-{before,after}.png`.
- **Crash check**: the Map-view hold (search → Open on the Fraser) 20× per build on four builds — 80 opens, same pid, no
  `Fatal signal` / SIGSEGV in logcat (the T498 SIGSEGV did not recur; it predated the `collapsable` fix).
- **Feed update** (T498 follow-up): yes — a DFO feed taken handed `useWaterMap` a new salmon reading, and the walk was
  keyed on its identity: an open mark, chooser or parts list closed. Fixed (keyed on the grouping) — 02364791a.
