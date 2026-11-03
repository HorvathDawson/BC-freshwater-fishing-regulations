# T369 — One load path per screen: same output whatever the way in (in 0.5.0)

Status: OPEN — batch 6c of RELEASE-PLAN-0.5.0, beside T368 (owner 2026-11-03).

## The rule (owner 2026-11-03)
"Make sure all pages have single path to load and that it produces same output regardless of the item that triggers
the load whether a refresh or tab switch in regs pane or search etc. Should always be consistent regardless of
navigation pathway. This is an important audit to prevent bugs like this again."

## Why (bugs found 2026-11-03, all one class)
- Coquitlam Map view: a part picked from "Show all N parts" lit a 440 m sliver; ▼ to the same part lit it whole (T345).
- ▲/▼ moved the camera but the highlight stayed on the old part (web).
- A reload on `?regs=…&part=2&view=map` drew the river "Base rules" blue instead of "Own rules" orange, framed wider.
- Entering by a map tap (frame holds `tapSid`) vs search: only the tap route crashed the 0.4.0 app (T345).

## Scope
Every screen a route can show (map tabs, a water's Regulations / Conditions / Map views, More pages, sheets):
1. **List the ways in** for each: cold start / reload on its URL, tab switch, view-strip switch, search result (Show /
   Open), map tap, ▲/▼ step, parts-list pick, flows-into / continues links, back, deep link, offline start.
2. **One load path:** each screen's state is derived from the route (+ the release) by ONE function; no way in may
   carry extra state (a held ref, a stepped index, a tapped section) that changes what is drawn.
3. **A net:** a test harness that opens each screen by every way in and compares what it draws (the view model, the
   map's highlight/colouring/feature-state, the camera's target) — they must be equal. Fails on a new divergence.

Method: as T368 — one read-only pass (screens × ways in, where state can diverge), owner sees the list once, then fixes
with the net. Web and native one logic path.

## Done for the water map (2026-11-03, 8a5b5bd)
Frame carries the stretch; no held ref decides the drawing; a tapped frame adopts its part (URL `part=`, never a
section id); harness in WaterMapView.test.tsx compares 6 ways in + reloads. Left for the audit: ⤢ Whole river is
screen-held; the Regulations view's tapped frame does not adopt its part (part-less URL); Conditions' `at` not in the
URL; MapLibre Native's queue for whole-feature/layer removals + sets (useColouring.ts). Render tests
(WaterScreen.regs-props, WaterMode) flake under machine load: make their waits load-proof here.

## Decided (owner 2026-11-03): links address a PLACE on the water, not a part number
`part=N` is an index into one release's parts; a re-split moves it. A link (URL, share, reload across an update)
carries the FWA stream key + a distance along it (the part's midpoint when made); the app resolves it in the release
it holds: the section whose span holds that distance → its part (pure, offline, release only). UNIVERSAL (owner):
named streams, UNNAMED streams (no item: the reach the point falls on), lakes (and split-lake areas) all open to the
current item and part. Inside the app the frame keeps its part index (one release). No older link form (`part=`) is read (owner 2026-11-03: no old-link support). AGENTS rule 5 gets the distinction: a stream key + distance is a durable FWA location, not
a section id.
