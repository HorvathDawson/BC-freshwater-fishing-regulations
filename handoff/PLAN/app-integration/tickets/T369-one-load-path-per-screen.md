# T369 — One load path per screen: same output whatever the way in (in 0.5.0)

Status: DONE — 75a907e3b (2026-11-04; batch 6c of RELEASE-PLAN-0.5.0, beside T368).

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

## Done (2026-11-04, 75a907e3b) — the audit's 14 divergences (T369-audit.md) and the owner's answers
- ONE reading of a frame: `@app/ui` `frameView.ts` (`useFrameView`: part, release stretch, reach, "you are here"),
  read by Regulations (`WaterScreen`), Conditions (`WaterConditions`) and Map (`useWaterMap`). Conditions shows the tap
  in the part, else the part's / stretch's middle (`mid_sid`) and its point — owner Q1 (D1).
- Route: a part/stretch move drops a tap of another part (D1/D2); ⤢ is the frame's (`whole` action, in the address;
  D7); `carry` (D8, owner Q3: applied at once, carried by item + point in the new release, `carry.ts`; spinner while).
- Opener: a tapped water opens AT its part (`regsPartOf`, the adopt effect gone: D3); a Map asked of a solo water asks
  first (opener.item + start link: D13); the Runs sheet and a water map's other-water tap carry their point (D15).
- Frame `stretch` = the release's `stretch_ix`; the map shows the piece HOLDING it (`stretchOf` / `waterMapStartIx`
  over the raw group), online or offline alike (D6; the 499 multi-stretch parts).
- Address (owner Q2): a tap's place + `here=lat,lon` / `tap=1` (D5), `browse=`, `spot=`, `date=` (not today),
  `horizon=` (D10/D11); a place that could not be READ says so (`place-unread`, D9); cameras, day and horizon survive
  Android recreation (`cameraHold`, `onAddress`; D12).
- Native colouring: a layer clear is null writes in queue order (`StateQueue`), never `removeFeatureState` after
  the frame's sets (D14) + `map/src/native-colouring-queue.test.ts` (mutation-pinned).
- THE NET: `ui-native/src/water/oneLoadPath.test.tsx` (Regulations page text + Conditions reach/point by every way in
  and each reloaded; mutation pins), `WaterMapView.test.tsx` (taps via `partOf`, ⤢ reload), `ui/src/frameView.test.ts`
  (address round trip of every key, carry).
- Load-proof: `useDelayedShow`'s show timer no longer fires over content already drawn (the T379 race); waits in
  WaterMode / WaterScreen.regs-props given the 20 s ceiling or waiting on the element read.

Left: an UNNAMED reach (or a water the new release drops) open across a data update is closed, not carried — a phone
holds no section line (`lines` is web-only); carrying it needs `section_measure` in the native bundle (pipeline change,
owner). A multi-section lake part's tap is not in the address (lakes have no line). Two suite failures are not T369's:
`WaterScreen.unnamed` (desktop side tabs expect Licence) and `surfaces.test` (a stale allowlist line) — both follow
c574bef9e (no Licence on a closed day).
