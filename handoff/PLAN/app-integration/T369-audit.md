# T369 audit — one load path per screen (read-only pass, 2026-11-04)

Read against `redesign/stream-sections` @ 8f71403de. No code changed. Rule: a screen's drawing = f(route frame, release);
no way in may carry extra state that changes what is drawn. `Dn` = divergence below.

**Already landed since the ticket was written:** the link change is *implemented* — af7814cae (place links),
a44e69f44 (no `part=` read), b6588a3c2 (web-only `lines` file). The live release `r-ac5a4e4f734c` ships `lines`
(1.2 MB gz). See "Link change scope".

## Screens × ways in

✓ = state from route (+release) through one function · ✗ Dn = a way in that carries extra state or draws differently ·
— = not a way into this screen · `n/a` = deliberately not addressed (menu pages: "NOT ROUTE FRAMES", menu.ts:6).

| Screen | reload / cold URL (web) | tab switch | view strip | search Show / Open | map tap | ▲/▼ step | parts pick | flows-into / continues | back | deep link (native) | offline start | Android recreation |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Map tab | ✗ D11 (always Map; camera HOME) | ✓ | — | ✓ (Show = search focus) | — | — | — | — | ✓ (camera ref) | none (D1) | ✓ | ✗ D12 camera |
| Conditions tab | ✗ D11 (opens Map) | ✓ (horizon session-held, ASK 2) | — | ✓ | — | — | — | — | ✓ | none | ✓ | ✗ D12 |
| Runs tab | ✗ D11 | ✓ | — | ✓ | — | — | — | — | ✓ | none | ✓ | ✗ D12 |
| Spots tab / spot screen | ✗ D11 (spot not addressed) | ✓ | — | — | ✓ pin → `openSpot` | — | — | — | ✓ (`useSpotEdit` resets on leave) | none | ✓ | ✓ route.spot |
| More tab / More pages | ✗ D11 / n/a | ✓ | — | — | — | — | — | — | ✓ back layers | none | ✓ | n/a (page lost) |
| Search (over a map) | ✗ D11 | ✓ (tab closes it) | — | — | — | — | — | — | ✓ | none | ✓ | ✓ flag only |
| Water · Regulations | ✗ D3 D10 (tapped frame reloads as part frame) | — | ✓ | ✓ Open (solo → Regs) | ✗ D3 | ✗ D2 | ✗ D2 | ✓ (`exitAction`: part, no tap) | ✓ | none | ✗ D9 | ✗ D10 day |
| Water · Conditions | ✗ D1 D5 (sections[0], no "you are here") | — | ✗ D1 | ✗ D1 (sections[0]) | ✓ (tapSid + at) / ✗ D15 from Runs sheet | ✗ D1 | ✗ D1 | ✗ D1 (sections[0]) | ✓ | none | ✗ D9 | ✓ |
| Water · Map | ✓ (8a5b5bd harness) / ✗ D6 D7 D13 | — | ✗ D7 (⤢ lost) | ✓ Open = whole | ✓ adopts part | ✓ / ✗ D6 | ✓ / ✗ D6 | ✓ cross | ✗ D7 | none | ✗ D6 D9 | ✓ |
| Regs page sheets (SheetHost), Day sheet, Layers, obstacle / tidal / not-fishing / runs sheets, bell, first-run | ✓ `sheet=` once (start) | — | — | — | ✓ | — | — | — | ✓ back layers | none | ✓ | n/a |
| Any open water | — | — | — | — | — | — | — | — | — | — | — | ✗ D8 (release applied from background) |

## Divergences (14)

**D1 — Conditions view ignores the frame's part.** `WaterViews.tsx:55-56`: section = `tapSid ?? sheet.sections[0]`; the
frame's `part` is never read. And `route.ts:261` keeps `tapSid`/`at` when a step moves the part. So frame
{part 3, tapSid in part 1} shows part 1's reach; search → step to part 3 → Conditions shows the water's FIRST section;
reload of either shows `sections[0]`. Three answers for one frame. *Fix:* one pure `frameSection(frame, nav)` in `@app/ui`:
`tapSid` if it lies in the frame's part/stretch, else that stretch's / part's `mid_sid` (already in the release:
`part_geo.mid_sid`, `part_stretch.mid_sid`; the "until P14 supplies mid_sid" note in WaterViews.tsx:11 is stale). Drop
`tapSid`/`at` in the `part` action when the new part does not hold it (route.ts:261).

**D2 — Regulations of a stepped part show the TAPPED part's access.** `WaterScreen.tsx:123` passes `section` (the tap)
to `useAccessNotices` (`regs.ts:239-253`: section notices lead) while the target is `part` (WaterScreen.tsx:100-101). Tap
part 1 → Map → ▼ to part 3 → Regulations: part 3's rules under part 1's no-access banner/gentle note. Reload: part 3's
own. *Fix:* same `frameSection` as D1 (and the `part` action dropping the tap).

**D3 — A tapped frame's Regulations change once the Map view has been visited.** The Map adopts the tapped part
(`waterMap.ts:398-402`) but the Regulations view does not; `WaterScreen.tsx:125-127` reads the Salmon card by section
while `part` is null and by part after. Tap → Regs, tap → Map → Regs and reload (`place.ts:118-121`: part, no tap) draw
the Salmon card three ways; the scroll memory key (`WaterScreen.tsx:235`, `item|part|tapSid`) also changes. *Fix:* adopt
the part AT OPEN — `openTap` resolves the tapped section's part (the `useRegsTarget` answer) before dispatching, so every
tapped frame has `part`; the adopt effect in `useWaterMap` goes. Then Salmon/access/scroll key read one rule.

**D5 — `at` and the tap are not in the address.** For a named water `placeOf` (place.ts:59-68) writes the PART's
midpoint once `part` is set; `resolveStartLink` reopens item+part with no `tapSid`/`at`. A reload of a tapped Conditions
view loses its reach (D1) and "you are here". Unnamed reach: `tapSid` comes back (place.ts:119), `at` does not. *Fix:*
when the frame has a tap, the place is the tap's (`m` = the tapped measure, or the tapped section's middle) and
`resolvePlace`'s `sid` comes back as `tapSid`; `at` see ASK 1.

**D6 — `stretch` means two different indexes.** The frame's `stretch` is set from `pieceOf` (waterMap.ts:416) — the index
among pieces AFTER `mergeNav` joins stretches by the live salmon key (core `waterNav.ts:246-285`); `waterMapStartIx`
(waterMap.ts:324-331) reads it that way. `placeOf` (place.ts:65) and `resolvePlace` (place.ts:84-85) use the release's
`part_stretch.stretch_ix`. Where salmon joins stretches 0+1, a frame on piece 1 (release stretch 2) writes stretch 1's
midpoint; a link resolving to release stretch 2 opens piece 0 (no piece 2 → falls back). The salmon key is also null
offline or before it loads (all joined), so the same frame draws different pieces online/offline. Live release: 499
parts carry 2+ stretches (862 rows). *Fix:* frame `stretch` = release `stretch_ix` only; the map shows the piece holding
that stretch's `mid_sid`; `pieceOf` stays display-only.

**D7 — ⤢ Whole river is screen-held.** `waterMap.ts:44-45,94` (`WaterMapState.whole`), toggled at WaterMapView.tsx:90.
Lost on a view switch (the view unmounts), back, reload; only search's `frame.whole` is on the route and in the address
(`whole=1`). *Fix:* ⤢ dispatches a route action toggling `frame.whole`; the map reads only the frame.

**D8 — A release applied with a water open.** App.tsx:351-356 applies a ready update on return from background with no
"safe" check (the Shell's own gate is browse-only, Shell.tsx:263). The Shell stays mounted (boot.ts:60-67), so the open
frame's part/stretch indexes are read in the new release — the "a re-split moves it" bug, in-app. *Fix:* apply from
background only when the Shell is safe, or carry frames across by place. ASK 3.

**D9 — Offline / failed `lines` fetch on a place link says "not on this version of the map".** App.tsx:436 turns any
`resolveStart` rejection into `notice: "place-gone"`. Offline start with a place link: wrong reason, whole water shown.
*Fix:* a fetch failure is said as the net words (+ retry when online), "place-gone" only for `resolvePlace` → null.
(Overlaps T368.)

**D10 — The day is not addressed.** `ROUTE_KEYS` (startLink.ts:27) excludes `date`; `routeSearch` keeps whatever
`date=` the page started with (startLink.ts:37-39). Change the day, reload: the URL's old day (or today). Android
recreation resets it too (App.tsx:578 `initialDay` from the start link only; Shell.tsx:156). ASK 2.

**D11 — Browse state is not addressed.** `routeSearch` writes nothing while browsing (startLink.ts:41): a reload of the
Conditions/Runs/Spots/More tab, an open spot or search opens the Map tab. ASK 2.

**D12 — Android recreation restores the route but not the cameras.** `place.route` (App.tsx:69-71) survives;
`camera`/`waterCamera` are Shell refs re-made at HOME (Shell.tsx:181,183). Back from a restored water lands at HOME, not
where the reader was. *Fix:* hold the browse camera in the App's `place` beside the route.

**D13 — A solo water opened other than by search mounts its Map first.** Search asks `solo` before opening
(openWater.ts:142-147); a tap, `opener.item`, or a reload with `view=map` opens the Map, then `useShellNav.ts:90-94`
drops it (and the URL says `view=map` meanwhile). *Fix:* `resolveStartLink`/`ownPart` and the opener ask `solo` the
way `searched` does.

**D14 — Native whole-layer clear mixed with sets (ticket's left item).** `packages/map/src/native/useColouring.ts:129-137`
`clearAll` sends `removeFeatureState({sourceLayer})` at once while sets are flushed on a timer — the same queue shape
8a5b5bd fixed for the highlight with null writes. Untouched since 2026-10-29, no test. *Fix:* as the highlight —
explicit null writes in the same batch, plus a queue-fake test.

**D15 — Runs sheet → Regulations opens a tap with no point.** Shell.tsx:614 `opener.tap(sid, null, "regs")`: the same
reach from the map tab carries `at`, so its Conditions view has "you are here" and this one has none. *Fix:* pass the
tap's point through the Runs sheet (or derive `at` from the section, ASK 1).

(D4 folded into D3.)

## Ticket's "left for the audit" — status

| Item | Status |
|---|---|
| ⤢ Whole river screen-held | OPEN — D7 |
| Regulations view's tapped frame does not adopt its part | OPEN — D3. The address is OK now (place.ts writes the tap's middle when `part` is null), but the reload draws differently |
| Conditions' `at` not in the URL | OPEN — D5 (+ D1, bigger: Conditions ignores the part) |
| MapLibre Native queue: whole-feature/layer removals + sets (useColouring.ts) | OPEN — D14 |
| Render tests flake under load (WaterScreen.regs-props, WaterMode) | OPEN — T379 tracks WaterMode "ONE MAP ENTRY" (fails alone, 3/3). regs-props last touched 5940a63db, waits unchanged |
| The net (harness comparing every way in) | PARTIAL — only `ui-native/src/water/WaterMapView.test.tsx` (6 ways + reloads, Map view). None for Regulations/Conditions views or browse |

## Link change scope (stream key + distance → part)

**Done** (af7814cae, a44e69f44, b6588a3c2):
- Pipeline: `pipeline/deliver/bundle/section_measure.py` (bundle `section_measure`), `pipeline/deliver/release/lines.py`
  (the `lines/1` file: per-block stream key + measure span of every stream section; lakes have none), manifest
  `web_only`. Tests: `pipeline/tests/test_section_line.py`, `app/tools/section-line-parity.test.ts`.
- App: `@app/data/regs/place` (`sectionAt`, `sectionLine`, lazy), `@app/ui/place` (`placeOf`, `resolvePlace`,
  `linkSearch`, `resolveStartLink`), `startLink.ts` (`blk`/`m` parsed, `part=` not read), `systemBack.web.ts` (address
  written at once, place added after paint), `startLink.web.ts`, App.tsx:426-439. Web-only enforced by
  `tools/native-no-links.test.ts`.
- Coverage: named stream part/stretch ✓, unnamed reach ✓ (`?blk=&m=` alone), one-part water ✓ (item only), lakes /
  split-lake areas ✓ (item only). Measured in the live release: **0 lake items have more than one part**, so a lake
  never needs a place.

**`part=` today:** read nowhere (startLink.test.ts:39 pins it ignored). Stale mentions only: comment `waterMap.ts:396`;
built `apps/mobile/dist/routeUrl.web.d.ts:3`; `tools/seo/legacy.test.ts:96-97` keeps `?part=1` through a redirect
(harmless, the app ignores it).

**What is left (size S, ~4 files):** D6 (one stretch index: waterMap.ts, place.ts, route frame doc), D5 (the tap's place
+ `tapSid` back from `resolvePlace`), D9 (failure ≠ place-gone, App.tsx), stale comments. The release already carries
everything the pure resolver needs (stream key + measure span per stream section in `lines`, `part_geo`/`part_stretch`
mids and sids in `geo`); no pipeline change.

## The net (proposal)

One test file per water view and one for browse: for a fixed water/part (Coquitlam part 1, a salmon-merged stretch, an
unnamed reach, a solo water), reach the screen by every way in the table and compare the view model (Regulations:
target + access + salmon key; Conditions: section + `from`; Map: highlight/fit/stretch), then reload each URL through
`linkSearch` → `parseStartLink` → `resolveStartLink` and compare again. Extend WaterMapView.test.tsx's pattern.

## ASK (owner)

1. **"You are here" after a reload or a non-map open:** should the Conditions view's point (`at`) survive a reload — e.g.
   derived from the link's place (the section's point at that distance) — or is "no point, the reach only" right for any
   frame not opened by a map tap?
2. **What else belongs in the address?** Today only the open water + view is. Should a reload also keep the day
   (`date=`), the browse tab (Conditions / Runs / Spots / More), an open spot, and the Conditions horizon — or do those
   reset by design on a reload?
3. **A data update applied while a water is open** (return from background): hold the update until the reader is
   back on a map (safe moment), or apply and move the open water to the same place in the new data?
