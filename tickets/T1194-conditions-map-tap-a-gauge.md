# T1194 — Conditions map: tap a gauge to open that gauge's page (BEFORE the 0.10.0 re-cut)

Status: OPEN. BEFORE the 0.10.0 re-cut (owner: "should do before recut"). Visible change: owner looks at screenshots before it merges (owner: see ## Owner).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1194 | Conditions map: tap a gauge dot to open that gauge's page, tidal and water-less gauges included | OPEN | on feat/T1194 (see Built) | built; owner looks at the last sheet; OWED: the tap checked on a device (emulator run); T1206 after the re-cut |

Source: owner 2026-11-26, verbatim: "also we should make it so that in the conditions map add a way to click on the gauegs to
get the gauge page itself. that way even the lower tidal fraser gaguges are accessible etc. should do before recut".
Scoped from the code at `origin/main` 334f493d5 and the release r-71d09ad82e81 (conditions + core files, the 2026-10-28
feed index). Counts are from those files; the build re-measures them (acceptance 9).

## Findings

### 1. The Conditions map today: dots you cannot tap

- Screen: the Conditions tab of the browse map (`BrowseHost.tsx:242`, `gauges={onConditions ? gauges ?? undefined : undefined}`;
  the dots only exist on this tab). Data: `useGaugeGeoJSON` (`packages/ui/src/hooks/gauge.ts:181`) over `source.gaugePoints()`
  (`SELECT … FROM gauge WHERE lon/lat NOT NULL`, `sql/queries.ts:170`) and the feed index; `gaugeGeoJSON`
  (`packages/ui/src/gaugePoints.ts:68`) makes one point per station with `properties.station`, `name`, `label`, `ramp`, `muted`.
- Layers: `gauge-dot` (circle, z7+) and `gauge-label` (pill, z8+), `packages/map/src/gaugeDots.ts:154` and below; added by
  `runtime-style.ts`.
- A tap on a dot does NOTHING of its own. `Map` has no gauge tap prop: the web click (`web/useInput.ts:86-139`) and the phone
  tap (`Map.native.tsx:~810-855`) ask, in order, a spot, a boundary mark, a titled pin, a fish obstacle, then the WATER SECTION
  under the finger (`renderer.press`), then not-fishing water. `gauge-dot` is in none of those queries. So a tap near a dot
  opens whatever water section lies under it (its Conditions view, `onPressFeature` -> `openWater`, "ONE WATER SCREEN"), not the gauge.
  Tidal water answers no tap at all (`sections.ts:74`, T692: "drawn and labelled but never asked on a tap").
- There is NO in-app gauge screen. The only gauge surfaces are: (a) a water's Conditions view, which charts the water's LEAD
  gauge (`ConditionsPanel.tsx`; the card carries Share, `GaugeShare.tsx`, `useGaugeShare` `ui/src/share.ts:144`); (b) the
  website's static gauge page `/gauge/<station>/<slug>` (T487, `tools/seo/gauges.ts`, `core/gaugePage.ts`), whose chart is the
  app's own `StationChart` (`ui-native/StationChart.tsx`) fed by `stationChartSource` (data) and `useStationChart` (ui) in
  `tools/seo/gaugeChart.tsx`. "The gauge page itself" = that chart/reading page. The app already has the parts; it has no route to them.

### 2. Why gauges are unreachable (counts, release r-71d09ad82e81, feed index 2026-10-28)

| group | gauges | why |
|---|---|---|
| In the feed index | 434 | what the feeds worker publishes today |
| Drawn as a dot (release `gauge` row with a point) | 430 | 4 feed stations have no release row (07FA008, 08DA013, 08DB015, 08FB012): known, never drawn |
| of the 430: reachable only by luck | 419 | each serves at least one water (own / lake / section lead), so a tap on the right section opens a Conditions view whose lead gauge may be it; no way to ask for THIS gauge, and a non-lead gauge never shows on a card |
| of the 430: tidal | 7 | `gauge.tidal=1`: 08MH024 Mission, 08MH044 Albion, 08MH126 Port Mann, 08MH053 Deas Island, 08MH028 Steveston, 08MH032 North Arm, 08MH035 Pitt. All 7 DO serve waters (the Fraser `gnis:39325` as `own` for five of them) and lead 1 to 100+ sections each, but Albion and Deas Island lead one section each and Steveston two: the lower, tidal reaches, which take no tap (T692). A dot on tidal water cannot open its own water |
| of the 430: serves NO water | 8 | 07EF002, 07EF003 (Williston Lake), 08EC003 (Babine Lake), 08HB087 (Comox Harbour, match `none`), 08JB002 (Stellako, match `wrong`), 08EC014 (match `wrong`), 08NH064, 08NH067 (Kootenay Lake). No water to open, so no card, no website page (the site lists only stations that serve a water with a page) |
| never a lead gauge anywhere | 11 | the 8 above plus 08GC007 (Theodosia below the diversion), 08NK031 (Elko forebay), 08MH103 (Chilliwack above Slesse): on a river's card they only appear as a non-lead choice |
| level-only today (a muted grey dot, `muted`) | 6 now without a percentile | drawn (T586) and ringed, still no way to open them; under Temperature only 274 of the roster draw |

So: all 430 drawn gauges are un-openable BY THE DOT (the map ignores the tap); 15 are unreachable even by luck or by
design today (the 8 water-less, the 7 tidal whose own reach takes no tap). "Demoted in the match": `matches.json` marks 2
drawn stations `wrong` and 1 `none` (the three above); the gauge-regenerate drift (memory: 25 demoted) is not in this
roster. The website page is a second, separate gap: it exists for stations that serve a page-water (about 419 plus the tidal 7,
subject to `waterHasPage`), and for none of the 8.

Standing rules that bear on it: flow leads, Level is auxiliary (the page opens on the station's own lead quantity,
`shownParameter`, never on a made-up one); one lead gauge per water and day (a gauge page is NOT a second lead: opening a
gauge does not change what any water's card leads with); gauge representativeness is policy (the page states what the
match says, `weak` as a rough guide, and decides nothing new); map UI layout (top = search + day; legend and its control
together: the tap adds no control and no legend row); one load path per screen (a gauge opened by a tap, a link, a
reload or Back is the same screen with the same words); no silent fallbacks (a feed that fails shows its failure).

## Design (smallest clean)

1. **Tap target.** A tap on a `gauge-dot` or its `gauge-label` pill opens that gauge. New `Map` prop `onPressGauge?(station)`,
   asked FIRST on the Conditions tab only (before the water, like obstacles; a dot sits on its river and the finger means the
   dot), with a 44 px hit box around the 5 px dot (as the marks' `markBox`/`tapBox`). Web: `useInput.ts`; phone:
   `Map.native.tsx` (`queryRenderedFeatures` over `gauge-dot`, `gauge-label`); one shared `gaugeHit` in `packages/map/src`
   so both engines read one rule. Absent prop (Map tab, pick mode, search open): no change. Feedback: the existing
   selection look (a ring on the tapped dot) until the page opens; the page opens at once from the release, no spinner on the tap.
2. **Route.** A new frame in the route reducer (`packages/ui/src/route.ts`): `mode: "gauge", station, from: BrowseRoute`
   (the water route's own `from` pattern), built only by a new `openGauge` beside `openWater`. Address on the web:
   `?gauge=<station>` (a `ROUTE_KEYS` entry, read once at start, written by `routeSearch`, restored by reload: one load path).
   Phone: no address (D1), the app's Share/link target is the website page. Deep link `?gauge=08MH024` opens the same screen;
   a station the release does not hold is a SHOWN error with Go back (never the map, never a nearest gauge).
3. **The page.** One screen from existing parts: title (name, station number), `StationChart` over `useStationChart` /
   `stationChartSource` (the very chart the site's page draws), today's reading with its `shownParameter` quantity and
   percentile, the tidal / backwater / diversion note where the station is flagged (`tidalNote` etc., so Mission and Steveston
   say the tide), Share (the site's `/gauge/<station>/<slug>`, only where the page exists: `useGaugeShare`), and "Waters this gauge
   serves": one line per water, best trust first, `weak` folded as "rough guide", each opening that water's Conditions view
   (`openWater`; Back from it returns to the gauge). A gauge serving NO water shows the chart, reading and one plain line
   "This gauge is not matched to a water we cover"; nothing is invented or hidden (no fallback to a neighbour). The website
   page is unchanged (it stays the public face; it lists no water-less station).
4. **Back.** One layer: Back (system, browser, the header's Back) from the gauge returns to the Conditions map exactly as
   left (camera in the Shell's ref, day, quantity); opening a served water from the page pushes a water frame whose `from` is the
   gauge route, so Back there lands on the gauge. `backDepth` counts the frame so the browser history and Android back agree
   (`tools/back-layers.test.ts` gets the case).
5. **Offline / failed feed.** Release facts (name, serves, climatology bands) are on the phone, so the page opens offline. The
   live reading and the 72 h series come from the feed: on failure the chart area shows the feed's failure line
   (`ErrorState`, "no record" wording the chart already has), reported through `reportFailure`; the percentile and
   dot colour are not guessed. A stale feed reads as stale (existing `freshness` words).

**Data.** App-only: every field above is in the release (`gauge`, `lake_gauge`, `section_gauge`, `gauge_clim`) or the existing
feed. No release file change, no worker change, no tile change. (Optional, NOT in this ticket: the 4 feed stations with no release
row stay undrawn until a release carries them; listed so the owner knows.)

## Work

1. `packages/map`: `onPressGauge` prop (`map-props.ts`), the shared hit rule, web (`web/useInput.ts`) and phone (`Map.native.tsx`)
   wiring, the tapped-dot ring; Conditions tab only; `map-props-gate.test.ts` and the adapter contract stay green.
2. `packages/ui`: route mode `gauge`, `openGauge`, `?gauge=` in `startLink`/`routeSearch`, `backDepth`; a `useGaugePage(station)` hook
   (release facts + serves + chart, one load path) that decides what the page says (AGENTS 25: ui decides, ui-native draws).
3. `packages/ui-native`: `GaugeScreen` (mounted through `ScreenBoundary` in `Shell.tsx`'s `screen(…)`), the serves list, the
   no-water line, the shown failure; `BrowseHost.tsx` passes `onPressGauge` to the Conditions map.
4. Strings in core (`GAUGE_PAGE_WORDS`) so the words are one place; docs: `app/MAP.md` row, `BACK.md` row.
5. Shots for the owner: Conditions map with the ring; Fraser at Mission (tidal note); Steveston (tidal, serves Fraser); a water-less
   gauge (Kootenay Lake at Queens Bay); offline / failed feed; phone and wide web.

## Acceptance

1. Tapping any of the 430 drawn dots on the Conditions map opens that station's page (a station, not the water under it);
   a test walks every dot of the live release (`liveRelease`, no typed ids) and fails a station that opens another.
2. The 7 tidal gauges open (08MH024, 08MH028, 08MH032, 08MH035, 08MH044, 08MH053, 08MH126), each saying the tide on Level; the 8
   water-less open with the no-water line and no served-water rows.
3. Tapping a dot never opens a water; a tap beside it still opens the water (hit box bounded, mutation-pinned).
4. Reload on the web with `?gauge=<station>` opens the same screen as the tap; an unknown station shows the error and Go back.
5. Back returns to the Conditions map whole (camera, day, quantity); Back from a water opened off the page returns to the page;
   browser history and Android back agree (`back-layers.test.ts`).
6. The page changes no water's lead gauge and adds no state to any water card (flow leads, one lead per water and day unchanged:
   the water card golden oracles do not move).
7. Failed feed: the chart area shows the failure and `reportFailure` ran; no percentile or colour is invented; offline opens
   the release half. `tools/failures.test.ts`, `no-render-throw` green.
8. Map UI layout unchanged: no new button, no legend row; Map tab, Runs tab and pick mode unchanged (a test per tab).
9. The counts in Findings 2 are re-measured on the re-cut release and pinned by a test (drawn, water-less, tidal), so a future
   release that loses a page is a red test.
10. Owner looks at the shots (item 5) before merge.

## Risks (as built)
Two dots under one finger: the nearest, never a list. The phone's tap waits out a double tap (T716): the lift asks the gauge in the same round trip as the water. No release file or worker change; it merges before the 0.10.0 re-cut and does not move the release id.

## Built (2026-11-27)

Where it differs from the Design above (sheets: the build lane's `T1194-pictures/final/`):
- **Tap** (`map/src/gauge-hit.ts`, both engines): the DOT's target is a 44 px square centred on it; a PILL is hit at its own drawn size and opens ITS station; a river beside a pill or 30 px from a dot opens the water. On the phone the lift asks gauge, obstacle and water in ONE round trip (T716). Conditions tab only.
- **Route and back**: mode `gauge` (`?gauge=`, one load path); a water opened off the page has `via`; the Conditions map stays MOUNTED under the page (`pageOpen`, `Shell.tsx`), so Back never rebuilds it. The phone opens a web `?gauge=` link; `?gauge=` beside a water, box, `show`, `spot` or `browse` is refused by name.
- **Page** (owner's pick A): `ui-native/GaugeReading.tsx`, shared by the app and the website (`tools/seo/gaugeChart.tsx`): reading, standing, trend word, the Conditions colour bar (the mark says "This gauge: …" aloud) and chart, judged by the clock (T1205: over 8 h old is "No current reading. Last reported …", with no figure, standing, mark or trend). App-only: Back, the waters served, the tide note, Share and "Open on the website".
- **Website**: no intro paragraph; the foot is one soft card; a slow chart script is "Still loading this gauge's reading…" after 15 s (no report); "Could not load …" with the source's link, and a report, only on the script's own error or the window's load (the hard limit is the browser's); a late script replaces either line. Gauge-page JS against r-71d09ad82e81: before 986,440 B raw (gzip 306,554), after 997,366 B (gzip 309,934): +10.9 KB raw, +3.4 KB gzip. The reading was never in the static HTML.
- **A `?gauge=` link to a gauge with no current reading or no water** opens the page ("No current reading. Last reported …"; "not matched to a water we cover"): built, not silent. By tap a dead gauge has no dot (a dot needs a current ranked reading).
- **Trend word**: Flow by the ratio (`STEADY_RATIO` 1.25) against the value a day earlier; Level (owner: "Yes, on Level too") by the CHANGE in the stage over the same day, **2 cm deadband** (`LEVEL_DEADBAND_M`): from the feed's levels (2026-10-28, 424 stations) a 30-minute reading moves a median 1 mm (p90 4 mm), the 24 h change is a median 1 cm and 2.2 cm at p75. Kootenay Lake 0.1 cm: steady; Harrison Lake 3.4 cm down: falling. **Tidal gauges have no Level trend word** (the stage swings metres with the tide; the page carries the tide's note). None either when not current, no look-back within 3 h of the 24 h mark, or across a gap (that 3 h rule now applies to Flow too: a user sees FEWER trend words when the day-ago reading is missing). **Known limits of the Level word**: ice-affected winter stages can rise while flow falls and nothing flags ice; a datum reset reads as a large rise; a tide-influenced station without the `tidal` flag still gets a word.
- **Pinned** (`tools/gauge-page.release.test.ts`): 422 dots over the frozen 2026-10-23 index; each of the 15 unreachable gauges is drawn and its tap opens it. The 430 (live index) was measured by hand.
- **OWED**: the tap on a device or emulator (native: typechecked and run through the contract's mocked binding only); T1206 after the re-cut.

## Owner

- [ ] 2026-11-26 — Look at the screenshots (Conditions map with a tapped dot, the gauge page proposal beside the website's page, a water-less and a stale gauge) before this merges.
- [x] 2026-11-26 — Does "the gauge page" mean the in-app screen of Design 3, or should a tap open the website's `/gauge/...` page? → "the website pages but the same page or simialr in the app maybe? do proposal dont want it to diverge too much" (2026-11-26)
- [x] 2026-11-26 — A gauge serving no water: open the page with the "not matched to a water" line, or leave those dots untappable? → "yes as long as they have up to date data" (2026-11-26)
- [x] 2026-11-26 — Pick the gauge page (A one shared piece for the website and the app, B the website page in a web view, C open the browser)? → "one shared piece used by both website and app is a yes. ideally we have the colour bar showing current conditions with where it currently is like the condition tab. we want this page to have minimal text all the sources at bottom like all other pages. clean ui etc etc. also i think we might want to change the short term forecast to show slightly more data? like it could have the more coarse data from 72hr to 1 week? or we have 3 time periods 72 hrs 1 week and 1 year. the first 2 will show forecast. the week will use coarser data for the rest. think about it. but when forecast shows it feels like the meassured data leading up to it is diminished. otherwise looking good and use same for both" (2026-11-26)
- [x] 2026-11-26 — Changes to the gauge page after the sheet (the website's intro, the trend word)? → "yes we want a few changes first the web has a big \"live level and flow, nd the weather ...\" at the top this is bloated and can be in sources at bottom. or actually just removed.... the app is much cleaner here. i like app design. the trend word seems good to have? can be big near the big units." (2026-11-26)
- [x] 2026-11-26 — One more look at the re-shot app and website pages before merge (`T1194-owner-sheet-2.png`). → "way better please make the open on the website button and get the app stff look a lot better at the bottom it looks janky rn ..." (2026-11-26)
- [x] 2026-11-26 — OK on the redesigned foot of the gauge page, website and app (`T1194-owner-sheet-3.png`; variant A calm section or B soft card)? → "Variant B" (2026-11-26)
- [x] 2026-11-26 — Should a Level reading get a trend word too? Built Flow-only (a ratio of stages is not a ratio of water, as in the Change view). → "Yes, on Level too (Recommended)" (2026-11-27)
