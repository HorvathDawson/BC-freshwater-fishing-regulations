# T567 — Fraser at Mission: the app shows no current readings, the gauge page does

Status: IN REVIEW — fb042cdfa, 1c57e7c07 (open parts a, c, d done; b (one lead gauge per water) waits for an owner decision, below; reaches the phone with the next build; owner confirms there)

Source: Owner 2026-11-09: "the fraser at mission gauge has no current readings (but the gauge page for it does so there is an issue there?)".

## Work
1. Find why the two disagree for 08MH024. Lead, not confirmed: `data/curated/gauges/tidal_stations.json` says Mission's HYDAT record reads "tidal effects", so the app may treat the station as tidal and withhold its reading while the public page prints it. Other leads from the data review (T550): a station ECCC answers 304 for is absent from that run's obs; a reading's age is not gated.
2. One rule for what a tidal / backwater gauge shows, used by the app and the page (same presenter).
3. Check the other lower-Fraser stations the same way (Whonock, Port Mann, Hope).

## Acceptance
App and gauge page show the same thing for 08MH024, with the reason said if the reading is withheld. A test with a Mission fixture.

Depends: —. Size: S. Owner: no.

## Found and fixed 2026-11-09 (fb042cdfa)
- Cause: 08MH024 reports a LEVEL and no flow (staging feed 2026-11-10: `parameter: level`, `now.discharge: null`). The
  app asks one quantity for the whole map, Flow by default, so under Flow the station had nothing to rank: its row said
  "Reporting, but its reading cannot be ranked today" and its water "None of the gauges … has a reading today". Under
  Level it read fine. The public page prints whichever quantity the station has. 61 stations are level only.
- NOT the tide: the tidal flag only adds a sentence under Level. That lead is closed.
- Fix: new donor state `other-quantity` (core `donorState`, data `otherOnly`) and refusals `level-only` / `flow-only`.
  The row, the map dot and the sheet now say "Reports water level only today, not flow. Show Level to see it." A level
  still never stands in for a flow. Test: `livePanel.test.ts` (Mission's feed row), `one-donor-state.test.ts`.
- Left: the alert page's "No reading now" for such a station (goes with T563); the gauge page itself (T564).
- Seen on the way, for T550: staging `obs.json` had 0 stations on the 03:35Z run (it lists only stations written that run).

## Owner's phone picture 2026-11-09 21:08 (build 24) — second fix, and one thing still open
- Seen: Conditions → gauge card "Fraser River at Mission", Flow picked: the figure read "1.548 m · Normal · 66th
  percentile" (its LEVEL) over a m³/s chart with the CLEVER flow forecast and no reading line. The card's figure is the
  station's own default quantity whatever the chart shows (`ConditionsPanel.tsx`, `vm.reading`).
- Fixed: when the figure's quantity is not the chart's, the card says "Reports water level only today, not flow. Show
  Level to see it." in the figure's place (the same words as the row and the dot). ui-native tests pass.
- STILL OPEN: owner: "the webapp looks proper, this issue is only on the app … like it is showing different gauges by
  default". The card code is shared, so the difference is what each holds (the phone's build vs the web deploy, its
  kept feed / bundle, or which gauge leads). Not reproduced. Do with T568 on the emulator: same water, web and app side
  by side; a conformance test that both pick the same lead gauge and quantity from one feed.

## Owner's evidence 2026-11-09 21:17 PT (shots/T567-phone-1..4; a report was also sent from About)
- Phone: 0.7.2 build 24, commit 2e8bedf, STAGING, release r-a178299f6005, tiles d5242c0c70509877.18ef9c824054. Updates
  log (exported 04:17:50Z): gauges index last changed 04:07:49Z (7c339f84 → c2dd0f85), 304 since; release applied
  02:28:22Z. So the phone's data is current: this is not a stale phone.
- Water: Fraser River, CPR Bridge at Mission → Landstrom Bar sign zone boundary (km 156.4).
- Shot 2, Flow: card "Fraser River at Mission", figure "1.548 m · Normal · 66th percentile" over a m³/s chart (CLEVER
  forecast, no reading line). Fixed in 9a9a247d1 (not on the phone yet).
- Shot 3, Level: the card SWITCHES GAUGE to "Fraser River at Hansard" (far upstream, near Prince George) and shows
  "197 m³/s · 2.18 m stage · 27th percentile" over a level chart. Two more defects: (a) the lead gauge changes with the
  quantity (owner: "it is like it is showing different gauges by default"), and Hansard leading a Mission reach under
  Level looks wrong in itself; (b) the figure again leads with the other quantity (flow over a level chart) — the
  9a9a247d1 guard does not cover it if `reading.quantity` says level while the figure leads with m³/s: check.
- Web app on the same water looks right (owner). Compare: the web deploy's commit vs 2e8bedf, and the lead gauge each picks.
- Tomorrow: reproduce on the emulator with this release + feed; fix lead-gauge choice (one lead per water whatever the
  quantity, or said plainly when it must differ), the figure's quantity, then T568's block order.

## Shot 5 (21:19, the gauge list under Flow) — the day asked is a FORECAST day, not Now
- "forecast from 6 gauges nearby", fold "6 not counted this far ahead": the sheet is on a day ahead. Members counted:
  Mission 08MH024 70%, Hansard 08KA004 6%, Shelley 08KB001 6%, Hope 08MF005 6%, Above Texas Creek 08MF040 6%, McBride
  08KA005 5%.
- This explains the pictures: ahead, Mission has a FLOW forecast (CLEVER) and no ranked level forecast, so it leads
  under Flow and drops out under Level, where Hansard leads. The card's figure under the list is still TODAY'S gauge
  reading (1.548 m level) while everything above it is about the forecast day in flow.
- So the defects are: (1) on a forecast day the card shows today's reading in whatever quantity the gauge has, with
  nothing saying it is today's and not the day asked; (2) the lead gauge flips with the quantity because each
  quantity's forecast coverage differs; (3) the web app may simply have been on Now. Check the day chip first tomorrow.

## Owner's rule 2026-11-09 (for the fix)
"forecast days still need to show current day on the chart i think to make it easy for user to know." So on a forecast
day the chart keeps today in view: the readings up to now, a marked "today", then the forecast out to the day asked
(marked too). Today's figure stays on the card, labelled as today's; the day asked gets its own forecast figure.

## Open parts, 2026-11-10 (1c57e7c07)
- (d) Web vs phone: REFUTED as a platform difference. One `ConditionsPanel` / `useConditionsView` on both; a fixture in the
  owner's shape (`conformance/src/leadGauge.test.ts`) gives the same lead for the same (quantity, day). On Now both
  quantities lead with Hansard (08KA004); on a forecast day Flow leads with Mission, Level with Hansard. The web was on Now.
- (a) Forecast day: the figure is labelled "Today's reading"; the day asked has its own line ("Mon 7 Sep, forecast: Reads
  …", core `forecastFigureLine`, the lead's own forecast row); the chart already kept the record and a TODAY rule, and now
  draws the day asked as a second dated rule (`BuildInput.day` -> `Hydrograph.day`).
- (c) The 9a9a247d1 guard covered the figure but told a station with BOTH quantities (Hansard) that it "reports flow only".
  Now the card shows the station's value in the quantity the chart shows, with the standing "(ranked on flow)"; only a
  station without that quantity gets the words.
- (b) NOT DONE, owner decision: the lead flips with the quantity on a forecast day because each quantity's forecast
  coverage differs (Mission has a CLEVER flow forecast and no ranked level forecast). Options: A) one lead per water and
  day, chosen in the default quantity (flow); under Level the card keeps that gauge and says it has no level forecast that
  day (its chart still shows its level record); B) keep a lead per quantity and say so on the card ("Level this day:
  Hansard, Mission has no level forecast"). A matches "one lead per water"; B keeps a forecast figure on the card.
- Tests: `conditions.render.test.tsx`, `ConditionsPanel.test.tsx`, `hydrograph.zoom.test.ts`, `leadGauge.test.ts`.

## Owner decision 2026-11-10 (verbatim) and part (b) done
"flow is always the leading option. flow is what we care about most. [level] is just auxiliary."
- ONE lead per water and day, chosen in FLOW (`leadOver`, data `standing.ts`; read once in `useConditionsView`, so card,
  row list, map dot and chart all read it). Level never picks its own lead. Only where no gauge answers in flow that day
  does the lead fall to the level ranking, and the card says so (`LEVEL_ONLY_LEAD`, core).
- Under Level on a forecast day, a lead without a level forecast keeps its place; the card says so in the figure's place
  (core `leadGapLine`, asserted whole in `conditions.test.ts`), the chart shows its record. No switch to another gauge.
- Conformance (`leadGauge.test.ts`): Now and day 3, Flow and Level -> the same lead (Hansard now, Mission day 3), one code path.
- Defaults to Level: only `ConditionsPanel.tsx` `shown = param ?? vm.reading?.quantity ?? "level"` (when the station's
  quantities are not yet known); every ask default is Flow ("discharge"). Not changed.
- Web shots on staging data (Fraser, CPR Bridge at Mission -> Landstrom Bar): `T568-shots/`; see T568. Emulator: not run.
