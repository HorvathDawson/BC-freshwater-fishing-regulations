# T511 — The gauge share card: forecast in words, a readable axis, the normal range behind the chart

Status: DONE — 35276cdc2, 53b90e7f9; owner 2026-11-09 on build 24: card shares; page follow-up → T564

Was: IN REVIEW — 35276cdc2, in the staging feeds worker deployed at 53b90e7f9; owner: OWNER-TODO "Look at" (a shared gauge link's preview card, with T487).
Earlier: IN REVIEW — 35276cdc2; owner looks at a shared link's preview (not deployed: feeds worker deploy is the owner's / next release). Follow-up of T487 (owner 2026-11-08, after seeing the Chilliwack at Vedder Crossing card).

## Asked
1. The forecast in words on the card ("Forecast: rising to 31 m³/s by Wed" / falling / steady), from the same series the
   chart draws; nothing (said plainly) when there is none. The card never says open / closed (T487).
2. Axis scales and units legible at share-preview size (about 1/3, Messages / Facebook).
3. Faint background: the median for the day of year and a percentile band over the chart's window, from the climatology
   that words "low for the time of year". Cheap at refresh.

## Done (card.ts, raster.ts, test/cards.test.ts)
- **Forecast words** (`outlookOf`): the reading against the forecast's highest and lowest point (the drawn series). Both
  within 10 % (`STEADY_WITHIN`) of the reading: "Steady around <mean>"; else the larger move: "Rising to <peak> by <day>" /
  "Falling to <low> by <day>". Day = weekday within six days, "Nov 14" beyond, "today". Card line: "Forecast: rising to
  49.3 m³/s by Nov 3". No current run: "No forecast"; run past its working-day line (T347): "Forecast out of date". A level
  gauge has no flow forecast: "No forecast". Also in `feeds/cards/<station>.json` (`outlook`, `normal`).
- **Axis**: tick labels, the unit (once, above the figures), "7 days ago" and "Forecast"/"Now" are the 34 px body face in
  full ink (were 26 px soft, and the chart had no tick figures at all); 2–3 round figures (1/2/2.5/5 x 10^k) each on a
  faint line. Checked at 400 x 210: ticks, unit and words read.
- **Normal range**: `clim.json` (feeds/live, the yearly HYDAT envelope: 73 pentads x p10/25/50/75/90 per parameter), read
  ONCE per run and only when a card is to be drawn, kept for the listed stations; `normalRange` samples core's own `bandFor`
  (the call that ranks the reading for the standing) a point a day over the chart's window. Drawn: p10–p90 (faintest),
  p25–p75, median line; key "normal range". The scale holds the readings and p25–p75; the outer band is cut at the edge.
  The area fill under the line was dropped (it read as a band). No climatology (file missing / no station): cards draw
  without the band, `CardRun.climateError`, and the digest says so (event `card-clim`), never silent.
- `CARD_REV` 2: every card is redrawn once.

## Cost (436 seeded gauges, vitest harness, this Mac)
Draw all cards: 3.06 s before -> 3.63–3.73 s after (about 7.0 -> 8.4 ms a card; +20 %); the model (band) 40 -> 48 ms;
parse of the 8 MB seed clim.json about 30 ms, once a run (and an extra R2 read of clim.json per run that draws).

## Tests
Wording (rising / falling / steady / larger move / none / unit / weekday vs date / never open or closed); the band is
`bandFor`'s output and the median ranks 0.5; niceTicks 2–3 round figures; `runCards` reads clim.json once, band on the
card facts, no-clim run draws + digest line. Feeds worker suite 251 pass.

Shots `shots/T511-{vedder,no-forecast,high-for-time-of-year}-{before,after,after-third}.png` (Vedder Crossing 08MH001,
Murray River 07FB002 with no forecast, Dickebusch Creek 07FB004 very high for the time of year; seed of 2026-10-28).

## Follow-up (owner 2026-11-08): honest shapes, scale to the line, bigger plot
- **Forecast by shape** (`outlookOf`): steady; brief spike/dip that returns ("Brief rise to 49 m³/s on Nov 3, then back to
  12"); longer excursion that returns ("Rise to … around …, then back to …"); sustained rise ("Rising to 30 m³/s by Wed",
  ", then easing to N" when it comes part-way back); sustained fall; fall after a recent week peak ("Dropping after a recent
  peak, to … by …"); none / out of date said plainly. Figures to 2 significant figures (`about`). One line across the card's
  foot ("Forecast: …", fitted to 1072 px). Tests for every shape.
- **Scale**: the y-scale is the readings + forecast only (8 % headroom); the normal range is cut at the plot's edges, so
  the line uses the plot's height (Vedder: 12–49 on the axis, not 0–80).
- **Bigger plot**: 512 px wide (was 354), tick labels beside it; brand moved to the kicker row; left column 490 px; forecast
  sentence in its own foot row. Shots overwritten, same three gauges (+ 1/3 size).
