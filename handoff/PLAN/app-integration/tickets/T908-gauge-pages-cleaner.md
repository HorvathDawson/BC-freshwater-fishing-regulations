# T908 — Gauge (forecast) pages: cleaner, answering how the river is now

Status: IN PROGRESS (main chat; lane app/t908-gauge-page; pictures to the owner before it lands; web only). left (Claude): owner sees pictures, then integrate.

Owner 2026-11-19 ~07:00 (main chat), on https://canifishthis.ca/gauge/08MG013/harrison-river-near-harrison-hot-springs,
verbatim: "can we clean up the forecast pages? … right now it feels very busy and liek it doesnt provide the info we want?"

## Owner's picks (dialog, ~07:05)
Asked what the page should answer, top to bottom (Now vs normal + trend / next days as a day strip / one simple chart /
waters it serves, short); owner picked, verbatim: "Now vs normal + trend, One simple chart, Waters it serves, short,
also drop this (Browse by region: Region 1 · Region 2 · … Region 8) because it goes to the hidden site map".
So, in this order:
1. Now vs normal + trend first: one line ("Above normal for Nov 19 · rising"), today's flow and level, in the map's
   Flow vs. past years colours.
2. One simple chart (last 7 days + forecast against the normal band), smaller, below the summary; model toggles folded.
3. Waters it serves, short: the main waters only; the "rough guide" creeks folded or dropped.
4. Drop "Browse by region" (it leads to the hidden site map). The day strip was NOT picked.
Weather: not picked either; fold it or drop it (say which in the pictures).

## Built (lane app/t908-gauge-page, 2026-11-19)
Order as picked: (1) one line first, "Above normal for Nov 19 · steady", in the map's Flow vs. past years colours (tokens
color.flow.f1/f3/f4/f5/f7, one per band), core's rank under it, today's flow, the reading's age. Words from core, none retyped:
`bandOfWord`, `standingNowLine` (standing.ts), `observedTrend` / `OBSERVED_TREND` (change.ts, the forecast's 1.25x "little change"
factor over the last ~day), all written into the page script by `CORE_INLINE`. The forecast's expected change sits under it when
there is one. Level: the card facts carry one quantity per gauge (the lead one), so only that is shown; nothing for the other.
(2) one chart, smaller (0.6 x width on a phone, 0.3 on desktop), the key and the day-by-day table folded. The page has no model
toggle (one forecast model per gauge), so nothing to fold there. (3) "Waters this gauge serves": own / lake / good gauges listed
open; fair + weak guides are ONE fold ("Also a guide for N more waters"), kept in the HTML (internal links), not dropped.
(4) "Browse by region" dropped on gauge pages only (site.ts `shellOf(..., regionNav=false)`). Still shown on: water pages' shell,
region hubs, letter, topic, changes, about, privacy and the /gauges index (not changed). Weather: FOLDED into one line
("Weather at the gauge: 8C Clear"), not dropped.
Tests changed with the owner's words as the reason: gaugeClient.test (reading block text, chart sizes), gaugePage.parity.test
(the standing is the band line + core's rank, not one run), gauges.test (+ no Browse by region, main vs fold). SEO tests 150 pass;
check:fast green. Pictures: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t908-shots/contact-sheet-390.png, /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t908-shots/contact-sheet-1440.png (before | after; Harrison + Chilliwack at Vedder).

## Round 2 (owner 2026-11-19 10:50, verbatim: "the gauge page picture should have the same color bar at the top. like we have in condition tab. gauge page should have clever and coffee forecast options")
Built: (1) the Conditions tab's "Flow vs. past years, this date" bar under the now line: the map's ramp (css `--flow-ramp` from the style's
own standing stops), Low / High, the bracket marked Normal from core's new `NORMAL_BAND` (moved from ui-native Chrome.tsx, which now imports
it), this gauge's reading marked at its percentile. (2) One switch per model the gauge has (CLEVER, COFFEE, ELF) plus "Real-time only", in
the app's own logic (`pickModel`, `realtimeOnly`, `forecastDrawn`, `forecastModelOption`, moved to core change.ts / conditions.ts and
re-exported by @app/ui), each model's own colour token (color.ui.forecast.*) and dash (`FORECAST_DASH`, moved to core); the lead model is on
as in the app; a gauge with no run says "No flow forecast for this gauge." All written into the page script by `CORE_INLINE`.
NEEDS THE FEEDS WORKER REDEPLOYED (main chat; never done here): `cardFacts` now also writes `forecasts` (every current run, with
`horizonDays`) and `percentile` (CARD_REV 3 so every card is redrawn). Until then the page shows the one `forecast` it was given and NO colour bar
(no percentile, nothing rendered). Pictures use a LOCAL facts file: live 08MH001 / 08MG013 facts plus the two new fields, with COFFEE
SYNTHESIZED (0.9 x CLEVER, 5 days), because the live feed's cards carry one model. Pictures: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t908-shots/r2-contact-sheet-390.png, /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t908-shots/r2-contact-sheet-1440.png.
