# T527 — Gauge alerts, stock-style: crosses above / below a value or a percentile for the day

Status: IN REVIEW (build 22) — phone check on the APK (checklist below); worker unchanged

Owner 2026-11-09: "we want it to be a clean alerting system like you would have for stocks. also we want the percentile
alert so percentile on the day (like it passes the median or the 75th percentile or whatever percentile …)".
Supersedes T453 (percentile alerts + an easier menu) and the T234 v1.1 alert model.

## What was built (402cf8b39)
- **One model** (`@app/core` `alerts.ts`): per gauge, flow or level, **crosses above / crosses below** a line. The line is
  a **value** (m³/s, m) or a **percentile for the day** (presets 10th / 25th / median / 75th / 90th, custom 10–90 whole).
- **Edge-triggered:** fires once on the crossing; quiet while past; re-arms once the reading is back across by 2 % of the
  line (min 0.05 m³/s / 0.01 m). The first reading only sets the side (an alert saved while already past waits; the
  editor says so). Stale (> 6 h), missing, or a day with no line: nothing.
- **Percentile source:** the release's climatology, the chart's own envelope (`bandAt` → `Series.yearBand`); the value at
  a percentile is `percentileInBand` run backwards on the same knots (`flow.ts` `valueAtPercentile`, round-trip tested).
  The line is the reading's own day (as the chart's band), so it moves with the date. The background check runs headless
  with no release open, so the alert keeps its line for every day of the year (366 values) taken when it is saved.
  A gauge with no envelope in that quantity shows "A percentile for the day" disabled with the reason.
- **Notification:** title "Cowichan River: flow alert"; body "Cowichan R. at Lake Cowichan crossed above the 75th
  percentile for Nov 9 — 41.2 m³/s at 14:00 PT." (the reading as the gauge page shows it, `chartFigure` + `bcClockTime`).
- **UI:** More → Notifications is a plain list, one line per alert ("gauge · Flow crosses above 50 m³/s · last fired …"),
  its own On/Off and Delete; **Add an alert**: 1. pick a gauge (the release's gauges by name or number) → 2. condition →
  3. value or percentile → Save. A gauge card's **Alert me** opens the same editor on that gauge.
- **Removed (replaced, no second model):** the four kinds (±% in 24 h gone), the per-alert cadence and once-per-water-
  per-day (`gapH`, `waterFired`), the ±% station-file read (`fetchStationRows`, `dayAgoReading`), Edit in place,
  "Add an alert" by water search; `test-updates.mjs gauge-day`. `test-updates.mjs gauge` now stamps like the feed (PST
  offset), so a percentile alert reads the right day.
- **Migration** (`notify.json` v3, `migrateAlertRule`): v1/v2 above/below a value → kept as a crossing of that value,
  with its switch and state (its cadence dropped). A v1.1 ±% alert has no line to cross → **refused**: reported once,
  and listed on the page ("Alerts not kept: Vedder · Flow rises 50 % or more within 24 h: a change within a day is no
  longer an alert kind") until the reader taps OK. Anything unreadable is refused the same way.

## Worker
No change: the check reads `obs.json` (newest rows) only; the percentile line travels with the alert.

## Tests
core `alerts.test.ts` (39: crossing both ways and on the line, margin edge ±, noise, stale edge, the date-moving line up
and down, a no-record day, day ±1, value-at-percentile round trip, migration incl. refusals, every string whole);
data `check.test.ts` (20: end to end, percentile alert, migration v1/v2 + dismiss); ui-native `Notifications.test.tsx`
(15: list, create flow gauge → condition → value, percentile presets/custom, disabled with reason, refused note).
Shots (light, web harness): `shots/T527-list.png`, `T527-create-1-gauge.png`, `T527-create-2-value.png`,
`T527-create-3-percentile.png`, `T527-no-record.png`.

## Phone checklist (APK on the staging test channel; `test-updates.mjs reset` first)
1. Upgrade over a build with T234 alerts: above/below alerts listed as "crosses above/below"; a ±% alert listed under
   "Alerts not kept" until OK.
2. More → Notifications → Add an alert → type "chilliwack" → pick the gauge → Crosses above → value → Save: one line in
   the list, "not fired yet".
3. `test-updates.mjs gauge <station> flow <under>` + force the job (sets the side), then `<over>` + force → ONE
   notification "<water>: flow alert / <gauge> crossed above …"; force again → none; just under (inside 2 %) then over →
   none; well under then over → one more.
4. A percentile alert (75th) on a gauged water: the editor says today's line ("… m³/s for Nov 9"); push a reading over
   it → one "… crossed above the 75th percentile for <day> — …".
5. A gauge with no record (level-only station): "A percentile for the day" greyed, reason under it.

## Left
- The kept percentile line is the record at save time: after a new HYDAT release, delete + add again (OWNER-TODO).
