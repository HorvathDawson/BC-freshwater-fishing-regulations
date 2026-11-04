# T347 — River forecast is published on weekdays only: keep it on the Conditions screen over the weekend

Status: DONE (61f819c52) 2026-11-04 (was: OPEN, owner 2026-11-02). Live once the feeds worker is redeployed (batch 7, with 0.5.0).

The BC River Forecast Centre forecast updates on working days only. Over a weekend (and on stat holidays) the app
currently treats Friday's forecast as stale, so it either disappears from the Conditions screen or gets a stale
warning.

Fix:
1. **Staleness by publishing schedule, not raw age.** A forecast counts as current until the next expected issue.
   Friday's issue stays current through Monday's expected issue time, and the same applies around BC stat holidays.
   It's only stale when an expected weekday issue is missed (plus a grace period).
2. **Keep it showing:** over the weekend it stays on the Conditions screen with no stale wording. It still shows its
   own "issued Fri …" date.
3. **More tab note:** one line wherever the More tab explains data sources, e.g. "River forecasts are updated on
   weekdays."
4. **Tests:** Fri issue viewed on Sat, Sun, Mon morning → current. A missed Monday issue → stale. A stat-holiday Monday
   → still current.
Check whether the feeds worker's forecast staleness check (if any) needs the same schedule, so it doesn't alert over
weekends.
Size: S.

## Done (61f819c52, 2026-11-04)
One rule, three copies held together: pipeline `change.due_by` (issue instant + whole days to the next working day on
the Centre's calendar + `STALE_GRACE_HOURS` 12; `bc_holidays` = statutory + Easter Monday + Boxing Day, weekend fixed
dates observed next), emitted to core as `FORECAST_HOLIDAYS` (2025–2035; a later run throws, never guesses), core
`forecastDueBy` / `byWeekdays` (`STALE_AFTER.forecast` is `{by: "weekdays"}`: the run line and the Data block), and the
feeds worker's port (`src/gauges/change.ts`, which is what dropped weekend runs from the live feed; its test checks
its holidays against the emitted list). More → Sources: "River forecasts are updated on weekdays."
