# T347 — River forecast is published on weekdays only: keep it on the Conditions screen over the weekend

Status: OPEN (owner 2026-11-02).

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
