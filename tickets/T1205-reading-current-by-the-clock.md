# T1205 — A gauge reading is current by the clock, not by the feed's own stamp

Status: IN PROGRESS (lane wt-T1205, branch fix/T1205-reading-current-by-the-clock).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1205 | A gauge reading is current by the clock, not by the feed's own stamp | IN PROGRESS | — | building: one clock gate at the feed's index read; tests; before/after pictures |

Source: found 2026-11-26 building T1194: a local render over the 2026-10-27 feed (29 days old) drew every gauge as a live
dot on the Conditions map. Owner 2026-11-26 (via the planning chat): "A new ticket, in the 0.10.0 re-cut (Recommended)".

## What the code does today (verified 2026-11-26, one detail corrected)

- `readingCurrent(observedAt, ref)` (`app/packages/core/src/freshness.ts:31`) judges a reading against `ref`, the index's own
  `fetchedAt`, not the clock; `READING_MAX_AGE_MS` is 6 h (`staleness.ts:56`, from the publisher's `READING_MAX_AGE_HOURS`).
  The feed itself is stale after `STALE_AFTER.feed` = 2 h (`freshness.ts:19`).
- CORRECTION: the app does not call `readingCurrent` at all. Only the feeds WORKER does (`workers/feeds/src/gauges/publish.ts:92,1178,1227`,
  `card/card.ts:180,186`), at write time, where `fetchedAt` is the clock. The worker's `ageGate` writes a too-old entry with
  no percentile. The app then treats "has a percentile" as "current": `reading` (`data/src/panel.ts:201`), `donorState`
  (`core/src/conditions.ts:507`), `gaugeGeoJSON` (`ui/src/gaugePoints.ts:80`), the panel rows, standings, likeVisit/visits/capture.
  Nothing re-asks the question later, so a feed that stops, or a phone offline for a day (`httpFeed` answers from its held or kept
  index with its old `fetchedAt`, `data/src/feed/http.ts:405`), keeps every written percentile "live".
- `donorState`'s `unranked` rule (`conditions.ts:516`) compares the reading with `o.ref`, which is the PICKED DAY's midnight
  (`ui/src/panel/conditions.ts:220`), not the real now.

## The rule

A reading is current only if `now() - observedAt <= READING_BY_CLOCK_MS` = `STALE_AFTER.reading` (6 h) + `STALE_AFTER.feed` (2 h) = 8 h,
`now()` from `core/src/clock.ts`, never the picked day. Why 8 h: 6 h is what the publisher already allows a reading at write
time (ECCC reports hourly, about an hour behind; a slow station can sit near 5 h and the publisher still ranks it), and 2 h is
how long our copy of the feed may itself be late before it is called stale. A healthy gauge is therefore never turned old
between publishes (the feed's own staleness line takes over first); 8 h is the longest a reading the publisher called current
can sit un-refreshed. An unjudgeable stamp (missing, naive, not a time) stays unjudged, as in `readingCurrent`.
The gate sits once, at the index read (`httpFeed().index()`): a station past the line loses its now-reading fields (percentiles,
`reports`, temperature) and keeps `observedAt`, so every reader says what it already says for a silent gauge ("Not reporting today",
"last reported …") and no reader can disagree. Forecast days (`ahead`, `change`) have their own run times and are untouched.
The worker keeps `readingCurrent(…, fetchedAt)` (it IS the clock there). Alerts' 6 h gate (`ALERT_STALE_MS`) is unchanged.

## Acceptance

- A 29-day-old feed with self-consistent timestamps draws no current dot (`gaugeGeoJSON`) and a water card says last reported.
- A fresh feed is unchanged; the boundary (8 h, and 8 h + 1 s); offline a day: old, not live, not an error.
- A picked past or future day does not change freshness.
- Pictures: Conditions map and one water card, before and after.

Depends: none (stays out of T1194's files). Size: S. Release: 0.10.0 re-cut. Model: Sonnet. Owner: no (answered).
Default: as built.

## Owner

- [x] 2026-11-26 — judge "current" against the clock, in the 0.10.0 re-cut, with a test? → "A new ticket, in the 0.10.0 re-cut (Recommended)" (2026-11-26). His condition for gauges that serve no water (T1194): reachable "yes as long as they have up to date data".
