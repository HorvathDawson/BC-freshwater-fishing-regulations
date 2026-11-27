# T1205 — A gauge reading is current by the clock, not by the feed's own stamp

Status: IN PROGRESS (lane wt-T1205, branch fix/T1205-reading-current-by-the-clock).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1205 | A gauge reading is current by the clock, not by the feed's own stamp | IN PROGRESS | 0188dd1 | built + tested (`ui/src/readingByClock.test.ts`); before/after pictures not taken (no heavy lane free for Expo web) |

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
The gate sits at the feed's two reads (`httpFeed().index()` and `.now()`, one `ageIndex`): a station past the line loses its now-reading fields (percentiles,
`reports`, temperature) and keeps `observedAt`, so every reader says what it already says for a silent gauge ("Not reporting today",
"last reported …") and no reader can disagree. Forecast days (`ahead`, `change`) have their own run times and are untouched.
The worker keeps `readingCurrent(…, fetchedAt)` (it IS the clock there). Alerts' 6 h gate (`ALERT_STALE_MS`) is unchanged.

## Built (0188dd1, review fixes after)

Two gates, one rule (`READING_BY_CLOCK_MS`, `readingCurrentNow(observedAt, now)`, `readingCurrentUntil`, all exported from `@app/core`,
`core/src/freshness.ts`). `ageIndex` (`data/src/feed/http.ts`) strips the now-reading from a station past the line, keeps `observedAt`;
it is cached per raw index for the span in which no station crosses the line, so a poll gets the same object. The words are the
existing ones ("Not reporting today.", "Last reported …").

Every reader of a gauge reading or percentile, and its gate (the first version of this ticket wrongly said capture and visits were covered by the index gate; they read `feed.now()`):

| reader | gate |
|---|---|
| `feed.index()`: map dots, panel rows and standings, conditionsColour, basinDays, likeVisit, visits and capture panels, dayFlow, leadup, facts | `ageIndex` in `index()` |
| the phone's kept index (offline) | same: it comes out of `index()` |
| `feed.now()`: `gaugeNow`, `stationReading`, lead card, lake count, capture and visits reading, `fillLive`, the public page's `stationChartSource` | `ageIndex` on its index row; with no row, `readingCurrentNow(f.now.at)` on the file's own rank |
| `donorState` `unranked` | `readingCurrentNow` at `DonorAsk.now` (the real instant; `ref`, the picked day, stands in only when no `now`); no lower bound, as `readingCurrent` |
| `useDonorView` / `DonorPanel` | pass `now` (minute-rounded) through to `donorView` |
| `observations()`, `lastReport()` | raw series and last-report words, no rank: not gated (their own age line says how old) |
| worker `ageGate`, share cards, the SEO pages' build-time index | judged at write time against `fetchedAt`, which is the clock there |
| alerts (`ALERT_STALE_MS`) | own 6 h gate by the clock |

As time passes with no fetch: `useFeedVersion("gauges")` (the key every gauge reader already carries) also moves on one beat per
feed lifetime (`FEED_TTL_MS`) and on `appBecameActive` (`ui/src/feedVersion.ts`, `today.ts`), only while something watches it; the
answer store is not cleared (not a feed change). Readers re-ask `index()`, which judges at the clock; no fetch is made.

What the reader sees, and why it is fine:
- A device clock set AHEAD makes readings look old: every gauge says "Not reporting today" with "Last reported <a recent time>", no hint
  of the clock. Same for the river colours already (they key off the same stamps). Accepted: a wrong clock is the device's fault, and
  the line says when the gauge last reported, which is true.
- A future-dated reading (clock behind) is current: one rule, no lower bound, in `donorState` too (it had an absolute value).
- Share cards and alerts cut at 6 h by the worker's clock at write time; the app uses 8 h because it judges later: the 2 h is the
  feed's own lateness allowance. A card is a snapshot of that run, so the difference is by design.

## Acceptance

- A 29-day-old feed with self-consistent timestamps draws no current dot (`gaugeGeoJSON`) and a water card says last reported.
- A fresh feed is unchanged; the boundary (8 h, and 8 h + 1 s); offline a day: old, not live, not an error.
- A picked past or future day does not change freshness.
- Pictures: Conditions map and one water card, before and after.

Depends: none (stays out of T1194's files). Size: S. Release: 0.10.0 re-cut. Model: Sonnet. Owner: no (answered).
Default: as built.

## Owner

- [x] 2026-11-26 — judge "current" against the clock, in the 0.10.0 re-cut, with a test? → "A new ticket, in the 0.10.0 re-cut (Recommended)" (2026-11-26). His condition for gauges that serve no water (T1194): reachable "yes as long as they have up to date data".
