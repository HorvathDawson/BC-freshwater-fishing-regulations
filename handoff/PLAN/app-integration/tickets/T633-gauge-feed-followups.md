# T633 — Gauge feed: follow-ups from T550, T557 and T585

Status: PARTIAL — items 1–8 built (a6bf038be, 2ebfe0787, 52878ceb6, 9995fcf13, 07dbf3791, 78b6846e2; item 4 by T585: 04316bd91, c58640b86, e4add6e9a; item 6 this pass); left (Claude): item 9 (next bundle build)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T633 | Gauge feed follow-ups: generated policy constants, refuse untagged feed, contract test on worker output, card wording, HYDAT mismatch warning | PARTIAL — items 1–8 built (4 by T585, 6: the web keeps the week in IndexedDB); left (Claude): item 9 (next bundle build) | a6bf038be, 2ebfe0787, 52878ceb6, 9995fcf13, 07dbf3791, 78b6846e2 | T550/T557/T585 leftovers; owner questions answered 11-11 |

Source: gauges lane report 2026-11-11.

## Done (2026-11-11)
- 1 (a6bf038be, with T557 item 2): the worker's policy constants (FLOOD_WEIGHTS …) are generated from change.py / skill.py.
- 2 (2ebfe0787): a gauge feed file with no format tag is refused (reported; held data answers).
- 3 (52878ceb6): the contract test runs on the worker's own output (workers/feeds/test/gauges.contract.test.ts).
- 4 (T585's lane: 04316bd91, c58640b86, e4add6e9a): `readingStanding`, `dataProvenanceLine`, `bootSaid`; words and
  device checks are in T585. Not built, by T585's choice: installing a newer set BEFORE the map on a cold start.
- 5 (9995fcf13): `hydatNotice` carries the feed's own HYDAT; when it is not the release's, the Data sources page / More
  reads "Gauge ranks use another HYDAT" (stale). Wording is a default (owner may reword).
- 6: the web keeps the week in IndexedDB (`openGaugeHistoryStore()`; `HistoryText` `load`, `gaugeHistory` `ready()`).
  No service worker, so a fully offline web start is not in this ticket. Tests: `data/src/feed/history.test.ts`
  "(T633 item 6)", `data/src/spots/store.web.test.ts`. The kept lines read "… saved on this phone" (`conditions.ts`,
  `freshness.ts`) on a laptop too; "this device" would fit both; not changed (agreed strings).
- 7 (07dbf3791): `ctxOf` (publish.ts) hashes the station's envelope, rating, rating pad and recent years.
- 8 (78b6846e2): before 03:00 UTC the daily archive is not asked (`DAILY_NOT_BEFORE_UTC_H`), counted on /status as
  `dailyRefillDeferred` (ECCC writes the day's first daily CSV at about 02:18 UTC).
- HYDAT envelope rebuilt locally on STAGING (2026-11-10/11, HYDAT 20261019): clim.json 1,959 → 1,951 stations; level
  changed for exactly the 45 shifted stations; uploaded to `feeds/live/clim.json`. Owner list:
  handoff/PLAN/app-integration/T550-datum-shift-stations.md. The bundle's copy (chart bands) still needs `pipeline
  build` + a release.

## Work (left)
9. The 8 level-only stations the datum cut removed from clim (under 3 years left; none live: 08KH017 08MD014 08NH060
   08NH094 08NH096 08NH098 08NJ075 08NL058): 7 have a row in matches.json. Check the next bundle build handles them, and
   say so in the release notes.
- Not seen in a real browser (item 6, tested over a fake IndexedDB). Check on staging: open a gauged water, reload with
  the feed blocked (DevTools: block `feeds/live/*`), the chart draws its measured line with the kept words.

Depends: T550, T557, T585. Size: M.

## Owner

- [x] 2026-11-11 — Is 6 hours the right cutoff for a reading to count as current? → "6 hours stays as the cutoff." (2026-11-11)
- [ ] 2026-11-11 — Run the HYDAT workflow on staging so the datum-shift cut reaches clim.json (45 stations in HYDAT, 21 live), then send the datum-shift station list to read.
