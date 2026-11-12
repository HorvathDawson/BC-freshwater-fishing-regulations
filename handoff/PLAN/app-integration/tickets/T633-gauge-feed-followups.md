# T633 — Gauge feed: follow-ups from T550, T557 and T585

Status: PARTIAL — items 1, 2, 3, 5, 7, 8 built (a6bf038be, 2ebfe0787, 52878ceb6, 9995fcf13, 07dbf3791, 78b6846e2); left (Claude): item 4 (card `rankedAt` / kept words, "as of" line: the ui-native + T555 boot lane), item 6 (web offline week), item 9 (next bundle build)

Source: gauges lane report 2026-11-11.

## Work
1. The worker's FLOOD_WEIGHTS and other policy constants are generated from the Python spec, not hand-copied (rest of T557 item 2).
2. The app refuses a gauge feed file with no format tag, once both workers write `gauge-feed/1`.
3. The feed-contract test runs on the worker's own output.
4. The gauge card says which reading was ranked (`rankedAt`) and when a figure is kept; the "as of" line names release, environment and channel; a staged release is applied at cold start (T585 F12, with T555).
5. A warning when the feed's HYDAT differs from the release's.
6. The web keeps an offline week like the phone (T585 F14, web half).

## Owner
- Is 6 hours the right cutoff for a reading to count as current?
- Run the HYDAT workflow so the datum-shift cut reaches clim.json (45 stations in HYDAT, 21 live), then read the shift list.

Depends: T550, T557, T585. Size: M.

## Owner answers 2026-11-11
- 6 hours stays as the cutoff.
- Run the HYDAT workflow on staging, then send the datum-shift station list to read.

## 2026-11-10 (UTC 11-11): HYDAT envelope rebuilt locally, on STAGING
- Why local: the branch is ~312 commits ahead of origin, so the workflow would have built with old code. Ran the
  workflow's step 2 on the Mac with the current code (HYDAT 20261019 = ECCC's newest = what staging is on):
  `pipeline.gauges.feed.climatology` and `pipeline.gauges.feed.hydat_daily --index <staging index.json>` (heavy.sh lane 2).
- Old (staging, built 10-30) vs new clim.json: 1,959 → 1,951 stations. The 8 gone are level-only historic gauges with
  under 3 years after their shift (08KH017 08MD014 08NH060 08NH094 08NH096 08NH098 08NJ075 08NL058), none live.
  Flow: envelope, stats and recent years identical for every station. Level: changed for exactly the 45 shifted stations
  and no other; recent-year level traces unchanged. New keys: levelShifts (45), ratings (430), ratingPadM 0.5.
- hydat_daily: every file identical to staging's (377 built; staging also holds 11 for stations no longer in the index).
  Nothing to upload, so not uploaded.
- Tests before upload: 107 passed (test_level_datum, test_bundle_clim, test_pentad_window, test_record_years,
  test_feed_maintenance, test_hydat_daily, test_update_hydat_script, test_publish, test_feed_reading_age,
  test_feed_shown_parameter, test_feed_gaps, test_feed_daily, test_feed_year_backfill, test_gauge_policy; unfiltered).
- Uploaded clim.json to bc-fishing-regulations-staging `feeds/live/clim.json` (rclone `r2:`, md5 checked). Old copy
  kept in the session scratchpad. One `POST /run/gauges`: ok, 428/428 answered, 415 ranked, 0 unchanged (all re-ranked),
  status ok. Level standings changed for 18 of the 21 live shifted stations (3 unchanged: still at an end of the band);
  08HD006 and four unshifted stations (07FD001 08EF001 08KH019 08MH153) lost their level standing to the rating check,
  as T550 item 2 predicted. Every other level change came with a newer reading. No flow moved without a newer reading.
- Owner list: handoff/PLAN/app-integration/T550-datum-shift-stations.md. The bundle's copy (chart bands) still needs
  `pipeline build` + a release; not done here.
- Gap found: the worker's change-aware cache keys a station on clim's `release`, not its content. A clim.json rebuilt
  on the same HYDAT is not re-ranked for a station whose CSV answers 304 until the UTC day turns. This run happened to
  get 0 unchanged. Proposed: put a hash of the station's envelope + rating into `ctxOf` (publish.ts).
- eccc-daily http_404, 8 or 9 a run: the hourly health record shows every 404 falls between 00:00 and about 02:30 UTC,
  every day since 11-04, and none at other hours. ECCC writes the day's first daily CSV into `/today/.../daily/` at about
  02:18 UTC (Last-Modified on BC_08HB022 today), so until then every daily refill gets 404. The 8 are just the stations
  with a gap in the last 29 days that run (08DA005 08EE013 08GA079 08HB022 08KB006 08NB014 08NM241 and one more), all
  200 again after 02:30. Upstream timing, not a wrong URL; ours only in that we ask then and count it as a failure.
  Proposed fix (not deployed): between 00:00 and 03:00 UTC read the previous day's dated folder
  (`https://dd.weather.gc.ca/<YYYYMMDD>/WXO-DD/hydrometric/csv/BC/daily/`, answers 200) or skip the refill in that
  window. Same rollover as T550 item 4 (hourly), which only forgives the first 30 minutes.

## Added 2026-11-11 (from the staging clim rebuild)
7. Worker: the per-station reuse key (`ctxOf`, publish.ts) names clim by release, not content, so a clim rebuilt on the same
   HYDAT is not re-ranked for an unchanged station until the UTC day turns. Add a hash of the station's envelope and rating.
8. Worker: `eccc-daily` asks for today's daily file between 00:00 and about 02:30 UTC, before ECCC writes it, and counts the
   404 as a failure (8 per run, every day since 11-04; this is the "degraded" on /status). Read yesterday's dated folder in
   that window, or skip the refill then.
9. The 8 level-only stations the datum cut removed from clim (under 3 years left; none live): 7 have a row in matches.json.
   Check the next bundle build handles them, and say so in the release notes.

## Done 2026-11-11
- 1 (a6bf038be, with T557 item 2): the worker's policy constants are generated from change.py / skill.py.
- 2 (2ebfe0787): a gauge feed file with no format is refused (reported; held data answers). Staging writes the tag on all
  429 station files + index + obs (read 2026-11-11); production serves no gauge feed yet.
- 3 (52878ceb6): the contract test runs on the worker's own output (workers/feeds/test/gauges.contract.test.ts).
- 5 (9995fcf13): `hydatNotice` carries the feed's own HYDAT (`feed`); when it is not the release's, the HYDAT line on the
  Data sources page / More reads "Gauge ranks use another HYDAT" (stale). An index naming none keeps the old rule.
  Wording is a default (owner may reword; no answer needed to ship).
- Owner questions: both already answered 2026-11-11 (6 h stays; HYDAT workflow run on staging).

## Done 2026-11-11 (items 7, 8: 07dbf3791, 78b6846e2)
- 7: `ctxOf` hashes the station's envelope, rating, rating pad and recent years; a clim.json rebuilt on the same HYDAT
  re-ranks the station on its next run.
- 8: before 03:00 UTC the daily archive is not asked (`DAILY_NOT_BEFORE_UTC_H`); said in the run log, counted on /status
  as `dailyRefillDeferred`. Staging 2026-11-12 00:09 UTC: 9 deferred, no failed source, feed ok. The other proposal
  (yesterday's dated folder) was not taken: skipping needs no new URL and the gap is asked again after the window.
