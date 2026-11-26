# T552 — Run timing and forecasts: floor checks, error bodies read as errors, a new NuSEDS really rebuilds, age rule for old surveys

Status: PARTIAL (3cfe44e53, 0c0fc849b, f25fbedff, 1e71ff652, d52149c30, 305117089, 09553ef9d). Done: items 1, 2, 3, 6, 8 (but core staleness.ts), the pipeline half of 4, the source half of 5, the run-timing half of 7. item 4 worker half 683f2d7d2 (lane L5, 2026-11-22: below). left (Claude): item 5 update-service `refresh` wiring and the Runs hook re-asking, item 7 review/tidal name keys (marks._norm, dfo_auto._norm, review/names.key, two tidal audit keys), item 8 staleness.ts:45 wording ('publish.py runs every 30 minutes').

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T552 | Run timing and forecasts: floor checks, error bodies read as errors, a new NuSEDS really rebuilds, age rule for old surveys | PARTIAL | 3cfe44e53, 0c0fc849b, f25fbedff, 1e71ff652, d52149c30, 305117089, 09553ef9d, 683f2d7d2 | item 4 worker half done 683f2d7d2 (lane L5); left (Claude): update-service refresh + Runs hook re-ask (5), review/tidal name keys (7), staleness.ts:45 wording (8); PSF pages untracked (owner Q163, 9be342976) |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 8 of 17; highest severity 4 of 5.

Line numbers are the reviewers' and may be off by a few. Fixes are proposals: re-read the code before building.

Owner 2026-11-09 (Q180): "dfo surveys are just in addition to psf curve. psf is primary." Built (item 3).

Done: 1 floor checks (`3cfe44e53`: `NUSEDS_COLUMNS` / `SITES_COLUMNS`, `feed.diff` > 10% shrink exits 1, `--allow-shrink
REASON`); 2 worker dispatches refresh + `expect_dfo` (`0c0fc849b`); 3 DFO beside PSF (`f25fbedff`, decisions.md); 4
ArcGIS error bodies, Python `1e71ff652` + worker `683f2d7d2` (PR #41, `fix/T552-forecast-error-body`, lane L5, `test/forecast.errors.test.ts`; not
deployed); 5+6 `httpRunTiming` re-asks hourly, never holds a null, keeps a capture of the device's own atlas
(`d52149c30`); 7 run-timing `name_key` = `names.key` (`305117089`); 8 README + forecast.py series paragraph (`09553ef9d`).

## Work (left)
5. **Run timing re-asks on update.** (Severity 3, area deliver-contract.) Wire `httpRunTiming` into the update service's `refresh` like in-season
   (feed/inseason.ts:23, feed/salmon.ts:36 SALMON_FEED_TTL_MS), and make the Runs hook (`ui/src/runTiming.ts`, asks
   once per mount) re-ask. Today, when a new release with another atlas is applied in the running app, the Runs tab reads
   'stale' (ui/src/runTiming.ts:118) until the app restarts. One shared 'captured feed' helper for the three feeds would
   remove the three hand-written copies of this logic.
7. **The other name keys call names.key** (Severity 2, duplication.) (pipeline/common/names.py:59): make `marks._norm` (pipeline/regs/review/
   marks.py:1095, casefold + spaces only; decides 'same proposal' at :1115,:1125) and the tidal audit keys
   (pipeline/tools/audit/tidal_candidates.py:189 and tidal_preverify.py:70, identical copies, no accent fold) call
   names.key; delete `dfo_auto._norm` (pipeline/regs/review/dfo_auto.py:37, unused); keep `review/names.key`
   (pipeline/regs/review/names.py:76, feeds candidate ids at :86) only as a frozen id slug and rename it so.
8. (Severity 1, doc-drift.) **`core/src/staleness.ts:45`** says 'pipeline/gauges/feed/publish.py runs every 30 minutes' (the worker does).
   Owner (Q163, 9be342976): whether to untrack the three PSF pages under data/source/runtiming (run-timing.html,
   biological-status.html, catch-and-run-size.html), which sits oddly with the rule that PSF pages are not re-hosted
   (workflow header :18-19, T162).

## Acceptance
A collapsed or empty upstream table fails the run-timing build and keeps the kept copy. An ArcGIS 200-with-error body is a recorded failure, not "out of season". The worker trigger for a new NuSEDS runs with refresh. Run timing refetches when the release changes. Old-survey age rule applied with the count of waters changed.

Depends: —. Size: M. Owner: yes (Q180).
