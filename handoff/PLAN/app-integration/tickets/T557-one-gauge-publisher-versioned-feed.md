# T557 — One gauge publisher (the worker); the gauge feed carries a format version the app checks

Status: DONE — 8779d6298, 31ee818a0, a6bf038be (item 2: worker constants generated), 52878ceb6 (contract on worker output), 2ebfe0787 (format-less file refused).

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 13 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. Two full gauge publishers: the Python "spec" still runs in update-hydat.sh and has drifted from the worker that serves users
Severity 3, duplication, confirmed; area gauges-hydat.
- Evidence: pipeline/gauges/feed/publish.py (1,089 lines) and app/workers/feeds/src/gauges/publish.ts (1,062), plus twins change/forecast/skill/liveness. The TS header calls Python "the spec", yet TS alone has carry-stale (:1010-1026), obs.json (:1052), change-aware runs, and private temp_history. TS comments record Python bugs left in place: `forecastIssuedAt` is written twice in one dict (publish.py:953 and :962, the second wins) and the sort crash at publish.ts:71-72. Parity runs only with `changeAware: false` (parity/gauges/parity.ts:29), so the production path is outside it. scripts/update-hydat.sh step 3 still runs the Python publisher.
- Example: data/generated/gauges/feeds/index.json is dated 2026-10-28 on HYDAT 20260717, beside a clim.json on 20261019. hydat_daily.py:72 defaults its station list to that directory (386 files; live station 08KB006 has none). The local index has no obs.json and no stale carry.
- Second opinion: The '386 files' is the hydat_daily OUTPUT directory count, not the feed directory, which holds 438 station files; the point (the default station set comes from a weeks-old local directory) still holds. The local feed is not served to users, so user impact comes only through a fix landing in one publisher and not the other, or a stale hydat_daily station set (the T301 workflow passes --index, which avoids it).
- Proposed fix: Decide one publisher. Either retire the Python `publish()` to a fixture generator for the pure functions (percentile_of, band_for, change) and drop step 3 of update-hydat.sh, or generate the shared constants and pure maths from one source. Make hydat_daily require `--index`.

### 2. The Python gauge publisher ('the spec') and the production TS worker now behave differently on outage and on forecastIssuedAt
Severity 2, divergence, partly; area forecast-runtiming. Related ticket: T178 (PARTIAL; weights only).
- Evidence: Production is app/workers/feeds/src/gauges/publish.ts; pipeline/gauges/feed/publish.py still builds the local feed (scripts/dev-build.sh:30,33; scripts/update-hydat.sh:69). (1) publish.py:952 and :962 write `forecastIssuedAt` twice in one dict literal; the second (a STRING max of issue stamps of runs that answered a change, :929-930) wins. publish.ts:1042-1045 writes it once from newestForecastIssue. (2) On a failed model publish.py:1036-1061 drops its runs; publish.ts:575-581 carries the last good ones. (3) Policy constants (FLOOD_WEIGHTS, RISE_RATIO, STALE_GRACE_HOURS, DAY_COVER, MIN_PAIRS) are hand-copied in workers/feeds/src/gauges/change.ts:9-22 and score.ts:23-24, while core gets them generated (pipeline/tools/emit/gauge_policy.py:89-92). Parity (test/gauges.parity.test.ts) folds the known differences back via normalizeIntended.
- Example: A locally built feed (Python) reports the More tab's Forecasts line from the newest run that answered a change; the staging feed (TS) reports the newest run feed-wide. score.ts:277 tells the owner to edit FLOOD_WEIGHTS in the TS file; Python's copy must be edited by hand too.
- Second opinion: Overstated on two points. (1) The forecastIssuedAt difference is a declared, owner-dated intended difference (test/gauges.parity.test.ts:6-7, "forecastIssuedAt written once", folded by normalizeIntended), not drift; the Python duplicate key is a leftover. (2) The carry needs the worker's saved forecast state, so its absence in Python is by design. What stands: the worker's policy constants are hand-copied with no generated file, pinned only by a parity test that is skipped when the repo venv is absent (describe.skipIf(!existsSync(PY))), and the Python feed is local/dev only, so an angler is not served it.
- Proposed fix: Fix the Python duplicate key and add the last-good carry to Python (or declare Python a test oracle only and stop building local feeds with it). Generate the worker's constants from change.py with the same emitter that writes gauge-policy.generated.ts, with a freshness check in CI.

### 3. Gauge feed is the one boundary with no format version; its shape is hand-typed in the app and the contract test never sees worker output
Severity 2, fragility, partly, reaches an angler; area deliver-contract.
- Evidence: Every release file, the DFO feed (`dfo-feed/3`), in-season (`inseason-feed/1`, `inseason-links/3`) and run timing carry a format the reader refuses on. The gauge feed does not: app/packages/data/src/feed/http.ts:45 hand-types `IndexFile`; the only checks are 'has a stations object' (http.ts:266-268 obs.json, 362-364 index). The worker app/workers/feeds/src/gauges/publish.ts imports no shared type and hand-copies `HORIZONS` (line 29, comment 'publish.HORIZONS') while core's is generated from publish.py. app/tools/feed-contract.test.ts:25-27 checks only the committed fixture packages/data/dev/feeds/gauge.
- Example: The fixture index it checks is stamped fetchedAt 2026-08-30T07:20:00Z; a worker change to a station's fields (rename `percentile`, change `ahead` units) passes every test and is read by old apps as undefined values.
- Second opinion: No format stamp and no app-side shape check: confirmed. Overstated: the worker output is not unpinned. gauges.parity.test.ts runs the Python publisher (the spec) and the TS port on one recording and requires every output file to be the same JSON, so a one-sided worker change (including a HORIZONS drift, which changes the `ahead` keys) fails, where the repo venv exists (the test is skipped otherwise). What is really unguarded is a deliberate change made to both publishers, and the app's IndexFile type against either; nothing ties the app fixture to the publishers' output.
- Proposed fix: Add `format: "gauge-feed/N"` to index.json, obs.json and station files; the app refuses another N (keeps the capture, reports). Generate the IndexFile/StationFile types and HORIZONS for both worker and app from one source, and run feed-contract.test.ts against the worker's own test output as well as the fixture.

### 4. Gauge feed is the only feed with no format tag; the app accepts any JSON as the index or a station file
Severity 2, fragility, confirmed; area app-update-integrity.
- Evidence: app/packages/data/src/feed/http.ts:361-364: offer() checks only that `stations` is an object. :302-327: load() caches and persists (history.noteIndex / noteFile) whatever JSON a 200 returned, with no shape check. IndexFile (:46-60) has no format field, and pipeline/gauges/feed/publish.py writes none (grep for "format" finds nothing). By contrast DFO is dfo-feed/3 (feed/salmon.ts offer), in-season is inseason-feed/1 and inseason-links/3, and the release has per-file formats checked by readsRelease (manifest.ts:65).
- Second opinion: Accurate as a fragility. Nothing is wrong today: publisher and app are in step. It bites only when publish.py changes a field's meaning or name while older apps are installed. The 'wrong standing' outcome is the finder's inference, not shown. No example from data was given and none exists.
- Proposed fix: Add `format: "gauge-feed/N"` to index.json and station files in publish.py (and the worker if it writes them). Add one parseGaugeIndex / parseStationFile in @app/core, used by both load() and offer(), that refuses and reports an unknown format while the kept copy answers with its age. Add a parity fixture shared by the Python publisher test and the TS parser test.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
update-hydat.sh no longer runs a second publisher (or it is a test-only spec, named so). The gauge index and station files carry a format tag; the app refuses an unknown one loudly (reportFailure) and keeps its held data.

Depends: —. Size: M. Owner: no.

## Done 2026-11-10
- Item 1 (8779d6298): update-hydat.sh rebuilds clim.json only (no step 3); publish.py says it is the worker's parity spec
  and the local dev feed; hydat_daily requires --index. AGENTS rule 64 says so.
- Item 2 (8779d6298, part): Python's duplicate `forecastIssuedAt` key is gone (parity compares it as is). The worker's
  HORIZONS is core's generated FORECAST_DAYS (31ee818a0). LEFT: FLOOD_WEIGHTS / RISE_RATIO / STALE_GRACE_HOURS /
  DAY_COVER / MIN_PAIRS are still hand-copied in change.ts / score.ts (proposed: emit them with gauge_policy).
- Items 3-4 (31ee818a0): `gauge-feed/1` first in index.json, obs.json and every station file (worker imports core
  GAUGE_FEED_FORMAT; publish.py FEED_FORMAT; check-format-pairs.py). The app reads every gauge feed file through core
  gaugeFeedRefusal: another format / wrong shape / an index with no fetchedAt is refused, reported, and held data
  answers. A file with NO format is still read (every feed before this deploy). LEFT: refuse a format-less file once
  the staging and production workers both write the tag; feed-contract.test.ts on the worker's own output.

## Done 2026-11-11
- Item 2 (a6bf038be): `pipeline.tools.emit.gauge_policy` also writes app/workers/feeds/src/gauges/policy.generated.ts from
  change.py / skill.py (FLOOD_WEIGHTS, RISE_RATIO, FALL_RATIO, BIG_POINTS, SPIKE_BACK, STALE_GRACE_HOURS, DAY_COVER,
  CHANGE_SCALE, CODE_FLOOR, model names, LEADS, LOW/HIGH, MIN_PAIRS, MIN_ISSUE_DAYS); change.ts / score.ts re-export; the
  `--check` (pnpm check `gauge-policy`) covers both files; test_gauge_policy pins that neither TS file types them again.
- Contract (52878ceb6): app/tools/gauge-feed-contract.ts is the one check (format, fetchedAt, no section id, nothing the
  bundle owns, only keys the app reads, percentiles), run on the fixture (tools/feed-contract.test.ts) and on the worker's
  own output (workers/feeds/test/gauges.contract.test.ts). The fixture was stale (index `forecast` arrays, a station
  `forecast` key): fixed and tagged. Staging's live feed (index, obs, 429 station files, 2026-11-11 22:35Z) passes it.
- Format-less refused (2ebfe0787): staging writes gauge-feed/1 on every file (all 429 read); production serves no gauge
  feed yet (data domain 404), so its first deploy is this code. A phone's kept copy still answers offline.
