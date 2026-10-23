# T68 — HYDAT / feed follow-ups

Status: DONE — 6b1103d1. All three lines built. Ships on the next feed republish (no atlas/reach/deliver/tiles).
Until then the live index has no `forecastIssuedAt`, so the Forecasts line reads "Date not recorded · Unknown" and the
Data block summary reads "Some dates not recorded". Follow-ups F1–F3 below.
- A "Forecasts" line on the More Data block: the feed index needs a feed-wide `forecastIssuedAt` (publish.py), the
  reader, a line. Feed republish only.
- The roster-drift notice's command uses the default roster path even with `--stations`.
- scripts/update-hydat.sh stops before upload; document/automate the R2 upload step (human-run with credentials).

## As built (6b1103d1)
1. **Forecasts line.** `publish.py` `newest_forecast_issue` writes `index.json` `forecastIssuedAt`. It holds the newest
   run's `issuedAt` across every station and model, copied as the station files carry it, so the line and a station's
   chart parse it the same way. It is null when the run holds no forecast. Older indexes do not have the field.
   - @app/data `IndexFile` has the field. `useSync` carries it on the same index read the sync icon already makes,
     with three states: the stamp, null, or undefined (no word).
   - Core: `forecastWhen` is the one rule for both surfaces.
   - Data block line: "Forecasts · Issued 9h ago · Up to date". It sits after River flows (`DATA_LINES`).
   - Details row: "River forecasts — The BC River Forecast Centre's models, issued daily in season" (`SOURCE_ORDER`,
     after the feed). The feed row's "what" is now "Environment and Climate Change Canada"; forecasts have their own row.
   - Freshness comes from the existing `STALE_AFTER.forecast` (36 h).
   - None running (null): "None running now" / "No forecast running now", with the FEED's own badge. The check is
     only as fresh as the feed that made it, so out of season does not count as unknown.
   - Feed reached but no field: "Date not recorded" / "Not stated by the gauge feed", Unknown.
   - Feed not reached: "Not checked yet", Unknown.
2. **Roster notice.** `roster_shown(--stations)` gives the repo-relative default for the default file and the given
   path otherwise. `roster_action(path)` returns the default command unchanged. For another file it inserts
   `cp data/source/bc_hydrometric_stations.json <path>` before the build (`fetch_data.py` writes only the default).
   The notice's `current`, the kept-notice path and the console line all use it.
3. **R2 upload.** `scripts/update-hydat.sh` has a step 5. It always prints `pipeline.tools.publish --release <newest
   r-*>` (the plan; it sends and spends nothing) and the `--yes` command. `--upload` sends it.
   - With `--upload`, R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET and boto3 are checked BEFORE
     the half-hour of work, and again right before the send.
   - The script was not run.

## Tests
- pytest: test_feed_maintenance.py + test_feed_forecast_issued.py + test_feed_roster.py + test_feed_notify.py: 44 passed
  (11 new). The other feed/forecast files: 83 passed.
- vitest: more / menu / conditions / sync.render / Menu / http: 151 passed.
- tsc -b, boundaries and platform are clean.
- Mutation-checked: each of these fails its tests.
  - pipeline: newest → oldest; the roster notice back to the default path; the index key dropped; a normalised stamp
    in place of the station-file form.
  - app: the forecast measured by the feed's stamp; none-running not special-cased; useSync collapsing null and
    undefined.

## Rebuild needed
- Feed republish only:
  `.venv/bin/python -m pipeline.gauges.feed.publish --out data/generated/gauges/feeds --clim data/generated/gauges/feeds/clim.json`
  (the cron's normal run). Then check `jq .forecastIssuedAt data/generated/gauges/feeds/index.json`: a stamp, or null
  out of season.
- No bundle, reach, atlas, deliver or tiles.

## Follow-ups (not done)
- **F1 — naive forecast stamps.** BCRFC `issuedAt` values are naive (e.g. `2026-10-22T09:22:00`), and two parts of the
  code read them differently:
  - publish.py `_parse` and forecast.py's docstring treat them as UTC.
  - JS `Date.parse` (forecastRunLine, and now the Forecasts line) treats them as device-local.
  - The times look like Pacific mornings, so the pipeline may be 7–8 h off. The app is right for B.C. readers.
  - Default: forecast.py `_issued` writes an explicit offset. That waits for T52 to merge, since T52 owns forecast.py.
- **F2 — the production cron's clim.json.** The production feed is published by its own cron (T86). After
  update-hydat.sh, the cron's `--clim` must be the new clim.json, or the hosted feed keeps the old envelope.
  - Default: T86 syncs clim.json with the release.
- **Q (owner, small) — `--stations` with another roster file.** The command copies the refreshed default roster over
  that file.
  - Default: as built. A hand-trimmed subset roster would be overwritten, but a subset also drifts every run (every
    listed station it lacks shows as "joined"), so `--stations` is in practice a full copy.
