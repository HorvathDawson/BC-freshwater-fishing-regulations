# T274 - Past-day flow from our own server (no ECCC call from the device)

Status: DONE in code 2026-10-29 (owner: "we should have it stored"); staging R2 seeded; worker not involved.

Source: the T270 audit found `dayFlow.ts` asking ECCC GeoMet (`api.weather.gc.ca`) from the device for a spot's visit-day flow older than the feed's record.

## Done
1. `pipeline/gauges/feed/hydat_daily.py` (`python -m pipeline.gauges.feed.hydat_daily`): HYDAT daily mean flow for every station the feed publishes
   (386), newest 30 years, 366-slot array per year (Feb 29 = slot 59), 4 significant figures, null gaps; `hydat-daily/1`, release stamped. 20 MiB total,
   about 54 KB a station (about 15 KB over the wire). Built from the local HYDAT file, not by the worker: HYDAT changes once a release, so it is a
   build step beside the climatology (rerun when `scripts/update-hydat.sh` brings a new release), seeded by `deploy/staging/upload.sh`.
2. Staging R2 `feeds/hydat/<station>.json` uploaded (200, CORS open) at data-staging.canifishthis.ca. Local tile server serves `/feeds/hydat/`.
3. `dayFlow.ts`: `geometHydat` -> `storedHydat(ENDPOINTS.hydat)` (per APP_ENV; one small file per station, cached for the session; a day before the
   file = no record, after its last year or no file = not published). `tools/one-history-read.test.ts` now pins that ECCC's API is named nowhere.
4. Privacy page: the api.weather.gc.ca line removed; downloads line mentions past days' flow.

## Notes
- Flow only (a level never stands in); a day with a level but no flow now reads "no record" where GeoMet could say "no flow". Days older than 30 years
  of record read "no record".
- Prod: upload `data/generated/gauges/hydat_daily/` to `<prod bucket>/feeds/hydat/` (human), with the same release as the prod bundle.
