# T301 — New HYDAT release: refresh stored daily flows and normals

Status: DONE (fetched 2026-10-30, release r-c7e8327b781d; prod upload is the owner's; the old archive `data/source/hydat.20260717.sqlite3` can be deleted).

## Work
Confirm the new ECCC HYDAT release (version/date), download it (resumable, checksum), rebuild what depends on it: the
stored past-day flows (T274 `pipeline/gauges/feed/hydat_daily.py` → staging `feeds/hydat/`), normals/percentiles used by
Conditions, gauge station metadata; check the app's "new HYDAT release" notice clears. Diff station counts/periods vs the
old release; report anything that changes a gauge match (gauge-match one frozen file — never re-match). Prod upload stays human.

## Acceptance
App shows the new HYDAT version; notice gone on staging; diff summary.

Depends: T274. Size: S–M. Owner: no (prod upload).

## Outcome (2026-10-30)
- ECCC listing: `Hydat_sqlite3_20261019.zip` (posted 2026-10-29, 279,999,388 B, sha256 `644d0b26…602ed4`; ECCC publishes
  no checksum: size == Content-Length + zip CRC + `PRAGMA quick_check` + our sha256). VERSION table: 2026-10-19.
  Old archive kept as `data/source/hydat.20260717.sqlite3` (delete when happy).
- Rebuilt: clim.json (1,959 stations, +08NM249), hydat_daily (386 stations), release **r-c7e8327b781d** (`pipeline build
  --no-tiles`: same tiles + basemap; changed files climate / down / flow-area / paint only; manifest `hydat 2026-10-19`).
  matches.json untouched.
- Diff 20260717 → 20261019 (BC): 2,324 stations both, none added/removed; 2,048 with daily flow both; 1,499 station-months
  added; last year moved on 123 stations (mostly 2024→2025/2026). Normals: mean pentad-median shift 0.14%; 55 stations
  move >5% somewhere, 22 >10% (biggest 08MC049 Borland Ck, 08EB007, 08NM248, 08MG028, 08JB013, 10DA001, 07FD019).
  Metadata: 11 matched stations now Discontinued (08JB013 08KH030 08MC049 08NE008 08NH016 08NH138 08NN028 08NP003 08OA003,
  + 08DA012 08NB016 also lose real-time); 08HB083 renamed (5th Avenue → 5th Street Bridge); 08MH053 moved ~0.9 km;
  **08MH044 renamed FRASER RIVER AT WHONOCK → AT ALBION and moved ~6 km downstream** — its frozen match coordinate
  (Whonock) no longer is where the station is. Owner call; not re-matched.
- Staging: feeds/live/clim.json + feeds/hydat/ uploaded (also 08EG012/08KB006 from the staging index), release uploaded.
- Workflow: `.github/workflows/feeds-hydat.yml` (manual) now also builds + uploads `feeds/hydat/` (hydat_daily --index
  over the staging feed's index), fetches with `scripts/hydat-fetch.sh`, and stops early unless ECCC lists a newer HYDAT
  than the staging feed. Dispatch needs the file on main (`gh workflow run feeds-hydat.yml --ref redesign/stream-sections`).
- Prod (human): see the T301 report command block.
