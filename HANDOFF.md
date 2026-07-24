# Handoff — cron/container removal, fetch↔match decoupling, layer manifest, geo gate

Date: 2026-07-24

## TL;DR
Removed all Docker/container infra (containers aren't free) and made the Cloudflare
piece a free **Worker that dispatches GitHub Actions** on a punctual cron. Re-architected
hydro + stocking so the **crons only fetch + shape per-identifier data**, while the
**reach→identifier matching tables are static artifacts built once per full pipeline run**.
This drops the persisted `hydro.db` / `hydro_seed.db` and all matching logic from the crons.
Also made the map layer menu **manifest-driven** (single Private Land toggle) and added a
**BC geolocation gate** (out-of-province → default to Vancouver).

All code compiles: pipeline `py_compile` + 315 pytest pass (touched suites), `webapp` `tsc`
clean. **Not yet deployed** — awaiting a full pipeline rebuild + R2 reseed (see Deploy).

---

## What changed, by area

### 1. Docker → Cloudflare trigger Worker  (`cron-runner/`)
- Deleted `cron-runner/Dockerfile` and the repo-root `.dockerignore` (existed only for it).
- `cron-runner/src/index.ts` is now a plain Worker: `scheduled()` POSTs GitHub
  `workflow_dispatch` for each workflow in the `WORKFLOWS` var. Manual `GET /__run?token=`
  kept. No container / Durable Object / image.
- `wrangler.toml`: dropped `[[containers]]` / `[[durable_objects.bindings]]` / `[[migrations]]`;
  kept `[triggers]` (punctual cron); added `[vars]` GITHUB_OWNER=`HorvathDawson`,
  GITHUB_REPO, GITHUB_REF=main, WORKFLOWS=`update-hydro.yml`.
- `package.json`: removed `@cloudflare/containers`; bumped `@cloudflare/workers-types` peer range.
- **New secret required:** `GITHUB_TOKEN` (fine-grained PAT, Actions: read/write). Optional `TRIGGER_TOKEN`.

### 2. Hydro — one unified, DB-less cron  (`pipeline/recurring/hydro/`)
- `jobs.py` rewritten to a single `run` job. **No persisted DB in R2** — state lives in the
  JSON artifacts; each run uses an ephemeral `/tmp` sqlite as scratch (reuses hydro_poc
  fetchers + export_hydro shapers). Internal cadence gating:
  - every run: fetch latest → `stations.json` (index) + `gauges.geojson` + `recent/<id>.json`
  - daily (marker `history_date.json`): pull-merge-push `history/<id>.json` (idempotent by date)
  - HYDAT (marker `hydat_release.json`): rebuild `climatology/<id>.json` only when the HYDAT
    *listing date* advances (cheap check, no 266 MB download otherwise)
  - first run (no prior `stations.json`): full bootstrap incl. HYDAT
- `export_hydro.py`: dropped the `fwa`/fid-shard resolution from `stations.json`; added
  `build_geojson()` (map points); `build_stations_index` takes an optional `clim_station_ids`
  override (has_climatology preserved from prior index on non-HYDAT runs).
- `seed.py` deleted. `pipeline/tests/test_hydro_compact.py` deleted (compaction removed).
- **New:** `gauge_matches.py` — builds the static `gauge_matches.json`
  `{reach: {reach_id:[station_id]}, waterbody:{wbk:[station_id]}}` at atlas/enrich time
  (resolves gauge_fwa_match fid→reach via deploy shards). `match_fwa.py` unchanged (still
  writes gauge_fwa_match into a local hydro.db).

### 3. Stocking — fetch vs match decoupled  (`pipeline/recurring/stocking/resolver.py`)
- `export_records(db, out)` (CRON): per-waterbody `cron/stocking/records/<waterbody_id>.json`
  + `stocking_index.json`. No reach resolution, no geopandas.
- `build_matches(db, poly_reaches)` (PIPELINE): static `stocking_matches.json`
  `{reach:{reach_id:[waterbody_id]}, waterbody:{wbk:[waterbody_id]}}`.
- `builder.py` enrich step now emits `gauge_matches.json` + `stocking_matches.json`, and also
  **seeds** `records/` + `stocking_index.json` so the first deploy ships stocking content.

### 4. Frontend join  (`webapp/src/services/waterbodyDataService.ts`, `components/*`)
- Gauge + stocking join at lookup time via the small match tables; per-id record fetch.
  Dropped the 22 MB whole-file `stocking.json` load and the `stations.json` `fwa` reliance.
- `GaugeChart.tsx`: draws the this-year seasonal trace even without a ≥10 yr climatology.
- `Map.tsx`: gauge map source built via `getGaugePoints()` (reach_id resolved from match table).

### 5. Layer manifest — menu is manifest-driven  (single source of truth)
- `pipeline/tiles/tile_exporter.py`: `_LAYER_MANIFEST` has a single toggleable entry keyed
  `land_parcels_private` → `{visible:false, toggleable:true, label:"Private Land"}`. Only
  Private is surfaced (Crown/Public is its inverse → implied). The tile **source layer stays
  `land_parcels_crown`** (it holds all parcels; renaming would need a data re-fetch).
- `scripts/seed-r2.sh`: now ships `layer_manifest.json` (un-excluded).
- `waterbodyDataService.ts`: `LayerManifest` type + `getLayerManifest()` loader.
- `Map.tsx`: menu rows generated from the manifest into a `.layer-menu-layers` placeholder;
  the ONLY frontend piece is `LAYER_STYLE_MAP` (`land_parcels_private` → private style-layer
  ids). Old hardcoded `LAYER_TOGGLES` array removed.
- **Decision:** `admin_visibility.json` kept SEPARATE from the manifest (different concern /
  pipeline stage / lifecycle; "visible" would be overloaded).

### 6. Geolocation BC gate  (`webapp/src/components/Map.tsx`)
- Added `VANCOUVER` fallback + `isInBC(lng,lat)` (against `BC_BOUNDS`). In `updateGpsDot`, an
  out-of-BC position clears the dot and points "center on me" at Vancouver. Map already
  defaults its initial center to Vancouver.

### 7. Workflows / scripts / docs
- `.github/workflows/update-hydro.yml` (new, `*/30`, one unified job) replaces
  `update-hydro-realtime.yml` + `update-hydro-nightly.yml` (deleted).
- `scripts/update-hydro.sh` (new) replaces `update-hydro-{realtime,nightly}.sh` (deleted).
- `scripts/update-stocking.sh` + `.github/workflows/update-stocking.yml`: export records +
  upload the `cron/stocking/` tree (no poly_reaches / reach resolution).
- `config.yaml`: removed `hydro_seed` path.
- Docs updated: `pipeline/recurring/CRON_DATA_FLOW.md`, `DEPLOY.md`, hydro `__init__.py`,
  stocking `__init__.py`.

---

## Deploy / rebuild (do in this order)
1. **Full pipeline build (local):** `python -m pipeline --step all`. Regenerates the deploy
   tree incl. `cron/hydro/*` + `gauge_matches.json`, `cron/stocking/{records,stocking_index,
   stocking_matches}.json`, and `layer_manifest.json`.
2. **Seed R2:** `DEPLOY_ENV=production ./scripts/seed-r2.sh` (try staging first).
   `rclone copy` does NOT delete — **manually remove stale keys** afterward:
   `cron/hydro/hydro.db`, `cron/hydro/hydro_seed.db`, `cron/stocking/stocking.json`.
3. **Cloudflare Worker:** in `cron-runner/`, `npx wrangler secret put GITHUB_TOKEN`
   (+ optional `TRIGGER_TOKEN`), then `npx wrangler deploy`. Verify with
   `GET /__run?token=…` → the repo Actions tab shows `update-hydro.yml` dispatched.
4. First hydro cron run does a one-time 266 MB HYDAT pull (no marker yet), then settles.
5. **webapp / mobile:** rebuild + deploy as usual (frontend already typechecks).

## Verification already done
- Pipeline `py_compile` on all touched modules; `.venv/bin/python -m pytest` → 315 pass.
- `export_hydro` + `gauge_matches` validated vs the real local `hydro.db`: 415/415 stream
  gauges resolved, `gauges.geojson` 450 features, `stations.json` has no `fwa`.
- `export_records` + `build_matches` validated vs real `anglerinfo.db`: 2318 record files,
  2223 reach keys.
- `layer_manifest` emits exactly one toggleable entry `land_parcels_private`.
- `webapp` `tsc -p tsconfig.app.json` clean after every change.

## Open decisions / follow-ups
1. **GHA `schedule:` fallback** — kept as a coarse fallback alongside the CF Worker. Remove it
   later if you want dispatch-only.
2. **R2 write budget** — recent/ at 30-min ≈ 500K Class-A writes/mo (under the 1M free cap).
   Can shard `recent/` by station_id prefix later to raise cadence toward 15 min.
3. **ESLint** — pre-existing `no-explicit-any` noise in `Map.tsx` (the file already had ~41);
   my additions are typed. Not a gate; `tsc` is clean.
4. **Layer-toggle set** — deliberately just the single Private Land toggle. To add more, add a
   `toggleable` manifest entry + a `LAYER_STYLE_MAP` entry (menu row auto-generates).

## Key files
- Worker: `cron-runner/src/index.ts`, `cron-runner/wrangler.toml`
- Hydro: `pipeline/recurring/hydro/{jobs.py,export_hydro.py,gauge_matches.py}`
- Stocking: `pipeline/recurring/stocking/resolver.py`, `pipeline/enrichment/builder.py`
- Manifest: `pipeline/tiles/tile_exporter.py`, `scripts/seed-r2.sh`
- Frontend: `webapp/src/services/waterbodyDataService.ts`,
  `webapp/src/components/{Map.tsx,GaugeChart.tsx}`
- Workflows/scripts: `.github/workflows/update-hydro.yml`, `scripts/update-hydro.sh`,
  `scripts/update-stocking.sh`
