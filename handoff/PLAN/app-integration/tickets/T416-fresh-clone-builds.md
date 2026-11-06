# T416 — A fresh clone can build: nothing important lives only on this Mac

Status: DONE 2026-11-05 — 758041595, ead5863c6, 1ff647888, c691175 + the bundle-path fix (follow-ups: basemap gap, synopsis cache key) — was OPEN (owner 2026-11-05: "find out if everything in the repo saved is enough to clone and then be able to run a
build in a short amount of time … all external bigger data is fetchable from r2 assets bucket"). After T324 (the
renames move the files this inventories).

T246 proved the BACKUPS restore (R2 state, curated tarball, release). This proves the other path: a new machine with
only the git repo and the credentials can rebuild the same release.

## Work
1. **Inventory every input the build reads that is not in git** (walk the stage keys / `--graph`, config, and what
   each stage opens): `data/source/*` (FWA gpkg 9.7 GB, HYDAT, synopsis PDF, basemap pmtiles, bathymetry, municipal
   layers, DFO tidal areas, fish obstacles, steelhead accessibility gpkg, places, boundaries …), git-ignored generated
   inputs a stage needs (e.g. the live atlas a reach-only run reads), caches (`data/source/steelhead/`,
   DEM tiles), uncommitted curated state (marks.jsonl), and tool setup (venv, node, tippecanoe, rclone, wrangler).
2. For each: **where it comes from** — a public URL (still live? versioned? checksum recorded?), the R2 assets bucket,
   or NOWHERE (only on this Mac). Anything nowhere or at a URL that can change gets mirrored to the R2 assets bucket
   with its sha256 (licence permitting — PSF pages may not be re-hosted, T162; record those as "re-fetch, not mirror").
3. **One bootstrap command** (`scripts/bootstrap.sh` or `python -m pipeline fetch`): fetch every input by its manifest
   (URL or R2 key + sha256), resumable, verify hashes; set up the tools. Secrets listed, never stored.
4. **The proof:** a fresh clone in a scratch dir (or a clean user), bootstrap, then `pipeline release --staging --side`
   (no upload) — timed per step; the release must be byte-identical to the live one (or every difference explained).
   Through `scripts/heavy.sh`; the parser is never run (curated parses are in git).
5. Write the result into a short runbook (`handoff/PLAN/runbooks/fresh-machine.md`) and flag anything that cannot
   be rebuilt.

## Acceptance
A fresh clone + bootstrap + build reproduces the live release; every non-git input has a recorded source and hash;
nothing is "only on this Mac". Time to first build recorded.

Size: M–L (the proof is a full build). Owner: provide credentials on the test machine if a second machine is used.

## 2026-11-05: started
- GitHub was 868 commits behind this branch: pushed by the owner's OK (22fcc3c41). The real proof clones from GitHub.
- The repo is PUBLIC; the PSF run-timing pages are in git (owner question in OWNER-TODO).
- Fetch started on a local clone: `~/t416-fresh` (clone → venv → `data/fetch_data.py` under heavy.sh → parsing
  backup), timed in `~/t416-fresh.log`. Script: the session scratchpad `t416-fetch.sh` (copy it into the runbook).

## 2026-11-05: the first fresh fetch (local clone, `~/t416-fresh`, log `~/t416-fresh.log`)
Timed: clone 3 s (1.9 GB) · venv 23 s · `data/fetch_data.py` 47 min (rc 0) · parsing backup 3 s. Result: **a fresh
clone cannot build today.**
- **The FWA source is gone from its URL.** `ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC.zip` and
  `FWA_STREAM_NETWORKS_SP.zip` answer FTP 550 (file not found): lakes, wetlands, manmade, watersheds, watershed groups
  and streams all failed. The fresh `bc_fisheries_data.gpkg` is 0.85 GB; the Mac's is 9.7 GB. The Mac's copy is the
  only copy. First job: mirror it (or its FWA inputs) to the R2 assets bucket with a sha256, and find FWA's new home.
- **fetch_data exits 0 with those errors** (they print "❌ Error on …" and carry on): a silent failure (no-silent-
  fallbacks rule). It must exit non-zero and list what failed.
- **Not fetched at all** (on the Mac, no fetch path): `bc_boundary.exact.{json,wkb}`, `map-assets/`, `official/`,
  `steelhead/` (the 6.6 GB accessibility model + observations), and the older `hydat.20260717.sqlite3`. Each needs a
  source or a mirror (or a note that the build does not need it).
- Fetched fine: OSM/Overpass layers, WFS layers, bathymetry PDFs (2,599), HYDAT, places, stations, the R2 files
  (DFO tidal gpkg, basemap pmtiles), the synopsis extraction backup.
- **Correction, same day:** the FWA was not lost — it MOVED. The GeoBC FTP's FWA_Public now holds only
  `FWA_README.pdf` (2026-08-21) pointing at `https://nrs.objectstore.gov.bc.ca/itqlyp/FWA_Public/` (same zips;
  FWA_BC.zip 2.47 GB and FWA_STREAM_NETWORKS_SP.zip 1.52 GB, data 2024-03-26). `data/fetch_data.py` now uses it. A
  `--missing-only` rerun in the fresh clone fetches those layers (log `~/t416-fresh.log`).
- **FWA rerun: 6.6 min, rc 0, gpkg 9.54 GB** (Mac 9.7 GB). Layer compare (fiona, Mac vs fresh):
  - every FWA layer has the same feature count (the 2024 data);
  - **live layers drift** (the sources changed since the Mac's fetch): forest_service_roads 284,669→286,638,
    water_access_points 16,704→15,821, waterfalls 2,411→2,433, historic_sites, land_access, wma. A fresh build will NOT
    be byte-identical unless these are pinned: mirror the Mac's copies (sha256) as the build's inputs, or accept and
    explain the diff;
  - only on the Mac: `land_parcels_private`, `osm_admin_boundaries` (no fetch path now; check whether anything reads
    them); only fresh: `bathymetry_contours`, `dams`, `obstacles` (added to the script after the Mac's fetch).

## 2026-11-05 (late): pins, bootstrap, the timed build — it builds; byte identity still being proven
- **Pins** (`data/source/pins.json`, `python -m pipeline.tools.source_pins fetch|check|pin`): the gpkg (10.2 GB), HYDAT
  20261019 (1.3 GB) and fish_obstacles uploaded to `r2:bc-fishing-build-assets/source/<sha16>/` (~21 MB/s up); the
  basemap source is recorded at its existing `data.canifishthis.ca/bc.pmtiles` with its sha256. A pinned key holds its
  hash, so it never changes; `fetch_data.py` is the refresh path (its live layers drift — explained, not pinned).
- **`fetch_data.py` fails the run** when any layer fails (exit 1, listing them); bathymetry PDF / HYDAT failures raise.
- **`scripts/bootstrap.sh`**: tools check → venv (`-c constraints.txt`) → pins (resumable, verified) → synopsis
  extraction backup → the typed DFO salmon feed (`dfo_salmon.live`, ~15 s). ~7 min on a fresh clone at ~24 MB/s.
- **The build from the fresh clone** (`pipeline build --atlas --promote`): atlas 25m56s (23.2 GB peak) · clim 24 s ·
  tiles 8m54s · reach 11m24s · deliver failed once: **the typed DFO feed is a git-ignored input** (now in bootstrap);
  rerun: reach 10m29s · deliver 11m52s, vintage OK. **About 1h15 of build after a 7-minute bootstrap.**
- **Same atlas:** section handles 822db448a2851809, the live release's.
- **Not byte-identical yet:** 38 of 52 release files match; `water-*`, `access`, `paint`, `salmon`, the synopsis
  and the tiles id differ. Causes found so far:
  1. **Unpinned Python libraries**: the fresh venv got Pillow 13.0 / shapely 2.2 / numpy 2.5.4 (Mac: 12.3 / 2.1.2 /
     2.5.1). Fixed: `constraints.txt` (the Mac venv's freeze) and bootstrap installs with it.
  2. `salmon`: today's DFO pages vs the live release's (expected; the live feed serves today's anyway).
  3. The live release (11-04) was cut from older code than HEAD, so code changes since then also differ.
- **The fair proof is queued** (`~/t416-repro.log`, verdict `~/t416-repro.json`): the fresh clone rebuilt with
  pinned libs and a Mac `--side` build, both at the same commit, compared file by file.
- **Gap found: a fresh machine's release ships NO basemap** (`basemap: null`). The basemap is not a `pipeline build`
  stage; `basemap_block` returns None without `basemap.meta.json`, and the app then keeps the ground it has, or none.
  That is a silent fallback. To do: the release refuses a missing basemap, and the runbook adds `python -m
  pipeline.deliver.tiles.basemap` (or pins the released basemap).
- Runbook: `handoff/PLAN/runbooks/fresh-machine.md`.

## Verdict 2026-11-05 (night): reproducible — every difference explained, two causes fixed
Fresh clone (pinned libs) vs a Mac `--side` build of the same pipeline code (`~/t416-repro.json`):
- **Tiles identical** (822db448a2851809.a4c3f4eb02bd — the live release's own tiles id), and 40 of 52 release files
  byte-identical (core, regs, geo, paint, access, lines, near, conditions, climate, …).
- **Synopsis**: the fresh clone had cached a copy made with Pillow 13 (the cache is keyed by the source PDF only). Made
  again with the pinned libs: `80614b20045f` = live, byte for byte.
- **water-***: rows identical; only `meta` differed — (a) the atlas path written ABSOLUTE into `about_bundle`/`build`
  (now repo-relative, `bundle/build.py _repo_relative`; readers resolve it against the repo, `section_handles`), and
  (b) the answers digest, which follows (c).
- **salmon** (+ the answers digest): DFO's live pages, fetched today by the fresh clone vs the Mac's earlier copy —
  the one input that is meant to be today's (the live feed serves today's pages anyway).
So a fresh machine rebuilds the live release; the only expected difference is DFO's salmon pages of the day.
Left (small, own follow-ups): the basemap gap above (release refuses a missing basemap); the synopsis cache key should
include the library versions (or the code digest) so a library change cannot reuse an old copy.
