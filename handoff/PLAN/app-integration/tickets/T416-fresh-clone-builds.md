# T416 — A fresh clone can build: nothing important lives only on this Mac

Status: OPEN (owner 2026-11-05: "find out if everything in the repo saved is enough to clone and then be able to run a
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
