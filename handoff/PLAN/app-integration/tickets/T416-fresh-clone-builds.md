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
