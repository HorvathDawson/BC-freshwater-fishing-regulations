# Runbook: build from a fresh clone (T416)

A new machine with only the git repo can build a release. Nothing the build needs lives only on the owner's Mac. This
was proven on 2026-11-05 from a fresh clone of GitHub (`~/t416-fresh`); the timings below are from that run on the
owner's Mac (M-series, 64 GB).

## 1. Clone and bootstrap

```bash
git clone --branch redesign/stream-sections git@github.com:HorvathDawson/BC-freshwater-fishing-regulations.git
cd BC-freshwater-fishing-regulations
bash scripts/bootstrap.sh            # PYTHON=python3.13 by default
```

`bootstrap.sh` checks the tools (tippecanoe, tile-join, pmtiles, curl, git; rclone and node are needed only to upload
or for the app), makes `.venv`, then fetches:

| what | from | size | how it is checked |
|---|---|---|---|
| `bc_fisheries_data.gpkg` (FWA 2024 + live layers as of the Mac's fetch) | `build.canifishthis.ca/source/<sha16>/` | 10.2 GB | sha256 in `data/source/pins.json` |
| `hydat.sqlite3` (HYDAT 20261019) | same bucket | 1.3 GB | sha256 |
| `fish_obstacles/fish_obstacles.gpkg` | same bucket | 33 MB | sha256 |
| `bc.pmtiles` (basemap, Protomaps 2026-03-05, pruned upstream) | `data.canifishthis.ca/bc.pmtiles` | 4.4 GB | sha256 |
| `basemap.pmtiles` (the RELEASED basemap, basemap-6960ed933a82: the live release's ground) | `build.canifishthis.ca/source/<sha16>/` | 225 MB | sha256; bootstrap then releases it (`pipeline.deliver.tiles.basemap --release-only`) |
| `generated/regs/extraction/{synopsis_raw_data,row_boxes}.json` (the synopsis extraction and its row boxes: the PDF highlight of every rule) | `build.canifishthis.ca/source/<sha16>/` | 0.8 MB | sha256 (the old `extraction/` backup was stale; a release without the row boxes is refused) |
| DFO salmon pages, typed | DFO's live pages (`pipeline.regs.dfo_salmon.live`) | small | today's pages |

Downloads resume (`curl -C -`); rerun the script after a drop. A file whose sha256 differs is deleted and the run fails.
Time: about 7 minutes at ~24 MB/s; a rerun with everything present takes seconds.

Everything else the build reads is in git (curated data, places, stations, the synopsis PDF, municipal layers, DFO
tidal pages, run-timing parse). Map fonts and sprites are fetched by the tiles stage itself, pinned to a commit.

## 2. Build

```bash
python -m pipeline build --dry-run           # on a fresh clone: "no atlas … run with --atlas --promote first"
bash scripts/heavy.sh first-build 50 -- .venv/bin/python -m pipeline build --atlas --promote --jobs 2
```

| stage | time | peak memory |
|---|---|---|
| atlas (+ parity, promote) | 25m56s | 23.2 GB |
| clim | 24 s | 0.9 GB |
| tiles | 8m54s | 10.7 GB |
| reach | 11m24s | 17.3 GB |
| deliver | 11m52s | 13.8 GB |

The machine needs about 24 GB of free memory for the atlas. About 1h15 in all.

**The basemap is not a build stage**; bootstrap releases the pinned one (above), so the release names the same
ground as the live one. **A release without a basemap is refused** (`basemap_block`: no `basemap.meta.json` beside the
tiles is an error, T416 — it used to ship `basemap: null` silently). To move to a newer OSM ground instead, build it
from `bc.pmtiles`: `python -m pipeline.deliver.tiles.basemap` (then pin it again). After this first build, `python -m pipeline release
--staging` works as in `release.md` (the first release has no live baseline; fetch `data/release/` from R2 or accept the
first binding diff).

## 3. Refreshing the inputs (not part of a rebuild)

The pins are the exact bytes the live release was built from. To move to newer upstream data:

```bash
PYTHONPATH=. .venv/bin/python data/fetch_data.py --missing-only   # or without the flag for everything (~55 min)
.venv/bin/python -m pipeline.tools.source_pins pin data/source/bc_fisheries_data.gpkg --upstream "…"
```

`fetch_data.py` fails (exit 1, listing each layer) when any layer fails. Live layers drift between fetches
(forest_service_roads, water_access_points, waterfalls, historic_sites, land_access, wma), so a refreshed gpkg gives a
slightly different release; that is a data update, reviewed through the binding diff like any other.

## Secrets

None are needed to build. Uploading needs the rclone remote `r2:` (an R2 S3 token) and a wrangler login; Play uploads
need the files under `~/.canifishthis/` (the owner's).

## Not mirrored on purpose

- `data/source/steelhead/` (6.6 GB accessibility model): only `pipeline.regs.steelhead.known_waters` reads it, to
  propose the curated steelhead list; the build reads the curated list in git. It re-fetches from the province.
- `data/source/official/` (region PDFs): nothing reads them.
- `data/source/runtiming/cu_tiles/`: the run-timing workflow fetches its own; the build does not read them.
- `bathymetry_pdfs/`: served from R2 already; the build does not read them.
