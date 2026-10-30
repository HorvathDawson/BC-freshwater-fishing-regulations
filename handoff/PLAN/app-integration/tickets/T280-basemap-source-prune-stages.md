# T280 — Basemap in two stages: raw source → pruned/simplified ship copy (forest simplification for pan speed)

Status: DONE 2026-10-29 (65894d8bf; staging r-164ca35636ea; prod upload human, below) (owner 2026-10-29: option A from T273, and "could we make it so the basemap we have a pruning stage? if it
doesnt look much different simplifying is always good … basemap in data that we get from pmtile directly then a translation
from that to the one we ship (with the other simplification we already did before). make sure it uses proper range requests").

Source: T273 measured pan hitches on new basemap tiles: 89 frames >50 ms on the drawn map vs 10 with the forest/green-space
layer removed (emulator, indicative).

## Work
1. **Source stage:** the raw Protomaps extract for the BC bbox, fetched straight from the upstream pmtiles with HTTP range
   requests (`pmtiles extract` / equivalent: header + directories + only the needed tile runs; resumable; verified), kept under
   data/ as the untouched source with its build date + upstream URL + hash. Re-fetch only on purpose.
2. **Ship stage:** one deterministic translation source → shipped basemap: today's trim (pipeline/deliver/tiles/basemap.py:
   bbox, zoom cap, roads rebuild, LANDUSE_DRAWN/WATER_LINES_KEPT, dropped layers) PLUS geometry simplification per zoom for
   polygons — landcover (forest), landuse/green space first; tolerance chosen by a visual gate (pixel diff of rendered tiles at
   several zooms over fixed BC extents, threshold in the ticket) and a size/vertex report.
3. Rebuild the basemap from the existing source (no re-fetch needed if the current raw extract is usable), re-run T273's pan
   benchmark (app/tools/testbed `run.mjs pan`), staging upload. Prod upload stays human (command in the Outcome).

## Acceptance
Two-stage build reproducible from the source file; render diff under threshold; pan benchmark frames >50 ms clearly down vs
T273's 89; basemap size before/after; tests pin the stages (basemap-filter tests still pass).

Depends: T273 (measurements). Size: M. Owner: no (prod upload is human).

## Outcome (2026-10-29)
**Source stage** — `pipeline/deliver/tiles/basemap_source.py`: `fetch` reads the upstream daily build
(`build.protomaps.com/<YYYYMMDD>.pmtiles`, ~140 GB) only by range requests (`pmtiles extract <url>`: header, directories,
the bbox's tile runs), one zoom band per part (z0-10, 11, 12, 13, 14, 15). Each part is verified and marked done. A re-run
skips the done bands and refuses a different upstream build. The parts are merged, verified again and moved into place,
with a sidecar `bc.pmtiles.source.json` {upstream_url, version, OSM replication, bbox, maxzoom, fetched_at, bytes, sha256}.
Tried for real on a small bbox from 20261029 (3 bands, merged, verified). The existing 4.36 GB source was kept (no
re-fetch). Its sidecar is committed: build 20260305, inferred from the OSM replication time and since pruned upstream,
so the sha256 identifies it.
**Ship stage** — `basemap.py` refuses a source that is not the file its sidecar names (`basemap_source.check`), and
stamps `source_*` (url, version, replication, sha256) into the shipped archive's metadata. New cut 6
(`basemap_grid.SIMPLIFY`), in the same re-encode pass as the grid: landuse and landcover rings are Douglas-Peucker
simplified per zoom, and slivers are dropped (with their holes).

Render gate: `app/tools/basemap-shots.mjs` (headless Chrome + maplibre-gl). It draws the app's own basemap layers
(`runtimeStyle`, non-symbol), old vs new, at 7 BC extents × z8/10/12/13/14/16 × light/dark = 84 shots, and counts the
pixels off by more than 24/255. **Threshold: 2 % per shot.**

| tolerance (landuse <z13 / z13, landcover) | green vertices | worst shot | mean | archive |
|---|---|---|---|---|
| live (no simplification) | 83.0 M | — | — | 274.5 MB |
| 1.5 / 1.0, 6 | −40 % | 0.89 % | 0.10 % | 239.1 MB |
| **2.5 / 1.5, 8 (shipped)** | **−52 %** | **1.97 %** | **0.23 %** | **225.5 MB** |
| 4.0 / 2.0, 12 | −62 % | 3.32 % (over) | 0.40 % | 214.6 MB |

The worst shots are light z8/z10, where wood edges shift by one pixel and specks of wood go. Side by side they read the same.
Densest tile's green vertices: z10 33.2k → 8.8k, z11 20.1k → 6.5k, z12 14.9k → 5.3k, z13 4.4k → 3.1k.
**Rebuild** from the source (261 s, 4 workers, 0.7 GB RSS): zoom audit clean on all 6 extents, `pmtiles verify` ok,
**basemap-b515359fc62f, 225,458,468 B (−17.9 %)**; every shot passes the gate against the live file. Promoted file by
file. The old file is kept: `data/generated/scratch/basemap-pre-T280-20261029.pmtiles` (+ `.meta.json`).
**Release** — `pipeline/deliver/release/rebase.py`: the same release with only the basemap swapped, under the id
`build.ship` would give it. It first proves the source release's id recomputes. Result: r-fa1915a56adb → **r-164ca35636ea**.
LATEST is not moved; the next data release names the new basemap by itself (`basemap.meta.json`).
**Staging:** `deploy/staging/upload.sh r-164ca35636ea`, smoke 18/18 pass; staging site not redeployed (not needed).

**Pan benchmark** (`run.mjs pan`, n1_pixel emulator; map-surface frames >50 ms per 16-swipe round; `PAN_UPDATE=1` takes
the live staging release first, which is new in `scenarios/pan.mjs`):

| run | old basemap (147bd) | new basemap (b5153) |
|---|---|---|
| first pan after the release update (circle, travel) | 174, 57 | **31, 11** |
| steady, 4 rounds (circle, travel, circle, travel) | 19, 8, 13, 11 (Σ 51) | 20, 17, 27, 44 (Σ 108) · 22, 11, 10, 12 (Σ 55) |

Read honestly: right after a release lands (cold tiles, new set), the new basemap is clearly better. In steady state
the emulator can't separate the two: two after-runs differ from each other by 2×, and today's old-basemap baseline
(Σ 51) is already far below T273's 89. The geometry win is certain (half the vertices per tile, −18 % bytes).
Whether a real phone feels it has to be measured on a real phone with the same scenario. That is T245's
lane, and its baseline should start from this basemap.

**Prod upload (human):** `.venv/bin/python -m pipeline.deliver.release.publish --release data/release/r-164ca35636ea --yes`
(or skip it, and the next data release carries basemap-b515359fc62f).
