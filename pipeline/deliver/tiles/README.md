# pipeline/deliver/tiles: the map

Cuts the atlas into the map the app draws: `atlas.pmtiles` (the water, z4-13), `atlas.meta.json` (a sidecar recording which atlas and
registry it was cut from) and the `layers/*.geojsonl` files, with tippecanoe + pmtiles (`brew install tippecanoe pmtiles`). The OSM basemap
(`basemap.py`, `basemap_roads.py`) is built separately, by hand, and is never replaced by a tiles run.

- Entry: `python -m pipeline.deliver.tiles --build data/generated/atlas/full` (the `tiles` stage of `python -m pipeline build`).
- Reads: the atlas and its registry; writes `data/generated/tiles/` (`atlas.pmtiles`, `atlas.meta.json`, `layers/`, `layers/undrawn.json` listing
  the sections left out on purpose). The release names the basemap it uses (`data/release/basemap/<id>/`).
- `near_bc.py` is a by-hand step: when `data/source/bc_boundary.geojson` changes it rewrites `app/packages/core/src/nearBcOutline.ts`.
- Rules: tiles and bundle must share one atlas and one registry vintage (`pipeline.common.vintage` refuses a stale pair;
  the strict check ends every build). Promote tiles file by file, never replace the tiles directory (it holds the basemap).
- Tests: `pipeline/tests/test_tiles*.py`, `test_basemap*.py`; the style contract is `tile-contract.json`.
