"""Trim the OSM basemap to what a fishing map in British Columbia actually draws.

`data/source/bc.pmtiles` is a full Protomaps build of OpenStreetMap for the region: 4.36 GB, nine
layers, everything from building footprints to ATMs, over a bounding box that is mostly not
British Columbia. Shipping it whole is the single largest cost in the product — larger than
every regulation, every gauge and our own 875 MB atlas combined.

Four cuts, cheapest first. None of them re-tiles: `pmtiles extract` and `tile-join` both
copy tiles they keep (the feature filter re-encodes the tiles it thins, at the same zoom).

    1. CLIP TO THE PROVINCE.  The fetch bbox spans 48.2-60.0N, 139.1-114.0W — a rectangle
       whose corners are Alberta, Alaska, Washington and the Pacific. British Columbia is
       roughly a third of it. Tiles that do not touch the province are the largest single
       category of waste and the easiest to be sure about.

    2. CAP AT z14.  Our atlas stops at z14, so a z15 basemap is ground under water that is
       not there. It is also the most expensive level in the archive by a wide margin.

    3. DROP LAYERS NOBODY LOOKS AT.  Buildings and points of interest. Not `landuse`:
       dropping that was a mistake — the Protomaps style draws ELEVEN layers from it, and
       without it the map loses most of its colour and reads as a grey wireframe.

WHAT STAYS, and why
    earth       the coastline. Without it there is no land, only labels.
    water       lakes, rivers and sea AS THE BASEMAP DRAWS THEM. Ours goes on top.
    landcover   forest and ice — what makes a valley legible at a glance.
    landuse     parks, green space, built-up areas. 11 of the style's layers.
    roads       how you get there. A spot you cannot reach is not a spot.
    places      settlement labels, the thing people navigate by.
    boundaries  the provincial and international lines.

WHAT GOES
    buildings   the largest layer in an OSM extract and invisible above z14 anyway.
                Nobody has ever picked a run by its footprint.
    pois        restaurants, ATMs, benches. A different app's data.

    4. (T85) WITHIN landuse AND water, the features the style never draws — see LANDUSE_DRAWN.

Run: python -m pipeline.deliver.tiles.basemap
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from pipeline.common.curated import CURATED, GENERATED, SOURCE

KEEP = ("earth", "water", "landcover", "landuse", "roads", "places", "boundaries")
DROP = ("buildings", "pois")
MAX_ZOOM = 14

# 4. KEEP ONLY THE FEATURES THE STYLE DRAWS (T85, decisions D32 "the basemap landuse filter"). A
#    feature filter per layer (`tile-join -j`), measured on the live basemap (918.4 MB, 2026-10-24):
#
#    landuse  345.0 MB. The Protomaps style (@protomaps/basemaps 5.7.2) draws only the kinds below;
#             the app also takes its parks and military out (`runtime-style.ts` OURS_NOW: we draw
#             those from the province's own layers). Not drawn, so dropped: park 8.9, nature_reserve
#             5.7, residential 4.0, farmland 2.7, bare_rock 2.6, national_park 0.7, pitch 0.5, ...
#             about 26 MB. `wetland` (52.0 MB) is NOT drawn either and is KEPT on purpose: it is the
#             muted stand-in for the unnamed wetlands the atlas no longer draws (T64), should the map
#             read worse without them — one style layer away, with no basemap rebuild.
#    water    362 MB, of which `stream` LINES are 241.8 MB. The style draws stream lines only from z14
#             (`water_stream`), under our own FWA streams, which the atlas draws at every zoom; the
#             OSM-only ones (culverted urban creeks) carry no regulation. Dropped. River lines (z9+,
#             9.4 MB), every polygon and every label point stay.
#
#    `app/tools/basemap-filter.test.ts` holds these lists to what the app's style draws: a Protomaps
#    upgrade that draws a new kind fails there before it ships blank ground.
LANDUSE_DRAWN = ("aerodrome", "airfield", "allotments", "beach", "cemetery", "college", "dam", "forest",
                 "glacier", "golf_course", "grass", "grassland", "hospital", "industrial", "pedestrian", "pier",
                 "playground", "runway", "sand", "school", "scrub", "taxiway", "university", "village_green",
                 "wood", "zoo")
LANDUSE_STANDIN = ("wetland",)
WATER_LINES_KEPT = ("river",)
WATER_LINES_DROPPED = ("stream",)


def feature_filter() -> dict:
    """The per-layer `tile-join -j` filter (cut 4): what the style draws, and nothing else."""
    return {
        "landuse": ["in", "kind", *LANDUSE_DRAWN, *LANDUSE_STANDIN],
        "water": ["any", ["!=", "$type", "LineString"], ["in", "kind", *WATER_LINES_KEPT]],
    }

# The clip is generous on purpose. A tile that straddles the border carries ground on both
# sides, and losing the far bank of a boundary river to save a few megabytes would be a bad
# trade — the Columbia and the Kootenay both leave and re-enter the province.
BUFFER_M = 5_000


def _boundary_4326(src: Path, out: Path) -> Path:
    """BC's outline in WGS84, buffered. `pmtiles extract` needs lon/lat; ours is BC Albers."""
    import geopandas as gpd

    gdf = gpd.read_file(src)
    gdf = gdf.to_crs(3005)                       # buffer in metres, not degrees
    gdf["geometry"] = gdf.geometry.buffer(BUFFER_M)
    gdf = gdf.to_crs(4326)
    # pmtiles wants a bare Polygon/MultiPolygon, not a FeatureCollection.
    geom = gdf.geometry.union_all()
    out.write_text(json.dumps(geom.__geo_interface__))
    return out


def build(src: Path, out: Path, boundary: Path) -> Path:
    for tool in ("pmtiles", "tile-join"):
        if shutil.which(tool) is None:
            raise FileNotFoundError(f"{tool} is not on $PATH")
    if not src.exists():
        raise FileNotFoundError(f"{src} not found")

    before = src.stat().st_size
    out.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        region = _boundary_4326(boundary, Path(tmp) / "bc.geojson")
        clipped = Path(tmp) / "clipped.pmtiles"

        print(f"  {src.name}  {before / 1e9:.2f} GB")
        print(f"  1. clipping to British Columbia (+{BUFFER_M / 1000:.0f} km) and z<={MAX_ZOOM}")
        _run(["pmtiles", "extract", str(src), str(clipped),
              f"--region={region}", f"--maxzoom={MAX_ZOOM}"])
        step1 = clipped.stat().st_size
        print(f"     {step1 / 1e9:.2f} GB  ({100 * (1 - step1 / before):.0f}% smaller)")

        print(f"  2. dropping {', '.join(DROP)}")
        print(f"  3. keeping only the landuse kinds and water lines the style draws "
              f"(water lines dropped: {', '.join(WATER_LINES_DROPPED)})")
        # Keeping by name, not excluding: a future Protomaps schema that adds a layer
        # should not start shipping it without anyone deciding to.
        _run(["tile-join", "-f", "-o", str(out), "--no-tile-size-limit",
              *[a for lyr in KEEP for a in ("-l", lyr)],
              "-j", json.dumps(feature_filter(), separators=(",", ":")), str(clipped)])

    after = out.stat().st_size
    print(f"  ✅ {after / 1e9:.2f} GB  ({100 * (1 - after / before):.0f}% smaller overall)")
    return out


def _run(cmd: list[str]) -> None:
    # Both tools name every tile they touch; that is megabytes of progress noise.
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({r.returncode})\n{r.stderr[-3000:]}")


if __name__ == "__main__":
    # DELIBERATELY ITS OWN ENTRY POINT, and nothing else calls it.
    #
    # This is the OSM half: take Protomaps' 4.36 GB planet extract, clip it to BC and drop
    # the layers we do not draw. It depends on `data/source/bc.pmtiles` and on NOTHING this
    # pipeline builds — no graph, no registry, no atlas — so it must never be a step of
    # `python -m pipeline.deliver.tiles`, which is the ATLAS build and runs on every
    # geometry change. The basemap changes when Protomaps publishes, which is rarely.
    #
    #     python -m pipeline.deliver.tiles.basemap   the OSM ground: clip + reduce
    #     python -m pipeline.deliver.tiles           the atlas: our water, from a build
    #
    # `parents[3]`, not [2]: this module sits at pipeline/deliver/tiles/, and the move that
    # added `deliver/` silently repointed the output at pipeline/output/. Asserted rather
    # than trusted, for the same reason `common/curated.py` asserts its root.
    build(SOURCE / "bc.pmtiles",
          GENERATED.tiles / "basemap.pmtiles",
          SOURCE / "bc_boundary.geojson")
