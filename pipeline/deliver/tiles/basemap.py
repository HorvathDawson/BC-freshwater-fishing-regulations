"""Trim the OSM basemap to what a fishing map in British Columbia actually draws.

`data/bc.pmtiles` is a full Protomaps build of OpenStreetMap for the region: 4.36 GB, nine
layers, everything from building footprints to ATMs, over a bounding box that is mostly not
British Columbia. Shipping it whole is the single largest cost in the product — larger than
every regulation, every gauge and our own 875 MB atlas combined.

Three cuts, cheapest first. None of them re-tiles: `pmtiles extract` and `tile-join` both
copy tiles they keep.

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

Run: python -m pipeline.deliver.tiles.basemap
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from pipeline.common.curated import CURATED, SOURCE

KEEP = ("earth", "water", "landcover", "landuse", "roads", "places", "boundaries")
DROP = ("buildings", "pois")
MAX_ZOOM = 14

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
        # Keeping by name, not excluding: a future Protomaps schema that adds a layer
        # should not start shipping it without anyone deciding to.
        _run(["tile-join", "-f", "-o", str(out), "--no-tile-size-limit",
              *[a for lyr in KEEP for a in ("-l", lyr)], str(clipped)])

    after = out.stat().st_size
    print(f"  ✅ {after / 1e9:.2f} GB  ({100 * (1 - after / before):.0f}% smaller overall)")
    return out


def _run(cmd: list[str]) -> None:
    # Both tools name every tile they touch; that is megabytes of progress noise.
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({r.returncode})\n{r.stderr[-3000:]}")


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    build(SOURCE / "bc.pmtiles",
          root / "output" / "tiles" / "basemap.pmtiles",
          SOURCE / "bc_boundary.geojson")
