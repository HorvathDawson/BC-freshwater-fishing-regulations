"""The province as a FIELD — one polygon per watershed group, for the zooms where a river
is a hairline.

WHY A SHAPE AND NOT A GLOW. The first answer to "what does the map say at z5" was a soft
disc around each gauge, and it was wrong three ways at once: a disc paints land the gauge
says nothing about, two overlapping discs blend into a colour that is not on the scale, and
a blurred edge is a lighter shade — which on a sequential ramp is a different number. A
percentile is a claim about a CATCHMENT. A catchment is a shape.

WHY THE GROUPS AND NOT THE NAMED WATERSHEDS. The second answer used
`FWA_NAMED_WATERSHEDS_POLY`, and that layer answers a different question: it is 11,580
basins covering 2.68 MILLION km2 of a 945,000 km2 province, because a named watershed
contains its tributaries' watersheds. Drawn as they come they stack four deep and the
largest wins; filtered to the small ones they leave the province full of holes. Both
failures were visible on screen — busy where they overlapped, blank where they did not.

`FWA_WATERSHED_GROUPS_POLY` is the Province's own complete cover: 246 polygons, 948,072 km2,
ZERO overlap — which is British Columbia, once. It is also the unit the rest of the FWA is
keyed on, so a stream already carries its `WATERSHED_GROUP_CODE` and a gauge's group is a
lookup rather than a point-in-polygon.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.deliver.tiles.layers import BY_NAME

#: Simplification, in metres, and it is DERIVED rather than chosen by eye.
#:
#: At z8 — the deepest zoom this layer draws — one tile pixel is 351 m of ground at BC's
#: latitude. A vertex finer than a pixel cannot be drawn, so anything below that is detail
#: the screen throws away and the reader pays for. 500 m is the next step out and is what
#: the coastline actually needs: these are large shapes and most of their vertices are in
#: fjords that read as one edge at this zoom.
SIMPLIFY_M = 500.0

#: The layer in the FWA gpkg. See `data/fetch_data.py` for where it comes from.
LAYER = "watershed_groups"

#: The column that joins a group to the streams and lakes inside it.
KEY = "WATERSHED_GROUP_CODE"


def groups(gpkg: str):
    """The watershed groups, in BC Albers, with a `basin_id`. Shared with the bundle build."""
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer=LAYER, engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    # The CODE rather than the id: it is short, stable, human-readable in a tile inspector,
    # and it is what the stream and lake tables carry.
    gdf["basin_id"] = gdf[KEY].astype(str)
    return gdf


def export_basins(gpkg: str, out_dir: Path) -> dict:
    """Write the basin layer. A missing groups layer is a skip, never a failure."""
    import fiona
    from pyproj import Transformer

    from pipeline.deliver.tiles.export import _to4326, _writer

    if LAYER not in set(fiona.listlayers(gpkg)):
        print(f"  basin            <- no {LAYER} layer, skipping")
        return {"basin": 0}

    spec = BY_NAME["basin"]
    write, close = _writer(out_dir, spec)
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    for row in groups(gpkg).itertuples():
        g = row.geometry.simplify(SIMPLIFY_M, preserve_topology=True)
        if g is None or g.is_empty:
            continue
        write(_to4326(g, tf), {"basin_id": row.basin_id}, spec.minzoom)
    n = close()
    print(f"  basin            <- {LAYER} {n:>6,}  (simplified {SIMPLIFY_M:g} m)")
    return {"basin": n}
