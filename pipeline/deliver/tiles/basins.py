"""The province as a FIELD — one polygon per catchment, for the zooms where a river is a
hairline.

WHY A SHAPE AND NOT A GLOW. The first answer to "what does the map say at z5" was a soft
disc around each gauge, and it was wrong in three ways at once: a disc paints land the
gauge says nothing about, two overlapping discs blend into a colour that is not on the
scale, and a blurred edge is a lighter shade — which on a sequential ramp is a different
number. A percentile is a claim about a CATCHMENT. A catchment is a shape. The Province
publishes 11,580 of them and we already read the file for the drainage model.

THE LEAVES ONLY. Those 11,580 cover 2.68 million km2 of a 945,000 km2 province, because a
named watershed contains its tributaries' watersheds — drawn as they come they stack four
deep and the largest wins. Under 500 km2 they stop nesting and become a mosaic: 11,024
polygons over 586,000 km2, which is a map.

WHICH STATION SPEAKS FOR EACH IS NOT DECIDED HERE. It is decided in the bundle build
(`basin_station`), because it changes when the gauge network changes and this changes when
the Province republishes the FWA. Two clocks, two artifacts — the same reason the tiles and
the bundle are separate in the first place.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.deliver.tiles.layers import BY_NAME

#: Above this a named watershed is a container for other named watersheds rather than a
#: piece of ground, and the mosaic stops being a mosaic. Measured: at 500 km2, 11,024 of
#: 11,580 polygons remain and they cover 586,000 km2 with almost no overlap.
LEAF_MAX_KM2 = 500.0

#: Simplification, in metres, and it is DERIVED rather than chosen by eye.
#:
#: At z8 — the deepest zoom this layer draws — one tile pixel is 351 m of ground at BC's
#: latitude. A vertex finer than a pixel cannot be drawn, so anything below that is detail
#: the screen throws away and the reader pays for. Measured over the 11,024 leaves:
#:
#:     tolerance   vertices   GeoJSON   outline area moved
#:       150 m      401,171    18.0 MB        2.2%
#:       300 m      222,567    10.5 MB        4.3%     <- here
#:       500 m      144,466     7.2 MB        6.8%
#:      1200 m       76,697     4.4 MB       14.7%
SIMPLIFY_M = 300.0


def basin_id(code: str) -> str:
    """The FWA watershed code with its padding off — which is what makes it a hierarchy.

    A code is `300-432687-380566-000000-...` out to twenty-one segments. The trailing
    `000000`s are padding, not levels, and leaving them on makes every code look the same
    depth: a basin's parent is its code minus one REAL segment, and with the padding on
    there is no such thing. This is the key `basin_station` joins on, so it is defined once,
    here, and imported by the bundle builder rather than re-derived there.
    """
    parts = str(code).split("-")
    while len(parts) > 1 and parts[-1] == "000000":
        parts.pop()
    return "-".join(parts)


def leaves(gpkg: str):
    """The leaf watersheds, in BC Albers, with a `basin_id`. Shared with the bundle build."""
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer="watersheds", engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    gdf["km2"] = gdf.geometry.area / 1e6
    gdf["basin_id"] = gdf["FWA_WATERSHED_CODE"].map(basin_id)
    leaf = gdf[gdf["km2"] <= LEAF_MAX_KM2].copy()
    # One polygon per basin id: a code can appear on several rows where the Province split
    # a watershed across sheets, and two features with one id would fight over the same
    # feature-state.
    return leaf.dissolve(by="basin_id", as_index=False, aggfunc="first")


def export_basins(gpkg: str, out_dir: Path) -> dict:
    """Write the basin layer. A missing watersheds layer is a skip, never a failure."""
    import fiona
    from pyproj import Transformer

    from pipeline.deliver.tiles.export import _to4326, _writer

    if "watersheds" not in set(fiona.listlayers(gpkg)):
        print("  basin            <- no watersheds layer, skipping")
        return {"basin": 0}

    spec = BY_NAME["basin"]
    write, close = _writer(out_dir, spec)
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    leaf = leaves(gpkg)
    for row in leaf.itertuples():
        g = row.geometry.simplify(SIMPLIFY_M, preserve_topology=True)
        if g is None or g.is_empty:
            continue
        write(_to4326(g, tf), {"basin_id": row.basin_id}, spec.minzoom)
    n = close()
    print(f"  basin            <- watersheds {n:>6,}  "
          f"(leaves under {LEAF_MAX_KM2:g} km2, simplified {SIMPLIFY_M:g} m)")
    return {"basin": n}
