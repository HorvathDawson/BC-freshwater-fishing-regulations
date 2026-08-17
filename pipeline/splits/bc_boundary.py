"""Build (once) and load the cached BC provincial boundary used by the border split stage.

The province outline never changes, so there is no reason to re-union the ~225 WMU polygons on
every build (that union was the border stage's multi-minute bottleneck). This module computes the
outline ONE time, simplifies it, and writes ``data/bc_boundary.geojson`` (EPSG:3005) next to the
gpkg. ``pipeline.splits.border.bc_outline`` then just loads that single polygon — turning the
border stage's union cost into a cheap file read.

Regenerate only if the WMU layer itself changes:

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.splits.bc_boundary --gpkg data/bc_fisheries_data.gpkg

Speed: the one-time union is made fast by simplifying each WMU polygon BEFORE the union and using a
tiny positive buffer so adjacent units always overlap — no gaps that would read as fake inland
borders. Vertex detail is irrelevant at border scale (streams cross the line by kilometres)."""

from __future__ import annotations

import argparse
from pathlib import Path

BOUNDARY_FILENAME = "bc_boundary.geojson"
_INPUT_SIMPLIFY = 50.0     # per-WMU vertex thinning before the union (m)
_GAP_BUFFER = 1.0          # tiny overlap so simplified neighbours never leave a sliver gap (m)
_RESULT_SIMPLIFY = 100.0   # final outline thinning (m)


def boundary_path(gpkg_path: str | Path) -> Path:
    """Where the cached boundary lives — beside the gpkg, in the same data dir."""
    return Path(gpkg_path).parent / BOUNDARY_FILENAME


def load_cached_boundary(gpkg_path: str | Path):
    """The cached BC outline as a shapely geometry, or None if it hasn't been built yet."""
    path = boundary_path(gpkg_path)
    if not path.exists():
        return None
    import geopandas as gpd

    gdf = gpd.read_file(path)
    geoms = [g for g in gdf.geometry if g is not None and not g.is_empty]
    if not geoms:
        return None
    import shapely

    return shapely.union_all(geoms) if len(geoms) > 1 else geoms[0]


def fast_wmu_union(fwa):
    """The BC outline unioned from the WMU polygons, made fast by simplifying each poly (50 m) and
    micro-buffering (1 m) BEFORE the union — so no gap between adjacent units reads as a fake inland
    border. Returns (outline, crs) or (None, None). Fast enough (~seconds) to run every build; the
    cached geojson is just an even-cheaper file read. Shared by ``build_boundary`` and the
    ``bc_outline`` fallback so both take the same fast path and produce the same geometry."""
    if "wmu" not in getattr(fwa, "layer_names", []):
        return None, None
    import shapely

    gdf = fwa.get_layer("wmu", columns=["WILDLIFE_MGMT_UNIT_ID"])
    polys = [g for g in gdf.geometry if g is not None and not g.is_empty]
    if not polys:
        return None, None
    prepped = [g.simplify(_INPUT_SIMPLIFY, preserve_topology=True).buffer(_GAP_BUFFER) for g in polys]
    outline = shapely.union_all(prepped)
    if _RESULT_SIMPLIFY:
        outline = outline.simplify(_RESULT_SIMPLIFY, preserve_topology=True)
    return outline, (getattr(gdf, "crs", None) or "EPSG:3005")


def build_boundary(fwa, out_path: Path) -> object:
    """Union the WMU polygons into one simplified outline and write it as GeoJSON. Returns the geom."""
    outline, crs = fast_wmu_union(fwa)
    if outline is None:
        raise RuntimeError("no usable 'wmu' polygons in the gpkg — cannot build the BC boundary")
    import geopandas as gpd

    out_path.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"name": ["BC"]}, geometry=[outline], crs=crs).to_file(out_path, driver="GeoJSON")
    return outline


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the cached BC provincial boundary (one-time).")
    ap.add_argument("--gpkg", default="data/bc_fisheries_data.gpkg")
    args = ap.parse_args()

    from data.data_extractor import FWADataAccessor

    fwa = FWADataAccessor(args.gpkg)
    out = boundary_path(args.gpkg)
    print(f"building BC boundary from WMU union -> {out} ...")
    geom = build_boundary(fwa, out)
    print(f"done: {out} ({out.stat().st_size / 1024:.0f} KB), geom type={geom.geom_type}, "
          f"bounds={tuple(round(b) for b in geom.bounds)}")


if __name__ == "__main__":
    main()
