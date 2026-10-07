"""The B.C. outline: the EXACT union of the wildlife management units (BOUND round, 2026-10-06).

The province's outline is a fact of the same polygons the regions are dissolved from — the `wmu`
layer's 225 units — so it is computed FROM them, exactly, and nothing else. Its edge is then the
same geometry, vertex for vertex, as every region's outer edge, and the border pass and the region
cutter find the SAME crossing where a river leaves the province through a region's edge: one cut,
carrying both names, with `bc_border` as its token (`sectionizer._coincident`).

It used to be built for speed: each unit simplified by 50 m before the union, the result by 100 m,
plus a 1 m buffer to close the gaps the simplification opened and a pass filling the 4,594 hairline
holes that still came out. The outline then disagreed with the region polygons by up to 128 m
(Hausdorff), and a river crossing the border cut twice — once at the outline, once at the region's
edge — leaving a strip between them that lay in no region: 31 stretches inside B.C. that the reach
run called OUTSIDE B.C. (Beaver Creek, the Pasayten, the Ashnola, Russian Creek …), and 108 cuts
under a metre.

The exact union needs none of that. Measured on the layer (2026-10-06): 0 invalid units, a VALID
COVERAGE (`shapely.coverage_is_valid`, no overlaps, no gaps between neighbours), and its
`coverage_union_all` is one polygon with no holes in 0.13 s. A coverage that is not valid is
REFUSED with the offending edges named — fix the source layer, never buffer it shut.

The cache sits beside the gpkg as WKB (exact doubles — a GeoJSON round trip rounds coordinates, and
a rounded outline no longer coincides with the regions), keyed by the gpkg's size and mtime.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.splits.bc_boundary
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project_config import get_config

#: The drawing copy (tile mask, place filter) — written from the same exact union by `main`.
BOUNDARY_FILENAME = "bc_boundary.geojson"
#: The atlas's copy: exact, binary.
EXACT_FILENAME = "bc_boundary.exact.wkb"


def boundary_path(gpkg_path: str | Path) -> Path:
    """The drawing copy, beside the gpkg."""
    return Path(gpkg_path).parent / BOUNDARY_FILENAME


def exact_path(gpkg_path: str | Path) -> Path:
    return Path(gpkg_path).parent / EXACT_FILENAME


def _stamp(gpkg_path: str | Path) -> dict:
    st = Path(gpkg_path).stat()
    return {"gpkg": Path(gpkg_path).name, "size": st.st_size, "mtime_ns": st.st_mtime_ns}


def exact_union(polys):
    """The exact union of a polygon COVERAGE. Refuses an invalid coverage, naming its bad edges."""
    import numpy as np
    import shapely

    arr = np.asarray([p for p in polys if p is not None and not p.is_empty], dtype=object)
    if not len(arr):
        return None
    bad = [i for i, p in enumerate(arr) if not p.is_valid]
    if bad:
        raise ValueError(f"bc_boundary: {len(bad)} invalid unit polygon(s) (indexes {bad[:10]})")
    if not shapely.coverage_is_valid(arr):
        edges = shapely.coverage_invalid_edges(arr)
        where = [(i, round(e.length, 1)) for i, e in enumerate(edges)
                 if e is not None and not e.is_empty]
        raise ValueError(f"bc_boundary: the units are not a valid coverage — {len(where)} unit(s) "
                         f"with overlapping or mismatched edges (index, metres): {where[:10]}. "
                         f"Fix the source layer; the outline is never buffered shut.")
    return shapely.coverage_union_all(arr)


def wmu_outline(gpkg_path: str | Path):
    """(outline, crs) from the gpkg's `wmu` layer, exactly. (None, None) without the layer."""
    import geopandas as gpd
    import pyogrio

    if "wmu" not in {n for n, *_ in pyogrio.list_layers(str(gpkg_path))}:
        return None, None
    gdf = gpd.read_file(str(gpkg_path), layer="wmu", engine="pyogrio",
                        columns=["WILDLIFE_MGMT_UNIT_ID"])
    return exact_union(list(gdf.geometry)), (gdf.crs or "EPSG:3005")


def load_outline(gpkg_path: str | Path, write_cache: bool = True):
    """The exact B.C. outline: from the cache when it was made from this gpkg, else computed (and
    cached). None when the gpkg has no `wmu` layer."""
    import shapely

    gpkg_path = Path(gpkg_path)
    path = exact_path(gpkg_path)
    meta = path.with_suffix(".json")
    if path.exists() and meta.exists() and gpkg_path.exists():
        try:
            if json.loads(meta.read_text()) == _stamp(gpkg_path):
                return shapely.from_wkb(path.read_bytes())
        except (ValueError, OSError):
            pass
    if not gpkg_path.exists():
        return None
    outline, _crs = wmu_outline(gpkg_path)
    if outline is not None and write_cache:
        path.write_bytes(shapely.to_wkb(outline))
        meta.write_text(json.dumps(_stamp(gpkg_path)))
    return outline


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the exact BC outline cache (+ the drawing copy).")
    ap.add_argument("--gpkg", default=str(get_config().fwa_data_gpkg))
    ap.add_argument("--drawing-copy", action="store_true",
                    help=f"also rewrite {BOUNDARY_FILENAME} (tile mask, bundle place filter)")
    args = ap.parse_args()
    outline = load_outline(args.gpkg)
    if outline is None:
        raise SystemExit("no `wmu` layer in the gpkg — cannot build the BC outline")
    print(f"exact outline: {outline.geom_type}, {outline.area / 1e6:,.0f} km2 -> {exact_path(args.gpkg)}")
    if args.drawing_copy:
        import geopandas as gpd
        out = boundary_path(args.gpkg)
        gpd.GeoDataFrame({"name": ["BC"]}, geometry=[outline], crs="EPSG:3005").to_file(
            out, driver="GeoJSON")
        print(f"drawing copy -> {out}")


if __name__ == "__main__":
    main()
