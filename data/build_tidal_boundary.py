"""build_tidal_boundary.py — Process DFO tidal polygon and upload to R2.

Processing pipeline (mirrors the interactive steps from March 2026):
  1. Load raw DFO source layer (2 rows)
  2. Fix any invalid geometries (make_valid + unary_union per row)
  3. Fill holes < 1 ha (removes ~26K ocean fill artefacts)
  4. Merge all rows into a single polygon (unary_union)
  5. Morphological smooth: buffer +1 m then -0.5 m
  6. Outward buffer by BUFFER_M (baked into the stored polygon)
  7. Simplify to SIMPLIFY_M tolerance (always ≤ BUFFER_M, never retreats past original edge)
  8. Write each step as a named layer in DFO_TIDAL_BOUNDARY.gpkg
  9. Copy final tidal_boundary layer into bc_fisheries_data.gpkg
  10. Upload DFO_TIDAL_BOUNDARY.gpkg to R2 via rclone

Usage:
    python -m data.build_tidal_boundary
    python -m data.build_tidal_boundary --skip-upload   # skip R2 upload
    python -m data.build_tidal_boundary --skip-copy     # skip bc_fisheries copy
"""

import argparse
import logging
import subprocess
import time
from pathlib import Path

import geopandas as gpd
from shapely.ops import unary_union
from shapely.validation import make_valid

from project_config import get_config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# ── Constants ─────────────────────────────────────────────────────────────────

DFO_GPKG = Path("data/DFO_TIDAL_BOUNDARY.gpkg")
SOURCE_LAYER = "dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp"
HOLE_AREA_THRESHOLD_M2 = 10_000  # 1 ha — holes smaller than this are filled
BUFFER_OUT_M = 1.0  # morphological smooth: expand
BUFFER_IN_M = 0.5  # morphological smooth: contract (net +0.5 m)
BUFFER_M = 15.0  # outward buffer baked into the stored polygon — ensures polygons cover streams near tidal edge
SIMPLIFY_M = (
    5.0  # simplification tolerance after buffer — never retreats past buffered edge
)
R2_BUCKET = "r2:bc-fishing-regulations"
R2_FILENAME = "DFO_TIDAL_BOUNDARY.gpkg"


# ── Geometry helpers ──────────────────────────────────────────────────────────


def fix_geometry(geom):
    """Return a valid version of geom, merging any self-intersection artifacts."""
    if geom is None:
        return None
    if geom.is_valid:
        return geom
    fixed = make_valid(geom)
    # make_valid can return a GeometryCollection for figure-8 bowties;
    # unary_union collapses it into the simplest valid form.
    return unary_union(fixed)


def fill_small_holes(geom, max_area_m2):
    """Remove interior holes whose area is below max_area_m2.

    Works on Polygon and MultiPolygon; returns same geometry type.
    """
    from shapely.geometry import Polygon, MultiPolygon

    def _fill_poly(poly):
        kept_holes = [
            ring for ring in poly.interiors if Polygon(ring).area >= max_area_m2
        ]
        return Polygon(poly.exterior, kept_holes)

    if geom.geom_type == "Polygon":
        return _fill_poly(geom)
    if geom.geom_type == "MultiPolygon":
        return MultiPolygon([_fill_poly(p) for p in geom.geoms])
    return geom


def smooth(geom, out_m, in_m):
    """Morphological smooth: expand then contract."""
    return geom.buffer(out_m).buffer(-in_m)


# ── Layer write helper ────────────────────────────────────────────────────────


def save_layer(geom, layer_name, gpkg_path, crs="EPSG:3005"):
    """Write a single geometry as a 1-row layer in a GPKG (overwrite)."""
    gdf = gpd.GeoDataFrame({"geometry": [geom]}, crs=crs)
    gdf.to_file(gpkg_path, layer=layer_name, driver="GPKG", engine="pyogrio")
    coords = sum(
        len(g.exterior.coords) + sum(len(h.coords) for h in g.interiors)
        for g in (geom.geoms if hasattr(geom, "geoms") else [geom])
        if hasattr(g, "exterior")
    )
    logger.info(f"  Saved '{layer_name}': {coords:,} vertices")


# ── Main pipeline ─────────────────────────────────────────────────────────────


def run(skip_upload: bool = False, skip_copy: bool = False):
    config = get_config()
    main_gpkg = Path(config.fetch_output_gpkg_path)

    if not DFO_GPKG.exists():
        raise FileNotFoundError(f"Source GPKG not found: {DFO_GPKG}")

    # ── Step 0: load raw source ───────────────────────────────────────────────
    logger.info(f"Loading source layer '{SOURCE_LAYER}' from {DFO_GPKG}")
    gdf = gpd.read_file(DFO_GPKG, layer=SOURCE_LAYER, engine="pyogrio")
    if gdf.crs is None or gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs("EPSG:3005")
    logger.info(f"  Loaded {len(gdf)} row(s)")

    # ── Step 1: fix invalid geometries ───────────────────────────────────────
    logger.info("Step 1: fixing invalid geometries…")
    geoms = [fix_geometry(g) for g in gdf.geometry]
    invalid = sum(1 for g in gdf.geometry if g is not None and not g.is_valid)
    logger.info(f"  Fixed {invalid} invalid geometry(ies)")

    # ── Step 2: fill holes < 1 ha ─────────────────────────────────────────────
    logger.info(f"Step 2: filling holes < {HOLE_AREA_THRESHOLD_M2 / 10_000:.0f} ha…")
    t = time.time()
    geoms_filled = [fill_small_holes(g, HOLE_AREA_THRESHOLD_M2) for g in geoms]

    # Count removed holes
    def hole_count(g):
        if g is None:
            return 0
        parts = g.geoms if hasattr(g, "geoms") else [g]
        return sum(len(list(p.interiors)) for p in parts if hasattr(p, "interiors"))

    holes_before = sum(hole_count(g) for g in geoms)
    holes_after = sum(hole_count(g) for g in geoms_filled)
    logger.info(
        f"  Holes: {holes_before:,} → {holes_after:,} "
        f"(removed {holes_before - holes_after:,}) in {time.time()-t:.1f}s"
    )
    # Union the per-row filled geoms into one for saving
    step1_geom = unary_union(geoms_filled)
    save_layer(step1_geom, "step1_holes_filled", DFO_GPKG)

    # ── Step 3: merge all rows ────────────────────────────────────────────────
    logger.info("Step 3: merging rows…")
    t = time.time()
    step2_geom = unary_union(geoms_filled)  # same as step1 for 2-row source
    logger.info(f"  Merged in {time.time()-t:.1f}s")
    save_layer(step2_geom, "step2_merged", DFO_GPKG)

    # ── Step 4: morphological smooth ─────────────────────────────────────────
    logger.info(f"Step 4: morphological smooth (+{BUFFER_OUT_M}m / -{BUFFER_IN_M}m)…")
    t = time.time()
    step3_geom = smooth(step2_geom, BUFFER_OUT_M, BUFFER_IN_M)
    logger.info(f"  Smoothed in {time.time()-t:.1f}s")
    save_layer(step3_geom, "step3_smoothed", DFO_GPKG)

    # ── Step 5: outward buffer ────────────────────────────────────────────────
    # Expands the tidal polygon by BUFFER_M so that stream segments that reach
    # right to the tidal edge are correctly clipped.  This margin is baked into
    # the stored polygon (same approach as process_tidal_boundary.py MARGIN_M).
    logger.info(f"Step 5: outward buffer +{BUFFER_M} m…")
    t = time.time()
    step4_geom = step3_geom.buffer(BUFFER_M)
    logger.info(f"  Buffered in {time.time()-t:.1f}s")
    save_layer(step4_geom, "step4_buffered", DFO_GPKG)

    # ── Step 6: simplify ─────────────────────────────────────────────────────
    # Tolerance ≤ BUFFER_M so the result never retreats past the original edge.
    logger.info(f"Step 6: simplify {SIMPLIFY_M} m…")
    t = time.time()
    step5_geom = step4_geom.simplify(SIMPLIFY_M, preserve_topology=True)
    logger.info(f"  Simplified in {time.time()-t:.1f}s")
    save_layer(step5_geom, "step5_simplified", DFO_GPKG)

    # ── Step 7: write final tidal_boundary layer ──────────────────────────────
    logger.info("Step 7: writing final 'tidal_boundary' layer…")
    save_layer(step5_geom, "tidal_boundary", DFO_GPKG)
    logger.info(f"  ✅ DFO_TIDAL_BOUNDARY.gpkg updated")

    # ── Step 8: copy into bc_fisheries_data.gpkg ─────────────────────────────
    if not skip_copy:
        logger.info(f"Step 8: copying 'tidal_boundary' into {main_gpkg}…")
        gdf_final = gpd.read_file(DFO_GPKG, layer="tidal_boundary", engine="pyogrio")
        gdf_final.to_file(
            main_gpkg, layer="tidal_boundary", driver="GPKG", engine="pyogrio"
        )
        logger.info(f"  ✅ Copied to {main_gpkg}")

    # ── Step 9: upload to R2 ─────────────────────────────────────────────────
    if not skip_upload:
        logger.info(f"Step 9: uploading {DFO_GPKG.name} to R2…")
        result = subprocess.run(
            ["rclone", "copy", str(DFO_GPKG), R2_BUCKET + "/", "--progress"],
            capture_output=False,
        )
        if result.returncode != 0:
            raise RuntimeError("rclone upload failed — check rclone config")
        logger.info(f"  ✅ Uploaded to {R2_BUCKET}/{R2_FILENAME}")

    logger.info("Done.")


# ── Entry point ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Build and upload tidal boundary polygon"
    )
    parser.add_argument("--skip-upload", action="store_true", help="Skip R2 upload")
    parser.add_argument(
        "--skip-copy", action="store_true", help="Skip copy to bc_fisheries_data.gpkg"
    )
    args = parser.parse_args()
    run(skip_upload=args.skip_upload, skip_copy=args.skip_copy)


if __name__ == "__main__":
    main()
