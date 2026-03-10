#!/usr/bin/env python3
"""
Process the DFO tidal boundary GPKG through successive cleaning steps, then
upload the result to R2 and copy the final layer into bc_fisheries_data.gpkg.

Steps
-----
1. Load raw DFO layer; fix any invalid geometries with make_valid().
2. Fill interior holes smaller than 5 ha (keeps significant inlets/fjords).
3. Union all rows into a single MultiPolygon.
4. Clip to a BC coastal mask (BC land boundary buffered 120 km offshore) so
   the polygon never extends outside BC jurisdiction.
5. Buffer outward by 5 m — this margin becomes part of the stored polygon and
   is therefore reflected in the front-end display too.
6. Simplify to 5 m tolerance (always ≤ 5 m deviation from the buffered edge,
   so the result never retreats past the original polygon boundary).
7. Save intermediate step layers + final 'tidal_boundary' to
   data/DFO_TIDAL_BOUNDARY.gpkg (for inspection in QGIS / re-runs).
8. Copy 'tidal_boundary' into data/bc_fisheries_data.gpkg.
9. Upload data/DFO_TIDAL_BOUNDARY.gpkg to R2 via rclone.

Usage
-----
    python data/process_tidal_boundary.py
    python data/process_tidal_boundary.py --skip-upload   # skip R2 upload
    python data/process_tidal_boundary.py --skip-copy     # skip GPKG copy step
    python data/process_tidal_boundary.py --no-bc-mask    # skip BC boundary clip
"""

import argparse
import logging
import subprocess
import sys
import urllib.parse
from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely.validation import make_valid

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DFO_GPKG = ROOT / "data" / "DFO_TIDAL_BOUNDARY.gpkg"
MAIN_GPKG = ROOT / "data" / "bc_fisheries_data.gpkg"
R2_BUCKET = "r2:bc-fishing-regulations"

# Layer names
RAW_LAYER = "dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp"
STEP1_LAYER = "step1_holes_filled"
STEP2_LAYER = "step2_merged"
STEP3_LAYER = "step3_clipped"
STEP4_LAYER = "step4_smoothed"
OUT_LAYER = "tidal_boundary"

# Processing parameters
HOLE_MAX_HA = 5          # fill holes smaller than this (hectares)
MARGIN_M = 5             # outward buffer baked into the stored polygon (metres)
SIMPLIFY_M = 5           # simplification tolerance (metres) — applied after buffer
BC_MASK_BUFFER_M = 120_000  # how far offshore to extend the BC land boundary (metres)


# ── Geometry helpers ───────────────────────────────────────────────────────────

def _fix_geom(geom):
    """Return a valid version of *geom* (make_valid → unary_union if needed)."""
    if geom is None:
        return None
    valid = make_valid(geom)
    if hasattr(valid, "geoms"):
        valid = unary_union(valid)
    return valid


def _fill_holes(geom, max_area_m2: float):
    """Remove interior rings (holes) whose area is < *max_area_m2*."""
    def _fill_poly(poly: Polygon) -> Polygon:
        kept = [ring for ring in poly.interiors if Polygon(ring).area >= max_area_m2]
        return Polygon(poly.exterior, kept)

    if isinstance(geom, Polygon):
        return _fill_poly(geom)
    if isinstance(geom, MultiPolygon):
        return MultiPolygon([_fill_poly(p) for p in geom.geoms])
    return geom


def _save_layer(gdf: gpd.GeoDataFrame, path: Path, layer: str):
    gdf.to_file(path, layer=layer, driver="GPKG", engine="pyogrio")
    logger.info(f"  Saved '{layer}' → {path.name}  ({len(gdf)} row(s))")


# ── BC coastal mask ────────────────────────────────────────────────────────────

def fetch_bc_mask() -> gpd.GeoDataFrame | None:
    """Fetch BC province boundary from BC DataCatalogue WFS, buffered offshore.

    Returns a single-row GeoDataFrame (EPSG:3005) or None if the fetch fails.
    """
    logger.info("Fetching BC province boundary from WFS ...")
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "typeNames": "WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_PROVINCES_SP",
        "outputFormat": "json",
        "SRSNAME": "EPSG:4326",
        "CQL_FILTER": "ENGLISH_NAME='British Columbia'",
        "count": 1,
    }
    url = f"https://openmaps.gov.bc.ca/geo/pub/ows?{urllib.parse.urlencode(params)}"
    try:
        gdf = gpd.read_file(url).to_crs("EPSG:3005")
        if gdf.empty:
            logger.warning("BC boundary WFS returned empty result — skipping mask")
            return None
        mask_geom = gdf.geometry.iloc[0].buffer(BC_MASK_BUFFER_M)
        result = gpd.GeoDataFrame(geometry=[mask_geom], crs="EPSG:3005")
        logger.info(f"  BC coastal mask ready (land + {BC_MASK_BUFFER_M/1000:.0f} km offshore)")
        return result
    except Exception as exc:
        logger.warning(f"BC boundary fetch failed ({exc}) — skipping mask step")
        return None


# ── Main processing ────────────────────────────────────────────────────────────

def process(use_bc_mask: bool = True) -> gpd.GeoDataFrame:
    """Run all processing steps, save intermediate layers, return final GDF."""
    logger.info(f"Loading raw layer '{RAW_LAYER}' from {DFO_GPKG.name} ...")
    raw = gpd.read_file(DFO_GPKG, layer=RAW_LAYER, engine="pyogrio").to_crs("EPSG:3005")
    logger.info(f"  {len(raw)} row(s) loaded")

    # ── Step 1: fix validity + fill small holes ────────────────────────────
    logger.info(f"Step 1: fixing validity and filling holes < {HOLE_MAX_HA} ha ...")
    max_area_m2 = HOLE_MAX_HA * 10_000
    fixed_geoms = []
    for geom in raw.geometry:
        g = _fix_geom(geom)
        g = _fill_holes(g, max_area_m2)
        fixed_geoms.append(g)

    step1_gdf = raw.copy()
    step1_gdf["geometry"] = fixed_geoms
    step1_gdf = step1_gdf.set_geometry("geometry")

    before = sum(
        len(list(getattr(g, "interiors", []))) if isinstance(g, Polygon)
        else sum(len(list(p.interiors)) for p in g.geoms)
        for g in raw.geometry
    )
    after = sum(
        len(list(getattr(g, "interiors", []))) if isinstance(g, Polygon)
        else sum(len(list(p.interiors)) for p in g.geoms)
        for g in step1_gdf.geometry
    )
    logger.info(f"  Holes: {before} → {after} (filled {before - after})")
    _save_layer(step1_gdf, DFO_GPKG, STEP1_LAYER)

    # ── Step 2: union all rows into one ───────────────────────────────────
    logger.info("Step 2: merging all rows into a single MultiPolygon ...")
    merged = unary_union(step1_gdf.geometry.tolist())
    step2_gdf = gpd.GeoDataFrame(geometry=[merged], crs="EPSG:3005")
    _save_layer(step2_gdf, DFO_GPKG, STEP2_LAYER)

    # ── Step 3: clip to BC coastal mask ───────────────────────────────────
    if use_bc_mask:
        bc_mask = fetch_bc_mask()
        if bc_mask is not None:
            logger.info("Step 3: clipping to BC coastal mask ...")
            clipped = merged.intersection(bc_mask.geometry.iloc[0])
            step3_gdf = gpd.GeoDataFrame(geometry=[clipped], crs="EPSG:3005")
            _save_layer(step3_gdf, DFO_GPKG, STEP3_LAYER)
            merged = clipped
        else:
            logger.warning("Step 3: BC mask unavailable — skipping clip")
            step3_gdf = step2_gdf.copy()
            _save_layer(step3_gdf, DFO_GPKG, STEP3_LAYER)
    else:
        logger.info("Step 3: BC mask skipped (--no-bc-mask)")
        step3_gdf = step2_gdf.copy()
        _save_layer(step3_gdf, DFO_GPKG, STEP3_LAYER)

    # ── Step 4: buffer + simplify (margin baked into stored polygon) ───────
    logger.info(f"Step 4: buffer +{MARGIN_M} m then simplify {SIMPLIFY_M} m ...")
    smoothed = merged.buffer(MARGIN_M).simplify(SIMPLIFY_M, preserve_topology=True)
    step4_gdf = gpd.GeoDataFrame(geometry=[smoothed], crs="EPSG:3005")

    import shapely as _shapely
    n_verts = int(_shapely.get_coordinates(smoothed).shape[0])
    logger.info(f"  Final polygon: {n_verts:,} vertices")
    _save_layer(step4_gdf, DFO_GPKG, STEP4_LAYER)

    # ── Output layer ───────────────────────────────────────────────────────
    out_gdf = gpd.GeoDataFrame(geometry=[smoothed], crs="EPSG:3005")
    _save_layer(out_gdf, DFO_GPKG, OUT_LAYER)

    return out_gdf


def copy_to_main_gpkg(out_gdf: gpd.GeoDataFrame):
    logger.info(f"Copying '{OUT_LAYER}' → {MAIN_GPKG.name} ...")
    out_gdf.to_file(MAIN_GPKG, layer=OUT_LAYER, driver="GPKG", engine="pyogrio")
    verify = gpd.read_file(MAIN_GPKG, layer=OUT_LAYER, engine="pyogrio")
    logger.info(f"  Verified: {len(verify)} row(s), CRS={verify.crs.to_epsg()}")


def upload_to_r2():
    logger.info(f"Uploading {DFO_GPKG.name} to R2 ...")
    size_mb = DFO_GPKG.stat().st_size / 1_048_576
    logger.info(f"  File size: {size_mb:.0f} MB")
    result = subprocess.run(
        ["rclone", "copy", str(DFO_GPKG), f"{R2_BUCKET}/",
         "--s3-no-check-bucket", "--progress"],
        capture_output=False,
    )
    if result.returncode != 0:
        logger.error("rclone upload failed")
        sys.exit(result.returncode)
    logger.info("  Upload complete.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Process DFO tidal boundary and upload to R2"
    )
    parser.add_argument(
        "--skip-upload", action="store_true",
        help="Skip rclone upload to R2",
    )
    parser.add_argument(
        "--skip-copy", action="store_true",
        help="Skip copying tidal_boundary into bc_fisheries_data.gpkg",
    )
    parser.add_argument(
        "--no-bc-mask", action="store_true",
        help="Skip BC coastal boundary clip step",
    )
    args = parser.parse_args()

    out_gdf = process(use_bc_mask=not args.no_bc_mask)

    if not args.skip_copy:
        copy_to_main_gpkg(out_gdf)

    if not args.skip_upload:
        upload_to_r2()

    logger.info("Done.")


if __name__ == "__main__":
    main()
