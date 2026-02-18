#!/usr/bin/env python3
"""
Metadata Builder - FWA Metadata Extraction & Zone Assignment

Consumes the optimized graph output and FWA polygon layers to create
a lightweight metadata lookup table.

OPTIMIZATION NOTE:
- Zone boundaries are buffered ONCE at startup.
- Point-in-Polygon checks are performed against these buffered zones.
- This avoids buffering 9+ million stream endpoints individually.
"""

import os
import sys
import logging
import pickle
import argparse
import gc
import geopandas as gpd
import pandas as pd
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from shapely.geometry import Point
from shapely.strtree import STRtree
from enum import Enum


# --- Shared Enum ---
class FeatureType(Enum):
    """Enum for FWA feature types."""

    STREAM = "streams"
    LAKE = "lakes"
    WETLAND = "wetlands"
    MANMADE = "manmade"
    UNMARKED = "lakes"
    POINT = "point"
    UNKNOWN = "unknown"


# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Buffer: Features within 500m of a border get assigned to BOTH zones
ZONE_BOUNDARY_BUFFER_M = 500.0

# --- Helper Functions (Static for Multiprocessing) ---


def find_zones_spatial(geometry, zones_gdf, zone_index):
    """
    Finds zones intersecting the geometry using pre-buffered zones.

    Args:
        geometry: Shapely Point or Polygon (Stream endpoint or Lake)
        zones_gdf: GeoDataFrame containing 'geometry_buffered' column
        zone_index: STRtree index built on 'geometry_buffered'
    """
    # 1. Broad Search (Spatial Index against buffered zones)
    candidate_indices = zone_index.query(geometry)

    zones = set()
    mgmt_units = set()

    # 2. Precise Check
    for idx in candidate_indices:
        zone_row = zones_gdf.iloc[idx]
        buffered_geom = zone_row["geometry_buffered"]

        # Check intersection against the PRE-BUFFERED zone
        # This is equivalent to buffering the point but much faster
        if buffered_geom.intersects(geometry):
            zones.add(zone_row["zone"])
            mgmt_units.add(zone_row["WILDLIFE_MGMT_UNIT_ID"])

    return {"zones": sorted(list(zones)), "mgmt_units": sorted(list(mgmt_units))}


def process_stream_chunk(chunk_data):
    """
    Worker function: Processes a batch of graph edges.
    """
    zones_gdf, zone_index, edges = chunk_data
    results = {}
    border_crossings = 0

    for key, u_x, u_y, v_x, v_y, attrs in edges:
        # Create endpoint geometries (No buffering needed here anymore!)
        u_pt = Point(u_x, u_y)
        v_pt = Point(v_x, v_y)

        # Spatial lookup for both ends
        u_z = find_zones_spatial(u_pt, zones_gdf, zone_index)
        v_z = find_zones_spatial(v_pt, zones_gdf, zone_index)

        # Union the results
        all_zones = sorted(set(u_z["zones"] + v_z["zones"]))
        all_mus = sorted(set(u_z["mgmt_units"] + v_z["mgmt_units"]))

        is_cross_boundary = len(all_zones) > 1
        if is_cross_boundary:
            border_crossings += 1

        # Build Metadata Record
        results[str(key)] = {
            "linear_feature_id": str(key),
            "gnis_name": attrs.get("gnis_name", ""),
            "gnis_id": attrs.get("gnis_id", ""),
            "fwa_watershed_code": attrs.get("fwa_watershed_code", ""),
            "fwa_watershed_code_clean": attrs.get("fwa_watershed_code_clean", ""),
            "stream_order": attrs.get("stream_order"),
            "stream_magnitude": attrs.get("stream_magnitude"),
            "length": attrs.get("length", 0),
            "waterbody_key": attrs.get("waterbody_key", ""),
            "feature_code": attrs.get("feature_code", ""),
            "blue_line_key": attrs.get("blue_line_key", ""),
            "edge_type": attrs.get("edge_type", ""),
            "zones": all_zones,
            "mgmt_units": all_mus,
            "cross_boundary": is_cross_boundary,
            "unnamed_depth_distance_corrected": attrs.get(
                "unnamed_depth_distance_corrected"
            ),
        }

    return results, border_crossings


# --- Main Class ---


class MetadataBuilder:
    def __init__(self, graph_path, zones_path, lakes_gdb, output_path):
        self.paths = {
            "graph": Path(graph_path),
            "zones": Path(zones_path),
            "lakes": Path(lakes_gdb),
            "output": Path(output_path),
        }
        self.data = {
            "zones_gdf": None,
            "zone_index": None,
            "edge_attrs": None,
            "node_coords": None,
        }
        self.metadata = {
            "zone_metadata": {},
            FeatureType.STREAM: {},
            FeatureType.LAKE: {},
            FeatureType.WETLAND: {},
            FeatureType.MANMADE: {},
        }

    def load_zones(self):
        logger.info(f"Loading zones from {self.paths['zones']}...")
        gdf = gpd.read_file(self.paths["zones"])

        # Parse Zone ID
        gdf["zone"] = gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]

        # OPTIMIZATION: Pre-calculate buffered geometries
        # This moves the O(N) buffering operation to O(1) (N=zones, not N=streams)
        logger.info(f"Pre-buffering zones by {ZONE_BOUNDARY_BUFFER_M}m...")
        gdf["geometry_buffered"] = gdf.geometry.buffer(ZONE_BOUNDARY_BUFFER_M)

        self.data["zones_gdf"] = gdf
        # Build index on the BUFFERED geometry to ensure wide enough search
        self.data["zone_index"] = STRtree(gdf["geometry_buffered"])

        # Build Zone Metadata Summary
        logger.info("Building zone definitions...")
        for _, row in gdf.iterrows():
            zid = row["zone"]
            mu = row["WILDLIFE_MGMT_UNIT_ID"]

            if zid not in self.metadata["zone_metadata"]:
                self.metadata["zone_metadata"][zid] = {
                    "zone_number": zid,
                    "mgmt_units": [],
                    "mgmt_unit_details": {},
                }

            entry = self.metadata["zone_metadata"][zid]
            entry["mgmt_units"].append(mu)
            entry["mgmt_unit_details"][mu] = {
                "full_id": mu,
                "region_name": row.get("REGION_RESPONSIBLE_NAME", ""),
                "bounds": list(row.geometry.bounds),
            }

        for z in self.metadata["zone_metadata"].values():
            z["mgmt_units"].sort()

    def load_graph(self):
        logger.info(f"Loading graph pickle from {self.paths['graph']}...")
        with open(self.paths["graph"], "rb") as f:
            dump = pickle.load(f)
            self.data["edge_attrs"] = dump["edge_attrs"]
            self.data["node_coords"] = dump["node_coords"]
        logger.info(f"Loaded {len(self.data['edge_attrs']):,} edges.")

    def extract_streams(self, workers=8):
        logger.info(f"Processing streams with {workers} workers...")

        edge_list = []
        for key, attrs in self.data["edge_attrs"].items():
            try:
                u_id = attrs["source"]
                v_id = attrs["target"]
                u_xy = self.data["node_coords"][u_id]
                v_xy = self.data["node_coords"][v_id]
                edge_list.append((key, u_xy[0], u_xy[1], v_xy[0], v_xy[1], attrs))
            except KeyError:
                continue

        chunk_size = max(5000, len(edge_list) // (workers * 2))
        chunks = [
            edge_list[i : i + chunk_size] for i in range(0, len(edge_list), chunk_size)
        ]

        processed = 0

        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Pass buffered zones and index to workers
            futures = [
                executor.submit(
                    process_stream_chunk,
                    (
                        self.data["zones_gdf"],
                        self.data["zone_index"],
                        chunk,
                    ),
                )
                for chunk in chunks
            ]

            for future in as_completed(futures):
                res, cross_count = future.result()
                self.metadata[FeatureType.STREAM].update(res)
                processed += len(res)
                if processed % 100000 == 0:  # Log less frequently
                    logger.info(f"  Processed {processed:,} streams...")

    def process_polygons(self):
        """Iterates over lakes, wetlands, and manmade waterbodies."""
        layers = {
            FeatureType.LAKE: "FWA_LAKES_POLY",
            FeatureType.WETLAND: "FWA_WETLANDS_POLY",
            FeatureType.MANMADE: "FWA_MANMADE_WATERBODIES_POLY",
        }

        for ftype_enum, layer_name in layers.items():
            logger.info(f"Processing {ftype_enum.name} ({layer_name})...")
            try:
                gdf = gpd.read_file(self.paths["lakes"], layer=layer_name)
                gdf = gdf[gdf["WATERBODY_KEY"].notna()]

                results = {}
                for idx, row in gdf.iterrows():
                    pid = str(int(row.get("WATERBODY_POLY_ID", row["WATERBODY_KEY"])))

                    # OPTIMIZATION: Use the pre-buffered zones for fast intersection
                    # Pass the UNBUFFERED lake geometry. The zones are already buffered.
                    z_data = find_zones_spatial(
                        row.geometry,
                        self.data["zones_gdf"],
                        self.data[
                            "zone_index"
                        ],  # This index is built on buffered zones
                    )

                    name = row.get("GNIS_NAME_1", row.get("GNIS_NAME", ""))
                    gid = row.get("GNIS_ID_1", row.get("GNIS_ID", ""))
                    name_2 = row.get("GNIS_NAME_2", "")
                    gid_2 = row.get("GNIS_ID_2", "")
                    blue_line_key_raw = row.get("BLUE_LINE_KEY")
                    blue_line_key = (
                        str(int(blue_line_key_raw))
                        if pd.notna(blue_line_key_raw) and blue_line_key_raw
                        else ""
                    )

                    results[pid] = {
                        "waterbody_poly_id": pid,
                        "waterbody_key": str(int(row["WATERBODY_KEY"])),
                        "gnis_name": name if pd.notna(name) else "",
                        "gnis_id": str(int(gid)) if pd.notna(gid) and gid else "",
                        "gnis_name_2": name_2 if pd.notna(name_2) else "",
                        "gnis_id_2": (
                            str(int(gid_2)) if pd.notna(gid_2) and gid_2 else ""
                        ),
                        "fwa_watershed_code": row.get("FWA_WATERSHED_CODE", ""),
                        "blue_line_key": blue_line_key,
                        "area_sqm": row.geometry.area,
                        "zones": z_data["zones"],
                        "mgmt_units": z_data["mgmt_units"],
                    }

                self.metadata[ftype_enum] = results
                logger.info(f"  Extracted {len(results):,} {ftype_enum.name}.")

            except Exception as e:
                logger.error(f"Failed to process {layer_name}: {e}")

    def save(self):
        self.paths["output"].parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving to {self.paths['output']}...")
        with open(self.paths["output"], "wb") as f:
            pickle.dump(self.metadata, f, protocol=5)
        logger.info("Done.")


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parents[2]

    # Define defaults
    default_graph = (
        project_root
        / "scripts"
        / "output"
        / "fwa_modules"
        / "fwa_bc_primal_full.gpickle"
    )
    default_zones = (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
        / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    )
    default_lakes = (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
        / "FWA_BC"
        / "FWA_BC.gdb"
    )
    default_output = (
        project_root / "scripts" / "output" / "fwa_modules" / "stream_metadata.pickle"
    )

    parser = argparse.ArgumentParser(
        description="Extract FWA metadata and assign zones."
    )

    parser.add_argument(
        "--graph-path",
        type=Path,
        default=default_graph,
        help=f"Path to graph pickle (Default: {default_graph.name})",
    )

    parser.add_argument(
        "--zones-path",
        type=Path,
        default=default_zones,
        help="Path to wildlife management units GeoPackage",
    )

    parser.add_argument(
        "--lakes-gdb-path",
        type=Path,
        default=default_lakes,
        help="Path to FWA_BC.gdb containing polygon layers",
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=default_output,
        help="Path to output pickle file",
    )

    parser.add_argument(
        "--workers", type=int, default=8, help="Number of parallel workers (Default: 8)"
    )

    args = parser.parse_args()

    # Validation
    if not args.graph_path.exists():
        logger.error(f"Graph file not found: {args.graph_path}")
        logger.error("Please run graph_builder.py first.")
        sys.exit(1)

    if not args.zones_path.exists():
        logger.error(f"Zones file not found: {args.zones_path}")
        sys.exit(1)

    if not args.lakes_gdb_path.exists():
        logger.error(f"Lakes GDB not found: {args.lakes_gdb_path}")
        sys.exit(1)

    # Run
    builder = MetadataBuilder(
        args.graph_path, args.zones_path, args.lakes_gdb_path, args.output_path
    )

    builder.load_zones()
    builder.load_graph()
    builder.extract_streams(workers=args.workers)
    builder.process_polygons()
    builder.save()


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    sys.exit(main())
