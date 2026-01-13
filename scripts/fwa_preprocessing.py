#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing: Named Streams, River Tributaries, Lake Tributaries, Wetlands, Manmade Waterbodies, Zones, and Labelled Points.

Logic Flow:
1. Load all Streams, Lakes, Wetlands, Manmade Waterbodies, and KML Points.
2. SAFETY LOCK: Identify exactly which streams are unnamed at the start.
3. RIVER ENRICHMENT: Rename locked unnamed streams based on parent codes.
4. LAKE ENRICHMENT:
   - Check streams from step 3.
   - If they touch a lake, rename them based on the lake name.
5. POINT ENRICHMENT:
   - Check KML points against Lakes, Wetlands, and Manmade polygons.
   - Assign POLY_ID for each type if the point falls inside.
   - ERROR LOGGING: If a point falls inside NOTHING, log it to a CSV.
6. Output: Split EVERYTHING by Zone (Streams, Lakes, Wetlands, Manmade, Points).
7. OPTIONAL: Build searchable waterbody index from processed data.
"""

import os
import sys
import argparse

# --- FIX FOR "Cannot find header.dxf" WARNING ---
os.environ["GDAL_SKIP"] = "DXF"

if "GDAL_DATA" not in os.environ:
    candidates = [
        os.path.join(sys.prefix, "share", "gdal"),
        os.path.join(sys.prefix, "Library", "share", "gdal"),
    ]
    for c in candidates:
        if os.path.exists(c):
            os.environ["GDAL_DATA"] = c
            break

import fiona
import geopandas as gpd
import pandas as pd
from pathlib import Path
import logging
import time
import shutil
import gc
import warnings
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import json
import re
from collections import defaultdict
from synopsis_pipeline.utils import normalize_name

# Enable KML Driver
fiona.drvsupport.supported_drivers["KML"] = "rw"
fiona.drvsupport.supported_drivers["LIBKML"] = "rw"

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --- WORKER FUNCTIONS FOR PARALLEL PROCESSING ---
# These functions are executed in separate processes to speed up data loading and spatial operations


def spatial_join_worker(args):
    """Worker function for parallel spatial joins.

    Takes a chunk of features and performs spatial intersection with a reference GeoDataFrame.
    Used to split large spatial joins across multiple CPU cores for faster processing.

    Args:
        args: Tuple of (left_chunk, right_gdf) where:
            - left_chunk: Subset of features to join
            - right_gdf: Reference geodataframe to join against

    Returns:
        GeoDataFrame with joined features
    """
    left_chunk, right_gdf = args
    return gpd.sjoin(left_chunk, right_gdf, predicate="intersects", how="inner")


def load_stream_layer_worker(args):
    """Worker function to load a single stream layer in parallel.

    Streams are stored across 200+ separate layers in the GDB. This function loads
    one layer at a time with all available columns.

    Args:
        args: Tuple of (gdb_path, layer_name)

    Returns:
        GeoDataFrame with all columns, or None if loading fails
    """
    gdb_path, layer_name = args
    try:
        gdf = gpd.read_file(str(gdb_path), layer=layer_name)
        return gdf
    except Exception as e:
        logger.warning(f"Skipping {layer_name}: {e}")
        return None


def process_polygon_layer_worker(args):
    """Worker function to process a polygon layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name, type_label)

    Returns:
        Tuple of (zone, layer_results, poly_cache_results, feature_count)
        where layer_results is a dict of {normalized_name: [features]}
        and poly_cache_results is a dict of {poly_id: feature_dict}
    """
    gdb_path, layer_name, type_label = args

    zone_match = re.search(r"ZONE_(\d+)", layer_name)
    if not zone_match:
        return None

    zone = zone_match.group(1)
    layer_results = {}
    poly_cache_results = {}
    feature_count = 0

    try:
        gdf = gpd.read_file(str(gdb_path), layer=layer_name)

        for idx, row in gdf.iterrows():
            feature_dict = (
                row.drop("geometry").to_dict() if "geometry" in row else row.to_dict()
            )
            feature_dict = {
                k: (
                    str(v)
                    if not isinstance(v, (str, int, float, bool, type(None)))
                    else v
                )
                for k, v in feature_dict.items()
            }

            # Cache polygon by ID
            poly_id = row.get("WATERBODY_POLY_ID")
            if poly_id and str(poly_id) != "nan":
                cached_feature = {
                    "type": type_label,
                    "gnis_name": row.get("GNIS_NAME_1") or row.get("GNIS_NAME"),
                    "layer": layer_name,
                    "feature_id": str(idx),
                    "attributes": feature_dict,
                    "is_primary_polygon": True,
                }
                poly_cache_results[int(poly_id)] = cached_feature

            # Index by GNIS Name
            check_fields = ["GNIS_NAME_1", "GNIS_NAME_2", "GNIS_NAME"]
            for name_field in check_fields:
                if name_field not in gdf.columns:
                    continue

                gnis_name = row.get(name_field)
                normalized = normalize_name(gnis_name)

                if normalized:
                    feature_data = {
                        "type": type_label,
                        "gnis_name": gnis_name,
                        "layer": layer_name,
                        "feature_id": str(idx),
                        "matched_field": name_field,
                        "attributes": feature_dict,
                    }

                    if normalized not in layer_results:
                        layer_results[normalized] = []

                    # Check for duplicates
                    if not any(
                        f["feature_id"] == str(idx) and f["layer"] == layer_name
                        for f in layer_results[normalized]
                    ):
                        layer_results[normalized].append(feature_data)
                        feature_count += 1

        return (
            zone,
            type_label,
            layer_results,
            poly_cache_results,
            feature_count,
            layer_name,
        )

    except Exception as e:
        return (zone, type_label, {}, {}, 0, layer_name, str(e))


def process_stream_layer_worker(args):
    """Worker function to process a stream layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name)

    Returns:
        Tuple of (zone, layer_results, feature_count)
    """
    gdb_path, layer_name = args

    zone_match = re.search(r"ZONE_(\d+)", layer_name)
    if not zone_match:
        return None

    zone = zone_match.group(1)
    layer_results = {}
    feature_count = 0

    try:
        streams = gpd.read_file(str(gdb_path), layer=layer_name)

        for idx, row in streams.iterrows():
            gnis_name = row.get("GNIS_NAME")
            tributary_of = row.get("TRIBUTARY_OF")
            normalized = normalize_name(gnis_name)

            if normalized:
                feature_dict = (
                    row.drop("geometry").to_dict()
                    if "geometry" in row
                    else row.to_dict()
                )
                feature_dict = {
                    k: (
                        str(v)
                        if not isinstance(v, (str, int, float, bool, type(None)))
                        else v
                    )
                    for k, v in feature_dict.items()
                }

                feature_data = {
                    "type": "stream",
                    "gnis_name": gnis_name,
                    "tributary_of": tributary_of,
                    "layer": layer_name,
                    "feature_id": str(idx),
                    "attributes": feature_dict,
                }

                # Index by GNIS_NAME
                if normalized not in layer_results:
                    layer_results[normalized] = []
                layer_results[normalized].append(feature_data)
                feature_count += 1

                # FIX 3: Also index by TRIBUTARY_OF as "X Tributary" for linking
                if tributary_of:
                    tributary_normalized = normalize_name(tributary_of + " tributary")
                    if tributary_normalized not in layer_results:
                        layer_results[tributary_normalized] = []
                    layer_results[tributary_normalized].append(feature_data)

        return (zone, layer_results, feature_count, layer_name)

    except Exception as e:
        return (zone, {}, 0, layer_name, str(e))


def process_point_layer_worker(args):
    """Worker function to process a point layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name, poly_cache_for_zone)

    Returns:
        Tuple of (zone, layer_results, feature_count)
    """
    gdb_path, layer_name, poly_cache_for_zone = args

    zone_match = re.search(r"ZONE_(\d+)", layer_name)
    if not zone_match:
        return None

    zone = zone_match.group(1)
    layer_results = {}
    feature_count = 0

    try:
        points = gpd.read_file(str(gdb_path), layer=layer_name)

        for idx, row in points.iterrows():
            # Determine point name
            name_candidates = ["Name", "name", "label", "GNIS_NAME"]
            point_name = None
            for col in name_candidates:
                if col in row and pd.notna(row[col]):
                    point_name = row[col]
                    break

            normalized = normalize_name(point_name)
            if not normalized:
                continue

            # Add point feature
            feature_dict = (
                row.drop("geometry").to_dict() if "geometry" in row else row.to_dict()
            )
            feature_dict = {
                k: (
                    str(v)
                    if not isinstance(v, (str, int, float, bool, type(None)))
                    else v
                )
                for k, v in feature_dict.items()
            }

            point_feature = {
                "type": "point",
                "gnis_name": point_name,
                "layer": layer_name,
                "feature_id": str(idx),
                "attributes": feature_dict,
            }

            if normalized not in layer_results:
                layer_results[normalized] = []

            layer_results[normalized].append(point_feature)
            feature_count += 1

            # Link matched polygons
            link_map = [
                ("LAKE_POLY_ID", "lake"),
                ("WETLAND_POLY_ID", "wetland"),
                ("MANMADE_POLY_ID", "manmade"),
            ]

            for col_id, type_key in link_map:
                poly_id_val = row.get(col_id)

                if pd.notna(poly_id_val):
                    try:
                        pid = int(poly_id_val)
                        if (
                            type_key in poly_cache_for_zone
                            and pid in poly_cache_for_zone[type_key]
                        ):
                            linked_poly = poly_cache_for_zone[type_key][pid].copy()
                            linked_poly["linked_via_point"] = True
                            linked_poly["point_name_used"] = point_name
                            layer_results[normalized].append(linked_poly)
                    except (ValueError, TypeError):
                        pass

        return (zone, layer_results, feature_count, layer_name)

    except Exception as e:
        return (zone, {}, 0, layer_name, str(e))


class FWAProcessor:
    """Main processor for BC Freshwater Atlas (FWA) data.

    This class orchestrates the entire pipeline:
    1. Load raw streams, lakes, wetlands, manmade waterbodies, and user-labeled points
    2. Enrich unnamed streams with tributary names based on hierarchical watershed codes
    3. Cross-reference streams with lakes to correct naming (lake tributaries)
    4. Link labeled points to their containing waterbody polygons
    5. Split all data by wildlife management zones for efficient querying
    6. Build searchable index for the web application
    """

    def __init__(
        self,
        streams_gdb: str,
        lakes_gdb: str,
        wildlife_gpkg: str,
        kml_path: str,
        output_gdb: str,
        process_streams=True,
        process_lakes=True,
        process_wetlands=True,
        process_manmade=True,
        process_points=True,
    ):
        """Initialize the processor with data paths and processing flags.

        Args:
            streams_gdb: Path to FWA_STREAM_NETWORKS_SP.gdb (246 layers, ~4.9M features)
            lakes_gdb: Path to FWA_BC.gdb (contains lakes, wetlands, manmade)
            wildlife_gpkg: Path to wildlife management units (zones for splitting)
            kml_path: Path to user-labeled unnamed waterbody points
            output_gdb: Path where zone-split output will be saved
            process_*: Flags to enable/disable processing of specific feature types
        """
        # Input data paths
        self.streams_gdb = Path(streams_gdb)
        self.lakes_gdb = Path(lakes_gdb)
        self.wildlife_gpkg = Path(wildlife_gpkg)
        self.kml_path = Path(kml_path)
        self.output_gdb = Path(output_gdb)

        # Use all but one CPU core for parallel processing (leave one for system)
        self.n_cores = max(1, os.cpu_count() - 1)

        # Feature type processing flags (allows selective processing/indexing)
        self.process_streams = process_streams
        self.process_lakes = process_lakes
        self.process_wetlands = process_wetlands
        self.process_manmade = process_manmade
        self.process_points = process_points

        # Statistics tracking for summary reporting
        self.stats = {
            "total_streams_read": 0,  # Raw stream count before filtering
            "original_named_streams": 0,  # Streams that came with GNIS names
            "river_tributaries_found": 0,  # Unnamed streams enriched from parent rivers
            "lake_tributaries_corrected": 0,  # River tributaries corrected to lake tributaries
            "final_streams_count": 0,  # Final named stream count after all enrichment
            "total_lakes": 0,
            "total_wetlands": 0,
            "total_manmade": 0,
            "total_kml_points": 0,
        }

    def cleanup_output(self):
        """Prepare output directory and remove old data.

        Ensures clean slate for new processing run. GDB files can sometimes have
        file locks on Windows, so we retry deletion with delays if needed.
        """
        # Create output directory if it doesn't exist
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)

        # Force garbage collection to release any file handles
        gc.collect()

        # Remove old error log from previous runs
        error_log = self.output_gdb.parent / "unmatched_points_error_log.csv"
        if error_log.exists():
            try:
                os.remove(error_log)
            except PermissionError:
                pass  # Will be overwritten anyway

        # Remove old output GDB (retry up to 3 times due to potential file locks)
        if self.output_gdb.exists():
            for i in range(3):
                try:
                    shutil.rmtree(self.output_gdb)
                    break
                except PermissionError:
                    time.sleep(2)  # Wait for file handles to release

    def get_stream_layers(self) -> list:
        """Get list of valid stream layer names from the GDB.

        Stream data is split across 246 layers (one per watershed region).
        Filter out system layers (start with _) and invalid layers.

        Returns:
            List of layer names to process
        """
        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            # Keep only layers that look like watershed IDs (short names, no _ prefix)
            return [l for l in layers if not l.startswith("_") and len(l) <= 4]
        except Exception:
            return []

    # --- WATERSHED CODE PARSING UTILITIES ---
    # FWA uses hierarchical watershed codes like "200-ABC123-XYZ456-000000"
    # These functions parse the hierarchy to determine parent-child relationships

    def clean_watershed_code(self, code):
        """Remove placeholder segments (000000) from watershed codes.

        Example: "200-ABC123-000000" -> "200-ABC123"

        Args:
            code: Raw FWA_WATERSHED_CODE string

        Returns:
            Cleaned code with only meaningful segments
        """
        if not isinstance(code, str):
            return None
        parts = code.split("-")
        # Filter out the 000000 placeholder values
        valid_parts = [p for p in parts if p != "000000"]
        return "-".join(valid_parts)

    def get_parent_code(self, clean_code):
        """Get the parent watershed code by removing the last segment.

        This allows us to find the "parent river" for tributary naming.
        Example: "200-ABC123-XYZ456" -> "200-ABC123"

        Args:
            clean_code: Watershed code with 000000 segments already removed

        Returns:
            Parent code, or None if already at top level
        """
        if not clean_code or "-" not in clean_code:
            return None
        # Remove last segment to get parent
        return clean_code.rsplit("-", 1)[0]

    def get_code_depth(self, code):
        """Calculate hierarchy depth of a watershed code.

        Deeper codes represent smaller tributaries.
        Example: "200" has depth 1, "200-ABC123" has depth 2

        Args:
            code: Raw FWA_WATERSHED_CODE string

        Returns:
            Integer depth (number of valid segments)
        """
        if not isinstance(code, str):
            return 0
        # Count segments, excluding 000000 placeholders
        return len([x for x in code.split("-") if x != "000000"])

    # --- DATA LOADERS ---
    # These methods load raw geospatial data from various sources
    # Streams are loaded in parallel with memory optimizations due to large dataset size

    def load_streams_raw(self, test_mode=False):
        """Load all stream features from 246 GDB layers in parallel.

        MEMORY OPTIMIZATION: Uses incremental concatenation instead of accumulating
        all layers in a list first. This significantly reduces peak memory usage.

        PARALLEL PROCESSING: Distributes loading across CPU cores for speed.

        Args:
            test_mode: If True, only load first 5 layers for testing

        Returns:
            GeoDataFrame with all stream features (~4.9M in full mode)
        """
        logger.info("=== STEP 1: Loading Streams ===")
        layers = self.get_stream_layers()
        if test_mode:
            layers = layers[:5]
            logger.info("TEST MODE: Loading 5 layers.")

        total_layers = len(layers)
        logger.info(
            f"Loading {total_layers} stream layers in parallel using {self.n_cores} cores..."
        )

        # Prepare arguments for parallel loading
        args = [(self.streams_gdb, layer) for layer in layers]

        # MEMORY STRATEGY: Process in small batches and concatenate incrementally
        # This avoids holding all layer GeoDataFrames in memory at once
        batch_size = max(self.n_cores, 5)  # Small batches to limit memory peaks
        total_features = 0
        loaded_layers = 0
        full_gdf = None
        first_crs = None

        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            # Process layers in batches
            for batch_start in range(0, len(args), batch_size):
                batch_end = min(batch_start + batch_size, len(args))
                batch_args = args[batch_start:batch_end]

                # Collect batch results from parallel workers
                batch_results = []
                for result in executor.map(load_stream_layer_worker, batch_args):
                    if result is not None:
                        batch_results.append(result)
                        total_features += len(result)
                        loaded_layers += 1

                # Concatenate batch and merge with main GDF immediately
                if batch_results:
                    # Preserve CRS from first successful load
                    if first_crs is None:
                        first_crs = batch_results[0].crs

                    # Combine batch results
                    batch_gdf = pd.concat(batch_results, ignore_index=True)
                    batch_gdf = gpd.GeoDataFrame(
                        batch_gdf, geometry="geometry", crs=first_crs
                    )

                    # Merge with existing data
                    if full_gdf is None:
                        full_gdf = batch_gdf
                    else:
                        full_gdf = pd.concat([full_gdf, batch_gdf], ignore_index=True)
                        full_gdf = gpd.GeoDataFrame(
                            full_gdf, geometry="geometry", crs=first_crs
                        )

                    # Clear batch data immediately to free memory
                    del batch_results, batch_gdf
                    gc.collect()

                # Progress reporting
                percent = (batch_end / total_layers) * 100
                logger.info(
                    f"Progress: {batch_end}/{total_layers} layers ({percent:.1f}%) - {total_features:,} features loaded"
                )

        if full_gdf is None:
            full_gdf = gpd.GeoDataFrame()

        self.stats["total_streams_read"] = len(full_gdf)
        logger.info(
            f"Stream loading complete: {len(full_gdf):,} features from {loaded_layers} layers"
        )
        return full_gdf

    def load_lakes(self):
        """Load lake polygon features from FWA_BC.gdb.

        Lakes are used for two purposes:
        1. Correcting stream names (streams touching lakes become "Lake X Tributary")
        2. Linking user-labeled points to their containing waterbody

        Returns:
            GeoDataFrame of lake polygons, or empty if disabled/failed
        """
        logger.info("=== STEP 2a: Loading Lakes ===")
        if not self.process_lakes:
            logger.info("Skipping lakes (disabled)")
            return gpd.GeoDataFrame()
        try:
            lakes = gpd.read_file(str(self.lakes_gdb), layer="FWA_LAKES_POLY")
            self.stats["total_lakes"] = len(lakes)
            return lakes
        except Exception as e:
            logger.error(f"Failed to load Lakes: {e}")
            return gpd.GeoDataFrame()

    def load_wetlands(self):
        """Load wetland polygon features from FWA_BC.gdb.

        Wetlands are linked to user-labeled points to identify which
        waterbody type a labeled point falls within.

        Returns:
            GeoDataFrame of wetland polygons, or empty if disabled/failed
        """
        logger.info("=== STEP 2b: Loading Wetlands ===")
        if not self.process_wetlands:
            logger.info("Skipping wetlands (disabled)")
            return gpd.GeoDataFrame()
        try:
            wetlands = gpd.read_file(str(self.lakes_gdb), layer="FWA_WETLANDS_POLY")
            self.stats["total_wetlands"] = len(wetlands)
            return wetlands
        except Exception as e:
            logger.warning(f"Failed to load Wetlands (Layer might be missing): {e}")
            return gpd.GeoDataFrame()

    def load_manmade(self):
        """Load manmade waterbody polygon features from FWA_BC.gdb.

        Includes reservoirs, canals, and other artificial waterbodies.
        Linked to user-labeled points for waterbody type identification.

        Returns:
            GeoDataFrame of manmade polygons, or empty if disabled/failed
        """
        logger.info("=== STEP 2c: Loading Manmade Waterbodies ===")
        if not self.process_manmade:
            logger.info("Skipping manmade waterbodies (disabled)")
            return gpd.GeoDataFrame()
        try:
            manmade = gpd.read_file(
                str(self.lakes_gdb), layer="FWA_MANMADE_WATERBODIES_POLY"
            )
            self.stats["total_manmade"] = len(manmade)
            return manmade
        except Exception as e:
            logger.warning(
                f"Failed to load Manmade Waterbodies (Layer might be missing): {e}"
            )
            return gpd.GeoDataFrame()

    def load_kml_points(self):
        """Load user-labeled unnamed waterbody points from KML file.

        These are points manually placed by users to mark unnamed lakes/wetlands
        that aren't in the official GNIS database. We link them to waterbody
        polygons to enable searching by the user-provided names.

        Returns:
            GeoDataFrame of labeled points in WGS84 (EPSG:4326), or empty if disabled/failed
        """
        logger.info("=== STEP 2d: Loading KML Points ===")
        if not self.process_points:
            logger.info("Skipping KML points (disabled)")
            return gpd.GeoDataFrame()
        if not self.kml_path.exists():
            logger.warning(f"KML file not found: {self.kml_path}")
            return gpd.GeoDataFrame()

        try:
            points = gpd.read_file(str(self.kml_path))
            # KML files are always WGS84 but sometimes miss CRS metadata
            if points.crs is None:
                points.set_crs(epsg=4326, inplace=True)

            self.stats["total_kml_points"] = len(points)
            logger.info(f"Loaded {len(points)} KML points.")
            return points
        except Exception as e:
            logger.error(f"Failed to load KML: {e}")
            return gpd.GeoDataFrame()

    # --- ENRICHMENT LOGIC ---
    # These methods add information to the raw data to make it more useful

    def enrich_kml_points(self, points_gdf, lakes_gdf, wetlands_gdf, manmade_gdf):
        """Link user-labeled points to their containing waterbody polygons.

        Each point is checked against all three polygon types (lakes, wetlands, manmade)
        and assigned the WATERBODY_POLY_ID if it falls inside. This allows the web app
        to show fishing regulations for waterbodies that don't have official GNIS names.

        ERROR DETECTION: Points that don't fall in any polygon are logged to CSV,
        as they likely represent GPS errors or waterbodies missing from FWA data.

        QUALITY CHECK: Warns if points match named polygons (they should match unnamed ones).

        Args:
            points_gdf: User-labeled point features from KML
            lakes_gdf: Lake polygon features
            wetlands_gdf: Wetland polygon features
            manmade_gdf: Manmade waterbody polygon features

        Returns:
            Enhanced points_gdf with LAKE_POLY_ID, WETLAND_POLY_ID, MANMADE_POLY_ID columns
        """
        logger.info("=== STEP 3a: Enriching KML Points with Waterbody IDs ===")

        if points_gdf.empty:
            return points_gdf

        # Reproject points to match polygon CRS for accurate spatial joins
        target_crs = lakes_gdf.crs
        if points_gdf.crs != target_crs:
            points_gdf = points_gdf.to_crs(target_crs)

        # Initialize ID columns (use None to allow nullable integers later)
        points_gdf["LAKE_POLY_ID"] = None
        points_gdf["WETLAND_POLY_ID"] = None
        points_gdf["MANMADE_POLY_ID"] = None

        def attach_id(points, polys, id_col_name, poly_type_name):
            """Inner function to perform spatial join and assign waterbody IDs.

            Args:
                points: Point GeoDataFrame to enrich
                polys: Polygon GeoDataFrame to join against
                id_col_name: Name of column to store matched polygon ID
                poly_type_name: Type label for logging (Lakes/Wetlands/Manmade)

            Returns:
                Points GeoDataFrame with ID column populated
            """
            if polys.empty:
                return points

            if "WATERBODY_POLY_ID" not in polys.columns:
                logger.warning(f"{poly_type_name} missing WATERBODY_POLY_ID column.")
                return points

            # Include polygon name in join for quality checking
            join_cols = ["geometry", "WATERBODY_POLY_ID"]
            name_col = None
            for col in ["GNIS_NAME_1", "GNIS_NAME", "name", "Name"]:
                if col in polys.columns:
                    join_cols.append(col)
                    name_col = col
                    break

            # Spatial join: find which polygon each point falls into
            joined = gpd.sjoin(
                points,
                polys[join_cols],
                how="left",
                predicate="intersects",
            )

            # Handle cases where point intersects multiple polygons (take first match)
            joined.index.name = "idx_temp"
            joined = joined.reset_index()

            id_map = joined.groupby("idx_temp")["WATERBODY_POLY_ID"].first()
            matched_count = id_map.notna().sum()
            points.loc[id_map.index, id_col_name] = id_map

            # QUALITY CHECK: Warn if points match NAMED polygons (should mostly be unnamed)
            if matched_count > 0 and name_col:
                name_map = joined.groupby("idx_temp")[name_col].first()
                matched_names = name_map[name_map.notna()]
                if len(matched_names) > 0:
                    logger.warning(
                        f"  ⚠ {len(matched_names)} KML points matched NAMED {poly_type_name} (expected: unnamed)"
                    )
                    # Sample logging (first 5 matches)
                    for idx, name in list(matched_names.head(5).items()):
                        point_name = (
                            points.loc[idx, "Name"]
                            if "Name" in points.columns
                            else f"Point {idx}"
                        )
                        logger.warning(f"    • {point_name} → {name}")
                    if len(matched_names) > 5:
                        logger.warning(f"    ... and {len(matched_names) - 5} more")

            return points

        if not lakes_gdf.empty:
            logger.info("Checking points in Lakes...")
            points_gdf = attach_id(points_gdf, lakes_gdf, "LAKE_POLY_ID", "Lakes")

        if not wetlands_gdf.empty:
            logger.info("Checking points in Wetlands...")
            points_gdf = attach_id(
                points_gdf, wetlands_gdf, "WETLAND_POLY_ID", "Wetlands"
            )

        if not manmade_gdf.empty:
            logger.info("Checking points in Manmade Waterbodies...")
            points_gdf = attach_id(
                points_gdf, manmade_gdf, "MANMADE_POLY_ID", "Manmade"
            )

        # --- CONVERT IDs TO NULLABLE INTEGER TYPE ---
        # Using 'Int64' (capital I) allows integers with NaN values
        # Regular 'int64' doesn't support NaN, which we need for points without matches
        for col in ["LAKE_POLY_ID", "WETLAND_POLY_ID", "MANMADE_POLY_ID"]:
            points_gdf[col] = points_gdf[col].astype("Int64")

        # --- ERROR LOGGING FOR UNMATCHED POINTS ---
        # Points that don't fall in ANY polygon likely indicate GPS errors
        # or waterbodies missing from the FWA database
        unmatched = points_gdf[
            points_gdf["LAKE_POLY_ID"].isna()
            & points_gdf["WETLAND_POLY_ID"].isna()
            & points_gdf["MANMADE_POLY_ID"].isna()
        ]

        if not unmatched.empty:
            error_log_path = self.output_gdb.parent / "unmatched_points_error_log.csv"
            logger.warning(
                f"!! ALERT !! {len(unmatched)} KML points did not fall inside any waterbody polygon."
            )
            logger.warning(f"Saving list of unmatched points to: {error_log_path}")

            # Ensure output directory exists
            error_log_path.parent.mkdir(parents=True, exist_ok=True)

            # Extract relevant identification columns for the error log
            log_cols = ["geometry"]
            for col in ["Name", "name", "Description", "description", "label"]:
                if col in unmatched.columns:
                    log_cols.insert(0, col)

            try:
                unmatched[log_cols].to_csv(error_log_path, index=True)
            except Exception as e:
                logger.error(f"Could not write error log: {e}")

        return points_gdf

    def enrich_streams(self, streams_gdf, lakes_gdf):
        logger.info("=== STEP 3b: Enriching Stream Names ===")

        logger.info("Calculating hierarchy depths...")
        streams_gdf["clean_code"] = streams_gdf["FWA_WATERSHED_CODE"].apply(
            self.clean_watershed_code
        )
        streams_gdf["parent_code"] = streams_gdf["clean_code"].apply(
            self.get_parent_code
        )
        streams_gdf["depth"] = streams_gdf["FWA_WATERSHED_CODE"].apply(
            self.get_code_depth
        )

        # Initialize TRIBUTARY_OF field
        streams_gdf["TRIBUTARY_OF"] = None

        originally_unnamed_mask = (streams_gdf["GNIS_NAME"].isna()) | (
            streams_gdf["GNIS_NAME"].str.strip() == ""
        )

        named_mask = ~originally_unnamed_mask
        self.stats["original_named_streams"] = named_mask.sum()
        logger.info(
            f"Protected {self.stats['original_named_streams']:,} originally named streams."
        )

        # FIX 1: Unnamed streams with same watershed code as named streams inherit the name
        logger.info(
            "Assigning names to unnamed streams with same watershed code as named streams..."
        )
        name_by_code = streams_gdf.loc[
            named_mask, ["clean_code", "GNIS_NAME"]
        ].drop_duplicates(subset="clean_code")
        code_to_name = pd.Series(
            name_by_code["GNIS_NAME"].values, index=name_by_code["clean_code"]
        )

        # Map unnamed streams to names via clean_code
        inherited_names = streams_gdf.loc[originally_unnamed_mask, "clean_code"].map(
            code_to_name
        )
        inherited_mask = originally_unnamed_mask & inherited_names.notna()
        streams_gdf.loc[inherited_mask, "GNIS_NAME"] = inherited_names[inherited_mask]
        logger.info(
            f" -> Inherited names for {inherited_mask.sum():,} unnamed stream segments (same watershed code)"
        )

        # Update masks after inheriting names
        originally_unnamed_mask = (streams_gdf["GNIS_NAME"].isna()) | (
            streams_gdf["GNIS_NAME"].str.strip() == ""
        )
        named_mask = ~originally_unnamed_mask

        logger.info("Assigning River Tributary names...")

        # Vectorized name mapping - much faster than iterative approach
        # Use drop_duplicates to handle cases where multiple streams have same clean_code
        name_df = streams_gdf.loc[
            named_mask, ["clean_code", "GNIS_NAME"]
        ].drop_duplicates(subset="clean_code")
        name_map = pd.Series(
            name_df["GNIS_NAME"].values,
            index=name_df["clean_code"],
        )

        # Use vectorized string operations to find parent names
        # This applies to BOTH unnamed and originally named streams
        all_streams_parents = streams_gdf["parent_code"].map(name_map)
        streams_with_parent = all_streams_parents.notna()

        # Set TRIBUTARY_OF for all streams with a parent (both named and unnamed)
        streams_gdf.loc[streams_with_parent, "TRIBUTARY_OF"] = all_streams_parents[
            streams_with_parent
        ]

        # Only update GNIS_NAME for originally unnamed streams
        unnamed_with_parent = originally_unnamed_mask & streams_with_parent
        streams_gdf.loc[unnamed_with_parent, "GNIS_NAME"] = (
            all_streams_parents[unnamed_with_parent] + " Tributary"
        )

        self.stats["river_tributaries_found"] = unnamed_with_parent.sum()
        logger.info(
            f" -> Set TRIBUTARY_OF for {streams_with_parent.sum():,} streams (named and unnamed)"
        )
        logger.info(
            f" -> Enriched names for {unnamed_with_parent.sum():,} unnamed tributaries"
        )

        if not lakes_gdf.empty:
            logger.info("Verifying Lake Tributaries...")

            # Check ALL streams (both named and unnamed) that touch lakes
            # We'll set TRIBUTARY_OF for all, but only update GNIS_NAME for unnamed
            candidate_streams = streams_gdf.copy()
            completed_codes = set()

            # Pre-filter and prepare lakes
            named_lakes = lakes_gdf[
                (lakes_gdf["GNIS_NAME_1"].notna())
                & (lakes_gdf["GNIS_NAME_1"].str.strip() != "")
            ].copy()

            # Vectorized depth calculation
            named_lakes["depth"] = named_lakes["FWA_WATERSHED_CODE"].apply(
                self.get_code_depth
            )
            named_lakes = named_lakes.sort_values("depth", ascending=False)
            unique_depths = sorted(named_lakes["depth"].unique(), reverse=True)

            total_corrected = 0
            total_trib_of_set = 0

            if candidate_streams.crs != named_lakes.crs:
                named_lakes = named_lakes.to_crs(candidate_streams.crs)

            for lake_depth in unique_depths:
                lakes_at_depth = named_lakes[named_lakes["depth"] == lake_depth][
                    ["geometry", "GNIS_NAME_1", "depth"]
                ]

                # Check streams with higher depth than lake (tributaries flow into lakes)
                current_candidates = candidate_streams[
                    (~candidate_streams["clean_code"].isin(completed_codes))
                    & (candidate_streams["depth"] > lake_depth)
                ]

                if current_candidates.empty:
                    continue

                join_result = self.parallel_spatial_join(
                    current_candidates[["geometry", "FWA_WATERSHED_CODE"]],
                    lakes_at_depth,
                )

                if not join_result.empty:
                    code_to_lake = (
                        join_result.groupby("FWA_WATERSHED_CODE")["GNIS_NAME_1"]
                        .first()
                        .to_dict()
                    )

                    mask_codes = streams_gdf["FWA_WATERSHED_CODE"].isin(
                        code_to_lake.keys()
                    )

                    # Set TRIBUTARY_OF for ALL streams that touch lakes (named and unnamed)
                    lake_names = streams_gdf.loc[mask_codes, "FWA_WATERSHED_CODE"].map(
                        code_to_lake
                    )
                    streams_gdf.loc[mask_codes, "TRIBUTARY_OF"] = lake_names
                    total_trib_of_set += mask_codes.sum()

                    # Only update GNIS_NAME for originally unnamed streams
                    mask_unnamed_update = mask_codes & originally_unnamed_mask
                    if mask_unnamed_update.any():
                        lake_names_unnamed = streams_gdf.loc[
                            mask_unnamed_update, "FWA_WATERSHED_CODE"
                        ].map(code_to_lake)
                        streams_gdf.loc[mask_unnamed_update, "GNIS_NAME"] = (
                            lake_names_unnamed + " Tributary"
                        )
                        total_corrected += mask_unnamed_update.sum()

                    completed_codes.update(code_to_lake.keys())

            self.stats["lake_tributaries_corrected"] = total_corrected
            logger.info(
                f" -> Set TRIBUTARY_OF for {total_trib_of_set:,} streams touching lakes (named and unnamed)"
            )
            logger.info(
                f" -> Enriched names for {total_corrected:,} unnamed lake tributaries"
            )
        else:
            logger.info(" -> Skipping lake tributary processing (no lakes loaded)")

        final_streams = streams_gdf[
            (streams_gdf["GNIS_NAME"].notna())
            & (streams_gdf["GNIS_NAME"].str.strip() != "")
        ].copy()

        # Drop temporary columns but keep TRIBUTARY_OF
        temp_cols = ["clean_code", "parent_code", "depth"]
        cols_to_drop = [c for c in temp_cols if c in final_streams.columns]
        if cols_to_drop:
            final_streams = final_streams.drop(columns=cols_to_drop)

        self.stats["final_streams_count"] = len(final_streams)

        # Force garbage collection after heavy processing
        gc.collect()

        return final_streams

    def parallel_spatial_join(self, target_gdf, zone_gdf):
        if len(target_gdf) == 0:
            return gpd.GeoDataFrame()

        logger.info(
            f"  Starting parallel spatial join for {len(target_gdf):,} features using {self.n_cores} cores..."
        )

        chunks = np.array_split(target_gdf, self.n_cores)
        args = [(chunk, zone_gdf) for chunk in chunks]
        results = []

        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            for i, res in enumerate(executor.map(spatial_join_worker, args), 1):
                results.append(res)
                if i % max(1, self.n_cores // 4) == 0:  # Progress every 25% of chunks
                    logger.info(f"  Completed {i}/{self.n_cores} chunks...")

        if results:
            logger.info(f"  Concatenating {len(results)} result chunks...")
            res_df = pd.concat(results, ignore_index=True)
            final_gdf = gpd.GeoDataFrame(
                res_df, geometry="geometry", crs=target_gdf.crs
            )

            # Clean up temporary data
            del results, res_df
            gc.collect()

            logger.info(f"  Spatial join complete: {len(final_gdf):,} features")
            return final_gdf
        return gpd.GeoDataFrame()

    def split_and_save(
        self, streams_gdf, lakes_gdf, wetlands_gdf, manmade_gdf, points_gdf
    ):
        logger.info("=== STEP 4: Spatial Processing by Zone ===")
        self.cleanup_output()

        wildlife = gpd.read_file(str(self.wildlife_gpkg))
        zone_field = next(
            col
            for col in wildlife.columns
            if "ZONE" in col.upper() or "UNIT" in col.upper()
        )
        wildlife["ZONE_GROUP"] = wildlife[zone_field].astype(str).str.split("-").str[0]
        target_crs = "EPSG:3005"  # BC Albers

        # Ensure wildlife zones are in target CRS
        if wildlife.crs != target_crs:
            wildlife = wildlife.to_crs(target_crs)

        wildlife.to_file(
            str(self.output_gdb), layer="WILDLIFE_MGMT_UNITS", driver="OpenFileGDB"
        )

        # Pre-compute zone outlines and prepare wildlife geometry
        zone_outlines = wildlife.dissolve(by="ZONE_GROUP")
        unique_zones = sorted(wildlife["ZONE_GROUP"].unique())

        # Include MU_NAME (management unit) along with ZONE_GROUP for spatial joins
        wildlife_cols = ["geometry", "ZONE_GROUP"]
        if zone_field in wildlife.columns:
            wildlife_cols.append(zone_field)
        wildlife_zones = wildlife[wildlife_cols].copy()  # Reuse this

        total_zones = len(unique_zones)
        logger.info(f"Processing {total_zones} zones...")

        # Batch CRS transformations for all datasets
        logger.info("Performing CRS transformations...")
        if not streams_gdf.empty and streams_gdf.crs != target_crs:
            streams_gdf = streams_gdf.to_crs(target_crs)
        if not lakes_gdf.empty and lakes_gdf.crs != target_crs:
            lakes_gdf = lakes_gdf.to_crs(target_crs)
        if not wetlands_gdf.empty and wetlands_gdf.crs != target_crs:
            wetlands_gdf = wetlands_gdf.to_crs(target_crs)
        if not manmade_gdf.empty and manmade_gdf.crs != target_crs:
            manmade_gdf = manmade_gdf.to_crs(target_crs)
        if not points_gdf.empty and points_gdf.crs != target_crs:
            points_gdf = points_gdf.to_crs(target_crs)

        # Spatial joins with reused wildlife geometry
        joined_streams = gpd.GeoDataFrame()
        if not streams_gdf.empty:
            logger.info("Joining Streams to Zones...")
            joined_streams = self.parallel_spatial_join(streams_gdf, wildlife_zones)

        joined_lakes = gpd.GeoDataFrame()
        if not lakes_gdf.empty:
            logger.info("Joining Lakes to Zones...")
            joined_lakes = self.parallel_spatial_join(lakes_gdf, wildlife_zones)

        joined_wetlands = gpd.GeoDataFrame()
        if not wetlands_gdf.empty:
            logger.info("Joining Wetlands to Zones...")
            joined_wetlands = self.parallel_spatial_join(wetlands_gdf, wildlife_zones)

        joined_manmade = gpd.GeoDataFrame()
        if not manmade_gdf.empty:
            logger.info("Joining Manmade Waterbodies to Zones...")
            joined_manmade = self.parallel_spatial_join(manmade_gdf, wildlife_zones)

        joined_points = gpd.GeoDataFrame()
        if not points_gdf.empty:
            logger.info("Joining KML Points to Zones...")
            joined_points = self.parallel_spatial_join(points_gdf, wildlife_zones)

        logger.info(f"Saving features for {total_zones} zones...")
        for zone_idx, zone in enumerate(unique_zones, 1):
            percent = (zone_idx / total_zones) * 100
            logger.info(
                f"[{zone_idx}/{total_zones}] ({percent:.1f}%) Saving Zone {zone}..."
            )

            if zone in zone_outlines.index:
                outline = zone_outlines.loc[[zone]]
                outline.to_file(
                    str(self.output_gdb),
                    layer=f"ZONE_OUTLINE_{zone}",
                    driver="OpenFileGDB",
                )
                time.sleep(0.1)

            if not joined_streams.empty:
                z_streams = joined_streams[joined_streams["ZONE_GROUP"] == zone]
                if not z_streams.empty:
                    # Streams are linear, use LINEAR_FEATURE_ID for deduplication
                    z_streams = z_streams.drop_duplicates(subset=["LINEAR_FEATURE_ID"])
                    keep_cols = [
                        c for c in streams_gdf.columns if c in z_streams.columns
                    ]
                    if "geometry" not in keep_cols:
                        keep_cols.append("geometry")
                    # Include management unit field from spatial join
                    if zone_field in z_streams.columns and zone_field not in keep_cols:
                        keep_cols.append(zone_field)
                    z_streams = gpd.GeoDataFrame(
                        z_streams[keep_cols], geometry="geometry", crs=target_crs
                    )
                    z_streams.to_file(
                        str(self.output_gdb),
                        layer=f"STREAMS_ZONE_{zone}",
                        driver="OpenFileGDB",
                    )

            if not joined_lakes.empty:
                z_lakes = joined_lakes[joined_lakes["ZONE_GROUP"] == zone]
                if not z_lakes.empty:
                    # FIX: Deduplicate by POLY_ID to keep multi-polygon lakes (like Kinbasket) intact
                    dedup = (
                        "WATERBODY_POLY_ID"
                        if "WATERBODY_POLY_ID" in z_lakes.columns
                        else (
                            "WATERBODY_KEY"
                            if "WATERBODY_KEY" in z_lakes.columns
                            else None
                        )
                    )

                    if dedup:
                        z_lakes = z_lakes.drop_duplicates(subset=[dedup])
                    else:
                        z_lakes = z_lakes.drop_duplicates()

                    keep_cols = [c for c in lakes_gdf.columns if c in z_lakes.columns]
                    if "geometry" not in keep_cols:
                        keep_cols.append("geometry")
                    # Include management unit field from spatial join
                    if zone_field in z_lakes.columns and zone_field not in keep_cols:
                        keep_cols.append(zone_field)
                    z_lakes = gpd.GeoDataFrame(
                        z_lakes[keep_cols], geometry="geometry", crs=target_crs
                    )
                    z_lakes.to_file(
                        str(self.output_gdb),
                        layer=f"LAKES_ZONE_{zone}",
                        driver="OpenFileGDB",
                    )

            if not joined_wetlands.empty:
                z_wet = joined_wetlands[joined_wetlands["ZONE_GROUP"] == zone]
                if not z_wet.empty:
                    # FIX: Deduplicate by POLY_ID
                    dedup = (
                        "WATERBODY_POLY_ID"
                        if "WATERBODY_POLY_ID" in z_wet.columns
                        else (
                            "WATERBODY_KEY"
                            if "WATERBODY_KEY" in z_wet.columns
                            else None
                        )
                    )

                    if dedup:
                        z_wet = z_wet.drop_duplicates(subset=[dedup])
                    else:
                        z_wet = z_wet.drop_duplicates()

                    keep_cols = [c for c in wetlands_gdf.columns if c in z_wet.columns]
                    if "geometry" not in keep_cols:
                        keep_cols.append("geometry")
                    # Include management unit field from spatial join
                    if zone_field in z_wet.columns and zone_field not in keep_cols:
                        keep_cols.append(zone_field)
                    z_wet = gpd.GeoDataFrame(
                        z_wet[keep_cols], geometry="geometry", crs=target_crs
                    )
                    z_wet.to_file(
                        str(self.output_gdb),
                        layer=f"WETLANDS_ZONE_{zone}",
                        driver="OpenFileGDB",
                    )

            if not joined_manmade.empty:
                z_man = joined_manmade[joined_manmade["ZONE_GROUP"] == zone]
                if not z_man.empty:
                    # FIX: Deduplicate by POLY_ID
                    dedup = (
                        "WATERBODY_POLY_ID"
                        if "WATERBODY_POLY_ID" in z_man.columns
                        else (
                            "WATERBODY_KEY"
                            if "WATERBODY_KEY" in z_man.columns
                            else None
                        )
                    )

                    if dedup:
                        z_man = z_man.drop_duplicates(subset=[dedup])
                    else:
                        z_man = z_man.drop_duplicates()

                    keep_cols = [c for c in manmade_gdf.columns if c in z_man.columns]
                    if "geometry" not in keep_cols:
                        keep_cols.append("geometry")
                    # Include management unit field from spatial join
                    if zone_field in z_man.columns and zone_field not in keep_cols:
                        keep_cols.append(zone_field)
                    z_man = gpd.GeoDataFrame(
                        z_man[keep_cols], geometry="geometry", crs=target_crs
                    )
                    z_man.to_file(
                        str(self.output_gdb),
                        layer=f"MANMADE_ZONE_{zone}",
                        driver="OpenFileGDB",
                    )

            if not joined_points.empty:
                z_pts = joined_points[joined_points["ZONE_GROUP"] == zone]
                if not z_pts.empty:
                    z_pts = z_pts.drop_duplicates(subset=["geometry"])
                    keep_cols = [c for c in points_gdf.columns if c in z_pts.columns]
                    if "geometry" not in keep_cols:
                        keep_cols.append("geometry")
                    # Include management unit field from spatial join
                    if zone_field in z_pts.columns and zone_field not in keep_cols:
                        keep_cols.append(zone_field)
                    z_pts = gpd.GeoDataFrame(
                        z_pts[keep_cols], geometry="geometry", crs=target_crs
                    )
                    z_pts.to_file(
                        str(self.output_gdb),
                        layer=f"LABELED_POINTS_ZONE_{zone}",
                        driver="OpenFileGDB",
                    )

            # Clean up zone-specific data every 10 zones to prevent memory buildup
            if zone_idx % 10 == 0:
                gc.collect()

            time.sleep(0.1)

        logger.info(f"All {total_zones} zones processed.")

        # Final cleanup of joined data
        del joined_streams, joined_lakes, joined_wetlands, joined_manmade, joined_points
        gc.collect()

    def build_waterbody_index(self):
        """Build indexed lookup structure from processed geodatabase."""
        logger.info("=== STEP 5: Building Waterbody Index ===")

        output_path = self.output_gdb.parent / "waterbody_index.json"

        if not self.output_gdb.exists():
            logger.error(f"GDB not found: {self.output_gdb}")
            return

        # Structure: index[zone][normalized_name] = [list of features]
        index = defaultdict(lambda: defaultdict(list))

        # Cache for Polygons
        poly_cache = defaultdict(lambda: defaultdict(dict))

        try:
            layers = fiona.listlayers(str(self.output_gdb))
        except Exception as e:
            logger.error(f"Error reading GDB layers: {e}")
            return

        # Categorize layers
        stream_layers = (
            [l for l in layers if l.startswith("STREAMS_ZONE_")]
            if self.process_streams
            else []
        )
        point_layers = (
            [l for l in layers if l.startswith("LABELED_POINTS_ZONE_")]
            if self.process_points
            else []
        )

        polygon_groups = []
        if self.process_lakes:
            polygon_groups.append(
                ("lake", [l for l in layers if l.startswith("LAKES_ZONE_")])
            )
        if self.process_wetlands:
            polygon_groups.append(
                ("wetland", [l for l in layers if l.startswith("WETLANDS_ZONE_")])
            )
        if self.process_manmade:
            polygon_groups.append(
                ("manmade", [l for l in layers if l.startswith("MANMADE_ZONE_")])
            )

        total_features = 0

        # --- PHASE 1: PROCESS POLYGONS IN PARALLEL ---
        for type_label, layer_list in polygon_groups:
            if not layer_list:
                continue

            logger.info(
                f"Indexing {len(layer_list)} {type_label} layers in parallel using {self.n_cores} cores..."
            )

            # Prepare worker arguments
            args = [
                (self.output_gdb, layer_name, type_label) for layer_name in layer_list
            ]

            # Process layers in parallel
            with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
                results = list(executor.map(process_polygon_layer_worker, args))

            # Merge results from workers
            successful = 0
            failed = 0
            for result in results:
                if result is None:
                    continue

                if len(result) == 7:  # Error case
                    (
                        zone,
                        type_label,
                        layer_results,
                        poly_cache_results,
                        feature_count,
                        layer_name,
                        error,
                    ) = result
                    logger.warning(f"  ⚠ {layer_name}: Error - {error}")
                    failed += 1
                else:  # Success case
                    (
                        zone,
                        result_type_label,
                        layer_results,
                        poly_cache_results,
                        feature_count,
                        layer_name,
                    ) = result

                    # Merge into main index
                    for normalized_name, features in layer_results.items():
                        index[zone][normalized_name].extend(features)

                    # Merge into polygon cache
                    for poly_id, feature in poly_cache_results.items():
                        poly_cache[zone][result_type_label][poly_id] = feature

                    total_features += feature_count
                    successful += 1

            logger.info(
                f"  ✓ Completed {successful} {type_label} layers, {failed} failed"
            )

        # --- PHASE 2: PROCESS STREAMS IN PARALLEL ---
        if stream_layers:
            logger.info(
                f"Indexing {len(stream_layers)} stream layers in parallel using {self.n_cores} cores..."
            )

            # Prepare worker arguments
            args = [(self.output_gdb, layer_name) for layer_name in stream_layers]

            # Process layers in parallel
            with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
                results = list(executor.map(process_stream_layer_worker, args))

            # Merge results from workers
            successful = 0
            failed = 0
            for result in results:
                if result is None:
                    continue

                if len(result) == 5:  # Error case
                    zone, layer_results, feature_count, layer_name, error = result
                    logger.warning(f"  ⚠ {layer_name}: Error - {error}")
                    failed += 1
                else:  # Success case
                    zone, layer_results, feature_count, layer_name = result

                    # Merge into main index
                    for normalized_name, features in layer_results.items():
                        index[zone][normalized_name].extend(features)

                    total_features += feature_count
                    successful += 1

            logger.info(f"  ✓ Completed {successful} stream layers, {failed} failed")

        # --- PHASE 3: PROCESS POINTS IN PARALLEL ---
        # Points need polygon cache, so we need to pass the appropriate cache for each zone
        if point_layers:
            logger.info(
                f"Indexing {len(point_layers)} point layers in parallel using {self.n_cores} cores..."
            )

            # Prepare worker arguments with zone-specific polygon caches
            args = []
            for layer_name in point_layers:
                zone_match = re.search(r"ZONE_(\d+)", layer_name)
                if zone_match:
                    zone = zone_match.group(1)
                    # Pass the polygon cache for this specific zone
                    zone_poly_cache = dict(poly_cache.get(zone, {}))
                    args.append((self.output_gdb, layer_name, zone_poly_cache))

            # Process layers in parallel
            with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
                results = list(executor.map(process_point_layer_worker, args))

            # Merge results from workers
            successful = 0
            failed = 0
            for result in results:
                if result is None:
                    continue

                if len(result) == 5:  # Error case
                    zone, layer_results, feature_count, layer_name, error = result
                    logger.warning(f"  ⚠ {layer_name}: Error - {error}")
                    failed += 1
                else:  # Success case
                    zone, layer_results, feature_count, layer_name = result

                    # Merge into main index
                    for normalized_name, features in layer_results.items():
                        index[zone][normalized_name].extend(features)

                    total_features += feature_count
                    successful += 1

            logger.info(f"  ✓ Completed {successful} point layers, {failed} failed")

        # Convert to regular dict and save
        output_index = {}
        for zone, names in index.items():
            output_index[zone] = dict(names)

        total_unique_names = sum(len(names) for names in index.values())

        logger.info(f"Index Statistics:")
        logger.info(f"  Total features indexed: {total_features}")
        logger.info(f"  Unique names: {total_unique_names}")
        logger.info(f"  Zones covered: {len(output_index)}")

        logger.info(f"Writing index to: {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            # Write without indentation to reduce file size dramatically
            json.dump(output_index, f, ensure_ascii=False, separators=(",", ":"))
        logger.info(
            f"Index saved successfully ({output_path.stat().st_size / 1024 / 1024:.1f} MB)"
        )

    def run(self, test_mode=False, build_index=False):
        start = time.time()

        raw_streams = (
            self.load_streams_raw(test_mode)
            if self.process_streams
            else gpd.GeoDataFrame()
        )
        lakes = self.load_lakes()
        wetlands = self.load_wetlands()
        manmade = self.load_manmade()
        points = self.load_kml_points()

        enriched_points = (
            self.enrich_kml_points(points, lakes, wetlands, manmade)
            if self.process_points
            else gpd.GeoDataFrame()
        )

        # Clear raw points after enrichment
        del points
        gc.collect()

        enriched_streams = (
            self.enrich_streams(raw_streams, lakes)
            if self.process_streams
            else gpd.GeoDataFrame()
        )

        # Clear raw streams after enrichment to free memory
        del raw_streams
        gc.collect()

        self.split_and_save(enriched_streams, lakes, wetlands, manmade, enriched_points)

        if build_index:
            self.build_waterbody_index()

        end = time.time()
        logger.info("=" * 50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total Time: {(end-start)/60:.2f} mins")
        if self.process_streams:
            logger.info(
                f"Original Named Streams: {self.stats['original_named_streams']:,}"
            )
            logger.info(
                f"River Tributaries Found: {self.stats['river_tributaries_found']:,}"
            )
            logger.info(
                f"Lake Tributaries Corrected: {self.stats['lake_tributaries_corrected']:,}"
            )
            logger.info(f"Final Streams: {self.stats['final_streams_count']:,}")
        if self.process_lakes:
            logger.info(f"Lakes: {self.stats['total_lakes']:,}")
        if self.process_wetlands:
            logger.info(f"Wetlands: {self.stats['total_wetlands']:,}")
        if self.process_manmade:
            logger.info(f"Manmade: {self.stats['total_manmade']:,}")
        if self.process_points:
            logger.info(f"KML Points: {self.stats['total_kml_points']:,}")
        logger.info(f"Output: {self.output_gdb}")
        logger.info("=" * 50)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Process BC FWA data and split by wildlife management zones",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full processing with index building
  python fwa_preprocessing.py --build-index
  
  # Build index only from existing GDB
  python fwa_preprocessing.py --build-index-only
  
  # Test mode (5 layers only)
  python fwa_preprocessing.py --test-mode
  
  # Skip streams processing
  python fwa_preprocessing.py --skip-streams
  
  # Process only lakes and build index
  python fwa_preprocessing.py --skip-streams --skip-wetlands --skip-manmade --skip-points --build-index
  
  # Rebuild index for only streams and lakes
  python fwa_preprocessing.py --build-index-only --skip-wetlands --skip-manmade --skip-points
        """,
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Test mode: process only 5 stream layers",
    )
    parser.add_argument(
        "--build-index",
        action="store_true",
        help="Build waterbody index after processing",
    )
    parser.add_argument(
        "--build-index-only",
        action="store_true",
        help="Only build the index from existing GDB (skip all processing)",
    )
    parser.add_argument(
        "--skip-streams", action="store_true", help="Skip stream processing"
    )
    parser.add_argument(
        "--skip-lakes", action="store_true", help="Skip lake processing"
    )
    parser.add_argument(
        "--skip-wetlands", action="store_true", help="Skip wetlands processing"
    )
    parser.add_argument(
        "--skip-manmade",
        action="store_true",
        help="Skip manmade waterbodies processing",
    )
    parser.add_argument(
        "--skip-points", action="store_true", help="Skip KML points processing"
    )

    args = parser.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    base_data = (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
    )

    streams_gdb = base_data / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    lakes_gdb = base_data / "FWA_BC" / "FWA_BC.gdb"
    wildlife_gpkg = base_data / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    kml_path = project_root / "data" / "labelled" / "unnamed_lakes.kml"

    output_gdb = script_dir / "output" / "fwa_preprocessing" / "FWA_Zone_Grouped.gdb"

    # Handle build-index-only mode
    if args.build_index_only:
        if not output_gdb.exists():
            print(f"Error: Output GDB not found at {output_gdb}")
            print("Please run the full processing first before building index.")
            return

        logger.info("Building index from existing GDB...")
        processor = FWAProcessor(
            str(streams_gdb),
            str(lakes_gdb),
            str(wildlife_gpkg),
            str(kml_path),
            str(output_gdb),
            process_streams=not args.skip_streams,
            process_lakes=not args.skip_lakes,
            process_wetlands=not args.skip_wetlands,
            process_manmade=not args.skip_manmade,
            process_points=not args.skip_points,
        )
        processor.build_waterbody_index()
        return

    if not streams_gdb.exists() and not args.skip_streams:
        print(f"Error: Streams GDB not found at {streams_gdb}")
        return

    processor = FWAProcessor(
        str(streams_gdb),
        str(lakes_gdb),
        str(wildlife_gpkg),
        str(kml_path),
        str(output_gdb),
        process_streams=not args.skip_streams,
        process_lakes=not args.skip_lakes,
        process_wetlands=not args.skip_wetlands,
        process_manmade=not args.skip_manmade,
        process_points=not args.skip_points,
    )
    processor.run(test_mode=args.test_mode, build_index=args.build_index)


if __name__ == "__main__":
    main()
