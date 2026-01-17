"""
Phase 5: Waterbody Index Building

Builds searchable JSON index from processed geodatabase for web application.

Index Structure:
    index[zone][normalized_name] = [list of features]

Features include:
- Streams (by GNIS_NAME and TRIBUTARY_OF)
- Lakes, wetlands, manmade waterbodies (by GNIS_NAME)
- Labeled points (linked to containing polygons)
"""

import json
import re
import logging
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple, Optional
import fiona
import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> Optional[str]:
    """Normalize waterbody name for indexing.

    Args:
        name: Raw name string

    Returns:
        Normalized lowercase name, or None if invalid
    """
    if pd.isna(name) or not name or str(name).lower() in ("none", "nan", ""):
        return None

    normalized = str(name).strip().lower()
    return normalized if normalized else None


def process_polygon_layer_worker(args: Tuple) -> Tuple:
    """Worker function to process a polygon layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name, type_label)

    Returns:
        Tuple of (zone, type_label, layer_results, poly_cache_results, feature_count, layer_name)
        or (zone, type_label, {}, {}, 0, layer_name, error) on failure
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


def process_stream_layer_worker(args: Tuple) -> Tuple:
    """Worker function to process a stream layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name)

    Returns:
        Tuple of (zone, layer_results, feature_count, layer_name)
        or (zone, {}, 0, layer_name, error) on failure
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

                # Also index by TRIBUTARY_OF as "X Tributary" for linking
                if tributary_of:
                    tributary_normalized = normalize_name(tributary_of + " tributary")
                    if tributary_normalized not in layer_results:
                        layer_results[tributary_normalized] = []
                    layer_results[tributary_normalized].append(feature_data)

        return (zone, layer_results, feature_count, layer_name)

    except Exception as e:
        return (zone, {}, 0, layer_name, str(e))


def process_point_layer_worker(args: Tuple) -> Tuple:
    """Worker function to process a point layer for indexing in parallel.

    Args:
        args: Tuple of (gdb_path, layer_name, poly_cache_for_zone)

    Returns:
        Tuple of (zone, layer_results, feature_count, layer_name)
        or (zone, {}, 0, layer_name, error) on failure
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


class IndexBuilder:
    """Builds searchable waterbody index from processed GDB."""

    def __init__(self, input_gdb: Path, output_json: Path, n_cores: int = 4):
        """Initialize index builder.

        Args:
            input_gdb: Path to zone-split GDB
            output_json: Path for output waterbody_index.json
            n_cores: Number of CPU cores for parallel processing
        """
        self.input_gdb = input_gdb
        self.output_json = output_json
        self.n_cores = n_cores

        self.stats = {
            "total_features": 0,
            "unique_names": 0,
            "zones": 0,
        }

    def run(self) -> Path:
        """Build waterbody index.

        Returns:
            Path to output JSON file
        """
        logger.info("=== Phase 5: Building Waterbody Index ===")

        if not self.input_gdb.exists():
            raise FileNotFoundError(f"GDB not found: {self.input_gdb}")

        # Structure: index[zone][normalized_name] = [list of features]
        index = defaultdict(lambda: defaultdict(list))

        # Cache for polygons
        poly_cache = defaultdict(lambda: defaultdict(dict))

        # Get all layers
        try:
            layers = fiona.listlayers(str(self.input_gdb))
        except Exception as e:
            logger.error(f"Error reading GDB layers: {e}")
            raise

        # Categorize layers
        stream_layers = [l for l in layers if l.startswith("STREAMS_ZONE_")]
        point_layers = [l for l in layers if l.startswith("LABELED_POINTS_ZONE_")]

        polygon_groups = [
            ("lake", [l for l in layers if l.startswith("LAKES_ZONE_")]),
            ("wetland", [l for l in layers if l.startswith("WETLANDS_ZONE_")]),
            ("manmade", [l for l in layers if l.startswith("MANMADE_ZONE_")]),
        ]

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
                (self.input_gdb, layer_name, type_label) for layer_name in layer_list
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
            args = [(self.input_gdb, layer_name) for layer_name in stream_layers]

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
                    args.append((self.input_gdb, layer_name, zone_poly_cache))

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

        self.stats["total_features"] = total_features
        self.stats["unique_names"] = total_unique_names
        self.stats["zones"] = len(output_index)

        logger.info(f"Index Statistics:")
        logger.info(f"  Total features indexed: {total_features:,}")
        logger.info(f"  Unique names: {total_unique_names:,}")
        logger.info(f"  Zones covered: {len(output_index)}")

        logger.info(f"Writing index to: {self.output_json}")
        self.output_json.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_json, "w", encoding="utf-8") as f:
            # Write without indentation to reduce file size
            json.dump(output_index, f, ensure_ascii=False, separators=(",", ":"))

        file_size_mb = self.output_json.stat().st_size / 1024 / 1024
        logger.info(f"Index saved successfully ({file_size_mb:.1f} MB)")

        return self.output_json
