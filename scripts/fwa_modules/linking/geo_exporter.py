"""
RegulationGeoExporter - Creates geographic exports from regulation mapping results

Takes merged regulation groups and creates:
1. GeoPackage with merged/individual features + regulation attributes
2. PMTiles for web visualization
3. Regulation lookup indices (already done by RegulationMapper)

Geometry loading from source GDBs:
- Streams: FWA_STREAM_NETWORKS_SP.gdb (by LINEAR_FEATURE_ID)
- Polygons: FWA_BC.gdb (by WATERBODY_KEY)
  - FWA_LAKES_POLY
  - FWA_WETLANDS_POLY
  - FWA_MANMADE_WATERBODIES_POLY
"""

import logging
import subprocess
import json
from pathlib import Path
from typing import Dict, Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiLineString, MultiPolygon

from .regulation_mapper import PipelineResult, RegulationMapper
from .metadata_gazetteer import FeatureType

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Scoring weights for stream importance
# Original values: order=30.0, magnitude=0.0, length_km=1.0, has_name=70.0, side_channel_penalty=-100.0
# Current: Testing magnitude-only scoring
WEIGHTS = {
    "order": 0.0,  # Stream order importance (disabled for magnitude testing)
    "magnitude": 1.0,  # Stream magnitude (primary factor for testing)
    "length_km": 0.0,  # Length contribution (disabled for magnitude testing)
    "has_name": 0.0,  # Named streams (disabled for magnitude testing)
    "side_channel_penalty": 0.0,  # Penalty for secondary channels (disabled for magnitude testing)
}

# Percentile-based zoom assignment for streams
# Higher percentile = fewer streams (only the most important)
# Adjust these values to control how many streams appear at each zoom
# NO streams appear below zoom 6
PERCENTILES = {
    5: 100.0,  # Top 1
    6: 99.99,  # Top 0.01% of streams start at zoom 6
    7: 99.9,  # Top 0.1% at zoom 7
    8: 95.0,  # Top 5% at zoom 8
    10: 0.0,  # All remaining streams at zoom 10
}

# Lake/polygon zoom thresholds (area-based)
LAKE_ZOOM_THRESHOLDS = {
    4: 100_000_000,
    5: 25_000_000,
    6: 5_000_000,
    7: 1_000_000,
    8: 250_000,
    9: 50_000,
    10: 10_000,
    11: 0,
}

# Main flow edge type codes - features NOT in this set are side channels
MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}


class RegulationGeoExporter:
    """Creates geographic exports from regulation mapping pipeline results."""

    def __init__(
        self,
        mapper: RegulationMapper,
        pipeline_result: PipelineResult,
        streams_gdb_path: Path,
        polygons_gdb_path: Path,
    ):
        self.merged_groups = pipeline_result.merged_groups
        self.feature_to_regs = pipeline_result.feature_to_regs
        self.stats = pipeline_result.stats
        self.streams_gdb = streams_gdb_path
        self.polygons_gdb = polygons_gdb_path
        self.gazetteer = mapper.gazetteer

        logger.info(
            f"Loaded {len(self.merged_groups)} merged groups, {len(self.feature_to_regs)} individual features"
        )

        self._stream_geometries = None
        self._polygon_geometries = None

        # Cache for merged/individual layers to avoid recalculation
        self._cached_streams_merged = None
        self._cached_streams_individual = None

        self._needed_stream_ids = set()
        self._needed_blue_line_keys = set()
        self._needed_polygon_ids = set()
        self._valid_stream_ids = self.gazetteer.get_valid_stream_ids()

        # Pre-calculate percentile-based zoom thresholds from all streams
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()

        # Build feature requirements
        for group in self.merged_groups.values():
            for feature_id in group.feature_ids:
                # Determine feature type using unified helper
                feature_type_enum = self.gazetteer.get_feature_type_from_id(feature_id)

                if feature_type_enum in [
                    FeatureType.LAKE,
                    FeatureType.WETLAND,
                    FeatureType.MANMADE,
                ]:
                    # Polygon feature - extract key
                    key = self.gazetteer.get_feature_key_from_id(feature_id)
                    self._needed_polygon_ids.add((feature_type_enum.value, key))
                elif feature_type_enum == FeatureType.STREAM:
                    # Stream feature (numeric linear_feature_id)
                    stream_id = self.gazetteer.get_feature_key_from_id(feature_id)
                    self._needed_stream_ids.add(stream_id)
                    # Get blue_line_key from metadata
                    meta = self.gazetteer.get_stream_metadata(stream_id)
                    if meta and meta.get("blue_line_key"):
                        self._needed_blue_line_keys.add(meta["blue_line_key"])

        logger.debug(
            f"Sample needed polygon IDs: {list(self._needed_polygon_ids)[:10]}"
        )

        logger.info(
            f"Need to load {len(self._needed_stream_ids):,} stream segments, "
            f"{len(self._needed_blue_line_keys):,} blue line keys, "
            f"{len(self._needed_polygon_ids):,} polygons"
        )

    def _preload_data(self):
        """Ensures all geometries are read into memory exactly once, enforcing the Polygon -> Stream order."""
        logger.info("--- PRE-LOADING GEOMETRY DATA ---")
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()
        logger.info("--- DATA PRE-LOAD COMPLETE ---")

    def _read_gdb_layer_fast(self, gdb_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        """Helper to read GDB layers quickly using pyogrio if available, fallback to fiona."""
        try:
            gdf = gpd.read_file(
                gdb_path, layer=layer_name, engine="pyogrio", use_arrow=True
            )
        except Exception as e:
            logger.debug(f"Pyogrio failed or not installed, falling back to fiona: {e}")
            gdf = gpd.read_file(gdb_path, layer=layer_name)

        if not gdf.empty:
            # Bulletproof attribute uppercasing to protect the geometry column
            geom_col = gdf.active_geometry_name or "geometry"

            gdf.columns = [
                (
                    str(col).upper()
                    if col != geom_col and str(col).lower() != "geometry"
                    else col
                )
                for col in gdf.columns
            ]

        return gdf

    def _load_layer_geometries(self, layer_name: str) -> Dict[str, dict]:
        """Load geometries from a single layer using strictly Gazetteer metadata."""
        gdf = self._read_gdb_layer_fast(self.streams_gdb, layer_name)
        if gdf.empty or "LINEAR_FEATURE_ID" not in gdf.columns:
            return {}

        gdf["LINEAR_FEATURE_ID"] = (
            gdf["LINEAR_FEATURE_ID"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

        # Vectorized Filtering: Only load what we need
        mask_id = gdf["LINEAR_FEATURE_ID"].isin(self._needed_stream_ids)
        mask_valid = gdf["LINEAR_FEATURE_ID"].isin(self._valid_stream_ids)
        filtered_gdf = gdf[mask_id & mask_valid]

        geometries = {}
        for _, row in filtered_gdf.iterrows():
            linear_id = row["LINEAR_FEATURE_ID"]
            meta = self.gazetteer.get_stream_metadata(linear_id)

            if not meta:
                logger.warning(
                    f"Feature {linear_id} skipped: Missing Gazetteer metadata."
                )
                continue

            geometries[linear_id] = {
                "geometry": row.geometry,
                "linear_feature_id": linear_id,
                "gnis_name": meta.get("gnis_name", ""),
                "watershed_code": meta.get("fwa_watershed_code", ""),
                "stream_order": meta.get("stream_order", 0),
                "stream_magnitude": meta.get("stream_magnitude", 0),
                "length": meta.get("length", 0),
                "blue_line_key": meta.get("blue_line_key"),
                "edge_type": meta.get("edge_type"),  # For side channel detection
                "zones": meta.get("zones", []),
                "mgmt_units": meta.get("mgmt_units", []),
                "crs": gdf.crs,
            }
        return geometries

    def _load_all_polygon_geometries(self):
        """Load polygon geometries using vectorized filtering with feedback."""
        if self._polygon_geometries is not None:
            return

        try:
            from tqdm import tqdm
        except ImportError:
            tqdm = lambda x, **kwargs: x

        logger.info("Loading polygon geometries into memory...")
        self._polygon_geometries = {}

        layers = {
            "FWA_LAKES_POLY": "lake",
            "FWA_WETLANDS_POLY": "wetland",
            "FWA_MANMADE_WATERBODIES_POLY": "manmade",
        }

        for layer_name, feature_type in tqdm(
            layers.items(), desc="Loading Polygon Layers"
        ):
            gdf = self._read_gdb_layer_fast(self.polygons_gdb, layer_name)

            if gdf.empty:
                continue

            # Use WATERBODY_POLY_ID for matching (not WATERBODY_KEY!)
            if "WATERBODY_POLY_ID" not in gdf.columns:
                logger.warning(
                    f"Skipping {layer_name}: WATERBODY_POLY_ID column missing. Found: {list(gdf.columns)}"
                )
                continue

            # BULLETPROOF CASTING #1: The GeoDataFrame
            # Force to numeric (converting garbage to NaN), drop NaNs, cast to strict integer, then string
            gdf["WATERBODY_POLY_ID"] = pd.to_numeric(
                gdf["WATERBODY_POLY_ID"], errors="coerce"
            )
            gdf = gdf.dropna(subset=["WATERBODY_POLY_ID"]).copy()
            gdf["WATERBODY_POLY_ID"] = gdf["WATERBODY_POLY_ID"].astype(int).astype(str)

            # BULLETPROOF CASTING #2: The Needed Keys
            # Clean our pipeline keys to match the exact same strict integer format
            needed_keys = set()
            for ftype, k in self._needed_polygon_ids:
                # Normalize both to singular for comparison (lakes → lake)
                if ftype.rstrip("s") == feature_type:
                    try:
                        # float() parses "1234.0", int() strips the decimal, str() finalizes it
                        needed_keys.add(str(int(float(k))))
                    except (ValueError, TypeError):
                        needed_keys.add(str(k).strip())

            # DEBUG: Show what we're looking for
            if needed_keys:
                logger.debug(
                    f"{layer_name} - Looking for {len(needed_keys)} keys, sample: {list(needed_keys)[:5]}"
                )
                logger.debug(
                    f"{layer_name} - GDB has {len(gdf)} features, sample WATERBODY_POLY_ID values: {gdf['WATERBODY_POLY_ID'].head(10).tolist()}"
                )

            filtered_gdf = gdf[gdf["WATERBODY_POLY_ID"].isin(needed_keys)]

            # Print feedback to confirm the filter worked
            logger.info(
                f"  -> {layer_name}: Found {len(filtered_gdf)} matching polygons out of {len(needed_keys)} needed."
            )

            for _, row in filtered_gdf.iterrows():
                poly_id = row["WATERBODY_POLY_ID"]

                # Get ALL attributes from gazetteer metadata
                meta = self.gazetteer.metadata.get(f"{feature_type}s", {}).get(
                    poly_id, {}
                )

                # Only use gazetteer metadata for attributes (no GDB fallback)
                waterbody_key = meta.get("waterbody_key", poly_id)
                gnis_name = meta.get("gnis_name", "")
                area_sqm = meta.get("area_sqm", 0)

                self._polygon_geometries[(feature_type, poly_id)] = {
                    "geometry": row.geometry,  # Geometry from GDB
                    "waterbody_key": waterbody_key,  # From gazetteer
                    "waterbody_poly_id": poly_id,
                    "gnis_name": gnis_name,  # From gazetteer
                    "area_sqm": area_sqm,  # From gazetteer
                    "zones": meta.get("zones", []),  # From gazetteer
                    "mgmt_units": meta.get("mgmt_units", []),  # From gazetteer
                    "crs": gdf.crs,  # GDB CRS metadata
                }

        logger.info(
            f"Loaded {len(self._polygon_geometries):,} total polygon geometries"
        )

    def _load_all_stream_geometries(self):
        """Load stream geometries in parallel with feedback."""
        if self._stream_geometries is not None:
            return

        try:
            from tqdm import tqdm
        except ImportError:
            tqdm = lambda x, **kwargs: x  # Dummy fallback if tqdm is missing

        logger.info("Loading stream geometries into memory...")
        self._stream_geometries = {}
        layers = fiona.listlayers(str(self.streams_gdb))

        # Reduced from 12 to 4. Too many threads causes disk thrashing on a single GDB.
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_layer = {
                executor.submit(self._load_layer_geometries, layer): layer
                for layer in layers
            }

            # Wrap the future iterator in a tqdm progress bar
            for future in tqdm(
                as_completed(future_to_layer),
                total=len(layers),
                desc="Loading Stream Layers",
            ):
                try:
                    self._stream_geometries.update(future.result())
                except Exception as e:
                    logger.warning(
                        f"Error loading layer {future_to_layer[future]}: {e}"
                    )

        logger.info(f"Loaded {len(self._stream_geometries):,} stream geometries")

    def _create_streams_layer(
        self, merge_geometries: bool, include_all: bool
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_stream_geometries()

        if (
            merge_geometries
            and not include_all
            and self._cached_streams_merged is not None
        ):
            return self._cached_streams_merged
        elif (
            not merge_geometries
            and not include_all
            and self._cached_streams_individual is not None
        ):
            return self._cached_streams_individual

        features = []

        if include_all or not merge_geometries:
            # Individual segments pass through
            for linear_id, geom_data in self._stream_geometries.items():
                reg_ids = self.feature_to_regs.get(linear_id, [])
                if not include_all and not reg_ids:
                    continue

                stream_order = geom_data.get("stream_order", 0) or 0
                stream_magnitude = geom_data.get("stream_magnitude", 0) or 0
                edge_type = geom_data.get("edge_type")
                is_side_channel = (
                    edge_type is not None and edge_type not in MAIN_FLOW_CODES
                )

                features.append(
                    {
                        "linear_feature_id": linear_id,
                        "gnis_name": geom_data.get("gnis_name", ""),
                        "stream_order": stream_order,
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "tippecanoe:minzoom": self._calculate_stream_minzoom(
                            max_order=stream_order,
                            magnitude=stream_magnitude,
                            total_length_km=(geom_data.get("length", 0) or 0) / 1000.0,
                            has_name=bool(geom_data.get("gnis_name")),
                            is_side_channel=is_side_channel,
                        ),
                        "geometry": geom_data["geometry"],
                    }
                )
        else:
            logger.info("Syncing zoom levels across Blue Line Keys...")

            # PASS 1: Aggregate stats globally for every Blue Line Key
            blk_stats = defaultdict(
                lambda: {
                    "len": 0,
                    "max_order": 0,
                    "max_magnitude": 0,
                    "has_name": False,
                    "is_side_channel": False,
                }
            )
            for gd in self._stream_geometries.values():
                blk = gd.get("blue_line_key")
                if not blk:
                    continue
                s = blk_stats[blk]
                s["len"] += gd.get("length", 0) or 0
                s["max_order"] = max(s["max_order"], gd.get("stream_order", 0) or 0)
                s["max_magnitude"] = max(
                    s["max_magnitude"], gd.get("stream_magnitude", 0) or 0
                )
                if gd.get("gnis_name"):
                    s["has_name"] = True
                edge_type = gd.get("edge_type")
                if edge_type is not None and edge_type not in MAIN_FLOW_CODES:
                    s["is_side_channel"] = True

            # PASS 2: Pre-calculate the synchronized zoom for each BLK
            blk_zooms = {
                blk: self._calculate_stream_minzoom(
                    max_order=v["max_order"],
                    magnitude=v["max_magnitude"],
                    total_length_km=v["len"] / 1000.0,
                    has_name=v["has_name"],
                    is_side_channel=v["is_side_channel"],
                )
                for blk, v in blk_stats.items()
            }

            # PASS 3: Map Merged Groups to these synced zooms
            for group in self.merged_groups.values():
                if group.feature_type not in ("stream", None):
                    continue

                geom_list, all_zones, all_mgmt_units, ws_codes = [], set(), set(), set()
                max_stream_order = 0

                # Identify the BLK for this group (Mapper now groups by BLK and feature_type)
                first_id = group.feature_ids[0]
                blk = self._stream_geometries.get(first_id, {}).get("blue_line_key")
                feature_minzoom = blk_zooms.get(blk, 12)

                for fid in group.feature_ids:
                    gd = self._stream_geometries.get(fid)
                    if not gd:
                        continue

                    geom = gd["geometry"]
                    geom_list.extend(
                        geom.geoms if geom.geom_type == "MultiLineString" else [geom]
                    )
                    if gd.get("watershed_code"):
                        ws_codes.add(gd["watershed_code"])
                    all_zones.update(gd.get("zones", []))
                    all_mgmt_units.update(gd.get("mgmt_units", []))
                    max_stream_order = max(
                        max_stream_order, gd.get("stream_order", 0) or 0
                    )

                if not geom_list:
                    continue

                features.append(
                    {
                        "group_id": group.group_id,
                        "gnis_name": group.gnis_name or "",
                        "blue_line_key": blk or "",
                        "watershed_code": ", ".join(sorted(ws_codes)),
                        "stream_order": max_stream_order,
                        "regulation_ids": ",".join(group.regulation_ids),
                        "regulation_count": len(group.regulation_ids),
                        "has_regulations": len(group.regulation_ids) > 0,
                        "zones": ",".join(sorted(all_zones)),
                        "mgmt_units": ",".join(sorted(all_mgmt_units)),
                        "tippecanoe:minzoom": feature_minzoom,
                        "geometry": (
                            MultiLineString(geom_list)
                            if len(geom_list) > 1
                            else geom_list[0]
                        ),
                    }
                )

        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None

        if result is not None:
            if merge_geometries and not include_all:
                self._cached_streams_merged = result
            elif not merge_geometries and not include_all:
                self._cached_streams_individual = result

        return result

    def _calculate_score(
        self,
        max_order: int = 0,
        magnitude: int = 0,
        total_length_km: float = 0.0,
        has_name: bool = False,
        is_side_channel: bool = False,
    ) -> float:
        """Calculate importance score for a stream segment with tie-breaking."""
        # Primary score from major factors
        base_score = (
            (max_order * WEIGHTS["order"])
            + (magnitude * WEIGHTS["magnitude"])
            + (int(has_name) * WEIGHTS["has_name"])
            + (int(is_side_channel) * WEIGHTS["side_channel_penalty"])
        )

        # Length as tie-breaker (normalized to 0-1 range, max ~1000km)
        # This ensures unique scores while keeping length contribution minimal
        length_tiebreaker = min(total_length_km / 1000.0, 1.0)

        return base_score + length_tiebreaker

    def _calculate_percentile_thresholds(self) -> dict:
        """Calculate zoom thresholds based on percentiles of merged Blue Line Key groups."""
        import numpy as np

        logger.info(
            "Calculating percentile-based zoom thresholds from Blue Line Key groups..."
        )

        # Aggregate streams by Blue Line Key (same logic as merged export)
        blk_stats = defaultdict(
            lambda: {
                "len": 0,
                "max_order": 0,
                "max_magnitude": 0,
                "has_name": False,
                "is_side_channel": False,
            }
        )

        valid_stream_ids = self.gazetteer.get_valid_stream_ids()

        for linear_id in valid_stream_ids:
            meta = self.gazetteer.get_stream_metadata(linear_id)
            if not meta:
                continue

            blk = meta.get("blue_line_key")
            if not blk:
                continue

            s = blk_stats[blk]
            s["len"] += meta.get("length", 0) or 0
            s["max_order"] = max(s["max_order"], meta.get("stream_order", 0) or 0)
            s["max_magnitude"] = max(
                s["max_magnitude"], meta.get("stream_magnitude", 0) or 0
            )
            if meta.get("gnis_name"):
                s["has_name"] = True
            edge_type = meta.get("edge_type")
            if edge_type is not None and edge_type not in MAIN_FLOW_CODES:
                s["is_side_channel"] = True

        logger.info(
            f"  Aggregated {len(valid_stream_ids):,} stream segments into {len(blk_stats):,} Blue Line Key groups"
        )

        # Calculate scores for each Blue Line Key group
        scores = []
        for blk, stats in blk_stats.items():
            score = self._calculate_score(
                max_order=stats["max_order"],
                magnitude=stats["max_magnitude"],
                total_length_km=stats["len"] / 1000.0,
                has_name=stats["has_name"],
                is_side_channel=stats["is_side_channel"],
            )
            scores.append(score)

        scores = np.array(scores)

        # Show score distribution histogram
        logger.info(f"\nScore distribution for {len(scores):,} Blue Line Key groups:")
        logger.info(
            f"  Min: {scores.min():.2f}, Max: {scores.max():.2f}, Mean: {scores.mean():.2f}, Median: {np.median(scores):.2f}"
        )

        # Create text-based histogram
        bins = 20
        hist, bin_edges = np.histogram(scores, bins=bins)
        max_count = hist.max()
        logger.info("\n  Score Histogram:")
        for i in range(bins):
            bar_length = int((hist[i] / max_count) * 50)
            bar = "█" * bar_length
            logger.info(
                f"  {bin_edges[i]:7.1f} - {bin_edges[i+1]:7.1f}: {hist[i]:8,} {bar}"
            )

        # Calculate threshold scores at each percentile
        logger.info("\n  Percentile Thresholds:")
        thresholds = []
        for zoom in sorted(PERCENTILES.keys()):
            threshold_score = np.percentile(scores, PERCENTILES[zoom])
            thresholds.append((threshold_score, zoom))
            logger.info(
                f"  Zoom {zoom}: Score >= {threshold_score:.2f} (top {100-PERCENTILES[zoom]:.2f}%)"
            )

        logger.info(f"\nCalculated {len(thresholds)} percentile-based thresholds")
        return thresholds

    def _calculate_stream_minzoom(
        self,
        max_order: int = 0,
        magnitude: int = 0,
        total_length_km: float = 0.0,
        has_name: bool = False,
        is_side_channel: bool = False,
        linear_id: str = None,
    ) -> int:
        if linear_id is not None:
            meta = self.gazetteer.get_stream_metadata(linear_id)
            if not meta:
                return 12
            max_order = meta.get("stream_order", 0) or 0
            magnitude = meta.get("stream_magnitude", 0) or 0
            total_length_km = (meta.get("length", 0) or 0) / 1000.0
            has_name = bool(meta.get("gnis_name"))
            edge_type = meta.get("edge_type")
            is_side_channel = edge_type is not None and edge_type not in MAIN_FLOW_CODES

        # Ensure all values are numeric (protect against None)
        max_order = max_order or 0
        magnitude = magnitude or 0
        total_length_km = total_length_km or 0.0

        score = self._calculate_score(
            max_order, magnitude, total_length_km, has_name, is_side_channel
        )

        # Use percentile-based thresholds calculated from all streams
        for threshold_score, zoom in self._stream_zoom_thresholds:
            if score >= threshold_score:
                return zoom
        return 12

    def _create_polygon_layer(
        self, feature_type: str, merge_geometries: bool, include_all: bool
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_polygon_geometries()  # Guaranteed cached hit
        singular_type = feature_type.rstrip("s")
        features = []

        if include_all:
            for (ftype, key), geom_data in self._polygon_geometries.items():
                if ftype != singular_type:
                    continue
                reg_ids = self.feature_to_regs.get(f"{ftype.upper()}_{key}", [])

                features.append(
                    {
                        "waterbody_key": key,
                        "gnis_name": geom_data.get("gnis_name", ""),
                        "area_sqm": geom_data.get("area_sqm", 0),
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_count": len(reg_ids),
                        "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                            geom_data.get("area_sqm", 0)
                        ),
                        "geometry": geom_data["geometry"],
                    }
                )
        else:
            prefix = singular_type.upper() + "_"
            for group in self.merged_groups.values():
                if group.feature_type != singular_type and not any(
                    fid.startswith(prefix) for fid in group.feature_ids
                ):
                    continue

                geometries = [
                    self._polygon_geometries.get(
                        (singular_type, self.gazetteer.get_feature_key_from_id(fid))
                    )
                    for fid in group.feature_ids
                ]
                geometries = [g for g in geometries if g]
                if not geometries:
                    continue

                if merge_geometries and len(geometries) > 1:
                    polygons = []
                    for g in geometries:
                        geom = g["geometry"]
                        polygons.extend(
                            geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
                        )

                    features.append(
                        {
                            "group_id": group.group_id,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "gnis_name": group.gnis_name or "",
                            "feature_count": group.feature_count,
                            "zones": ",".join(group.zones) if group.zones else "",
                            "mgmt_units": (
                                ",".join(group.mgmt_units) if group.mgmt_units else ""
                            ),
                            "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                max(g.get("area_sqm", 0) for g in geometries)
                            ),
                            "geometry": MultiPolygon(polygons),
                        }
                    )
                else:
                    for g in geometries:
                        features.append(
                            {
                                "waterbody_key": g["waterbody_key"],
                                "gnis_name": g.get("gnis_name", ""),
                                "area_sqm": g.get("area_sqm", 0),
                                "regulation_ids": ",".join(group.regulation_ids),
                                "regulation_count": len(group.regulation_ids),
                                "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                    g.get("area_sqm", 0)
                                ),
                                "geometry": g["geometry"],
                            }
                        )

        return gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None

    def _create_regions_layer(self, zones_path: Path) -> Optional[gpd.GeoDataFrame]:
        logger.info("Dissolving zones into regions...")
        zones_gdf = gpd.read_file(zones_path).to_crs("EPSG:3005")
        zones_gdf["zone"] = zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        regions_gdf = zones_gdf.dissolve(by="zone", as_index=False)

        # Extract only linestring bounds for outline
        regions_gdf["geometry"] = regions_gdf["geometry"].boundary

        zone_colors = {
            "1": "#FF6B6B",
            "2": "#4ECDC4",
            "3": "#45B7D1",
            "4": "#FFA07A",
            "5": "#98D8C8",
            "6": "#F7DC6F",
            "7": "#BB8FCE",
            "8": "#85C1E2",
        }

        regions_gdf["stroke_color"] = regions_gdf["zone"].map(zone_colors)
        regions_gdf["stroke_width"] = 3.0
        regions_gdf["tippecanoe:minzoom"] = 0

        return regions_gdf[
            ["zone", "stroke_color", "stroke_width", "tippecanoe:minzoom", "geometry"]
        ]

    def _calculate_polygon_minzoom(self, area_sqm: float) -> int:
        for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items()):
            if area_sqm >= limit:
                return zoom + 1
        return 12

    def export_gpkg(
        self,
        output_path: Path,
        merge_geometries: bool = True,
        include_all_features: bool = False,
        zones_path: Optional[Path] = None,
    ) -> Path:
        if self._is_file_locked(output_path):
            return None

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()

        # Enforce exactly one load sequence, starting with Polygons
        self._preload_data()

        layer_count = 0

        # Write Polygons first
        for feat_type in ["lakes", "wetlands", "manmade"]:
            poly_gdf = self._create_polygon_layer(
                feat_type, merge_geometries, include_all_features
            )
            if poly_gdf is not None:
                poly_gdf.to_file(output_path, layer=feat_type, driver="GPKG")
                layer_count += 1

        # Write Streams second
        streams_gdf = self._create_streams_layer(merge_geometries, include_all_features)
        if streams_gdf is not None:
            streams_gdf.to_file(output_path, layer="streams", driver="GPKG")
            layer_count += 1

        # Write Regions
        if zones_path and zones_path.exists():
            regions_gdf = self._create_regions_layer(zones_path)
            if regions_gdf is not None:
                regions_gdf.to_file(output_path, layer="regions", driver="GPKG")
                layer_count += 1

        if layer_count == 0:
            logger.error("No layers created!")
            return None

        logger.info(
            f"Created {output_path} ({output_path.stat().st_size / (1024 * 1024):.1f} MB)"
        )
        return output_path

    def export_pmtiles(
        self,
        output_path: Path,
        merge_geometries: bool = True,
        work_dir: Optional[Path] = None,
        zones_path: Optional[Path] = None,
    ) -> Path:
        if self._is_file_locked(output_path):
            return None

        work_dir = work_dir or output_path.parent / "temp"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Enforce exactly one load sequence, starting with Polygons
        self._preload_data()

        # Reordered configs: Polygons are processed before Streams
        layer_configs = [
            (
                "lakes",
                lambda: self._create_polygon_layer("lakes", merge_geometries, False),
            ),
            (
                "wetlands",
                lambda: self._create_polygon_layer("wetlands", merge_geometries, False),
            ),
            (
                "manmade",
                lambda: self._create_polygon_layer("manmade", merge_geometries, False),
            ),
            ("streams", lambda: self._create_streams_layer(merge_geometries, False)),
        ]

        # Add regions layer if zones_path provided
        if zones_path and zones_path.exists():
            layer_configs.append(
                ("regions", lambda: self._create_regions_layer(zones_path))
            )

        layer_files = []
        for layer_name, create_fn in layer_configs:
            gdf = create_fn()
            if gdf is not None and not gdf.empty:
                layer_path = work_dir / f"{layer_name}.geojsonseq"
                self._write_geojsonseq(gdf, layer_path, layer_name)
                layer_files.append(layer_path)

        if not layer_files:
            return None

        cmd = [
            "tippecanoe",
            "-o",
            str(output_path),
            "--force",
            "--hilbert",
            "--minimum-zoom=4",
            "--maximum-zoom=12",
            "--no-simplification-of-shared-nodes",
            "--no-tiny-polygon-reduction",
            "--simplification=8",
            # "--low-detail=10",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            # "--extend-zooms-if-still-dropping",
            # "--coalesce-densest-as-needed",
            # "--no-duplication",
            "--no-clipping",
            "--detect-shared-borders",
        ]

        for lp in layer_files:
            cmd.extend(["-L", f"{lp.stem}:{lp}"])

        logger.info(f"Running Tippecanoe: {' '.join(cmd[:10])}...")
        result = subprocess.run(cmd, text=True)
        if result.returncode == 0 and output_path.exists():
            logger.info(
                f"Created PMTiles {output_path} ({output_path.stat().st_size / (1024 * 1024):.1f} MB)"
            )
            return output_path

        logger.error(f"Tippecanoe failed with return code {result.returncode}")
        return None

    def _write_geojsonseq(
        self, gdf: gpd.GeoDataFrame, output_path: Path, layer_name: str
    ):
        gdf_4326 = gdf.to_crs("EPSG:4326")
        with open(output_path, "w") as f:
            for _, row in gdf_4326.iterrows():
                feat = {
                    "type": "Feature",
                    "properties": {
                        k: v for k, v in row.drop("geometry").items() if pd.notna(v)
                    },
                    "geometry": row["geometry"].__geo_interface__,
                    "tippecanoe": {
                        "layer": layer_name,
                        "minzoom": int(row["tippecanoe:minzoom"]),
                    },
                }
                f.write(json.dumps(feat) + "\n")

    def _is_file_locked(self, filepath: Path) -> bool:
        if filepath.exists():
            try:
                with open(filepath, "a"):
                    pass
            except PermissionError:
                logger.error(f"File {filepath.name} is locked. Skipping export.")
                return True
        return False
