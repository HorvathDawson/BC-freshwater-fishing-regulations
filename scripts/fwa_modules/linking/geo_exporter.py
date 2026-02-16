"""
RegulationGeoExporter - Creates geographic exports from regulation mapping results
"""

import logging
import subprocess
import json
import hashlib
import pickle
from pathlib import Path
from typing import Dict, Optional, Any, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import fiona
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import MultiLineString, MultiPolygon

from .regulation_mapper import RegulationMapper
from .metadata_gazetteer import FeatureType

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Fallback for tqdm if not installed
try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, **kwargs):
        return iterable


WEIGHTS = {
    "order": 0.0,
    "magnitude": 1.0,
    "length_km": 0.0,
    "has_name": 0.0,
    "side_channel_penalty": 0.0,
}

PERCENTILES = {
    5: 100.0,
    6: 99.99,
    7: 99.9,
    8: 95.0,
    10: 0.0,
}

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

MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}


class RegulationGeoExporter:
    """Creates geographic exports from regulation mapping pipeline results."""

    def __init__(
        self,
        mapper: RegulationMapper,
        streams_gdb_path: Path,
        polygons_gdb_path: Path,
        cache_dir: Optional[Path] = None,
    ):
        # Access data directly from mapper
        self.mapper = mapper
        self.merged_groups = mapper.merged_groups
        self.feature_to_regs = mapper.feature_to_regs
        self.stats = mapper.stats
        self.regulation_names = mapper.regulation_names
        self.feature_to_linked_regulation = mapper.feature_to_linked_regulation
        self.streams_gdb = streams_gdb_path
        self.polygons_gdb = polygons_gdb_path
        self.gazetteer = mapper.gazetteer

        # Set up cache directory
        self.cache_dir = cache_dir or Path(".geom_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Loaded {len(self.merged_groups)} merged groups, {len(self.feature_to_regs)} individual features"
        )

        # Data and Caches
        self._stream_geometries = None
        self._polygon_geometries = None
        self._layer_cache = (
            {}
        )  # Format: {(layer_type, merge_geometries, include_all): gdf}

        # Requirements built from mapping
        self._needed_stream_ids = set()
        self._needed_blue_line_keys = set()
        self._needed_polygon_ids = set()
        self._valid_stream_ids = self.gazetteer.get_valid_stream_ids()

        self._build_feature_requirements()
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()

    def _get_regulation_name(self, regulation_ids: List[str]) -> List[str]:
        """Get all unique regulation names from regulation IDs.

        Args:
            regulation_ids: List of regulation IDs (e.g., ['reg_0001_rule0', 'reg_0001_rule1', 'reg_0002_rule0'])

        Returns:
            List of unique regulation names (verbatim), empty list if none found
        """
        if not regulation_ids:
            return []

        # Extract unique base regulation IDs (strip _ruleX suffix)
        base_reg_ids = set()
        for reg_id in regulation_ids:
            base_reg_id = reg_id.rsplit("_rule", 1)[0] if "_rule" in reg_id else reg_id
            base_reg_ids.add(base_reg_id)

        # Collect all unique names
        names = []
        for base_reg_id in sorted(base_reg_ids):
            if name := self.regulation_names.get(base_reg_id):
                names.append(name)

        return names

    def _get_regulation_name_for_feature(
        self, feature_id: str, regulation_ids: List[str]
    ) -> List[str]:
        """Get all regulation names only for regulations explicitly linked by name (not tributaries).

        Args:
            feature_id: The feature ID to check
            regulation_ids: List of regulation IDs for this feature

        Returns:
            List of regulation names (verbatim) that were explicitly linked, empty list if none
        """
        if not regulation_ids:
            return []

        # Extract unique base regulation IDs (strip _ruleX suffix)
        base_reg_ids = set()
        for reg_id in regulation_ids:
            base_reg_id = reg_id.rsplit("_rule", 1)[0] if "_rule" in reg_id else reg_id
            base_reg_ids.add(base_reg_id)

        # Collect names only for explicitly linked regulations
        names = []
        linked_reg = self.feature_to_linked_regulation.get(feature_id)
        for base_reg_id in sorted(base_reg_ids):
            if linked_reg == base_reg_id:
                if name := self.regulation_names.get(base_reg_id):
                    names.append(name)

        return names

    def _get_regulation_name_for_group(
        self, feature_ids: List[str], regulation_ids: List[str]
    ) -> List[str]:
        """Get all regulation names only if ANY feature in group was explicitly linked by name.

        Args:
            feature_ids: List of feature IDs in the group
            regulation_ids: List of regulation IDs for this group

        Returns:
            List of regulation names (verbatim) where any feature was explicitly linked, empty list if none
        """
        if not regulation_ids:
            return []

        # Extract unique base regulation IDs (strip _ruleX suffix)
        base_reg_ids = set()
        for reg_id in regulation_ids:
            base_reg_id = reg_id.rsplit("_rule", 1)[0] if "_rule" in reg_id else reg_id
            base_reg_ids.add(base_reg_id)

        # Collect names only for regulations where ANY feature was explicitly linked
        names = []
        for base_reg_id in sorted(base_reg_ids):
            for feature_id in feature_ids:
                if self.feature_to_linked_regulation.get(feature_id) == base_reg_id:
                    if name := self.regulation_names.get(base_reg_id):
                        names.append(name)
                        break  # Found a linked feature for this regulation, move to next reg

        return names

    def _build_feature_requirements(self):
        """Extracts required feature IDs and keys from merged groups."""
        polygon_types = {FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE}

        for group in self.merged_groups.values():
            for feature_id in group.feature_ids:
                feature_type_enum = self.gazetteer.get_feature_type_from_id(feature_id)
                key = self.gazetteer.get_feature_key_from_id(feature_id)

                if feature_type_enum in polygon_types:
                    self._needed_polygon_ids.add((feature_type_enum.value, key))
                elif feature_type_enum == FeatureType.STREAM:
                    self._needed_stream_ids.add(key)
                    meta = self.gazetteer.get_stream_metadata(key)
                    if meta and meta.get("blue_line_key"):
                        self._needed_blue_line_keys.add(meta["blue_line_key"])

        logger.info(
            f"Need {len(self._needed_stream_ids):,} streams, "
            f"{len(self._needed_blue_line_keys):,} BLKs, {len(self._needed_polygon_ids):,} polygons"
        )

    # --- CACHING HELPERS ---

    def _get_gdb_mtime(self, gdb_path: Path) -> float:
        """Gets the most recent modification time of a GDB file/directory."""
        if gdb_path.is_file():
            return gdb_path.stat().st_mtime
        if gdb_path.is_dir():
            # Check all files inside the .gdb folder and get the newest
            mtimes = [f.stat().st_mtime for f in gdb_path.rglob("*") if f.is_file()]
            return max(mtimes) if mtimes else 0.0
        return 0.0

    def _generate_cache_hash(self, gdb_path: Path, ids_set: set, prefix: str) -> Path:
        """Generates a unique cache file path based on GDB state and requested IDs."""
        mtime = self._get_gdb_mtime(gdb_path)
        # Sort IDs so the hash remains consistent for the exact same set of requirements
        ids_string = ",".join(sorted(str(i) for i in ids_set))
        hash_payload = f"{gdb_path}_{mtime}_{ids_string}".encode("utf-8")

        md5_hash = hashlib.md5(hash_payload).hexdigest()
        return self.cache_dir / f"{prefix}_{md5_hash}.pkl"

    def _preload_data(self):
        """Ensures all geometries are read into memory exactly once."""
        logger.info("--- PRE-LOADING GEOMETRY DATA ---")
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()
        logger.info("--- DATA PRE-LOAD COMPLETE ---")

    def _read_gdb_layer_fast(self, gdb_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        """Reads GDB layers quickly with pyogrio fallback to fiona."""
        try:
            gdf = gpd.read_file(
                gdb_path, layer=layer_name, engine="pyogrio", use_arrow=True
            )
        except Exception as e:
            logger.debug(f"Pyogrio failed, falling back to fiona: {e}")
            gdf = gpd.read_file(gdb_path, layer=layer_name)

        if not gdf.empty:
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

    # --- GEOMETRY LOADING ---

    def _load_layer_geometries(self, layer_name: str) -> Dict[str, Any]:
        """Load geometries for a single stream layer."""
        gdf = self._read_gdb_layer_fast(self.streams_gdb, layer_name)
        if gdf.empty or "LINEAR_FEATURE_ID" not in gdf.columns:
            return {}

        gdf["LINEAR_FEATURE_ID"] = (
            gdf["LINEAR_FEATURE_ID"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        mask = gdf["LINEAR_FEATURE_ID"].isin(self._needed_stream_ids) & gdf[
            "LINEAR_FEATURE_ID"
        ].isin(self._valid_stream_ids)

        geometries = {}
        for _, row in gdf[mask].iterrows():
            linear_id = row["LINEAR_FEATURE_ID"]
            geometries[linear_id] = row.geometry
        return geometries

    def _load_all_stream_geometries(self):
        if self._stream_geometries is not None:
            return

        # Cache Check
        cache_file = self._generate_cache_hash(
            self.streams_gdb,
            self._needed_stream_ids | self._valid_stream_ids,
            "streams",
        )

        if cache_file.exists():
            logger.info("⚡ FAST RELOAD: Loading stream geometries from cache...")
            with open(cache_file, "rb") as f:
                self._stream_geometries = pickle.load(f)
            return

        logger.info("Loading stream geometries into memory (this may take a while)...")
        self._stream_geometries = {}
        layers = fiona.listlayers(str(self.streams_gdb))

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self._load_layer_geometries, layer): layer
                for layer in layers
            }
            for future in tqdm(
                as_completed(futures), total=len(layers), desc="Loading Stream Layers"
            ):
                try:
                    self._stream_geometries.update(future.result())
                except Exception as e:
                    logger.warning(f"Error loading layer {futures[future]}: {e}")

        # Save Cache
        logger.info("Saving streams to cache for fast reloading...")
        with open(cache_file, "wb") as f:
            pickle.dump(self._stream_geometries, f)

    def _load_all_polygon_geometries(self):
        if self._polygon_geometries is not None:
            return

        # Cache Check
        cache_file = self._generate_cache_hash(
            self.polygons_gdb, {str(p) for p in self._needed_polygon_ids}, "polygons"
        )

        if cache_file.exists():
            logger.info("⚡ FAST RELOAD: Loading polygon geometries from cache...")
            with open(cache_file, "rb") as f:
                self._polygon_geometries = pickle.load(f)
            return

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
            if gdf.empty or "WATERBODY_POLY_ID" not in gdf.columns:
                continue

            gdf["WATERBODY_POLY_ID"] = pd.to_numeric(
                gdf["WATERBODY_POLY_ID"], errors="coerce"
            )
            gdf = gdf.dropna(subset=["WATERBODY_POLY_ID"])
            gdf["WATERBODY_POLY_ID"] = gdf["WATERBODY_POLY_ID"].astype(int).astype(str)

            needed_keys = self._get_clean_poly_keys(feature_type)
            filtered_gdf = gdf[gdf["WATERBODY_POLY_ID"].isin(needed_keys)]

            logger.info(f"  -> {layer_name}: Found {len(filtered_gdf)} polygons.")

            for _, row in filtered_gdf.iterrows():
                poly_id = row["WATERBODY_POLY_ID"]
                feature_id = f"{feature_type.upper()}_{poly_id}"
                self._polygon_geometries[feature_id] = row.geometry

        # Save Cache
        logger.info("Saving polygons to cache for fast reloading...")
        with open(cache_file, "wb") as f:
            pickle.dump(self._polygon_geometries, f)

    def _get_clean_poly_keys(self, feature_type: str) -> set:
        keys = set()
        for ftype, k in self._needed_polygon_ids:
            if ftype.rstrip("s") == feature_type:
                try:
                    keys.add(str(int(float(k))))
                except (ValueError, TypeError):
                    keys.add(str(k).strip())
        return keys

    # --- LAYER CREATION ---

    def _create_streams_layer(
        self,
        merge_geometries: bool,
        include_all: bool,
        exclude_lake_streams: bool = False,
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_stream_geometries()

        cache_key = ("streams", merge_geometries, include_all, exclude_lake_streams)
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = []
        if include_all or not merge_geometries:
            for linear_id, geom in self._stream_geometries.items():
                reg_ids = self.feature_to_regs.get(linear_id, [])
                if not include_all and not reg_ids:
                    continue

                meta = self.gazetteer.get_stream_metadata(linear_id)
                magnitude = meta.get("stream_magnitude", 0) if meta else 0
                features.append(
                    {
                        "linear_feature_id": linear_id,
                        "gnis_name": meta.get("gnis_name", "") if meta else "",
                        "stream_order": meta.get("stream_order") or 0 if meta else 0,
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_names": " | ".join(
                            self._get_regulation_name_for_feature(linear_id, reg_ids)
                        )
                        or "",
                        "tippecanoe:minzoom": self._calculate_stream_minzoom(
                            magnitude=magnitude
                        ),
                        "geometry": geom,
                    }
                )
        else:
            blk_zooms = self._get_synchronized_blk_zooms()
            for group in self.merged_groups.values():
                if group.feature_type not in ("stream", None):
                    continue

                # Exclude streams linked to lakes (have waterbody_key) if flag is set
                if exclude_lake_streams and group.waterbody_key:
                    continue

                geom_list, all_zones, all_mgmt_units, ws_codes = [], set(), set(), set()
                max_order = 0
                blk = None

                for fid in group.feature_ids:
                    if geom := self._stream_geometries.get(fid):
                        meta = self.gazetteer.get_stream_metadata(fid)
                        if not meta:
                            continue
                        geom_list.extend(self._extract_geoms(geom))
                        ws_codes.add(meta.get("fwa_watershed_code", ""))
                        all_zones.update(meta.get("zones", []))
                        all_mgmt_units.update(meta.get("mgmt_units", []))
                        max_order = max(max_order, meta.get("stream_order") or 0)
                        if blk is None:
                            blk = meta.get("blue_line_key")

                if geom_list:
                    features.append(
                        {
                            "group_id": group.group_id,
                            "gnis_name": group.gnis_name or "",
                            "waterbody_key": group.waterbody_key or "",
                            "blue_line_key": blk or "",
                            "watershed_code": ", ".join(sorted(filter(None, ws_codes))),
                            "stream_order": max_order,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "regulation_names": " | ".join(
                                self._get_regulation_name_for_group(
                                    list(group.feature_ids), list(group.regulation_ids)
                                )
                            )
                            or "",
                            "has_regulations": bool(group.regulation_ids),
                            "zones": ",".join(sorted(all_zones)),
                            "mgmt_units": ",".join(sorted(all_mgmt_units)),
                            "tippecanoe:minzoom": blk_zooms.get(blk, 12),
                            "geometry": (
                                MultiLineString(geom_list)
                                if len(geom_list) > 1
                                else geom_list[0]
                            ),
                        }
                    )

        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_polygon_layer(
        self, feature_type: str, merge_geometries: bool, include_all: bool
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_polygon_geometries()
        singular_type = feature_type.rstrip("s")

        cache_key = (f"poly_{feature_type}", merge_geometries, include_all)
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = []
        if include_all:
            for feature_id, geom in self._polygon_geometries.items():
                ftype = self.gazetteer.get_feature_type_from_id(feature_id)
                if ftype.value != singular_type:
                    continue

                key = self.gazetteer.get_feature_key_from_id(feature_id)
                # Use numeric key to look up regulations, not prefixed feature_id
                reg_ids = self.feature_to_regs.get(key, [])
                meta = (
                    self.gazetteer.get_polygon_metadata(key, f"{singular_type}s") or {}
                )
                area_sqm = meta.get("area_sqm", 0)
                # Use waterbody_key from metadata (matches streams), fallback to polygon ID
                waterbody_key = meta.get("waterbody_key", key)
                features.append(
                    {
                        "waterbody_key": waterbody_key,
                        "gnis_name": meta.get("gnis_name", ""),
                        "area_sqm": area_sqm,
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_count": len(reg_ids),
                        "regulation_names": " | ".join(
                            self._get_regulation_name_for_feature(key, reg_ids)
                        )
                        or "",
                        "tippecanoe:minzoom": self._calculate_polygon_minzoom(area_sqm),
                        "geometry": geom,
                    }
                )
        else:
            prefix = singular_type.upper() + "_"
            for group in self.merged_groups.values():
                if group.feature_type != singular_type and not any(
                    fid.startswith(prefix) for fid in group.feature_ids
                ):
                    continue

                geom_and_meta = []
                for fid in group.feature_ids:
                    # Construct prefixed feature_id for geometry lookup
                    feature_id = f"{singular_type.upper()}_{fid}"
                    if geom := self._polygon_geometries.get(feature_id):
                        key = self.gazetteer.get_feature_key_from_id(feature_id)
                        meta = (
                            self.gazetteer.get_polygon_metadata(
                                key, f"{singular_type}s"
                            )
                            or {}
                        )
                        geom_and_meta.append((geom, meta, key))

                if not geom_and_meta:
                    continue

                if merge_geometries and len(geom_and_meta) > 1:
                    polygons = [
                        p
                        for geom, _, _ in geom_and_meta
                        for p in self._extract_geoms(geom)
                    ]
                    max_area = max(
                        meta.get("area_sqm", 0) for _, meta, _ in geom_and_meta
                    )
                    features.append(
                        {
                            "group_id": group.group_id,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "regulation_names": " | ".join(
                                self._get_regulation_name_for_group(
                                    list(group.feature_ids), list(group.regulation_ids)
                                )
                            )
                            or "",
                            "gnis_name": group.gnis_name or "",
                            "feature_count": group.feature_count,
                            "zones": ",".join(group.zones) if group.zones else "",
                            "mgmt_units": (
                                ",".join(group.mgmt_units) if group.mgmt_units else ""
                            ),
                            "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                max_area
                            ),
                            "geometry": MultiPolygon(polygons),
                        }
                    )
                else:
                    for geom, meta, key in geom_and_meta:
                        area_sqm = meta.get("area_sqm", 0)
                        # Use waterbody_key from metadata (matches streams), fallback to polygon ID
                        waterbody_key = meta.get("waterbody_key", key)
                        features.append(
                            {
                                "waterbody_key": waterbody_key,
                                "gnis_name": meta.get("gnis_name", ""),
                                "area_sqm": area_sqm,
                                "regulation_ids": ",".join(group.regulation_ids),
                                "regulation_count": len(group.regulation_ids),
                                "regulation_names": " | ".join(
                                    self._get_regulation_name_for_feature(
                                        key, list(group.regulation_ids)
                                    )
                                )
                                or "",
                                "zones": ",".join(group.zones) if group.zones else "",
                                "mgmt_units": (
                                    ",".join(group.mgmt_units)
                                    if group.mgmt_units
                                    else ""
                                ),
                                "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                    area_sqm
                                ),
                                "geometry": geom,
                            }
                        )

        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_regions_layer(self, zones_path: Path) -> Optional[gpd.GeoDataFrame]:
        logger.info("Dissolving zones into regions...")
        zones_gdf = gpd.read_file(zones_path).to_crs("EPSG:3005")
        zones_gdf["zone"] = zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        regions_gdf = zones_gdf.dissolve(by="zone", as_index=False)
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

    # --- MATH & SCORING ---

    def _calculate_score(
        self,
        max_order=0,
        magnitude=0,
        length_km=0.0,
        has_name=False,
        is_side_channel=False,
    ) -> float:
        base_score = (
            (max_order * WEIGHTS["order"])
            + (magnitude * WEIGHTS["magnitude"])
            + (int(has_name) * WEIGHTS["has_name"])
            + (int(is_side_channel) * WEIGHTS["side_channel_penalty"])
        )
        return base_score + min(length_km / 1000.0, 1.0)

    def _calculate_percentile_thresholds(self) -> dict:
        logger.info(
            "Calculating percentile-based zoom thresholds from Blue Line Key groups..."
        )
        blk_stats = defaultdict(
            lambda: {
                "len": 0,
                "max_order": 0,
                "max_magnitude": 0,
                "has_name": False,
                "is_side_channel": False,
            }
        )

        for linear_id in self.gazetteer.get_valid_stream_ids():
            if meta := self.gazetteer.get_stream_metadata(linear_id):
                if blk := meta.get("blue_line_key"):
                    s = blk_stats[blk]
                    s["len"] += meta.get("length", 0) or 0
                    s["max_order"] = max(s["max_order"], meta.get("stream_order") or 0)
                    s["max_magnitude"] = max(
                        s["max_magnitude"], meta.get("stream_magnitude") or 0
                    )
                    if meta.get("gnis_name"):
                        s["has_name"] = True
                    if (
                        meta.get("edge_type") not in MAIN_FLOW_CODES
                        and meta.get("edge_type") is not None
                    ):
                        s["is_side_channel"] = True

        scores = np.array(
            [
                self._calculate_score(
                    s["max_order"],
                    s["max_magnitude"],
                    s["len"] / 1000.0,
                    s["has_name"],
                    s["is_side_channel"],
                )
                for s in blk_stats.values()
            ]
        )

        thresholds = [
            (np.percentile(scores, PERCENTILES[z]), z)
            for z in sorted(PERCENTILES.keys())
        ]
        return thresholds

    def _get_synchronized_blk_zooms(self) -> dict:
        blk_stats = defaultdict(
            lambda: {
                "len": 0,
                "max_order": 0,
                "max_magnitude": 0,
                "has_name": False,
                "is_side_channel": False,
            }
        )
        for linear_id in self._stream_geometries.keys():
            meta = self.gazetteer.get_stream_metadata(linear_id)
            if not meta:
                continue
            if blk := meta.get("blue_line_key"):
                s = blk_stats[blk]
                s["len"] += meta.get("length") or 0
                s["max_order"] = max(s["max_order"], meta.get("stream_order") or 0)
                s["max_magnitude"] = max(
                    s["max_magnitude"], meta.get("stream_magnitude") or 0
                )
                if meta.get("gnis_name"):
                    s["has_name"] = True
                if (
                    meta.get("edge_type") not in MAIN_FLOW_CODES
                    and meta.get("edge_type") is not None
                ):
                    s["is_side_channel"] = True

        return {
            blk: self._calculate_stream_minzoom(magnitude=v["max_magnitude"])
            for blk, v in blk_stats.items()
        }

    def _calculate_stream_minzoom(self, magnitude=0) -> int:
        score = magnitude or 0
        for threshold, zoom in self._stream_zoom_thresholds:
            if score >= threshold:
                return zoom
        return 12

    def _calculate_polygon_minzoom(self, area_sqm: float) -> int:
        for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items()):
            if area_sqm >= limit:
                return zoom + 1
        return 12

    def _extract_geoms(self, geom) -> list:
        return geom.geoms if hasattr(geom, "geoms") else [geom]

    # --- EXPORTERS ---

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

        self._preload_data()
        layer_count = 0

        layers = [
            (
                "lakes",
                lambda: self._create_polygon_layer(
                    "lakes", merge_geometries, include_all_features
                ),
            ),
            (
                "wetlands",
                lambda: self._create_polygon_layer(
                    "wetlands", merge_geometries, include_all_features
                ),
            ),
            (
                "manmade",
                lambda: self._create_polygon_layer(
                    "manmade", merge_geometries, include_all_features
                ),
            ),
            (
                "streams",
                lambda: self._create_streams_layer(
                    merge_geometries, include_all_features
                ),
            ),
        ]

        if zones_path and zones_path.exists():
            layers.append(("regions", lambda: self._create_regions_layer(zones_path)))

        for layer_name, create_fn in layers:
            if (gdf := create_fn()) is not None:
                gdf.to_file(output_path, layer=layer_name, driver="GPKG")
                layer_count += 1

        if not layer_count:
            logger.error("No layers created!")
            return None

        logger.info(
            f"Created GPKG {output_path} ({output_path.stat().st_size / (1024 * 1024):.1f} MB)"
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
        self._preload_data()

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
            (
                "streams",
                lambda: self._create_streams_layer(
                    merge_geometries, False, exclude_lake_streams=True
                ),
            ),
        ]
        if zones_path and zones_path.exists():
            layer_configs.append(
                ("regions", lambda: self._create_regions_layer(zones_path))
            )

        layer_files = []
        for layer_name, create_fn in layer_configs:
            if (gdf := create_fn()) is not None and not gdf.empty:
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
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            "--no-clipping",
            "--detect-shared-borders",
        ] + [arg for lp in layer_files for arg in ("-L", f"{lp.stem}:{lp}")]

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
        with open(output_path, "w") as f:
            for _, row in gdf.to_crs("EPSG:4326").iterrows():
                # Extract properties, keeping regulation_names even if empty
                properties = {}
                for k, v in row.drop("geometry").items():
                    if k == "regulation_names":
                        # Always include regulation_names, even if None/empty
                        properties[k] = v if pd.notna(v) else ""
                    elif pd.notna(v):
                        properties[k] = v

                f.write(
                    json.dumps(
                        {
                            "type": "Feature",
                            "properties": properties,
                            "geometry": row["geometry"].__geo_interface__,
                            "tippecanoe": {
                                "layer": layer_name,
                                "minzoom": int(row["tippecanoe:minzoom"]),
                            },
                        }
                    )
                    + "\n"
                )

    def _is_file_locked(self, filepath: Path) -> bool:
        if filepath.exists():
            try:
                with open(filepath, "a"):
                    pass
            except PermissionError:
                logger.error(f"File {filepath.name} is locked. Skipping export.")
                return True
        return False

    def export_regulations_json(
        self,
        parsed_regulations: List[Dict[str, Any]],
        output_path: Path,
    ) -> Path:
        """
        Export regulations lookup table for frontend consumption.

        Creates a JSON file mapping rule_id -> regulation details for display in the UI.
        Designed to be served as a static file from Cloudflare Pages.

        Args:
            parsed_regulations: List of ParsedWaterbody objects (as dicts)
            output_path: Path to write regulations.json

        Returns:
            Path to created JSON file
        """
        logger.info(f"Exporting regulations lookup table...")

        regulations_lookup = {}
        total_rules = 0

        for idx, regulation in enumerate(parsed_regulations):
            regulation_id = f"reg_{idx:04d}"

            # Extract regulation metadata
            identity = regulation.get("identity", {})
            rules = regulation.get("rules", [])
            region = regulation.get("region")
            mgmt_units = regulation.get("mu", [])

            # Process each rule
            for rule_idx, rule in enumerate(rules):
                rule_id = f"{regulation_id}_rule{rule_idx}"
                total_rules += 1

                # Extract rule data
                restriction = rule.get("restriction", {})
                scope = rule.get("scope", {})

                # Build frontend-ready object
                regulations_lookup[rule_id] = {
                    # Identity
                    "waterbody_name": identity.get("name_verbatim"),
                    "waterbody_key": identity.get("waterbody_key"),
                    "region": region,
                    "management_units": mgmt_units,
                    # Rule text (verbatim from PDF)
                    "rule_text": rule.get("rule_text_verbatim"),
                    # Restriction details
                    "restriction_type": restriction.get("type"),
                    "restriction_details": restriction.get("details"),
                    "dates": restriction.get("dates"),
                    # Scope (where the rule applies)
                    "scope_type": scope.get("type"),
                    "scope_location": scope.get("location_verbatim"),
                    "includes_tributaries": scope.get("includes_tributaries"),
                }

        # Write to JSON
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(regulations_lookup, f, indent=2, ensure_ascii=False)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Exported {total_rules} rules from {len(parsed_regulations)} regulations "
            f"to {output_path} ({file_size_mb:.2f} MB)"
        )

        return output_path

    def export_search_index(
        self,
        output_path: Path,
    ) -> Path:
        """
        Export search index for frontend consumption.

        Creates a JSON file with waterbody names, bounding boxes, and metadata
        for fuzzy search functionality. Much smaller and faster than querying tiles.

        Only includes waterbodies with gnis_name (not unnamed tributaries).
        Groups by (gnis_name, regulation_ids, feature_type) to prevent duplicates.
        Returns regulation_names as an array for frontend handling.

        Args:
            output_path: Path to write search_index.json

        Returns:
            Path to created JSON file
        """
        logger.info("Exporting search index...")

        self._preload_data()

        # First pass: collect all groups with same name+regulations+type
        search_groups = defaultdict(
            lambda: {
                "geometries": [],
                "zones": set(),
                "mgmt_units": set(),
                "segment_count": 0,
                "waterbody_keys": set(),
                "group_ids": [],
                "feature_ids": [],  # Track all feature_ids to check for explicit linking
            }
        )

        for group in self.merged_groups.values():
            # Check if this group has a regulation name (explicitly linked)
            regulation_names = self._get_regulation_name_for_group(
                list(group.feature_ids), list(group.regulation_ids)
            )

            # Only include waterbodies with a GNIS name OR regulation name (skip unnamed tributaries)
            if not group.gnis_name and not regulation_names:
                continue

            feature_type = group.feature_type
            if not feature_type:
                # Determine type from first feature ID
                if group.feature_ids:
                    first_id = next(iter(group.feature_ids))
                    feature_type_enum = self.gazetteer.get_feature_type_from_id(
                        first_id
                    )
                    feature_type = (
                        feature_type_enum.value if feature_type_enum else "stream"
                    )
                else:
                    continue

            # Create grouping key: (gnis_name, regulation_ids, feature_type)
            # This ensures all segments of "Skeena River" with same regulations are one search result
            reg_ids_tuple = tuple(sorted(group.regulation_ids))
            search_key = (group.gnis_name, reg_ids_tuple, feature_type)

            search_group = search_groups[search_key]

            # Collect geometries for bbox calculation
            if feature_type == "stream":
                for fid in group.feature_ids:
                    if geom := self._stream_geometries.get(fid):
                        search_group["geometries"].extend(self._extract_geoms(geom))
            else:
                prefix = feature_type.upper() + "_"
                for fid in group.feature_ids:
                    feature_id = f"{prefix}{fid}"
                    if geom := self._polygon_geometries.get(feature_id):
                        search_group["geometries"].extend(self._extract_geoms(geom))

            # Accumulate metadata
            search_group["segment_count"] += len(group.feature_ids)
            search_group["feature_ids"].extend(group.feature_ids)
            if group.zones:
                search_group["zones"].update(group.zones)
            if group.mgmt_units:
                search_group["mgmt_units"].update(group.mgmt_units)
            if group.waterbody_key:
                search_group["waterbody_keys"].add(group.waterbody_key)
            search_group["group_ids"].append(group.group_id)

        # Second pass: create search items with combined bboxes
        search_items = []
        for (
            gnis_name,
            reg_ids_tuple,
            feature_type,
        ), group_data in search_groups.items():
            geometries = group_data["geometries"]
            if not geometries:
                continue

            # Calculate min_zoom based on feature type
            min_zoom = 12  # default
            if feature_type == "stream":
                # For streams: use maximum magnitude across all feature_ids
                max_magnitude = 0
                for fid in group_data["feature_ids"]:
                    meta = self.gazetteer.get_stream_metadata(fid)
                    if meta:
                        max_magnitude = max(
                            max_magnitude, meta.get("stream_magnitude") or 0
                        )
                min_zoom = self._calculate_stream_minzoom(magnitude=max_magnitude)
            else:
                # For polygons: calculate total area
                total_area = sum(geom.area for geom in geometries)
                min_zoom = self._calculate_polygon_minzoom(total_area)

            # Calculate bounding box from all geometries (in EPSG:3005)
            min_x, min_y, max_x, max_y = (
                float("inf"),
                float("inf"),
                float("-inf"),
                float("-inf"),
            )
            for geom in geometries:
                bounds = geom.bounds  # (minx, miny, maxx, maxy)
                min_x = min(min_x, bounds[0])
                min_y = min(min_y, bounds[1])
                max_x = max(max_x, bounds[2])
                max_y = max(max_y, bounds[3])

            # Transform bbox from EPSG:3005 (BC Albers) to EPSG:4326 (WGS84 lat/lon)
            from shapely.geometry import box

            bbox_geom = box(min_x, min_y, max_x, max_y)
            bbox_gdf = gpd.GeoDataFrame([1], geometry=[bbox_geom], crs="EPSG:3005")
            bbox_gdf_wgs84 = bbox_gdf.to_crs("EPSG:4326")
            wgs84_bounds = bbox_gdf_wgs84.geometry.iloc[0].bounds
            bbox = [wgs84_bounds[0], wgs84_bounds[1], wgs84_bounds[2], wgs84_bounds[3]]

            # Build search item
            reg_ids = list(reg_ids_tuple)

            # Get all regulation names as an array (only for explicitly linked features)
            regulation_names = self._get_regulation_name_for_group(
                group_data["feature_ids"], reg_ids
            )

            search_item = {
                "id": f"{gnis_name}|{','.join(reg_ids)}|{feature_type}",
                "gnis_name": gnis_name,
                "regulation_names": regulation_names,  # Array of regulation names
                "type": feature_type,
                "zones": (
                    ",".join(sorted(group_data["zones"])) if group_data["zones"] else ""
                ),
                "mgmt_units": (
                    ",".join(sorted(group_data["mgmt_units"]))
                    if group_data["mgmt_units"]
                    else ""
                ),
                "regulation_ids": ",".join(reg_ids),
                "segment_count": group_data["segment_count"],
                "bbox": bbox,
                "min_zoom": min_zoom,
                "properties": {
                    "group_id": (
                        group_data["group_ids"][0] if group_data["group_ids"] else ""
                    ),
                    "waterbody_key": (
                        ",".join(sorted(group_data["waterbody_keys"]))
                        if group_data["waterbody_keys"]
                        else ""
                    ),
                    "regulation_count": len(reg_ids),
                },
            }

            search_items.append(search_item)

        # Write to JSON
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"waterbodies": search_items}, f, indent=2, ensure_ascii=False)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Exported {len(search_items)} waterbodies to {output_path} ({file_size_mb:.2f} MB)"
        )

        return output_path
