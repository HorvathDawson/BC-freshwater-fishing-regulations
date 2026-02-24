"""
RegulationGeoExporter - Creates geographic exports from regulation mapping results
"""

import json
import logging
import hashlib
import pickle
import subprocess
from pathlib import Path
from typing import Dict, Optional, Any, List, Tuple, Callable
from collections import defaultdict

from data.data_extractor import FWADataAccessor
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiLineString, MultiPolygon, box

from .regulation_mapper import PipelineResult, generate_rule_id
from .provincial_base_regulations import PROVINCIAL_BASE_REGULATIONS
from fwa_pipeline.metadata_gazetteer import FeatureType
from fwa_pipeline.metadata_builder import ADMIN_LAYER_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, **kwargs):
        return iterable


# --- CONSTANTS ---
WEIGHTS = {
    "order": 0.0,
    "magnitude": 1.0,
    "length_km": 0.0,
    "has_name": 0.0,
    "side_channel_penalty": 0.0,
}
PERCENTILES = {5: 100.0, 6: 99.99, 7: 99.97, 8: 95.0, 10: 0.0}
LAKE_ZOOM_THRESHOLDS = {
    4: 100_000_000,
    5: 25_000_000,
    6: 1_000_000,
    # 7: 1_000_000,
    8: 250_000,
    9: 50_000,
    10: 10_000,
    11: 0,
}
MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}

# Mapping of GeoPackage layer names to FeatureType enums
POLYGON_LAYERS = {
    "lakes": FeatureType.LAKE,
    "wetlands": FeatureType.WETLAND,
    "manmade_water": FeatureType.MANMADE,
}


class RegulationGeoExporter:
    """Creates geographic exports from regulation mapping pipeline results."""

    def __init__(
        self,
        pipeline_result: PipelineResult,
        gpkg_path: Path,
        cache_dir: Path,
    ):
        self.pipeline_result = pipeline_result
        self.merged_groups = pipeline_result.merged_groups
        self.feature_to_regs = pipeline_result.feature_to_regs
        self.regulation_names = pipeline_result.regulation_names
        self.feature_to_linked_regulation = pipeline_result.feature_to_linked_regulation
        self.gazetteer = pipeline_result.gazetteer
        self.admin_area_reg_map = pipeline_result.admin_area_reg_map
        self.gpkg_path = gpkg_path
        self.data_accessor = FWADataAccessor(self.gpkg_path)
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._stream_geometries = None
        self._polygon_geometries = None
        self._layer_cache = {}
        self._valid_stream_ids = self.gazetteer.get_valid_stream_ids()
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()
        logger.info(
            f"Loaded {len(self.merged_groups)} merged groups, {len(self.feature_to_regs)} individual features"
        )

    # --- LOOKUPS & METADATA ---

    def _get_reg_names(
        self, reg_ids: List[str], feature_ids: Optional[List[str]] = None
    ) -> List[str]:
        if not reg_ids:
            return []

        base_ids = {r.rsplit("_rule", 1)[0] for r in reg_ids}
        if feature_ids is not None:
            linked_regs = {
                r
                for fid in feature_ids
                for r in self.feature_to_linked_regulation.get(fid, set())
            }
            base_ids &= linked_regs

        return [
            self.regulation_names[b]
            for b in sorted(base_ids)
            if b in self.regulation_names
        ]

    def _get_group_gnis_name(
        self, feature_ids: tuple[str, ...], ftype: FeatureType
    ) -> str:
        """Safely extract the gnis_name from the first available feature in a group."""
        for fid in feature_ids:
            if ftype == FeatureType.STREAM:
                meta = self.gazetteer.get_stream_metadata(fid)
            else:
                meta = self.gazetteer.get_polygon_metadata(fid, ftype)
            if meta and meta.get("gnis_name"):
                return meta.get("gnis_name")
        return ""

    # --- CACHING & I/O ---

    def _get_gdb_mtime(self, gdb_path: Path) -> float:
        if gdb_path.is_file():
            return gdb_path.stat().st_mtime
        if gdb_path.is_dir():
            return max(
                (f.stat().st_mtime for f in gdb_path.rglob("*") if f.is_file()),
                default=0.0,
            )
        return 0.0

    def _with_cache(self, gdb_path: Path, ids_set: set, prefix: str, load_fn: Callable):
        mtime = self._get_gdb_mtime(gdb_path)
        ids_str = ",".join(sorted(map(str, ids_set)))
        cache_hash = hashlib.md5(
            f"{gdb_path}_{mtime}_{ids_str}".encode("utf-8")
        ).hexdigest()
        cache_file = self.cache_dir / f"{prefix}_{cache_hash}.pkl"

        if cache_file.exists():
            logger.info(f"⚡ FAST RELOAD: Loading {prefix} from cache...")
            return pickle.loads(cache_file.read_bytes())

        logger.info(f"Loading {prefix} geometries into memory...")
        data = load_fn()
        cache_file.write_bytes(pickle.dumps(data))
        return data

    def _preload_data(self):
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()

    def _load_all_stream_geometries(self):
        if self._stream_geometries is not None:
            return

        def _load():
            geoms = {}
            gdf = self.data_accessor.get_layer("streams")
            # LINEAR_FEATURE_ID is already cleaned by FWADataAccessor
            mask = gdf["LINEAR_FEATURE_ID"].isin(self._valid_stream_ids)
            geoms.update(
                {
                    row["LINEAR_FEATURE_ID"]: row.geometry
                    for _, row in gdf[mask].iterrows()
                }
            )
            return geoms

        self._stream_geometries = self._with_cache(
            self.gpkg_path,
            self._valid_stream_ids,
            "streams",
            _load,
        )

    def _load_all_polygon_geometries(self):
        if self._polygon_geometries is not None:
            return

        # Gather all valid polygon IDs across all types
        valid_poly_ids = set()
        for ftype_enum in POLYGON_LAYERS.values():
            if ftype_enum in self.gazetteer.metadata:
                valid_poly_ids.update(
                    str(k) for k in self.gazetteer.metadata[ftype_enum].keys()
                )

        def _load():
            geoms = {}
            for layer_name, ftype_enum in tqdm(POLYGON_LAYERS.items(), desc="Polygons"):
                gdf = self.data_accessor.get_layer(layer_name)
                if gdf.empty or "WATERBODY_POLY_ID" not in gdf.columns:
                    continue

                # WATERBODY_POLY_ID already cleaned by FWADataAccessor (string, 0->"")
                # Get the specific valid IDs for this polygon type
                valid_ids_for_type = set()
                if ftype_enum in self.gazetteer.metadata:
                    valid_ids_for_type = {
                        str(k) for k in self.gazetteer.metadata[ftype_enum].keys()
                    }

                # Filter to only keep polygons that exist in our gazetteer
                mask = gdf["WATERBODY_POLY_ID"].isin(valid_ids_for_type)

                for _, row in gdf[mask].iterrows():
                    geoms[f"{ftype_enum.value.upper()}_{row['WATERBODY_POLY_ID']}"] = (
                        row.geometry
                    )
            return geoms

        self._polygon_geometries = self._with_cache(
            self.gpkg_path,
            valid_poly_ids,
            "polygons",
            _load,
        )

    # --- MATH & SCORING ---

    def _compute_blk_stats(self, keys_iterable) -> dict:
        stats = defaultdict(
            lambda: {
                "len": 0,
                "max_order": 0,
                "max_magnitude": 0,
                "has_name": False,
                "is_side_channel": False,
            }
        )
        for fid in keys_iterable:
            if not (meta := self.gazetteer.get_stream_metadata(fid)) or not (
                blk := meta.get("blue_line_key")
            ):
                continue
            s = stats[blk]
            s["len"] += meta.get("length", 0) or 0
            s["max_order"] = max(s["max_order"], meta.get("stream_order") or 0)
            s["max_magnitude"] = max(
                s["max_magnitude"], meta.get("stream_magnitude") or 0
            )
            s["has_name"] = s["has_name"] or bool(meta.get("gnis_name"))
            s["is_side_channel"] = s["is_side_channel"] or (
                meta.get("edge_type") not in MAIN_FLOW_CODES
                and meta.get("edge_type") is not None
            )
        return dict(stats)

    def _calculate_score(
        self,
        max_order=0,
        magnitude=0,
        length_km=0.0,
        has_name=False,
        is_side_channel=False,
    ) -> float:
        base = (
            (max_order * WEIGHTS["order"])
            + (magnitude * WEIGHTS["magnitude"])
            + (int(has_name) * WEIGHTS["has_name"])
            + (int(is_side_channel) * WEIGHTS["side_channel_penalty"])
        )
        return base + min(length_km / 1000.0, 1.0)

    def _calculate_percentile_thresholds(self) -> list:
        stats = self._compute_blk_stats(self.gazetteer.get_valid_stream_ids())
        scores = np.array(
            [
                self._calculate_score(
                    s["max_order"],
                    s["max_magnitude"],
                    s["len"] / 1000.0,
                    s["has_name"],
                    s["is_side_channel"],
                )
                for s in stats.values()
            ]
        )
        return [
            (np.percentile(scores, PERCENTILES[z]), z)
            for z in sorted(PERCENTILES.keys())
        ]

    def _get_synchronized_blk_zooms(self) -> dict:
        return {
            blk: self._calculate_stream_minzoom(v["max_magnitude"])
            for blk, v in self._compute_blk_stats(
                self._stream_geometries.keys()
            ).items()
        }

    def _calculate_stream_minzoom(self, magnitude=0) -> int:
        return next(
            (
                zoom
                for threshold, zoom in self._stream_zoom_thresholds
                if (magnitude or 0) >= threshold
            ),
            12,
        )

    def _calculate_polygon_minzoom(self, area_sqm: float) -> int:
        return next(
            (
                zoom + 1
                for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items())
                if area_sqm >= limit
            ),
            12,
        )

    def _extract_geoms(self, geom) -> list:
        return geom.geoms if hasattr(geom, "geoms") else [geom]

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
                meta = self.gazetteer.get_stream_metadata(linear_id) or {}
                features.append(
                    {
                        "linear_feature_id": linear_id,
                        "gnis_name": meta.get("gnis_name", ""),
                        "stream_order": meta.get("stream_order", 0),
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_names": " | ".join(
                            self._get_reg_names(reg_ids, [linear_id])
                        ),
                        "tippecanoe:minzoom": self._calculate_stream_minzoom(
                            meta.get("stream_magnitude", 0)
                        ),
                        "geometry": geom,
                    }
                )
        else:
            blk_zooms = self._get_synchronized_blk_zooms()
            for group in self.merged_groups.values():
                # Determine feature type of the group based on its first feature
                first_fid = next(iter(group.feature_ids), None)
                if not first_fid:
                    continue

                group_ftype = self.gazetteer.get_feature_type_from_id(first_fid)

                if group_ftype != FeatureType.STREAM or (
                    exclude_lake_streams and group.waterbody_key
                ):
                    continue

                geom_list, all_zones, all_mgmt_units, ws_codes = [], set(), set(), set()
                max_order, blk = 0, None

                # if "707538068" in group.feature_ids:
                #     logger.warning(
                #         f"Expected segment with fwa_id 707538068 found in mapped features."
                #     )

                blks = set()
                for fid in group.feature_ids:
                    meta = self.gazetteer.get_stream_metadata(fid)
                    geom = self._stream_geometries.get(fid)

                    # if "707538068" == fid:
                    #     logger.warning(
                    #         f"Expected segment with fwa_id 707538068 found in group with group_id {group.group_id}. meta and geom are {'present' if meta else 'missing'} and {'present' if geom else 'missing'}, respectively."
                    #     )
                    # Always capture the blue line key from metadata when available
                    if meta and meta.get("blue_line_key"):
                        blks.add(meta.get("blue_line_key"))

                    # Only extend geometries and collect other per-segment props when geometry exists
                    if geom and meta:
                        geom_list.extend(self._extract_geoms(geom))
                        ws_codes.add(meta.get("fwa_watershed_code", ""))
                        all_zones.update(meta.get("zones", []))
                        all_mgmt_units.update(meta.get("mgmt_units", []))
                        max_order = max(max_order, meta.get("stream_order") or 0)

                if len(blks) == 0:
                    logger.warning(f"No blue line key found in group {group.group_id}")
                    blk = None
                elif len(blks) == 1:
                    blk = blks.pop()
                else:
                    # Multiple BLKs are expected for cross-stream merged groups
                    # (e.g. unnamed streams sharing the same regulation set inside
                    # an admin area). Geometry is still collected per-segment above;
                    # we leave the tile field empty so the frontend does not bind
                    # this group to any single stream.
                    logger.debug(
                        f"Group {group.group_id} spans {len(blks)} blue line keys "
                        f"(cross-stream merge) — blue_line_key field left empty"
                    )
                    blk = None

                # if "707538068" in group.feature_ids:
                #     logger.warning(
                #         f"Expected segment with fwa_id 707538068 found in mapped features."
                #     )
                #     exit(0)

                if geom_list:
                    features.append(
                        {
                            "group_id": group.group_id,
                            "gnis_name": self._get_group_gnis_name(
                                group.feature_ids, FeatureType.STREAM
                            ),
                            "waterbody_key": (
                                group.waterbody_key
                                if group.waterbody_key is not None
                                else ""
                            ),
                            "blue_line_key": blk if blk is not None else "",
                            "watershed_code": ", ".join(sorted(filter(None, ws_codes))),
                            "stream_order": max_order,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "regulation_names": " | ".join(
                                self._get_reg_names(
                                    list(group.regulation_ids), list(group.feature_ids)
                                )
                            ),
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
        self, ftype_enum: FeatureType, merge_geometries: bool, include_all: bool
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_polygon_geometries()
        cache_key = (f"poly_{ftype_enum.value}", merge_geometries, include_all)
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = []
        prefix = f"{ftype_enum.value.upper()}_"

        if include_all:
            for fid, geom in self._polygon_geometries.items():
                # Extract the actual ID by removing the prefix
                clean_fid = fid.replace(prefix, "")
                if self.gazetteer.get_feature_type_from_id(clean_fid) != ftype_enum:
                    continue

                reg_ids = self.feature_to_regs.get(clean_fid, [])
                meta = self.gazetteer.get_polygon_metadata(clean_fid, ftype_enum) or {}

                features.append(
                    {
                        "waterbody_key": meta.get("waterbody_key", clean_fid) or "",
                        "gnis_name": meta.get("gnis_name", "") or "",
                        "area_sqm": meta.get("area_sqm", 0),
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_count": len(reg_ids),
                        "regulation_names": " | ".join(
                            self._get_reg_names(reg_ids, [clean_fid])
                        ),
                        "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                            meta.get("area_sqm", 0)
                        ),
                        "geometry": geom,
                    }
                )
        else:
            for group in self.merged_groups.values():
                first_fid = next(iter(group.feature_ids), None)
                if (
                    not first_fid
                    or self.gazetteer.get_feature_type_from_id(first_fid) != ftype_enum
                ):
                    continue

                geom_meta = [
                    (
                        g,
                        self.gazetteer.get_polygon_metadata(fid, ftype_enum) or {},
                        fid,
                    )
                    for fid in group.feature_ids
                    if (g := self._polygon_geometries.get(f"{prefix}{fid}"))
                ]
                if not geom_meta:
                    continue

                gnis_name = self._get_group_gnis_name(group.feature_ids, ftype_enum)

                if merge_geometries and len(geom_meta) > 1:
                    max_area = max(m.get("area_sqm", 0) for _, m, _ in geom_meta)
                    features.append(
                        {
                            "group_id": group.group_id,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "regulation_names": " | ".join(
                                self._get_reg_names(
                                    list(group.regulation_ids), list(group.feature_ids)
                                )
                            ),
                            "gnis_name": gnis_name,
                            "feature_count": group.feature_count,
                            "zones": ",".join(group.zones),
                            "mgmt_units": ",".join(group.mgmt_units),
                            "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                max_area
                            ),
                            "geometry": MultiPolygon(
                                [
                                    p
                                    for g, _, _ in geom_meta
                                    for p in self._extract_geoms(g)
                                ]
                            ),
                        }
                    )
                else:
                    for geom, meta, key in geom_meta:
                        features.append(
                            {
                                "waterbody_key": meta.get("waterbody_key", key) or "",
                                "gnis_name": meta.get("gnis_name", "") or "",
                                "area_sqm": meta.get("area_sqm", 0),
                                "regulation_ids": ",".join(group.regulation_ids),
                                "regulation_count": len(group.regulation_ids),
                                "regulation_names": " | ".join(
                                    self._get_reg_names(
                                        list(group.regulation_ids), [key]
                                    )
                                ),
                                "zones": ",".join(group.zones),
                                "mgmt_units": ",".join(group.mgmt_units),
                                "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                    meta.get("area_sqm", 0)
                                ),
                                "geometry": geom,
                            }
                        )

        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_admin_layer(self, layer_key: str) -> Optional[gpd.GeoDataFrame]:
        """
        Create a layer for an admin boundary type (matched features only).

        Loads the GPKG admin layer, filters to features that have at least one
        regulation match in admin_area_reg_map, and attaches regulation_ids +
        key attributes for frontend rendering.

        Args:
            layer_key: Admin layer key (e.g., 'parks_bc', 'parks_nat', 'wma', etc.)

        Returns:
            GeoDataFrame with matched admin polygons, or None.
        """
        cfg = ADMIN_LAYER_CONFIG.get(layer_key)
        if not cfg:
            return None

        matched_ids_map = self.admin_area_reg_map.get(layer_key, {})
        if not matched_ids_map:
            logger.debug(f"  No matched admin features for '{layer_key}', skipping")
            return None

        if layer_key not in self.data_accessor.list_layers():
            logger.warning(f"  Admin layer '{layer_key}' not in GPKG, skipping")
            return None

        id_field = cfg["id_field"]
        name_field = cfg.get("name_field")
        code_field = cfg.get("code_field")
        code_map = cfg.get("code_map", {})

        # Load admin layer and filter to only features with regulation matches
        gdf = self.data_accessor.get_layer(layer_key).to_crs("EPSG:3005")
        matched_ids = set(matched_ids_map.keys())
        gdf = gdf[gdf[id_field].isin(matched_ids)].copy()
        if gdf.empty:
            logger.debug(f"  No geometry matches for '{layer_key}' after ID filter")
            return None

        # Attach regulation IDs
        gdf["regulation_ids"] = gdf[id_field].apply(
            lambda fid: ",".join(sorted(matched_ids_map.get(fid, set())))
        )

        # Readable name
        if name_field and name_field in gdf.columns:
            gdf["name"] = gdf[name_field]
        else:
            gdf["name"] = ""

        # Admin feature ID (normalized string)
        gdf["admin_id"] = gdf[id_field]

        # Build output column list
        out_cols = ["admin_id", "name", "regulation_ids"]

        # admin_type — use code_map for layers that have classification codes
        # (e.g. parks_bc → PROVINCIAL_PARK / ECOLOGICAL_RESERVE / …),
        # otherwise default to the layer_key so every admin feature carries
        # a type the frontend can key off for colouring.
        if code_field and code_field in gdf.columns:
            gdf["admin_type"] = gdf[code_field].map(code_map).fillna(gdf[code_field])
        else:
            gdf["admin_type"] = layer_key
        out_cols.append("admin_type")

        # Tippecanoe zoom
        gdf["tippecanoe:minzoom"] = 0
        out_cols.extend(["tippecanoe:minzoom", "geometry"])

        logger.info(f"  Admin layer '{layer_key}': {len(gdf)} features")
        return gdf[out_cols]

    def _create_regions_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build region boundary layer from the 'wmu' layer in the main GPKG."""
        if "wmu" not in self.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping regions")
            return None
        zones_gdf = self.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        zones_gdf["zone"] = zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        regions_gdf = zones_gdf.dissolve(by="zone", as_index=False)
        regions_gdf["geometry"] = regions_gdf["geometry"].boundary

        regions_gdf["stroke_color"] = "#555555"
        regions_gdf["stroke_width"] = 2.5
        regions_gdf["stroke_dasharray"] = "3,3"
        regions_gdf["tippecanoe:minzoom"] = 0

        return regions_gdf[
            [
                "zone",
                "stroke_color",
                "stroke_width",
                "stroke_dasharray",
                "tippecanoe:minzoom",
                "geometry",
            ]
        ]

    # --- SHARED LAYER CONFIG ---

    def _get_layer_configs(
        self,
        merge: bool,
        include_all: bool,
        exclude_lake_streams: bool = False,
        include_regions: bool = True,
    ) -> List[Tuple[str, Callable]]:
        layers = [
            (
                "lakes",
                lambda: self._create_polygon_layer(
                    FeatureType.LAKE, merge, include_all
                ),
            ),
            (
                "wetlands",
                lambda: self._create_polygon_layer(
                    FeatureType.WETLAND, merge, include_all
                ),
            ),
            (
                "manmade",
                lambda: self._create_polygon_layer(
                    FeatureType.MANMADE, merge, include_all
                ),
            ),
            (
                "streams",
                lambda: self._create_streams_layer(
                    merge, include_all, exclude_lake_streams=exclude_lake_streams
                ),
            ),
        ]
        if include_regions:
            layers.append(("regions", lambda: self._create_regions_layer()))

        # Admin boundary layers — always include all configured admin layers
        for layer_key in ADMIN_LAYER_CONFIG:
            layers.append(
                (
                    f"admin_{layer_key}",
                    lambda lk=layer_key: self._create_admin_layer(lk),
                )
            )

        return layers

    def _is_file_locked(self, filepath: Path) -> bool:
        if not filepath.exists():
            return False
        try:
            with open(filepath, "a"):
                pass
            return False
        except PermissionError:
            logger.error(f"File {filepath.name} is locked. Skipping export.")
            return True

    # --- EXPORTERS ---

    def export_gpkg(
        self,
        output_path: Path,
        merge_geometries: bool = True,
        include_all_features: bool = False,
    ) -> Path:
        if self._is_file_locked(output_path):
            return None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()

        self._preload_data()
        layer_count = 0

        for name, create_fn in self._get_layer_configs(
            merge_geometries,
            include_all_features,
            exclude_lake_streams=False,
        ):
            if (gdf := create_fn()) is not None and not gdf.empty:
                gdf.to_file(output_path, layer=name, driver="GPKG")
                layer_count += 1

        if not layer_count:
            return None
        logger.info(
            f"Created GPKG {output_path} ({output_path.stat().st_size / 1048576:.1f} MB)"
        )
        return output_path

    def export_pmtiles(
        self,
        output_path: Path,
        merge_geometries: bool = True,
        work_dir: Optional[Path] = None,
    ) -> Path:
        if self._is_file_locked(output_path):
            return None
        work_dir = work_dir or output_path.parent / "temp"
        work_dir.mkdir(parents=True, exist_ok=True)
        self._preload_data()

        layer_files = []
        for name, create_fn in self._get_layer_configs(merge_geometries, False, True):
            if (gdf := create_fn()) is not None and not gdf.empty:
                layer_path = work_dir / f"{name}.geojsonseq"
                with open(layer_path, "w") as f:
                    for _, row in gdf.to_crs("EPSG:4326").iterrows():
                        props = {
                            k: ("" if pd.isna(v) and k == "regulation_names" else v)
                            for k, v in row.drop("geometry").items()
                            if pd.notna(v) or k == "regulation_names"
                        }
                        record = {
                            "type": "Feature",
                            "properties": props,
                            "geometry": row["geometry"].__geo_interface__,
                            "tippecanoe": {
                                "layer": name,
                                "minzoom": int(row["tippecanoe:minzoom"]),
                            },
                        }
                        f.write(json.dumps(record) + "\n")
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

        if (
            result := subprocess.run(cmd, text=True)
        ).returncode == 0 and output_path.exists():
            logger.info(
                f"Created PMTiles {output_path} ({output_path.stat().st_size / 1048576:.1f} MB)"
            )
            return output_path

        logger.error(f"Tippecanoe failed: {result.returncode}")
        return None

    def export_regulations_json(
        self, parsed_regulations: List[Dict[str, Any]], output_path: Path
    ) -> Path:
        reg_lookup = {}
        for idx, reg in enumerate(parsed_regulations):
            ident, rules = reg.get("identity", {}), reg.get("rules", [])
            for r_idx, rule in enumerate(rules):
                rest, scope = rule.get("restriction", {}), rule.get("scope", {})
                rule_id = generate_rule_id(idx, r_idx)
                reg_lookup[rule_id] = {
                    "waterbody_name": ident.get("name_verbatim"),
                    "waterbody_key": ident.get("waterbody_key"),
                    "region": reg.get("region"),
                    "management_units": reg.get("mu", []),
                    "rule_text": rule.get("rule_text_verbatim"),
                    "restriction_type": rest.get("type"),
                    "restriction_details": rest.get("details"),
                    "dates": rest.get("dates"),
                    "scope_type": scope.get("type"),
                    "scope_location": scope.get("location_verbatim"),
                    "includes_tributaries": scope.get("includes_tributaries"),
                    "source": "synopsis",
                }

        # Add provincial base regulations
        provincial_map = self.pipeline_result.provincial_feature_map or {}
        if provincial_map:
            for prov_reg in PROVINCIAL_BASE_REGULATIONS:
                if prov_reg.regulation_id in provincial_map:
                    reg_lookup[prov_reg.regulation_id] = {
                        "waterbody_name": prov_reg.regulation_id.replace(
                            "_", " "
                        ).title(),
                        "waterbody_key": None,
                        "region": None,
                        "management_units": [],
                        "rule_text": prov_reg.rule_text,
                        "restriction_type": (
                            prov_reg.restriction.get("type")
                            if prov_reg.restriction
                            else None
                        ),
                        "restriction_details": (
                            prov_reg.restriction.get("details")
                            if prov_reg.restriction
                            else None
                        ),
                        "dates": (
                            prov_reg.restriction.get("dates")
                            if prov_reg.restriction
                            else None
                        ),
                        "scope_type": prov_reg.scope_type,
                        "scope_location": prov_reg.admin_layer,
                        "includes_tributaries": None,
                        "source": "provincial",
                    }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(reg_lookup, f, indent=2, ensure_ascii=False)
        return output_path

    def export_search_index(self, output_path: Path) -> Path:
        self._preload_data()
        search_groups = defaultdict(
            lambda: {
                "geoms": [],
                "zones": set(),
                "mgmt_units": set(),
                "segment_count": 0,
                "wb_keys": set(),
                "group_ids": [],
                "feature_ids": [],
            }
        )

        for group in self.merged_groups.values():
            reg_names = self._get_reg_names(
                list(group.regulation_ids), list(group.feature_ids)
            )

            # Determine feature type of the group based on its first feature
            first_fid = next(iter(group.feature_ids), None)
            if not first_fid:
                continue

            ftype = self.gazetteer.get_feature_type_from_id(first_fid)
            gnis_name = self._get_group_gnis_name(group.feature_ids, ftype)

            if not gnis_name and not reg_names:
                continue

            sg = search_groups[
                (gnis_name, tuple(sorted(group.regulation_ids)), ftype.value)
            ]

            prefix = f"{ftype.value.upper()}_" if ftype != FeatureType.STREAM else ""
            geoms_dict = (
                self._stream_geometries
                if ftype == FeatureType.STREAM
                else self._polygon_geometries
            )

            for fid in group.feature_ids:
                if geom := geoms_dict.get(f"{prefix}{fid}" if prefix else fid):
                    sg["geoms"].extend(self._extract_geoms(geom))

            sg["segment_count"] += len(group.feature_ids)
            sg["feature_ids"].extend(group.feature_ids)
            sg["zones"].update(group.zones or [])
            sg["mgmt_units"].update(group.mgmt_units or [])
            if group.waterbody_key:
                sg["wb_keys"].add(group.waterbody_key)
            sg["group_ids"].append(group.group_id)

        search_items = []
        for (gnis, reg_ids_tuple, ftype_val), data in search_groups.items():
            if not data["geoms"]:
                continue
            reg_ids = list(reg_ids_tuple)

            if ftype_val == FeatureType.STREAM.value:
                mag = max(
                    (
                        (m.get("stream_magnitude") or 0)
                        for fid in data["feature_ids"]
                        if (m := self.gazetteer.get_stream_metadata(fid))
                    ),
                    default=0,
                )
                min_zoom = self._calculate_stream_minzoom(mag)
            else:
                min_zoom = self._calculate_polygon_minzoom(
                    sum(g.area for g in data["geoms"])
                )

            # Optimized Bound calculation using numpy
            bounds = np.array([g.bounds for g in data["geoms"]])
            min_x, min_y, max_x, max_y = (
                bounds[:, 0].min(),
                bounds[:, 1].min(),
                bounds[:, 2].max(),
                bounds[:, 3].max(),
            )
            wgs84_bounds = (
                gpd.GeoSeries([box(min_x, min_y, max_x, max_y)], crs="EPSG:3005")
                .to_crs("EPSG:4326")
                .iloc[0]
                .bounds
            )

            search_items.append(
                {
                    "id": f"{gnis}|{','.join(reg_ids)}|{ftype_val}",
                    "gnis_name": gnis,
                    "regulation_names": self._get_reg_names(
                        reg_ids, data["feature_ids"]
                    ),
                    "type": ftype_val,
                    "zones": ",".join(sorted(data["zones"])),
                    "mgmt_units": ",".join(sorted(data["mgmt_units"])),
                    "regulation_ids": ",".join(reg_ids),
                    "segment_count": data["segment_count"],
                    "bbox": list(wgs84_bounds),
                    "min_zoom": min_zoom,
                    "properties": {
                        "group_id": data["group_ids"][0] if data["group_ids"] else "",
                        "waterbody_key": ",".join(sorted(data["wb_keys"])),
                        "regulation_count": len(reg_ids),
                    },
                }
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"waterbodies": search_items}, f, indent=2, ensure_ascii=False)
        return output_path
