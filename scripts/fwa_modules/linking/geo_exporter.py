"""
RegulationGeoExporter - Creates geographic exports from regulation mapping results
"""

import logging
import subprocess
import json
import hashlib
import pickle
from pathlib import Path
from typing import Dict, Optional, Any, List, Set, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import fiona
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import MultiLineString, MultiPolygon, box

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

PERCENTILES = {5: 100.0, 6: 99.99, 7: 99.9, 8: 95.0, 10: 0.0}

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
        self.mapper = mapper
        self.merged_groups = mapper.merged_groups
        self.feature_to_regs = mapper.feature_to_regs
        self.stats = mapper.stats
        self.regulation_names = mapper.regulation_names
        self.feature_to_linked_regulation = mapper.feature_to_linked_regulation
        self.streams_gdb = streams_gdb_path
        self.polygons_gdb = polygons_gdb_path
        self.gazetteer = mapper.gazetteer

        self.cache_dir = cache_dir or Path(".geom_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Loaded {len(self.merged_groups)} merged groups, {len(self.feature_to_regs)} individual features"
        )

        self._stream_geometries = None
        self._polygon_geometries = None
        self._layer_cache = {}

        self._needed_stream_ids = set()
        self._needed_blue_line_keys = set()
        self._needed_polygon_ids = set()
        self._valid_stream_ids = self.gazetteer.get_valid_stream_ids()

        self._build_feature_requirements()
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()

    # --- LOOKUPS & REQUIREMENTS ---

    def _get_reg_names(
        self, reg_ids: List[str], feature_ids: Optional[List[str]] = None
    ) -> List[str]:
        """Unified helper to get unique regulation names, optionally filtered by explicit linking."""
        if not reg_ids:
            return []

        base_ids = {r.rsplit("_rule", 1)[0] for r in reg_ids}

        if feature_ids is not None:
            linked_regs = {
                r
                for fid in feature_ids
                for r in self.feature_to_linked_regulation.get(fid, set())
            }
            base_ids = base_ids.intersection(linked_regs)

        return [
            self.regulation_names[b]
            for b in sorted(base_ids)
            if b in self.regulation_names
        ]

    def _build_feature_requirements(self):
        """Extracts required feature IDs and keys from merged groups."""
        polygon_types = {FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE}

        for group in self.merged_groups.values():
            for feature_id in group.feature_ids:
                ftype = self.gazetteer.get_feature_type_from_id(feature_id)
                key = self.gazetteer.get_feature_key_from_id(feature_id)

                if ftype in polygon_types:
                    self._needed_polygon_ids.add((ftype.value, key))
                elif ftype == FeatureType.STREAM:
                    self._needed_stream_ids.add(key)
                    if meta := self.gazetteer.get_stream_metadata(key):
                        if blk := meta.get("blue_line_key"):
                            self._needed_blue_line_keys.add(blk)

        logger.info(
            f"Need {len(self._needed_stream_ids):,} streams, {len(self._needed_blue_line_keys):,} BLKs, {len(self._needed_polygon_ids):,} polygons"
        )

    # --- CACHING & GEOMETRY LOADING ---

    def _get_gdb_mtime(self, gdb_path: Path) -> float:
        """Gets the most recent modification time of a GDB file/directory."""
        if gdb_path.is_file():
            return gdb_path.stat().st_mtime
        if gdb_path.is_dir():
            mtimes = [f.stat().st_mtime for f in gdb_path.rglob("*") if f.is_file()]
            return max(mtimes) if mtimes else 0.0
        return 0.0

    def _with_cache(self, gdb_path: Path, ids_set: set, prefix: str, load_fn: callable):
        """Standardized cache checking and execution wrapper."""
        mtime = self._get_gdb_mtime(gdb_path)
        ids_str = ",".join(sorted(str(i) for i in ids_set))
        cache_hash = hashlib.md5(
            f"{gdb_path}_{mtime}_{ids_str}".encode("utf-8")
        ).hexdigest()
        cache_file = self.cache_dir / f"{prefix}_{cache_hash}.pkl"

        if cache_file.exists():
            logger.info(f"⚡ FAST RELOAD: Loading {prefix} geometries from cache...")
            with open(cache_file, "rb") as f:
                return pickle.load(f)

        logger.info(f"Loading {prefix} geometries into memory...")
        data = load_fn()

        logger.info(f"Saving {prefix} to cache...")
        with open(cache_file, "wb") as f:
            pickle.dump(data, f)
        return data

    def _preload_data(self):
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()

    def _read_gdb_layer_fast(self, gdb_path: Path, layer_name: str) -> gpd.GeoDataFrame:
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

    def _load_all_stream_geometries(self):
        if self._stream_geometries is not None:
            return

        def _load():
            geoms = {}
            layers = fiona.listlayers(str(self.streams_gdb))
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(
                        self._read_gdb_layer_fast, self.streams_gdb, lyr
                    ): lyr
                    for lyr in layers
                }
                for future in tqdm(
                    as_completed(futures),
                    total=len(layers),
                    desc="Loading Stream Layers",
                ):
                    gdf = future.result()
                    if gdf.empty or "LINEAR_FEATURE_ID" not in gdf.columns:
                        continue
                    gdf["LINEAR_FEATURE_ID"] = (
                        gdf["LINEAR_FEATURE_ID"]
                        .astype(str)
                        .str.replace(r"\.0$", "", regex=True)
                        .str.strip()
                    )
                    mask = gdf["LINEAR_FEATURE_ID"].isin(self._needed_stream_ids) & gdf[
                        "LINEAR_FEATURE_ID"
                    ].isin(self._valid_stream_ids)
                    geoms.update(
                        {
                            row["LINEAR_FEATURE_ID"]: row.geometry
                            for _, row in gdf[mask].iterrows()
                        }
                    )
            return geoms

        self._stream_geometries = self._with_cache(
            self.streams_gdb,
            self._needed_stream_ids | self._valid_stream_ids,
            "streams",
            _load,
        )

    def _load_all_polygon_geometries(self):
        if self._polygon_geometries is not None:
            return

        def _load():
            geoms = {}
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
                gdf["WATERBODY_POLY_ID"] = (
                    gdf["WATERBODY_POLY_ID"].astype(int).astype(str)
                )

                needed_keys = {
                    (
                        str(int(float(k)))
                        if str(k).replace(".", "").isdigit()
                        else str(k).strip()
                    )
                    for ftype, k in self._needed_polygon_ids
                    if ftype.rstrip("s") == feature_type
                }

                filtered_gdf = gdf[gdf["WATERBODY_POLY_ID"].isin(needed_keys)]
                for _, row in filtered_gdf.iterrows():
                    geoms[f"{feature_type.upper()}_{row['WATERBODY_POLY_ID']}"] = (
                        row.geometry
                    )
            return geoms

        self._polygon_geometries = self._with_cache(
            self.polygons_gdb,
            {str(p) for p in self._needed_polygon_ids},
            "polygons",
            _load,
        )

    # --- MATH & SCORING ---

    def _compute_blk_stats(self, keys_iterable) -> dict:
        """Unified computation for BLK magnitude, length, order, etc."""
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
            if meta.get("gnis_name"):
                s["has_name"] = True
            if (
                meta.get("edge_type") not in MAIN_FLOW_CODES
                and meta.get("edge_type") is not None
            ):
                s["is_side_channel"] = True
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
        logger.info("Calculating percentile-based zoom thresholds...")
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
        stats = self._compute_blk_stats(self._stream_geometries.keys())
        return {
            blk: self._calculate_stream_minzoom(v["max_magnitude"])
            for blk, v in stats.items()
        }

    def _calculate_stream_minzoom(self, magnitude=0) -> int:
        for threshold, zoom in self._stream_zoom_thresholds:
            if (magnitude or 0) >= threshold:
                return zoom
        return 12

    def _calculate_polygon_minzoom(self, area_sqm: float) -> int:
        for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items()):
            if area_sqm >= limit:
                return zoom + 1
        return 12

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
                mag = meta.get("stream_magnitude", 0)
                features.append(
                    {
                        "linear_feature_id": linear_id,
                        "gnis_name": meta.get("gnis_name", ""),
                        "stream_order": meta.get("stream_order") or 0,
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_names": " | ".join(
                            self._get_reg_names(reg_ids, [linear_id])
                        ),
                        "tippecanoe:minzoom": self._calculate_stream_minzoom(mag),
                        "geometry": geom,
                    }
                )
        else:
            blk_zooms = self._get_synchronized_blk_zooms()
            for group in self.merged_groups.values():
                if group.feature_type not in ("stream", None) or (
                    exclude_lake_streams and group.waterbody_key
                ):
                    continue

                geom_list, all_zones, all_mgmt_units, ws_codes = [], set(), set(), set()
                max_order, blk = 0, None

                for fid in group.feature_ids:
                    if (geom := self._stream_geometries.get(fid)) and (
                        meta := self.gazetteer.get_stream_metadata(fid)
                    ):
                        geom_list.extend(self._extract_geoms(geom))
                        ws_codes.add(meta.get("fwa_watershed_code", ""))
                        all_zones.update(meta.get("zones", []))
                        all_mgmt_units.update(meta.get("mgmt_units", []))
                        max_order = max(max_order, meta.get("stream_order") or 0)
                        blk = blk or meta.get("blue_line_key")

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
        self, feature_type: str, merge_geometries: bool, include_all: bool
    ) -> Optional[gpd.GeoDataFrame]:
        self._load_all_polygon_geometries()
        singular = feature_type.rstrip("s")
        cache_key = (f"poly_{feature_type}", merge_geometries, include_all)
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = []
        if include_all:
            for feature_id, geom in self._polygon_geometries.items():
                if (
                    self.gazetteer.get_feature_type_from_id(feature_id).value
                    != singular
                ):
                    continue
                key = self.gazetteer.get_feature_key_from_id(feature_id)
                reg_ids = self.feature_to_regs.get(key, [])
                meta = self.gazetteer.get_polygon_metadata(key, f"{singular}s") or {}
                features.append(
                    {
                        "waterbody_key": meta.get("waterbody_key", key),
                        "gnis_name": meta.get("gnis_name", ""),
                        "area_sqm": meta.get("area_sqm", 0),
                        "regulation_ids": ",".join(reg_ids) if reg_ids else None,
                        "regulation_count": len(reg_ids),
                        "regulation_names": " | ".join(
                            self._get_reg_names(reg_ids, [key])
                        ),
                        "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                            meta.get("area_sqm", 0)
                        ),
                        "geometry": geom,
                    }
                )
        else:
            prefix = f"{singular.upper()}_"
            for group in self.merged_groups.values():
                if group.feature_type != singular and not any(
                    f.startswith(prefix) for f in group.feature_ids
                ):
                    continue

                geom_meta = [
                    (
                        g,
                        self.gazetteer.get_polygon_metadata(
                            self.gazetteer.get_feature_key_from_id(fid), f"{singular}s"
                        )
                        or {},
                        self.gazetteer.get_feature_key_from_id(fid),
                    )
                    for fid in group.feature_ids
                    if (g := self._polygon_geometries.get(f"{prefix}{fid}"))
                ]

                if not geom_meta:
                    continue

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
                            "gnis_name": group.gnis_name or "",
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
                                "waterbody_key": meta.get("waterbody_key", key),
                                "gnis_name": meta.get("gnis_name", ""),
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

    # --- EXPORTERS ---

    def _is_file_locked(self, filepath: Path) -> bool:
        if filepath.exists():
            try:
                with open(filepath, "a"):
                    pass
            except PermissionError:
                logger.error(f"File {filepath.name} is locked. Skipping export.")
                return True
        return False

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
                with open(layer_path, "w") as f:
                    for _, row in gdf.to_crs("EPSG:4326").iterrows():
                        props = {
                            k: (
                                (v if pd.notna(v) else "")
                                if k == "regulation_names"
                                else v
                            )
                            for k, v in row.drop("geometry").items()
                            if pd.notna(v) or k == "regulation_names"
                        }
                        f.write(
                            json.dumps(
                                {
                                    "type": "Feature",
                                    "properties": props,
                                    "geometry": row["geometry"].__geo_interface__,
                                    "tippecanoe": {
                                        "layer": layer_name,
                                        "minzoom": int(row["tippecanoe:minzoom"]),
                                    },
                                }
                            )
                            + "\n"
                        )
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
        if (
            result := subprocess.run(cmd, text=True)
        ).returncode == 0 and output_path.exists():
            logger.info(
                f"Created PMTiles {output_path} ({output_path.stat().st_size / (1024 * 1024):.1f} MB)"
            )
            return output_path

        logger.error(f"Tippecanoe failed with return code {result.returncode}")
        return None

    def export_regulations_json(
        self, parsed_regulations: List[Dict[str, Any]], output_path: Path
    ) -> Path:
        logger.info("Exporting regulations lookup table...")
        regulations_lookup, total_rules = {}, 0

        for idx, regulation in enumerate(parsed_regulations):
            identity, rules = regulation.get("identity", {}), regulation.get(
                "rules", []
            )
            for rule_idx, rule in enumerate(rules):
                restriction, scope = rule.get("restriction", {}), rule.get("scope", {})
                regulations_lookup[f"reg_{idx:04d}_rule{rule_idx}"] = {
                    "waterbody_name": identity.get("name_verbatim"),
                    "waterbody_key": identity.get("waterbody_key"),
                    "region": regulation.get("region"),
                    "management_units": regulation.get("mu", []),
                    "rule_text": rule.get("rule_text_verbatim"),
                    "restriction_type": restriction.get("type"),
                    "restriction_details": restriction.get("details"),
                    "dates": restriction.get("dates"),
                    "scope_type": scope.get("type"),
                    "scope_location": scope.get("location_verbatim"),
                    "includes_tributaries": scope.get("includes_tributaries"),
                }
                total_rules += 1

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(regulations_lookup, f, indent=2, ensure_ascii=False)

        logger.info(
            f"Exported {total_rules} rules to {output_path} ({output_path.stat().st_size / (1024 * 1024):.2f} MB)"
        )
        return output_path

    def export_search_index(self, output_path: Path) -> Path:
        logger.info("Exporting search index...")
        self._preload_data()

        search_groups = defaultdict(
            lambda: {
                "geometries": [],
                "zones": set(),
                "mgmt_units": set(),
                "segment_count": 0,
                "waterbody_keys": set(),
                "group_ids": [],
                "feature_ids": [],
            }
        )

        for group in self.merged_groups.values():
            reg_names = self._get_reg_names(
                list(group.regulation_ids), list(group.feature_ids)
            )
            if not group.gnis_name and not reg_names:
                continue

            ftype = group.feature_type or (
                self.gazetteer.get_feature_type_from_id(
                    next(iter(group.feature_ids))
                ).value
                if group.feature_ids
                else "stream"
            )
            sg = search_groups[
                (group.gnis_name, tuple(sorted(group.regulation_ids)), ftype)
            ]

            prefix = f"{ftype.upper()}_" if ftype != "stream" else ""
            geoms_dict = (
                self._stream_geometries
                if ftype == "stream"
                else self._polygon_geometries
            )

            for fid in group.feature_ids:
                if geom := geoms_dict.get(f"{prefix}{fid}" if prefix else fid):
                    sg["geometries"].extend(self._extract_geoms(geom))

            sg["segment_count"] += len(group.feature_ids)
            sg["feature_ids"].extend(group.feature_ids)
            sg["zones"].update(group.zones or [])
            sg["mgmt_units"].update(group.mgmt_units or [])
            if group.waterbody_key:
                sg["waterbody_keys"].add(group.waterbody_key)
            sg["group_ids"].append(group.group_id)

        search_items = []
        for (gnis, reg_ids_tuple, ftype), data in search_groups.items():
            if not data["geometries"]:
                continue
            reg_ids = list(reg_ids_tuple)

            if ftype == "stream":
                mag = max(
                    (
                        m.get("stream_magnitude") or 0
                        for fid in data["feature_ids"]
                        if (m := self.gazetteer.get_stream_metadata(fid))
                    ),
                    default=0,
                )
                min_zoom = self._calculate_stream_minzoom(mag)
            else:
                min_zoom = self._calculate_polygon_minzoom(
                    sum(g.area for g in data["geometries"])
                )

            # Optimized Bound calculation
            min_x, min_y = min(g.bounds[0] for g in data["geometries"]), min(
                g.bounds[1] for g in data["geometries"]
            )
            max_x, max_y = max(g.bounds[2] for g in data["geometries"]), max(
                g.bounds[3] for g in data["geometries"]
            )

            wgs84_bounds = (
                gpd.GeoSeries([box(min_x, min_y, max_x, max_y)], crs="EPSG:3005")
                .to_crs("EPSG:4326")
                .iloc[0]
                .bounds
            )

            search_items.append(
                {
                    "id": f"{gnis}|{','.join(reg_ids)}|{ftype}",
                    "gnis_name": gnis,
                    "regulation_names": self._get_reg_names(
                        reg_ids, data["feature_ids"]
                    ),
                    "type": ftype,
                    "zones": ",".join(sorted(data["zones"])),
                    "mgmt_units": ",".join(sorted(data["mgmt_units"])),
                    "regulation_ids": ",".join(reg_ids),
                    "segment_count": data["segment_count"],
                    "bbox": list(wgs84_bounds),
                    "min_zoom": min_zoom,
                    "properties": {
                        "group_id": data["group_ids"][0] if data["group_ids"] else "",
                        "waterbody_key": ",".join(sorted(data["waterbody_keys"])),
                        "regulation_count": len(reg_ids),
                    },
                }
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"waterbodies": search_items}, f, indent=2, ensure_ascii=False)

        logger.info(
            f"Exported {len(search_items)} waterbodies to {output_path} ({output_path.stat().st_size / (1024 * 1024):.2f} MB)"
        )
        return output_path
