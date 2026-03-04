"""
RegulationGeoExporter - Creates geographic exports from regulation mapping results
"""

import json
import hashlib
import pickle
import subprocess
from pathlib import Path
from typing import Dict, Optional, Any, List, Tuple, Callable, Iterable
from collections import defaultdict

from data.data_extractor import FWADataAccessor
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    Point,
    box,
)
from shapely.geometry.base import BaseGeometry
from shapely.prepared import prep as shapely_prep, PreparedGeometry

from .regulation_mapper import PipelineResult, MergedGroup
from fwa_pipeline.metadata_gazetteer import FeatureType
from fwa_pipeline.metadata_builder import ADMIN_LAYER_CONFIG


def _round_coords(geom_dict: dict, precision: int = 7) -> dict:
    """Round all coordinates in a __geo_interface__ geometry dict.

    Reduces GeoJSON coordinate precision from float64 (14-15 digits) to
    *precision* decimal places.  7 digits ≈ 1.1 cm at the equator — more
    than sufficient for map display.  This typically halves the byte size
    of coordinate-heavy features.
    """

    def _round(coords):
        """Recursively round nested coordinate tuples/lists."""
        if isinstance(coords, (float, int)):
            return round(coords, precision)
        return [_round(c) for c in coords]

    return {
        **geom_dict,
        "coordinates": _round(geom_dict["coordinates"]),
    }

from .logger_config import get_logger

logger = get_logger(__name__)

try:
    from tqdm import tqdm
except ImportError:
    logger.debug("tqdm not available; progress bars disabled")

    def tqdm(iterable: Iterable, **kwargs: Any) -> Iterable:
        return iterable


# --- CONSTANTS ---
WEIGHTS = {
    "order": 0.0,
    "magnitude": 1.0,
    "length_km": 0.0,
    "has_name": 0.0,
    "side_channel_penalty": 0.0,
}
PERCENTILES = {5: 100.0, 6: 99.99, 7: 99.97, 8: 99.0, 9: 95.0, 10: 0.0}
LAKE_ZOOM_THRESHOLDS = {
    4: 100_000_000,
    5: 25_000_000,
    6: 1_000_000,
    7: 1_000_000,
    8: 10_000,
    9: 10_000,
    10: 5_000,
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
    ) -> None:
        self.pipeline_result = pipeline_result
        self.merged_groups = pipeline_result.merged_groups
        self.feature_to_regs = pipeline_result.feature_to_regs
        self.regulation_names = pipeline_result.regulation_names
        self.feature_to_linked_regulation = pipeline_result.feature_to_linked_regulation
        self.gazetteer = pipeline_result.gazetteer
        self.admin_area_reg_map = pipeline_result.admin_area_reg_map
        self.admin_regulation_ids = pipeline_result.admin_regulation_ids
        self.gpkg_path = gpkg_path
        # Reuse gazetteer's data accessor if available to avoid creating
        # a duplicate FWADataAccessor (each one lists GPKG layers on init).
        self.data_accessor = (
            self.gazetteer.data_accessor
            if self.gazetteer and self.gazetteer.data_accessor is not None
            else FWADataAccessor(self.gpkg_path)
        )
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._stream_geometries = None
        self._polygon_geometries = None
        self._layer_cache = {}
        self._admin_gdf_cache: dict = {}  # layer_key → reprojected GeoDataFrame
        self._valid_stream_ids = self.gazetteer.get_valid_stream_ids()
        self._lake_manmade_wbkeys: Optional[set] = None  # lazy; built on first use
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()
        logger.info(
            f"Loaded {len(self.merged_groups)} merged groups, {len(self.feature_to_regs)} individual features"
        )

    # --- WATERBODY-KEY HELPERS ---

    @staticmethod
    def _merge_lines(geom_list: List[LineString]) -> BaseGeometry:
        """Merge a list of line geometries into a single geometry.

        Returns a ``MultiLineString`` when *geom_list* contains more than
        one element; otherwise returns the single ``LineString`` directly.
        """
        return MultiLineString(geom_list) if len(geom_list) > 1 else geom_list[0]

    @staticmethod
    def _merge_name_variants(
        target: Dict[str, bool], variants: List[Dict[str, Any]]
    ) -> None:
        """Merge name variant dicts into *target* (name → from_tributary).

        ``False`` (direct name match) always wins over ``True`` (tributary).
        """
        for nv in variants:
            name = nv["name"]
            is_trib = nv["from_tributary"]
            if name in target:
                if not is_trib:
                    target[name] = False
            else:
                target[name] = is_trib

    @staticmethod
    def _geoms_to_wgs84_bbox(
        geoms: List[BaseGeometry],
    ) -> Tuple[float, float, float, float]:
        """Compute a WGS 84 bounding box from EPSG:3005 geometries."""
        bounds = np.array([g.bounds for g in geoms])
        bbox_3005 = box(
            bounds[:, 0].min(),
            bounds[:, 1].min(),
            bounds[:, 2].max(),
            bounds[:, 3].max(),
        )
        return (
            gpd.GeoSeries([bbox_3005], crs="EPSG:3005")
            .to_crs("EPSG:4326")
            .iloc[0]
            .bounds
        )

    def _get_lake_manmade_wbkeys(self) -> set:
        """Return the set of waterbody_keys that belong to lakes or manmade waterbodies.

        Streams overlapping these polygon types are excluded when
        ``exclude_lake_streams`` is True (the polygon layer already
        renders the waterbody).  Wetland waterbody_keys are deliberately
        excluded so that streams running through wetlands are kept.
        """
        if self._lake_manmade_wbkeys is None:
            keys: set = set()
            for ftype in (FeatureType.LAKE, FeatureType.MANMADE):
                for fid, meta in self.gazetteer.metadata.get(ftype, {}).items():
                    wbk = meta.get("waterbody_key") or fid
                    keys.add(str(wbk))
            self._lake_manmade_wbkeys = keys
            logger.debug(f"Built lake/manmade waterbody-key set: {len(keys):,} keys")
        return self._lake_manmade_wbkeys

    # --- LOOKUPS & METADATA ---

    def _expand_admin_reg_ids(self, base_ids: set) -> set:
        """Expand base synopsis regulation IDs to their ``_ruleN`` variants.

        The ``admin_area_reg_map`` stores base regulation IDs (e.g.
        ``reg_00618``) for synopsis-sourced regulations, but the
        ``regulations.json`` keys use per-rule suffixes
        (``reg_00618_rule0``, ``reg_00618_rule1``, …).  Provincial and
        zone IDs are already exact keys and pass through unchanged.
        """
        reg_details = self.pipeline_result.regulation_details
        expanded = set()
        for rid in base_ids:
            if rid in reg_details:
                # Exact key exists (provincial / zone) — keep as-is
                expanded.add(rid)
            else:
                # Look for _ruleN variants
                rule_keys = [k for k in reg_details if k.startswith(f"{rid}_rule")]
                if rule_keys:
                    expanded.update(rule_keys)
                else:
                    logger.error(
                        f"_expand_admin_reg_ids: regulation '{rid}' not found in "
                        f"regulation_details and has no _ruleN variants — skipping"
                    )
        return expanded

    def _get_reg_names(
        self, reg_ids: List[str], feature_ids: Optional[List[str]] = None
    ) -> List[str]:
        """Return human-readable regulation names for the given IDs.

        Provincial/admin regulations (prefixed ``prov_``) and admin-area
        synopsis matches (e.g. "Liard River Watershed") are excluded because
        they apply broadly to many features and are not waterbody-specific names.
        """
        if not reg_ids:
            return []

        base_ids = {r.rsplit("_rule", 1)[0] for r in reg_ids}
        # Exclude provincial/zone/admin regulations — they aren't waterbody names
        base_ids = {
            b
            for b in base_ids
            if not b.startswith("prov_") and not b.startswith("zone_")
        }
        # Exclude admin-area synopsis matches (e.g. watershed / WMA / park rules)
        base_ids -= self.admin_regulation_ids
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

    def _with_cache(
        self, gdb_path: Path, ids_set: set, prefix: str, load_fn: Callable
    ) -> Any:
        mtime = self._get_gdb_mtime(gdb_path)
        ids_str = ",".join(sorted(map(str, ids_set)))
        # Always resolve to absolute path so the hash is stable regardless of
        # whether a relative or absolute path was passed in.  A mismatch here
        # was the root cause of the PMTiles size bloat: the cache built on
        # Feb 20 used an absolute path; later runs were passed a relative path,
        # producing a different hash → cache miss on every run.
        cache_hash = hashlib.md5(
            f"{gdb_path.resolve()}_{mtime}_{ids_str}".encode("utf-8")
        ).hexdigest()
        cache_file = self.cache_dir / f"{prefix}_{cache_hash}.pkl"

        if cache_file.exists():
            logger.info(f"⚡ FAST RELOAD: Loading {prefix} from cache...")
            return pickle.loads(cache_file.read_bytes())

        logger.info(f"Loading {prefix} geometries into memory...")
        data = load_fn()
        cache_file.write_bytes(pickle.dumps(data))
        return data

    def _preload_data(self) -> None:
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()
        self._inject_ungazetted_geometries()

    def _inject_ungazetted_geometries(self) -> None:
        """Inject ungazetted waterbody geometries into the polygon geometry cache.

        This allows the search index builder and other code that looks up
        geometries via ``{ftype.value.upper()}_{id}`` to find ungazetted
        features without special-casing.
        """
        from .linking_corrections import UNGAZETTED_WATERBODIES

        if self._polygon_geometries is None:
            self._polygon_geometries = {}

        for uid, uw in UNGAZETTED_WATERBODIES.items():
            if uw.geometry_type == "point":
                key = f"{FeatureType.UNGAZETTED.value.upper()}_{uid}"
                self._polygon_geometries[key] = Point(uw.coordinates)

    def _load_all_stream_geometries(self) -> None:
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

    def _load_all_polygon_geometries(self) -> None:
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

    def _compute_blk_stats(self, keys_iterable: Iterable[str]) -> dict:
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
        max_order: int = 0,
        magnitude: int = 0,
        length_km: float = 0.0,
        has_name: bool = False,
        is_side_channel: bool = False,
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

    def _calculate_stream_minzoom(self, magnitude: int = 0) -> int:
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

    def _extract_geoms(self, geom: BaseGeometry) -> List[BaseGeometry]:
        return geom.geoms if hasattr(geom, "geoms") else [geom]

    # --- ADMIN BOUNDARY CLIPPING ---

    def _get_all_admin_reg_ids(self) -> set:
        """Collect all regulation IDs sourced from admin polygon spatial matching.

        Gathers every regulation ID stored in ``admin_area_reg_map`` across all
        admin layer types (parks, WMAs, watersheds, etc.), then expands synopsis
        base IDs to their ``_ruleN`` variants so the result can be directly
        compared against ``MergedGroup.regulation_ids``.
        """
        raw_ids: set = set()
        for features_map in self.admin_area_reg_map.values():
            for reg_ids in features_map.values():
                raw_ids.update(reg_ids)
        if not raw_ids:
            return set()
        return self._expand_admin_reg_ids(raw_ids)

    def _get_admin_gdf(self, layer_key: str) -> Optional[gpd.GeoDataFrame]:
        """Load an admin layer GeoDataFrame, reusing caches when possible.

        Checks (in order):
        1. Gazetteer's ``_reprojected_admin_cache`` (populated during mapper phase)
        2. Our own ``_admin_gdf_cache`` (populated on first exporter load)
        3. Fresh GPKG read (cached for subsequent calls)

        Returns ``None`` if the layer is not available.
        """
        cache_key = f"{self.gpkg_path}_{layer_key}"

        # 1. Gazetteer may already have the reprojected layer cached
        cached = self.gazetteer._reprojected_admin_cache.get(cache_key)
        if cached is not None:
            return cached

        # 2. Our own exporter-level cache
        if cache_key in self._admin_gdf_cache:
            return self._admin_gdf_cache[cache_key]

        # 3. Load from GPKG
        if layer_key not in self.data_accessor.list_layers():
            logger.warning(f"Admin layer '{layer_key}' not in GPKG")
            return None
        gdf = self.data_accessor.get_layer(layer_key).to_crs("EPSG:3005")
        self._admin_gdf_cache[cache_key] = gdf
        return gdf

    def _build_admin_clip_union(self) -> Optional[BaseGeometry]:
        """Build a single union polygon of all admin areas that carry regulations.

        Loads each admin layer from the GPKG (or cache), filters to the features
        present in ``admin_area_reg_map``, and unions all geometries into one
        shape suitable for clipping stream segments.

        Returns ``None`` if no admin polygons are available.
        """
        all_geoms = []
        for layer_key, features_map in self.admin_area_reg_map.items():
            if not features_map:
                continue
            cfg = ADMIN_LAYER_CONFIG.get(layer_key)
            if not cfg:
                continue
            gdf = self._get_admin_gdf(layer_key)
            if gdf is None:
                continue
            id_field = cfg["id_field"]
            matched_ids = set(features_map.keys())
            matched = gdf[gdf[id_field].isin(matched_ids)]
            if matched.empty:
                continue
            all_geoms.extend(matched.geometry.tolist())

        if not all_geoms:
            return None

        from shapely.ops import unary_union

        union = unary_union(all_geoms)
        logger.debug(
            f"Admin clip union built from {len(all_geoms)} polygon(s) "
            f"across {len(self.admin_area_reg_map)} layer(s)"
        )
        return union

    @staticmethod
    def _extract_line_components(geom: BaseGeometry) -> list:
        """Extract LineString/MultiLineString parts from a geometry result.

        ``intersection()`` and ``difference()`` can return GeometryCollections
        containing points or other degenerate artefacts.  This keeps only
        linear components.
        """
        if geom is None or geom.is_empty:
            return []
        if isinstance(geom, LineString):
            return [geom] if geom.length > 0 else []
        if isinstance(geom, MultiLineString):
            return [g for g in geom.geoms if g.length > 0]
        if isinstance(geom, GeometryCollection):
            parts = []
            for g in geom.geoms:
                if isinstance(g, (LineString, MultiLineString)) and g.length > 0:
                    if isinstance(g, MultiLineString):
                        parts.extend(p for p in g.geoms if p.length > 0)
                    else:
                        parts.append(g)
            return parts
        return []

    def _compute_frontend_group_id(
        self,
        watershed_code: str,
        gnis_name: str,
        regulation_ids: tuple,
    ) -> str:
        """Compute a unique frontend group ID for highlighting.

        Groups by: watershed_code + gnis_name + sorted regulation_ids.
        This ensures all tiles belonging to the same physical stream segment
        with the same regulations will highlight together.
        """
        key = f"{watershed_code or ''}|{gnis_name or ''}|{','.join(sorted(regulation_ids))}"
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def _emit_group_feature(
        self,
        group: MergedGroup,
        base_props: dict,
        suffix: str = "",
        reg_ids: Optional[tuple] = None,
        geometry: Optional[BaseGeometry] = None,
    ) -> dict:
        """Build a single feature dict for export.

        Args:
            group: The ``MergedGroup``.
            base_props: Shared metadata dict.
            suffix: Appended to group_id (e.g. ``'_admin_in'``).
            reg_ids: Override regulation IDs (defaults to ``group.regulation_ids``).
            geometry: The shapely geometry to use.
        """
        rids = reg_ids if reg_ids is not None else group.regulation_ids
        # Compute frontend_group_id for consistent highlighting
        watershed_code = base_props.get("watershed_code", "").split(",")[0].strip()
        frontend_group_id = self._compute_frontend_group_id(
            watershed_code, group.gnis_name, rids
        )
        return {
            **base_props,
            "group_id": f"{group.group_id}{suffix}" if suffix else group.group_id,
            "frontend_group_id": frontend_group_id,
            "regulation_ids": ",".join(rids),
            "regulation_count": len(rids),
            "regulation_names": " | ".join(
                self._get_reg_names(list(rids), list(group.feature_ids))
            ),
            "has_regulations": bool(rids),
            "geometry": geometry,
        }

    def _clip_group_at_admin_boundary(
        self,
        group: MergedGroup,
        geom_list: list,
        admin_clip_union: BaseGeometry,
        admin_prep: PreparedGeometry,
        admin_reg_ids: set,
        base_props: dict,
    ) -> Tuple[List[dict], str]:
        """Split a merged group's geometry at admin polygon boundaries.

        Uses a prepared geometry for fast ``contains`` / ``intersects`` checks
        so that groups fully inside or fully outside the admin polygon skip
        the expensive ``intersection()`` / ``difference()`` operations.

        Returns:
            ``(features, disposition)`` where *disposition* is one of
            ``'inside'``, ``'outside'``, ``'clipped'``, or ``'fallback'``.
        """
        MIN_LENGTH_M = 1.0  # discard slivers shorter than 1 metre

        merged_line = self._merge_lines(geom_list)
        non_admin_regs = tuple(
            r for r in group.regulation_ids if r not in admin_reg_ids
        )

        # --- Fast-path: entirely inside admin polygon ---
        if admin_prep.contains(merged_line):
            return [
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_in",
                    reg_ids=group.regulation_ids,
                    geometry=merged_line,
                )
            ], "inside"

        # --- Fast-path: entirely outside admin polygon ---
        if not admin_prep.intersects(merged_line):
            return [
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_out",
                    reg_ids=non_admin_regs,
                    geometry=merged_line,
                )
            ], "outside"

        # --- Slow path: boundary crossing — actual clip ---
        inside_parts = self._extract_line_components(
            merged_line.intersection(admin_clip_union)
        )
        outside_parts = self._extract_line_components(
            merged_line.difference(admin_clip_union)
        )

        results: List[dict] = []

        # Inside piece: full regulation set
        inside_parts = [g for g in inside_parts if g.length >= MIN_LENGTH_M]
        if inside_parts:
            inside_geom = self._merge_lines(inside_parts)
            results.append(
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_in",
                    reg_ids=group.regulation_ids,
                    geometry=inside_geom,
                )
            )

        # Outside piece: admin regs removed
        outside_parts = [g for g in outside_parts if g.length >= MIN_LENGTH_M]
        if outside_parts:
            outside_geom = self._merge_lines(outside_parts)
            results.append(
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_out",
                    reg_ids=non_admin_regs,
                    geometry=outside_geom,
                )
            )

        # Clipping produced nothing usable — emit original with a warning.
        if not results:
            logger.warning(
                f"Admin clip for group {group.group_id} produced no inside/outside "
                f"pieces — emitting unclipped geometry"
            )
            results.append(
                self._emit_group_feature(
                    group,
                    base_props,
                    reg_ids=group.regulation_ids,
                    geometry=merged_line,
                )
            )
            return results, "fallback"

        return results, "clipped"

    # --- LAYER CREATION ---

    def _merge_same_regulation_features(self, features: list) -> list:
        """Merge export features that share the same physical waterbody and regulation_ids.

        After admin boundary clipping, an ``_admin_out`` piece may have the
        same effective regulation set as a non-clipped group of the same
        waterbody.  This post-processing step re-merges those pieces so the
        map tiles and search index don't show redundant entries.

        Groups by ``(blue_line_key, waterbody_key, regulation_ids)`` — this
        ensures unnamed streams on different physical channels don't incorrectly
        merge together (they have different blue_line_keys).
        """
        if not features:
            return features

        groups = defaultdict(list)
        for feat in features:
            # Use physical waterbody identifiers, not gnis_name.
            # blue_line_key identifies the physical stream channel.
            # waterbody_key identifies lakes/wetlands the stream flows through.
            key = (
                feat.get("blue_line_key", ""),
                feat.get("waterbody_key", ""),
                feat.get("regulation_ids", ""),
            )
            groups[key].append(feat)

        merged = []
        merge_count = 0
        for (blk, wbk, reg_ids), group_features in groups.items():
            if len(group_features) == 1:
                merged.append(group_features[0])
                continue

            # Merge geometries from all features in this group
            all_geoms = []
            for feat in group_features:
                geom = feat.get("geometry")
                if geom is not None:
                    all_geoms.extend(self._extract_geoms(geom))

            if not all_geoms:
                merged.append(group_features[0])
                continue

            # Use first feature as template, merge geometry
            template = dict(group_features[0])
            template["geometry"] = self._merge_lines(all_geoms)

            # Merge group_id — pick the one without admin suffix, or the first
            group_ids = [f.get("group_id", "") for f in group_features]
            base_ids = [
                g for g in group_ids if not g.endswith(("_admin_in", "_admin_out"))
            ]
            template["group_id"] = base_ids[0] if base_ids else group_ids[0]

            # Merge mgmt_units from all features
            all_mu = set()
            for feat in group_features:
                for mu in feat.get("mgmt_units", "").split(","):
                    if mu:
                        all_mu.add(mu)
            template["mgmt_units"] = ",".join(sorted(all_mu))

            merged.append(template)
            merge_count += 1

        if merge_count:
            logger.info(
                f"Post-merge: combined {merge_count} groups with identical "
                f"regulation sets ({len(features)} → {len(merged)} features)"
            )
        return merged

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
                        "zones": ",".join(meta.get("zones", [])),
                        "mgmt_units": ",".join(meta.get("mgmt_units", [])),
                        "region_name": ",".join(meta.get("region_names", [])),
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

            # Pre-compute admin clipping data (once for all groups)
            admin_reg_ids = self._get_all_admin_reg_ids()
            admin_clip_union = self._build_admin_clip_union() if admin_reg_ids else None
            admin_prep = (
                shapely_prep(admin_clip_union) if admin_clip_union is not None else None
            )
            clip_stats = {"inside": 0, "outside": 0, "clipped": 0, "fallback": 0}

            for group in self.merged_groups.values():
                # Determine feature type of the group based on its first feature
                first_fid = next(iter(group.feature_ids), None)
                if not first_fid:
                    continue

                group_ftype = self.gazetteer.get_feature_type_from_id(first_fid)

                if group_ftype != FeatureType.STREAM or (
                    exclude_lake_streams
                    and group.waterbody_key
                    and group.waterbody_key in self._get_lake_manmade_wbkeys()
                ):
                    continue

                geom_list, all_mgmt_units, ws_codes = [], set(), set()
                max_order, blk = 0, None

                blks = set()
                zone_to_name = {}  # zone_id → region_name (maintains pairing)
                for fid in group.feature_ids:
                    meta = self.gazetteer.get_stream_metadata(fid)
                    geom = self._stream_geometries.get(fid)

                    # Always capture the blue line key from metadata when available
                    if meta and meta.get("blue_line_key"):
                        blks.add(meta.get("blue_line_key"))

                    # Only extend geometries and collect other per-segment props when geometry exists
                    if geom and meta:
                        geom_list.extend(self._extract_geoms(geom))
                        ws_codes.add(meta.get("fwa_watershed_code", ""))
                        for z, n in zip(
                            meta.get("zones", []), meta.get("region_names", [])
                        ):
                            zone_to_name[z] = n
                        all_mgmt_units.update(meta.get("mgmt_units", []))
                        max_order = max(max_order, meta.get("stream_order") or 0)

                if len(blks) == 0:
                    logger.warning(f"No blue line key found in group {group.group_id}")
                    blk = None
                elif len(blks) == 1:
                    blk = blks.pop()
                else:
                    logger.debug(
                        f"Group {group.group_id} spans {len(blks)} blue line keys "
                        f"(cross-stream merge) — blue_line_key field left empty"
                    )
                    blk = None

                if not geom_list:
                    continue

                # Base metadata shared by all pieces (clipped or not)
                base_props = {
                    "gnis_name": group.gnis_name,
                    "waterbody_key": (
                        group.waterbody_key if group.waterbody_key is not None else ""
                    ),
                    "blue_line_key": blk if blk is not None else "",
                    "watershed_code": ", ".join(sorted(filter(None, ws_codes))),
                    "stream_order": max_order,
                    "zones": ",".join(sorted(zone_to_name.keys())),
                    "mgmt_units": ",".join(sorted(all_mgmt_units)),
                    "region_name": ",".join(
                        zone_to_name[z] for z in sorted(zone_to_name.keys())
                    ),
                    "tippecanoe:minzoom": blk_zooms.get(blk, 12),
                }

                # Check if this group needs admin boundary clipping
                group_admin_regs = (
                    admin_reg_ids & set(group.regulation_ids)
                    if admin_clip_union is not None
                    else set()
                )

                if group_admin_regs:
                    clip_results, disposition = self._clip_group_at_admin_boundary(
                        group,
                        geom_list,
                        admin_clip_union,
                        admin_prep,
                        admin_reg_ids,
                        base_props,
                    )
                    features.extend(clip_results)
                    clip_stats[disposition] += 1
                else:
                    # No admin regs — emit normally
                    merged_geom = self._merge_lines(geom_list)
                    features.append(
                        self._emit_group_feature(
                            group,
                            base_props,
                            geometry=merged_geom,
                        )
                    )

            if any(clip_stats.values()):
                logger.debug(
                    f"Admin boundary clipping: "
                    f"{clip_stats['inside']} fully inside, "
                    f"{clip_stats['clipped']} split at boundary, "
                    f"{clip_stats['outside']} fully outside, "
                    f"{clip_stats['fallback']} fallback"
                )

            # Post-processing: merge features that have the same regulation_ids
            # and gnis_name but were split across admin boundaries or grouping
            # artifacts (e.g. _admin_out piece has same regs as non-clipped group).
            features = self._merge_same_regulation_features(features)

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
                        "zones": ",".join(meta.get("zones", [])),
                        "mgmt_units": ",".join(meta.get("mgmt_units", [])),
                        "region_name": ",".join(meta.get("region_names", [])),
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

                gnis_name = group.gnis_name

                if merge_geometries and len(geom_meta) > 1:
                    max_area = max(m.get("area_sqm", 0) for _, m, _ in geom_meta)
                    wbk = group.waterbody_key if group.waterbody_key is not None else ""
                    frontend_gid = self._compute_frontend_group_id(
                        wbk or group.group_id, gnis_name, group.regulation_ids
                    )
                    features.append(
                        {
                            "group_id": group.group_id,
                            "frontend_group_id": frontend_gid,
                            "waterbody_key": wbk,
                            "regulation_ids": ",".join(group.regulation_ids),
                            "regulation_count": len(group.regulation_ids),
                            "regulation_names": " | ".join(
                                self._get_reg_names(
                                    list(group.regulation_ids), list(group.feature_ids)
                                )
                            ),
                            "gnis_name": gnis_name,
                            "area_sqm": max_area,
                            "feature_count": group.feature_count,
                            "zones": ",".join(group.zones),
                            "mgmt_units": ",".join(group.mgmt_units),
                            "region_name": ",".join(group.region_names),
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
                        wbk = meta.get("waterbody_key", key) or ""
                        frontend_gid = self._compute_frontend_group_id(
                            wbk or group.group_id, group.gnis_name, group.regulation_ids
                        )
                        features.append(
                            {
                                "waterbody_key": wbk,
                                "frontend_group_id": frontend_gid,
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
                                "region_name": ",".join(group.region_names),
                                "tippecanoe:minzoom": self._calculate_polygon_minzoom(
                                    meta.get("area_sqm", 0)
                                ),
                                "geometry": geom,
                            }
                        )

        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_ungazetted_layer(
        self, merge_geometries: bool
    ) -> Optional[gpd.GeoDataFrame]:
        """Create a point layer for ungazetted waterbodies that have regulations.

        Ungazetted waterbodies are injected into the gazetteer at pipeline
        startup.  Their geometry (EPSG:3005 points) is stored in the
        UNGAZETTED_WATERBODIES dict from linking_corrections.  Features are
        rendered at ``tippecanoe:minzoom`` 10 so they only appear when the
        user is fairly zoomed in.
        """
        from .linking_corrections import UNGAZETTED_WATERBODIES

        ungaz_meta = self.gazetteer.metadata.get(FeatureType.UNGAZETTED, {})
        if not ungaz_meta:
            return None

        features = []
        for uid, meta in ungaz_meta.items():
            reg_ids = self.feature_to_regs.get(uid, [])
            if not reg_ids:
                continue

            # Build geometry from UNGAZETTED_WATERBODIES coordinates
            uw = UNGAZETTED_WATERBODIES.get(uid)
            if not uw:
                continue
            if uw.geometry_type == "point":
                geom = Point(uw.coordinates)
            else:
                # Future: support linestring/polygon ungazetted geometries
                continue

            features.append(
                {
                    "ungazetted_id": uid,
                    "gnis_name": meta.get("gnis_name", "") or "",
                    "regulation_ids": ",".join(reg_ids),
                    "regulation_count": len(reg_ids),
                    "regulation_names": " | ".join(self._get_reg_names(reg_ids, [uid])),
                    "zones": ",".join(meta.get("zones", [])),
                    "mgmt_units": ",".join(meta.get("mgmt_units", [])),
                    "tippecanoe:minzoom": 10,
                    "geometry": geom,
                }
            )

        if not features:
            return None
        return gpd.GeoDataFrame(features, crs="EPSG:3005")

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

        id_field = cfg["id_field"]
        name_field = cfg.get("name_field")
        code_field = cfg.get("code_field")
        code_map = cfg.get("code_map", {})

        # Load admin layer (uses gazetteer/exporter cache) and filter
        gdf = self._get_admin_gdf(layer_key)
        if gdf is None:
            logger.warning(f"  Admin layer '{layer_key}' not available, skipping")
            return None
        matched_ids = set(matched_ids_map.keys())
        gdf = gdf[gdf[id_field].isin(matched_ids)].copy()
        if gdf.empty:
            logger.debug(f"  No geometry matches for '{layer_key}' after ID filter")
            return None

        # Attach regulation IDs — expand base synopsis IDs to _ruleN variants
        gdf["regulation_ids"] = gdf[id_field].apply(
            lambda fid: ",".join(
                sorted(self._expand_admin_reg_ids(matched_ids_map.get(fid, set())))
            )
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

        # Tippecanoe zoom — scale by polygon area so small admin areas
        # only appear when the user is zoomed in (same thresholds as lakes).
        gdf["tippecanoe:minzoom"] = gdf.geometry.area.apply(
            self._calculate_polygon_minzoom
        )
        out_cols.extend(["tippecanoe:minzoom", "geometry"])

        logger.info(f"  Admin layer '{layer_key}': {len(gdf)} features")
        return gdf[out_cols]

    def _create_regions_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build region boundary layer from the 'wmu' layer in the main GPKG."""
        if "wmu" not in self.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping regions")
            return None
        zones_gdf = self.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        zones_gdf["zone"] = zones_gdf["REGION_RESPONSIBLE_ID"]
        zones_gdf["region_name"] = zones_gdf["REGION_RESPONSIBLE_NAME"]
        regions_gdf = zones_gdf.dissolve(by="zone", as_index=False, aggfunc="first")
        regions_gdf["geometry"] = regions_gdf["geometry"].boundary

        regions_gdf["stroke_color"] = "#555555"
        regions_gdf["stroke_width"] = 2.5
        regions_gdf["stroke_dasharray"] = "3,3"
        regions_gdf["tippecanoe:minzoom"] = 0

        return regions_gdf[
            [
                "zone",
                "region_name",
                "stroke_color",
                "stroke_width",
                "stroke_dasharray",
                "tippecanoe:minzoom",
                "geometry",
            ]
        ]

    def _create_management_units_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build management-unit boundary layer from the 'wmu' layer.

        Each WMU polygon is kept individually (not dissolved) so the
        management-unit code (``WILDLIFE_MGMT_UNIT_ID``, e.g. '1-15') can
        be displayed on the frontend.
        """
        if "wmu" not in self.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping management_units")
            return None

        mu_gdf = self.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        mu_gdf["mu_code"] = mu_gdf["WILDLIFE_MGMT_UNIT_ID"]
        mu_gdf["region_name"] = mu_gdf["REGION_RESPONSIBLE_NAME"]
        mu_gdf["zone"] = mu_gdf["REGION_RESPONSIBLE_ID"]
        mu_gdf["geometry"] = mu_gdf["geometry"].boundary

        mu_gdf["stroke_color"] = "#888888"
        mu_gdf["stroke_width"] = 1.0
        mu_gdf["tippecanoe:minzoom"] = 4

        return mu_gdf[
            [
                "mu_code",
                "zone",
                "region_name",
                "stroke_color",
                "stroke_width",
                "tippecanoe:minzoom",
                "geometry",
            ]
        ]

    def _create_bc_mask_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Create a polygon that covers area outside BC zones for grey masking.

        Returns a single polygon that is a square around the BC zones
        with the BC zone polygons cut out as a hole.
        """
        if "wmu" not in self.data_accessor.list_layers():
            logger.warning("'wmu' layer not found in GPKG — skipping bc_mask")
            return None

        zones_gdf = self.data_accessor.get_layer("wmu").to_crs("EPSG:3005")

        # Union all zone polygons to get BC boundary
        bc_union = zones_gdf.geometry.union_all()
        if bc_union.is_empty:
            logger.warning("BC zone union is empty — skipping bc_mask")
            return None

        # Create a square centered on BC that extends 1000km beyond the bounds
        minx, miny, maxx, maxy = bc_union.bounds
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2

        # Use the larger dimension + 1000km padding on each side
        width = maxx - minx
        height = maxy - miny
        half_size = max(width, height) / 2 + 1_000_000  # meters

        outer_box = box(
            center_x - half_size,
            center_y - half_size,
            center_x + half_size,
            center_y + half_size,
        )

        # Create mask polygon (outer box with BC cut out)
        mask_polygon = outer_box.difference(bc_union)

        # Simplify to reduce tile size (tolerance in meters for EPSG:3005)
        # 100m keeps coastline detail while reducing vertex count
        mask_polygon = mask_polygon.simplify(100, preserve_topology=True)

        mask_gdf = gpd.GeoDataFrame(
            {
                "fill_color": ["#374151"],  # Tailwind gray-700
                "fill_opacity": [0.65],
                "tippecanoe:minzoom": [0],
                "geometry": [mask_polygon],
            },
            crs="EPSG:3005",
        )

        logger.debug(
            f"  BC mask layer: 1 feature covering area outside {len(zones_gdf)} zones"
        )
        return mask_gdf

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
            layers.append(("management_units", lambda: self._create_management_units_layer()))
            layers.append(("bc_mask", lambda: self._create_bc_mask_layer()))

        # Ungazetted waterbody points (zoom 10+)
        layers.append(("ungazetted", lambda: self._create_ungazetted_layer(merge)))

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
    ) -> Optional[Path]:
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
    ) -> Optional[Path]:
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
                            "geometry": _round_coords(
                                row["geometry"].__geo_interface__
                            ),
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
            "--simplification=10",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            "--no-clipping",
            "--detect-shared-borders",
        ] + [arg for lp in layer_files for arg in ("-L", f"{lp.stem}:{lp}")]

        result = subprocess.run(cmd, text=True)
        if result.returncode == 0 and output_path.exists():
            logger.info(
                f"Created PMTiles {output_path} ({output_path.stat().st_size / 1048576:.1f} MB)"
            )
            return output_path

        logger.error(f"Tippecanoe failed (rc={result.returncode})")
        return None

    def _build_regulations_lookup(self) -> Dict[str, Any]:
        """Build regulations lookup dictionary from pipeline regulation_details."""
        return dict(self.pipeline_result.regulation_details)

    def _build_waterbodies_list(self) -> List[Dict[str, Any]]:
        self._preload_data()

        # First pass: group by physical stream identity (watershed_code + gnis_name + type)
        # Each physical stream becomes ONE search entry with multiple regulation_segments
        physical_groups = defaultdict(
            lambda: {
                "geoms": [],
                "zone_to_name": {},
                "mgmt_units": set(),
                "wb_keys": set(),
                "watershed_codes": set(),
                "name_variants": {},  # name -> from_tributary (False wins if both exist)
                # Track each regulation segment separately
                "segments": [],  # List of {group_id, regulation_ids, feature_ids, length_m}
            }
        )

        for group in self.merged_groups.values():
            reg_names = self._get_reg_names(
                list(group.regulation_ids), list(group.feature_ids)
            )

            # Skip empty groups
            if not group.feature_ids:
                continue

            ftype_val = group.feature_type
            ftype = FeatureType(ftype_val) if ftype_val else FeatureType.UNKNOWN
            gnis_name = group.gnis_name

            if not gnis_name and not reg_names:
                continue

            # Determine grouping identity per feature type
            wsc = group.fwa_watershed_code or ""
            if ftype_val != FeatureType.STREAM.value:
                # Lakes/manmade/wetlands: group by waterbody_key (unique per physical waterbody)
                grouping_id = group.waterbody_key or ""
            else:
                # Streams: group by watershed code (standard stream identity)
                grouping_id = wsc

            # 999-* is a catch-all/dummy code — never merge disparate features on it
            if not grouping_id or grouping_id.startswith("999-"):
                grouping_id = group.group_id  # Keep as separate entry

            physical_key = (grouping_id, gnis_name, ftype_val)

            sg = physical_groups[physical_key]
            if group.waterbody_key:
                sg["wb_keys"].add(group.waterbody_key)
            # Merge name_variants dicts: name -> from_tributary (False wins if both exist)
            self._merge_name_variants(sg["name_variants"], group.name_variants)
            if group.fwa_watershed_code:
                sg["watershed_codes"].add(group.fwa_watershed_code)
            for z, n in zip(group.zones or [], group.region_names or []):
                sg["zone_to_name"][z] = n
            sg["mgmt_units"].update(group.mgmt_units or [])

            prefix = f"{ftype.value.upper()}_" if ftype != FeatureType.STREAM else ""
            geoms_dict = (
                self._stream_geometries
                if ftype == FeatureType.STREAM
                else self._polygon_geometries
            )

            segment_geoms = []  # Track geoms for this segment's bbox
            for fid in group.feature_ids:
                if geom := geoms_dict.get(f"{prefix}{fid}" if prefix else fid):
                    extracted = self._extract_geoms(geom)
                    sg["geoms"].extend(extracted)
                    segment_geoms.extend(extracted)

            # Calculate length for this segment
            if ftype_val == FeatureType.STREAM.value:
                segment_length_m = sum(
                    (m.get("length") or 0)
                    for fid in group.feature_ids
                    if (m := self.gazetteer.get_stream_metadata(fid))
                )
            else:
                segment_length_m = (
                    sum(g.area for g in sg["geoms"]) if sg["geoms"] else 0
                )

            # Add this regulation segment
            sg["segments"].append(
                {
                    "group_id": group.group_id,
                    "regulation_ids": list(group.regulation_ids),
                    "feature_ids": list(group.feature_ids),
                    "length_m": segment_length_m,
                    "name_variants": list(
                        group.name_variants
                    ),  # Per-segment name variants
                    "gnis_name": gnis_name,
                    "geoms": segment_geoms,  # Track geoms for segment-level bbox
                }
            )

        search_items = []
        for (grouping_id, gnis, ftype_val), data in physical_groups.items():
            if not data["geoms"]:
                continue

            # Sort segments by length (longest first) for disambiguation
            segments = sorted(
                data["segments"], key=lambda s: s["length_m"], reverse=True
            )

            # Union of all regulation_ids across segments
            all_reg_ids = set()
            all_group_ids = []
            total_feature_count = 0
            for seg in segments:
                all_reg_ids.update(seg["regulation_ids"])
                all_group_ids.append(seg["group_id"])
                total_feature_count += len(seg["feature_ids"])
            reg_ids = sorted(all_reg_ids)

            if ftype_val == FeatureType.STREAM.value:
                # Collect all feature_ids from segments
                all_feature_ids = [
                    fid for seg in segments for fid in seg["feature_ids"]
                ]
                mag = max(
                    (
                        (m.get("stream_magnitude") or 0)
                        for fid in all_feature_ids
                        if (m := self.gazetteer.get_stream_metadata(fid))
                    ),
                    default=0,
                )
                min_zoom = self._calculate_stream_minzoom(mag)
            elif ftype_val == FeatureType.UNGAZETTED.value:
                min_zoom = 10
                all_feature_ids = [
                    fid for seg in segments for fid in seg["feature_ids"]
                ]
            else:
                min_zoom = self._calculate_polygon_minzoom(
                    sum(g.area for g in data["geoms"])
                )
                all_feature_ids = [
                    fid for seg in segments for fid in seg["feature_ids"]
                ]

            # Optimized Bound calculation using numpy
            wgs84_bounds = self._geoms_to_wgs84_bbox(data["geoms"])

            reg_names = self._get_reg_names(reg_ids, all_feature_ids)
            # Convert name_variants dict to sorted list of dicts
            name_variants = [
                {"name": name, "from_tributary": is_trib}
                for name, is_trib in sorted(data["name_variants"].items())
            ]

            # Total length from all segments
            total_length_m = sum(seg["length_m"] for seg in segments)
            if ftype_val == FeatureType.STREAM.value:
                length_km = round(total_length_m / 1000.0, 2)
            elif ftype_val == FeatureType.UNGAZETTED.value:
                length_km = 0.0
            else:
                length_km = round(total_length_m / 1_000_000.0, 2)

            # Identity key: watershed_code for streams, waterbody_key/group_id for lakes
            if ftype_val == FeatureType.STREAM.value:
                stream_key = next(iter(data["watershed_codes"]), "") or grouping_id
            else:
                # For lakes, prefer the waterbody_key we grouped on
                stream_key = next(iter(sorted(data["wb_keys"])), "") or grouping_id

            # Consolidate segments by regulation set (same regs = same frontend group)
            # Key: tuple of sorted regulation_ids
            consolidated = {}
            for seg in segments:
                seg_reg_ids = tuple(sorted(seg["regulation_ids"]))
                if seg_reg_ids in consolidated:
                    # Merge into existing
                    existing = consolidated[seg_reg_ids]
                    existing["length_m"] += seg["length_m"]
                    existing["feature_ids"].extend(seg["feature_ids"])
                    existing["geoms"].extend(seg.get("geoms", []))  # Merge geoms
                    # Merge name_variants dicts: name -> from_tributary (False wins)
                    self._merge_name_variants(
                        existing["name_variants"], seg["name_variants"]
                    )
                    existing["group_ids"].append(seg["group_id"])
                else:
                    # Convert list of dicts to name -> from_tributary dict
                    nv_dict = {
                        nv["name"]: nv["from_tributary"] for nv in seg["name_variants"]
                    }
                    consolidated[seg_reg_ids] = {
                        "regulation_ids": seg_reg_ids,
                        "length_m": seg["length_m"],
                        "feature_ids": list(seg["feature_ids"]),
                        "geoms": list(seg.get("geoms", [])),  # Track geoms for bbox
                        "name_variants": nv_dict,
                        "gnis_name": seg["gnis_name"],
                        "group_ids": [seg["group_id"]],
                    }

            # Sort consolidated segments by length (longest first)
            sorted_consolidated = sorted(
                consolidated.values(), key=lambda s: s["length_m"], reverse=True
            )

            # Build regulation segments for frontend disambiguation
            reg_segments = []
            for seg in sorted_consolidated:
                seg_reg_ids = sorted(seg["regulation_ids"])
                seg_reg_names = self._get_reg_names(seg_reg_ids, seg["feature_ids"])
                # Compute frontend_group_id for this segment
                frontend_group_id = self._compute_frontend_group_id(
                    stream_key, seg["gnis_name"], tuple(seg_reg_ids)
                )

                # Compute segment-specific bbox from its geometries
                seg_geoms = seg.get("geoms", [])
                if seg_geoms:
                    seg_bbox_wgs84 = list(self._geoms_to_wgs84_bbox(seg_geoms))
                else:
                    seg_bbox_wgs84 = None

                reg_segments.append(
                    {
                        "frontend_group_id": frontend_group_id,
                        "group_id": seg["group_ids"][0],  # Primary group_id
                        "group_ids": seg["group_ids"],  # All group_ids for this reg set
                        "regulation_ids": ",".join(seg_reg_ids),
                        "regulation_names": seg_reg_names,
                        "name_variants": [
                            {"name": name, "from_tributary": is_trib}
                            for name, is_trib in sorted(seg["name_variants"].items())
                        ],
                        "length_km": (
                            round(seg["length_m"] / 1000.0, 2)
                            if ftype_val == FeatureType.STREAM.value
                            else round(seg["length_m"] / 1_000_000.0, 2)
                        ),
                        "bbox": seg_bbox_wgs84,  # Per-segment bbox for fly-to
                    }
                )

            search_items.append(
                {
                    "id": f"{gnis}|{stream_key}|{ftype_val}",
                    "gnis_name": gnis,
                    "regulation_names": reg_names,
                    "name_variants": name_variants,
                    "type": ftype_val,
                    "zones": ",".join(sorted(data["zone_to_name"].keys())),
                    "mgmt_units": ",".join(sorted(data["mgmt_units"])),
                    "region_name": ",".join(
                        data["zone_to_name"][z]
                        for z in sorted(data["zone_to_name"].keys())
                    ),
                    "regulation_ids": ",".join(reg_ids),
                    "segment_count": total_feature_count,
                    "length_km": length_km,
                    "bbox": list(wgs84_bounds),
                    "min_zoom": min_zoom,
                    "properties": {
                        "group_id": all_group_ids[0] if all_group_ids else "",
                        "group_ids": all_group_ids,
                        "waterbody_key": ",".join(sorted(data["wb_keys"])),
                        "fwa_watershed_code": stream_key,
                        "regulation_count": len(reg_ids),
                    },
                    # Array of regulation segments for disambiguation
                    "regulation_segments": reg_segments,
                }
            )

        return search_items

    def export_waterbody_data(self, output_path: Path) -> Path:
        """Export unified waterbody_data.json with waterbodies and regulations.

        This is the single source of truth for the frontend, containing:
        - waterbodies: Search/map data with regulation segments
        - regulations: Full regulation details keyed by regulation_id
        """
        waterbodies = self._build_waterbodies_list()
        regulations = self._build_regulations_lookup()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {"waterbodies": waterbodies, "regulations": regulations},
                f,
                indent=2,
                ensure_ascii=False,
            )
        logger.info(
            f"Created {output_path} "
            f"({output_path.stat().st_size / 1048576:.1f} MB, "
            f"{len(waterbodies)} waterbodies, {len(regulations)} regulations)"
        )
        return output_path
