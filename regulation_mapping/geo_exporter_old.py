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
from shapely.ops import unary_union
from shapely.prepared import prep as shapely_prep, PreparedGeometry

from .regulation_mapper import PipelineResult, MergedGroup, parse_base_regulation_id
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
# Admin areas disappear ~1 zoom level sooner than lakes for the same size,
# so they don't clutter the map at medium zoom levels.
ADMIN_ZOOM_THRESHOLDS = {
    4: 500_000_000,
    5: 50_000_000,
    6: 5_000_000,
    7: 1_000_000,
    8: 100_000,
    9: 10_000,
    10: 5_000,
    11: 0,
}

# Pre-computed zoom lookups: list of (min_value, zoom_level) tuples.
# Sorted descending by threshold — first match gives the lowest visible zoom.
_LAKE_ZOOM_LOOKUP: List[Tuple[float, int]] = [
    (limit, zoom + 1) for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items())
]
_ADMIN_ZOOM_LOOKUP: List[Tuple[float, int]] = [
    (limit, zoom + 1) for zoom, limit in sorted(ADMIN_ZOOM_THRESHOLDS.items())
]

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
        self._canonical_features: Optional[List[dict]] = None
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

        base_ids = {parse_base_regulation_id(r) for r in reg_ids}
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
        if self._canonical_features is None:
            self._canonical_features = self._build_canonical_features()

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
            blk: self._calculate_minzoom(
                v["max_magnitude"] or 0, self._stream_zoom_thresholds
            )
            for blk, v in self._compute_blk_stats(
                self._stream_geometries.keys()
            ).items()
        }

    @staticmethod
    def _calculate_minzoom(
        value: float,
        thresholds: List[Tuple[float, int]],
        default: int = 12,
    ) -> int:
        """Return the minimum zoom level for a given value using threshold lookup.

        *thresholds* is a list of ``(min_value, zoom_level)`` tuples sorted
        descending by threshold.  Returns the zoom from the first tuple
        where ``value >= min_value``.
        """
        return next(
            (zoom for threshold, zoom in thresholds if value >= threshold),
            default,
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
        display_name: str,
        regulation_ids: tuple,
    ) -> str:
        """Compute a unique frontend group ID for highlighting.

        Groups by: watershed_code + display_name + sorted regulation_ids.
        Using display_name (which incorporates FeatureNameVariation names)
        ensures that features with different assigned names (e.g., a named
        side channel vs the unnamed mainstem) get distinct frontend IDs
        and highlight independently on the map.
        """
        key = f"{watershed_code or ''}|{display_name or ''}|{','.join(sorted(regulation_ids))}"
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def _emit_group_feature(
        self,
        group: MergedGroup,
        base_props: dict,
        suffix: str = "",
        reg_ids: Optional[tuple] = None,
        geometry: Optional[BaseGeometry] = None,
    ) -> dict:
        """Build a single canonical feature dict for export.

        Every merged-group feature flows through this method, ensuring a
        consistent schema across GPKG, PMTiles, and JSON exports.

        Args:
            group: The ``MergedGroup``.
            base_props: Full metadata dict (all fields except IDs/geometry).
            suffix: Appended to group_id (e.g. ``'_admin_in'``).
            reg_ids: Override regulation IDs (defaults to ``group.regulation_ids``).
            geometry: The shapely geometry to use.
        """
        rids = reg_ids if reg_ids is not None else group.regulation_ids
        display_name = group.display_name

        # Compute grouping key for frontend_group_id:
        # streams → fwa_watershed_code, polygons → waterbody_key, else group_id
        ftype = base_props["feature_type"]
        if ftype == FeatureType.STREAM.value:
            gk = (base_props.get("fwa_watershed_code") or "").split(",")[0].strip()
        else:
            gk = str(base_props.get("waterbody_key") or "")
        if not gk:
            gk = group.group_id
        frontend_group_id = self._compute_frontend_group_id(gk, display_name, rids)

        # Compute length: stream geometry length (m) / polygon area (m²)
        if geometry is None:
            length_m = 0.0
        elif ftype == FeatureType.STREAM.value:
            length_m = geometry.length
        elif ftype == FeatureType.UNGAZETTED.value:
            length_m = 0.0
        else:
            length_m = geometry.area

        return {
            **base_props,
            "group_id": f"{group.group_id}{suffix}" if suffix else group.group_id,
            "frontend_group_id": frontend_group_id,
            "regulation_ids": ",".join(sorted(rids)),
            "regulation_count": len(rids),
            "length_m": length_m,
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
            if "regulation_ids" not in feat:
                raise ValueError(
                    f"Feature missing 'regulation_ids': "
                    f"group_id={feat.get('group_id')}"
                )
            key = (
                feat["blue_line_key"],
                feat["waterbody_key"],
                feat["regulation_ids"],
            )
            groups[key].append(feat)

        merged = []
        merge_count = 0
        for key, group_features in groups.items():
            if len(group_features) == 1:
                merged.append(group_features[0])
                continue

            all_geoms = []
            for feat in group_features:
                geom = feat["geometry"]
                if geom is not None:
                    all_geoms.extend(self._extract_geoms(geom))

            if not all_geoms:
                merged.append(group_features[0])
                continue

            template = dict(group_features[0])
            template["geometry"] = self._merge_lines(all_geoms)
            template["length_m"] = template["geometry"].length

            # Prefer the group_id without admin suffix
            group_ids = [f["group_id"] for f in group_features]
            base_ids = [
                g for g in group_ids if not g.endswith(("_admin_in", "_admin_out"))
            ]
            template["group_id"] = base_ids[0] if base_ids else group_ids[0]

            # Union mgmt_units across merged features
            all_mu = set()
            for feat in group_features:
                for mu in feat["mgmt_units"].split(","):
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

    # --- CANONICAL FEATURE BUILD ---

    def _build_group_base_props(self, group: MergedGroup) -> dict:
        """Build shared metadata dict from MergedGroup fields.

        Contains everything except group_id, frontend_group_id,
        regulation_ids, regulation_count, length_m, and geometry —
        those are added by ``_emit_group_feature``.
        """
        return {
            "feature_type": group.feature_type,
            "waterbody_key": group.waterbody_key,
            "blue_line_key": group.blue_line_key,
            "fwa_watershed_code": group.fwa_watershed_code,
            "gnis_name": group.gnis_name,
            "display_name": group.display_name,
            "display_name_override": group.display_name_override,
            "inherited_gnis_name": group.inherited_gnis_name,
            "name_variants": json.dumps([dict(nv) for nv in group.name_variants]),
            "zones": ",".join(group.zones),
            "mgmt_units": ",".join(group.mgmt_units),
            "region_name": ",".join(group.region_names),
            "feature_count": group.feature_count,
            "feature_ids": ",".join(group.feature_ids),
        }

    def _build_stream_canonical(
        self,
        group: MergedGroup,
        blk_zooms: dict,
        admin_reg_ids: set,
        admin_clip_union: Optional[BaseGeometry],
        admin_prep: Optional[PreparedGeometry],
        clip_stats: dict,
    ) -> List[dict]:
        """Build canonical feature(s) for a stream group, with admin clipping."""
        geom_list = []
        max_order = 0

        for fid in group.feature_ids:
            geom = self._stream_geometries.get(fid)
            meta = self.gazetteer.get_stream_metadata(fid)
            if geom:
                geom_list.extend(self._extract_geoms(geom))
            if meta:
                max_order = max(max_order, meta.get("stream_order") or 0)

        if not geom_list:
            return []

        blk = group.blue_line_key
        if blk is None:
            logger.warning(
                f"Stream group {group.group_id} has no blue_line_key — "
                f"feature_ids: {list(group.feature_ids)[:3]}"
            )

        base_props = self._build_group_base_props(group)
        base_props["stream_order"] = max_order
        base_props["tippecanoe:minzoom"] = blk_zooms.get(blk, 12)

        # Check if admin boundary clipping is needed
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
            clip_stats[disposition] += 1
            return clip_results

        merged_geom = self._merge_lines(geom_list)
        return [self._emit_group_feature(group, base_props, geometry=merged_geom)]

    def _build_polygon_canonical(self, group: MergedGroup) -> List[dict]:
        """Build canonical feature(s) for a polygon group (lake/wetland/manmade)."""
        ftype_enum = FeatureType(group.feature_type)
        prefix = f"{ftype_enum.value.upper()}_"

        geom_list = []
        for fid in group.feature_ids:
            geom = self._polygon_geometries.get(f"{prefix}{fid}")
            if geom:
                geom_list.extend(self._extract_geoms(geom))

        if not geom_list:
            return []

        # Dissolve all polygon parts into a single geometry using unary_union.
        # Large lakes in FWA are often split across multiple WATERBODY_POLY_IDs
        # sharing one waterbody_key (e.g., Williston Lake = 3 polygons).
        # unary_union merges shared edges so they render as one clean shape,
        # rather than MultiPolygon with disjoint parts retaining seam lines.
        merged_geom = unary_union(geom_list)

        base_props = self._build_group_base_props(group)
        base_props["area_sqm"] = merged_geom.area
        base_props["tippecanoe:minzoom"] = self._calculate_minzoom(
            merged_geom.area, _LAKE_ZOOM_LOOKUP
        )
        return [self._emit_group_feature(group, base_props, geometry=merged_geom)]

    def _build_ungazetted_canonical(self, group: MergedGroup) -> Optional[dict]:
        """Build canonical feature for an ungazetted waterbody group."""
        from .linking_corrections import UNGAZETTED_WATERBODIES

        uid = group.feature_ids[0]
        uw = UNGAZETTED_WATERBODIES.get(uid)
        if not uw:
            logger.warning(
                f"Ungazetted waterbody {uid} not found in UNGAZETTED_WATERBODIES"
            )
            return None
        if uw.geometry_type != "point":
            return None

        base_props = self._build_group_base_props(group)
        base_props["ungazetted_id"] = uid
        base_props["tippecanoe:minzoom"] = 10

        return self._emit_group_feature(
            group, base_props, geometry=Point(uw.coordinates)
        )

    def _build_canonical_features(self) -> List[dict]:
        """Build canonical feature dicts — single source of truth for all exports.

        Iterates ``merged_groups`` once and produces one dict per output
        feature.  Streams may produce multiple features via admin boundary
        clipping.  All metadata is sourced from ``MergedGroup`` fields;
        only per-feature physical properties (stream_order, area_sqm,
        geometry) come from the gazetteer.

        Consumed by layer creators (GPKG/PMTiles) and the JSON search index.
        """
        features: List[dict] = []

        # Pre-compute admin clipping data (streams only)
        admin_reg_ids = self._get_all_admin_reg_ids()
        admin_clip_union = self._build_admin_clip_union() if admin_reg_ids else None
        admin_prep = (
            shapely_prep(admin_clip_union) if admin_clip_union is not None else None
        )
        blk_zooms = self._get_synchronized_blk_zooms()
        clip_stats = {"inside": 0, "outside": 0, "clipped": 0, "fallback": 0}

        for group in self.merged_groups.values():
            if not group.feature_ids:
                continue

            ftype = group.feature_type

            if ftype == FeatureType.STREAM.value:
                features.extend(
                    self._build_stream_canonical(
                        group,
                        blk_zooms,
                        admin_reg_ids,
                        admin_clip_union,
                        admin_prep,
                        clip_stats,
                    )
                )
            elif ftype == FeatureType.UNGAZETTED.value:
                feat = self._build_ungazetted_canonical(group)
                if feat:
                    features.append(feat)
            elif ftype in (
                FeatureType.LAKE.value,
                FeatureType.WETLAND.value,
                FeatureType.MANMADE.value,
            ):
                features.extend(self._build_polygon_canonical(group))

        # Post-process streams: merge pieces with identical BLK+WBK+regulation_ids
        stream_feats = [
            f for f in features if f["feature_type"] == FeatureType.STREAM.value
        ]
        other_feats = [
            f for f in features if f["feature_type"] != FeatureType.STREAM.value
        ]
        stream_feats = self._merge_same_regulation_features(stream_feats)
        features = stream_feats + other_feats

        if any(clip_stats.values()):
            logger.debug(
                f"Admin boundary clipping: "
                f"{clip_stats['inside']} fully inside, "
                f"{clip_stats['clipped']} split at boundary, "
                f"{clip_stats['outside']} fully outside, "
                f"{clip_stats['fallback']} fallback"
            )

        logger.info(
            f"Built {len(features)} canonical features "
            f"({len(stream_feats)} streams, {len(other_feats)} other)"
        )
        return features

    # --- LAYER CREATION (from canonical features) ---

    def _get_cached_layer(
        self,
        cache_key: Any,
        filter_fn: Callable[[dict], bool],
    ) -> Optional[gpd.GeoDataFrame]:
        """Create and cache a GeoDataFrame layer by filtering canonical features.

        Shared implementation for all canonical-feature layers: check cache,
        apply *filter_fn*, build GeoDataFrame, store in cache.
        """
        if cache_key in self._layer_cache:
            return self._layer_cache[cache_key]

        features = [f for f in (self._canonical_features or []) if filter_fn(f)]
        result = gpd.GeoDataFrame(features, crs="EPSG:3005") if features else None
        self._layer_cache[cache_key] = result
        return result

    def _create_streams_layer(
        self,
        exclude_lake_streams: bool = False,
    ) -> Optional[gpd.GeoDataFrame]:
        """Create streams layer by filtering canonical features."""
        lake_wbkeys = self._get_lake_manmade_wbkeys() if exclude_lake_streams else set()
        return self._get_cached_layer(
            ("streams", exclude_lake_streams),
            lambda f: (
                f["feature_type"] == FeatureType.STREAM.value
                and not (
                    exclude_lake_streams
                    and f["waterbody_key"]
                    and f["waterbody_key"] in lake_wbkeys
                )
            ),
        )

    def _create_polygon_layer(
        self,
        ftype_enum: FeatureType,
    ) -> Optional[gpd.GeoDataFrame]:
        """Create polygon layer by filtering canonical features."""
        return self._get_cached_layer(
            f"poly_{ftype_enum.value}",
            lambda f: f["feature_type"] == ftype_enum.value,
        )

    def _create_ungazetted_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Create ungazetted waterbody point layer from canonical features."""
        return self._get_cached_layer(
            "ungazetted",
            lambda f: f["feature_type"] == FeatureType.UNGAZETTED.value,
        )

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
        # only appear when the user is zoomed in (slightly more aggressive
        # than lakes so admin overlays don't clutter medium-zoom views).
        gdf["tippecanoe:minzoom"] = gdf.geometry.area.apply(
            lambda a: self._calculate_minzoom(a, _ADMIN_ZOOM_LOOKUP)
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

    def _create_management_units_fill_layer(self) -> Optional[gpd.GeoDataFrame]:
        """Build a lightweight polygon fill layer for management-unit centroid labels.

        Keeps the original polygon geometry (not converted to boundary lines)
        so the frontend can use ``symbol-placement: 'point'`` to place labels
        at the centroid of each unit.
        """
        if "wmu" not in self.data_accessor.list_layers():
            logger.warning(
                "'wmu' layer not found in GPKG — skipping management_units_fill"
            )
            return None

        mu_gdf = self.data_accessor.get_layer("wmu").to_crs("EPSG:3005")
        mu_gdf["mu_code"] = mu_gdf["WILDLIFE_MGMT_UNIT_ID"]
        mu_gdf["tippecanoe:minzoom"] = 4

        return mu_gdf[
            [
                "mu_code",
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

    # Columns included in PMTiles output per layer type.
    # Everything else is stripped to keep tile size small.
    _PMTILES_COLUMNS = {
        "streams": {
            "group_id",
            "frontend_group_id",
            "display_name",
            "waterbody_key",
            "blue_line_key",
            "stream_order",
            "tippecanoe:minzoom",
        },
        "lakes": {
            "group_id",
            "frontend_group_id",
            "display_name",
            "waterbody_key",
            "area_sqm",
            "tippecanoe:minzoom",
        },
        "wetlands": {
            "group_id",
            "frontend_group_id",
            "display_name",
            "waterbody_key",
            "area_sqm",
            "tippecanoe:minzoom",
        },
        "manmade": {
            "group_id",
            "frontend_group_id",
            "display_name",
            "waterbody_key",
            "area_sqm",
            "tippecanoe:minzoom",
        },
        "ungazetted": {
            "ungazetted_id",
            "group_id",
            "frontend_group_id",
            "display_name",
            "tippecanoe:minzoom",
        },
    }

    def _get_layer_configs(
        self,
        exclude_lake_streams: bool = False,
        include_regions: bool = True,
    ) -> List[Tuple[str, Callable]]:
        """Return layer name/creator pairs for GPKG and PMTiles exports."""
        layers = [
            ("lakes", lambda: self._create_polygon_layer(FeatureType.LAKE)),
            ("wetlands", lambda: self._create_polygon_layer(FeatureType.WETLAND)),
            ("manmade", lambda: self._create_polygon_layer(FeatureType.MANMADE)),
            (
                "streams",
                lambda: self._create_streams_layer(
                    exclude_lake_streams=exclude_lake_streams
                ),
            ),
        ]
        if include_regions:
            layers.append(("regions", lambda: self._create_regions_layer()))
            layers.append(
                ("management_units", lambda: self._create_management_units_layer())
            )
            layers.append(
                (
                    "management_units_fill",
                    lambda: self._create_management_units_fill_layer(),
                )
            )
            layers.append(("bc_mask", lambda: self._create_bc_mask_layer()))

        layers.append(("ungazetted", lambda: self._create_ungazetted_layer()))

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

    def export_gpkg(self, output_path: Path) -> Optional[Path]:
        """Export all layers to GPKG with full metadata for debugging."""
        if self._is_file_locked(output_path):
            return None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()

        self._preload_data()
        layer_count = 0

        for name, create_fn in self._get_layer_configs(exclude_lake_streams=False):
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
        work_dir: Optional[Path] = None,
    ) -> Optional[Path]:
        """Export lean PMTiles for frontend map rendering."""
        if self._is_file_locked(output_path):
            return None
        work_dir = work_dir or output_path.parent / "temp"
        work_dir.mkdir(parents=True, exist_ok=True)
        self._preload_data()

        layer_files = []
        for name, create_fn in self._get_layer_configs(exclude_lake_streams=True):
            if (gdf := create_fn()) is not None and not gdf.empty:
                # Strip columns not needed in tiles
                keep = self._PMTILES_COLUMNS.get(name)
                if keep:
                    drop_cols = [
                        c for c in gdf.columns if c not in keep and c != "geometry"
                    ]
                    gdf = gdf.drop(columns=drop_cols, errors="ignore")

                layer_path = work_dir / f"{name}.geojsonseq"
                with open(layer_path, "w") as f:
                    for _, row in gdf.to_crs("EPSG:4326").iterrows():
                        props = {
                            k: v for k, v in row.drop("geometry").items() if pd.notna(v)
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
            "--simplification=10",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            "--detect-shared-borders",
            "--no-clipping",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--no-simplification-of-shared-nodes",
            "--maximum-tile-bytes=2500000",
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

    def _build_waterbodies_list(self) -> Dict[str, Any]:
        """Build the compact waterbody data export.

        Groups canonical features by physical identity; features with a
        GNIS name, display name, or waterbody-specific regulations become
        full entries in ``waterbodies``.  Unnamed zone-only features get
        a minimal ``compact`` entry (``frontend_group_id → reg_set_index``)
        so the frontend can resolve their regulations at click-time from
        the JSON alone — no tile-property additions needed.

        Returns a dict with:
            - ``waterbodies``: list of full search entries (short keys)
            - ``reg_sets``: deduplicated regulation-ID strings (list)
            - ``compact``: ``{frontend_group_id: reg_set_index}`` for unnamed features
        """
        self._preload_data()

        # --- Regulation-set dedup table ---
        _reg_set_list: List[str] = []
        _reg_set_index: Dict[str, int] = {}

        def _get_ri(reg_ids_str: str) -> int:
            """Return the index into reg_sets for this regulation string."""
            if reg_ids_str in _reg_set_index:
                return _reg_set_index[reg_ids_str]
            idx = len(_reg_set_list)
            _reg_set_list.append(reg_ids_str)
            _reg_set_index[reg_ids_str] = idx
            return idx

        # --- Phase 1: Group canonical features by search key ---
        physical_groups: Dict[tuple, dict] = defaultdict(
            lambda: {
                "features": [],
                "zone_to_name": {},
                "mgmt_units": set(),
                "wb_keys": set(),
                "watershed_codes": set(),
                "name_variants": {},
            }
        )

        for feat in self._canonical_features:
            ftype = feat["feature_type"]
            display_name = feat["display_name"]

            # Skip features with zero regulations (nothing to show)
            if not feat["regulation_ids"]:
                continue

            # Determine grouping identity
            if ftype == FeatureType.STREAM.value:
                wsc = feat["fwa_watershed_code"] or ""
                grouping_id = (
                    wsc if wsc and not wsc.startswith("999-") else feat["group_id"]
                )
            else:
                grouping_id = str(feat["waterbody_key"] or "") or feat["group_id"]

            if not grouping_id:
                grouping_id = feat["group_id"]

            search_key = (grouping_id, display_name, ftype)
            sg = physical_groups[search_key]
            sg["features"].append(feat)

            if feat["waterbody_key"]:
                sg["wb_keys"].add(str(feat["waterbody_key"]))
            if feat.get("fwa_watershed_code"):
                sg["watershed_codes"].add(feat["fwa_watershed_code"])

            # Merge zones/region_names
            zones = feat["zones"].split(",") if feat["zones"] else []
            region_names = feat["region_name"].split(",") if feat["region_name"] else []
            for z, n in zip(zones, region_names):
                if z:
                    sg["zone_to_name"][z] = n
            sg["mgmt_units"].update(
                mu
                for mu in (feat["mgmt_units"].split(",") if feat["mgmt_units"] else [])
                if mu
            )

            # Merge name_variants (stored as JSON string on canonical features)
            nvs = json.loads(feat["name_variants"]) if feat["name_variants"] else []
            self._merge_name_variants(sg["name_variants"], nvs)

        # --- Phase 2: Build search items (full entries only) ---
        search_items = []
        compact_entries: Dict[str, int] = {}  # frontend_group_id → reg_set_index
        skipped_unnamed = 0

        for (grouping_id, display_name_key, ftype_val), data in physical_groups.items():
            feats = data["features"]
            if not feats:
                continue

            # Collect all geometries for bbox
            all_geoms = []
            for f in feats:
                geom = f["geometry"]
                if geom:
                    all_geoms.extend(self._extract_geoms(geom))
            if not all_geoms:
                continue

            # --- Consolidate features by regulation set into segments ---
            consolidated: Dict[str, dict] = {}
            for f in feats:
                reg_key = f["regulation_ids"]
                if reg_key in consolidated:
                    seg = consolidated[reg_key]
                    seg["length_m"] += f["length_m"]
                    seg["feature_ids_set"].update(
                        fid for fid in f["feature_ids"].split(",") if fid
                    )
                    seg_geom = f["geometry"]
                    if seg_geom:
                        seg["geoms"].extend(self._extract_geoms(seg_geom))
                    seg_nvs = (
                        json.loads(f["name_variants"]) if f["name_variants"] else []
                    )
                    self._merge_name_variants(seg["name_variants"], seg_nvs)
                    seg["group_ids"].append(f["group_id"])
                    seg["frontend_group_ids"].add(f["frontend_group_id"])
                else:
                    seg_geom = f["geometry"]
                    seg_geoms = self._extract_geoms(seg_geom) if seg_geom else []
                    seg_nvs = (
                        json.loads(f["name_variants"]) if f["name_variants"] else []
                    )
                    nv_dict = {nv["name"]: nv["from_tributary"] for nv in seg_nvs}
                    consolidated[reg_key] = {
                        "regulation_ids": reg_key,
                        "length_m": f["length_m"],
                        "feature_ids_set": set(
                            fid for fid in f["feature_ids"].split(",") if fid
                        ),
                        "geoms": list(seg_geoms),
                        "name_variants": nv_dict,
                        "display_name": f["display_name"],
                        "group_ids": [f["group_id"]],
                        "frontend_group_ids": {f["frontend_group_id"]},
                    }

            # Sort segments by length (longest first)
            sorted_segments = sorted(
                consolidated.values(), key=lambda s: s["length_m"], reverse=True
            )

            # Aggregate across all segments
            all_reg_ids: set = set()
            all_group_ids = []
            total_feature_ids: set = set()
            for seg in sorted_segments:
                all_reg_ids.update(
                    seg["regulation_ids"].split(",") if seg["regulation_ids"] else []
                )
                all_group_ids.extend(seg["group_ids"])
                total_feature_ids.update(seg["feature_ids_set"])
            reg_ids = sorted(all_reg_ids)

            # --- Classify: full entry or compact? ---
            # Named features get full searchable entries.
            # Unnamed features ALWAYS get compact entries (fgid → ri),
            # regardless of regulation type.  This keeps unnamed features
            # out of the search bar and Fuse.js index while still making
            # their regulations resolvable at click time via the compact dict.
            has_name = bool(display_name_key)

            if not has_name:
                # Unnamed → add compact entries (fgid → ri)
                skipped_unnamed += 1
                reg_ids_str = ",".join(reg_ids)
                ri = _get_ri(reg_ids_str)
                for seg in sorted_segments:
                    for fgid in seg["frontend_group_ids"]:
                        if fgid:
                            compact_entries[fgid] = ri
                continue

            # --- Full entry: build with short keys ---
            min_zoom = min(f["tippecanoe:minzoom"] for f in feats)
            wgs84_bounds = self._geoms_to_wgs84_bbox(all_geoms)

            name_variants = [
                {"name": name, "ft": is_trib}
                for name, is_trib in sorted(data["name_variants"].items())
            ]

            total_length_m = sum(seg["length_m"] for seg in sorted_segments)
            if ftype_val == FeatureType.STREAM.value:
                length_km = round(total_length_m / 1000.0, 2)
            elif ftype_val == FeatureType.UNGAZETTED.value:
                length_km = 0.0
            else:
                length_km = round(total_length_m / 1_000_000.0, 2)

            if ftype_val == FeatureType.STREAM.value:
                stream_key = next(iter(data["watershed_codes"]), "") or grouping_id
            else:
                stream_key = next(iter(sorted(data["wb_keys"])), "") or grouping_id

            # Build regulation segments (short keys)
            reg_segments = []
            for seg in sorted_segments:
                seg_reg_ids_str = ",".join(
                    sorted(
                        seg["regulation_ids"].split(",")
                        if seg["regulation_ids"]
                        else []
                    )
                )
                frontend_group_id = next(iter(seg["frontend_group_ids"]))
                seg_bbox_wgs84 = (
                    list(self._geoms_to_wgs84_bbox(seg["geoms"]))
                    if seg["geoms"]
                    else None
                )
                seg_name_variants = [
                    {"name": name, "ft": is_trib}
                    for name, is_trib in sorted(seg["name_variants"].items())
                ]
                reg_segments.append(
                    {
                        "fgid": frontend_group_id,
                        "gid": seg["group_ids"][0],
                        "ri": _get_ri(seg_reg_ids_str),
                        "dn": seg["display_name"],
                        "nv": seg_name_variants,
                        "lkm": (
                            round(seg["length_m"] / 1000.0, 2)
                            if ftype_val == FeatureType.STREAM.value
                            else round(seg["length_m"] / 1_000_000.0, 2)
                        ),
                        "bbox": seg_bbox_wgs84,
                    }
                )

            all_frontend_group_ids = [
                seg["fgid"] for seg in reg_segments if seg.get("fgid")
            ]

            search_items.append(
                {
                    "id": f"{display_name_key}|{stream_key}|{ftype_val}",
                    "gn": display_name_key,
                    "fgids": all_frontend_group_ids,
                    "dn": display_name_key,
                    "nv": name_variants,
                    "type": ftype_val,
                    "z": sorted(data["zone_to_name"].keys()),
                    "mu": sorted(data["mgmt_units"]),
                    "rn": [
                        data["zone_to_name"][z]
                        for z in sorted(data["zone_to_name"].keys())
                    ],
                    "ri": _get_ri(",".join(reg_ids)),
                    "tlkm": length_km,
                    "bbox": list(wgs84_bounds),
                    "mz": min_zoom,
                    "props": {
                        "gid": all_group_ids[0] if all_group_ids else "",
                        "wk": ",".join(sorted(data["wb_keys"])),
                        "fwc": stream_key,
                        "rc": len(reg_ids),
                    },
                    "rs": reg_segments,
                }
            )

        logger.info(
            f"Waterbody data: {len(search_items)} full entries, "
            f"{skipped_unnamed} unnamed zone-only ({len(compact_entries)} compact fgids), "
            f"{len(_reg_set_list)} unique reg sets"
        )

        return {
            "waterbodies": search_items,
            "reg_sets": _reg_set_list,
            "compact": compact_entries,
        }

    def export_waterbody_data(self, output_path: Path) -> Path:
        """Export unified waterbody_data.json with waterbodies and regulations.

        This is the **single source of truth** for the frontend:

        - ``reg_sets``: Deduplicated regulation-ID strings (list)
        - ``compact``: ``{frontend_group_id: reg_set_index}`` for unnamed
          zone-only features — the frontend resolves regulations at
          click-time by looking up ``reg_sets[compact[fgid]]``.
        - ``waterbodies``: Full search entries for named / specific-reg
          features (short keys, reg_ids replaced by ``ri`` index)
        - ``regulations``: Full regulation detail dicts keyed by reg ID

        Short key mapping (backend → frontend decode):
            gn=gnis_name  dn=display_name  fgids=frontend_group_ids
            nv=name_variants  ft=from_tributary  z=zones  mu=mgmt_units
            rn=region_name  ri=reg_set_index  tlkm=total_length_km
            mz=min_zoom  rs=regulation_segments  fgid=frontend_group_id
            gid=group_id  lkm=length_km  wk=waterbody_key
            fwc=fwa_watershed_code  rc=regulation_count
        """
        build_result = self._build_waterbodies_list()
        waterbodies = build_result["waterbodies"]
        reg_sets = build_result["reg_sets"]
        compact = build_result["compact"]
        regulations = self._build_regulations_lookup()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "reg_sets": reg_sets,
                    "compact": compact,
                    "waterbodies": waterbodies,
                    "regulations": regulations,
                },
                f,
                ensure_ascii=False,
            )
        logger.info(
            f"Created {output_path} "
            f"({output_path.stat().st_size / 1048576:.1f} MB, "
            f"{len(waterbodies)} waterbodies, "
            f"{len(reg_sets)} reg_sets, "
            f"{len(compact)} compact, "
            f"{len(regulations)} regulations)"
        )
        return output_path
