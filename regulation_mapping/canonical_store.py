"""
CanonicalDataStore — single source of truth for geometry and feature data.

Loads FWA geometries, applies admin-boundary clipping, computes zoom
thresholds, and yields fully-hydrated feature dicts consumed by
``GeoArtifactGenerator`` (tiles / GPKG) and ``SearchIndexBuilder`` (JSON).

No IO operations happen here — this module is pure computation.
"""

from __future__ import annotations

import hashlib
import json
import pickle
from collections import defaultdict
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.prepared import prep as shapely_prep, PreparedGeometry

from data.data_extractor import FWADataAccessor
from fwa_pipeline.metadata_builder import ADMIN_LAYER_CONFIG
from fwa_pipeline.metadata_gazetteer import FeatureType

from .geometry_utils import (
    extract_geoms,
    extract_line_components,
    merge_lines,
    merge_overlapping_polygons,
)
from .logger_config import get_logger
from .regulation_types import MergedGroup, PipelineResult
from .regulation_resolvers import parse_base_regulation_id

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Optional progress bar
# ---------------------------------------------------------------------------
try:
    from tqdm import tqdm
except ImportError:
    logger.debug("tqdm not available; progress bars disabled")

    def tqdm(iterable: Iterable, **kwargs: Any) -> Iterable:  # type: ignore[misc]
        return iterable


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

# Admin areas disappear ~1 zoom level sooner than lakes for the same size.
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

# Eco reserves and aboriginal lands: ~3-5x more aggressive area filtering.
# Small polygons only appear at higher zoom to reduce visual clutter.
ADMIN_ZOOM_THRESHOLDS_AGGRESSIVE = {
    4: 1_500_000_000,
    5: 200_000_000,
    6: 20_000_000,
    7: 5_000_000,
    8: 500_000,
    9: 50_000,
    10: 10_000,
    11: 0,
}

# Pre-computed zoom lookups: list of (min_value, zoom_level) tuples.
_LAKE_ZOOM_LOOKUP: List[Tuple[float, int]] = [
    (limit, zoom + 1) for zoom, limit in sorted(LAKE_ZOOM_THRESHOLDS.items())
]
_ADMIN_ZOOM_LOOKUP: List[Tuple[float, int]] = [
    (limit, zoom + 1) for zoom, limit in sorted(ADMIN_ZOOM_THRESHOLDS.items())
]
_ADMIN_ZOOM_LOOKUP_AGGRESSIVE: List[Tuple[float, int]] = [
    (limit, zoom + 1)
    for zoom, limit in sorted(ADMIN_ZOOM_THRESHOLDS_AGGRESSIVE.items())
]

MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}

# Mapping of GeoPackage layer names to FeatureType enums
POLYGON_LAYERS = {
    "lakes": FeatureType.LAKE,
    "wetlands": FeatureType.WETLAND,
    "manmade_water": FeatureType.MANMADE,
}

# GPKG layer name for the tidal boundary polygon
TIDAL_BOUNDARY_LAYER = "tidal_boundary"


# ---------------------------------------------------------------------------
# CanonicalDataStore
# ---------------------------------------------------------------------------


class CanonicalDataStore:
    """Single source of truth for geometry loading, clipping, and feature hydration.

    Accepts the immutable ``PipelineResult`` from the mapper and the path
    to the FWA GeoPackage.  All downstream consumers (tile exporter, GPKG
    exporter, search index) read from this store — never from raw geometry
    files directly.
    """

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

        # Reuse gazetteer's data accessor when possible.
        self.data_accessor: FWADataAccessor = (
            self.gazetteer.data_accessor
            if self.gazetteer and self.gazetteer.data_accessor is not None
            else FWADataAccessor(self.gpkg_path)
        )

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Lazy caches
        self._stream_geometries: Optional[Dict[str, BaseGeometry]] = None
        self._polygon_geometries: Optional[Dict[str, BaseGeometry]] = None
        self._admin_gdf_cache: Dict[str, gpd.GeoDataFrame] = {}
        self._lake_manmade_wbkeys: Optional[set] = None

        self._valid_stream_ids: set = self.gazetteer.get_valid_stream_ids()
        self._stream_zoom_thresholds = self._calculate_percentile_thresholds()

        # Materialised canonical features — populated on first call to
        # ``get_canonical_features()`` and re-used by the search exporter.
        self._canonical_features: Optional[List[dict]] = None

        logger.info(
            f"CanonicalDataStore: {len(self.merged_groups)} merged groups, "
            f"{len(self.feature_to_regs)} individual features"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def preload(self) -> None:
        """Eagerly load all geometry caches.  Call before iteration."""
        self._load_all_polygon_geometries()
        self._load_all_stream_geometries()
        self._inject_ungazetted_geometries()

    def get_canonical_features(self) -> List[dict]:
        """Materialise and cache the full canonical feature list.

        The search exporter needs random-access to all features (grouping,
        bbox computation, consolidation).  ``yield_features`` is still the
        preferred path for the streaming tile/GPKG exporter; this method
        is provided for consumers that need the full list.
        """
        if self._canonical_features is None:
            self.preload()
            self._canonical_features = list(self.yield_features())
        return self._canonical_features

    def yield_features(self) -> Iterator[dict]:
        """Yield fully-hydrated canonical feature dicts.

        Each dict contains **all** metadata (debug + visual) plus a
        ``"geometry"`` key holding a shapely object.  Downstream consumers
        decide which columns to keep.

        Streams may yield multiple features per group when admin-boundary
        clipping splits a group into inside/outside pieces.
        """
        self.preload()

        # Pre-compute admin clipping data (streams only)
        admin_reg_ids = self._get_all_admin_reg_ids()
        admin_clip_union = self._build_admin_clip_union() if admin_reg_ids else None
        admin_prep = (
            shapely_prep(admin_clip_union) if admin_clip_union is not None else None
        )
        blk_zooms = self._get_synchronized_blk_zooms()
        clip_stats = {"inside": 0, "outside": 0, "clipped": 0, "fallback": 0}

        # Pre-compute tidal clipping data (done once, before the feature loop)
        tidal_clip_union = self._build_tidal_clip_union()
        tidal_prep = (
            shapely_prep(tidal_clip_union) if tidal_clip_union is not None else None
        )
        # Pre-segment the tidal polygon into a grid of small local tiles so
        # that difference() in _clip_streams_at_tidal_boundary runs against a
        # tiny local sub-polygon (same accuracy, orders-of-magnitude faster).
        tidal_tiles: List[BaseGeometry] = []
        tidal_tile_tree: Any = None
        if tidal_clip_union is not None:
            tidal_tiles, tidal_tile_tree = self._build_tidal_tiles(tidal_clip_union)

        # Accumulate features for post-merge; yield ungazetted directly
        stream_features: List[dict] = []
        polygon_features: List[dict] = []

        for group in self.merged_groups.values():
            if not group.feature_ids:
                continue

            ftype = group.feature_type

            if ftype == FeatureType.STREAM.value:
                stream_features.extend(
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
                    yield feat
            elif ftype in (
                FeatureType.LAKE.value,
                FeatureType.WETLAND.value,
                FeatureType.MANMADE.value,
            ):
                polygon_features.extend(self._build_polygon_canonical(group))

        # Post-merge polygons with identical waterbody_key (zone splits → unified)
        merged_polygons = self._merge_same_waterbody_polygons(polygon_features)

        # Post-merge streams with identical BLK+WBK+regulation_ids
        merged_streams = self._merge_same_regulation_features(stream_features)

        # ── Tidal boundary clipping (runs last) ─────────────────────
        # Streams: clip at tidal boundary, keep only the freshwater side.
        # Wetlands: remove entirely if inside the tidal polygon.
        # Lakes: unchanged.
        if tidal_clip_union is not None:
            logger.info(
                f"Tidal clipping: {len(merged_streams):,} streams, "
                f"{len(merged_polygons):,} polygons..."
            )
            merged_streams = self._clip_streams_at_tidal_boundary(
                merged_streams,
                tidal_clip_union,
                tidal_prep,
                tidal_tiles,
                tidal_tile_tree,
            )
            merged_polygons = self._remove_tidal_wetlands(
                merged_polygons, tidal_clip_union
            )

        # ── Aboriginal lands advisory (non-splitting) ───────────────
        # Instead of clipping at aboriginal lands boundaries, attach a
        # single advisory regulation to any merged feature whose geometry
        # intersects aboriginal lands.  Applies to both polygons and
        # streams — avoids unwieldy section splits while still informing
        # anglers.
        merged_polygons = self._attach_aboriginal_advisory(merged_polygons)
        yield from merged_polygons

        merged_streams = self._attach_aboriginal_advisory(merged_streams)

        # Stamp each stream feature with is_under_lake so downstream
        # consumers (geo_exporter, search_exporter) use a single flag
        # instead of duplicating lake-key filtering logic.
        lake_wbkeys = self.get_lake_manmade_wbkeys()
        for feat in merged_streams:
            fids = [fid for fid in feat["feature_ids"].split(",") if fid]
            # A feature is under a lake when every constituent edge has a
            # waterbody_key that belongs to a lake or manmade polygon.
            # Edges with no waterbody_key (open air / river polygon) mean
            # the feature is NOT fully under a lake.
            feat["is_under_lake"] = bool(fids) and all(
                str(
                    (self.gazetteer.get_stream_metadata(fid) or {}).get(
                        "waterbody_key", ""
                    )
                    or ""
                )
                in lake_wbkeys
                for fid in fids
            )
        yield from merged_streams

        if any(clip_stats.values()):
            logger.debug(
                f"Admin boundary clipping: "
                f"{clip_stats['inside']} fully inside, "
                f"{clip_stats['clipped']} split at boundary, "
                f"{clip_stats['outside']} fully outside, "
                f"{clip_stats['fallback']} fallback"
            )
        logger.info(
            f"Yielded {len(merged_streams)} stream + "
            f"{len(merged_polygons)} polygon features + ungazetted"
        )

    # ------------------------------------------------------------------
    # Waterbody-key helpers
    # ------------------------------------------------------------------

    def get_lake_manmade_wbkeys(self) -> set:
        """Return waterbody_keys belonging to lakes or manmade waterbodies.

        Streams overlapping these polygons are excluded from the PMTiles
        stream layer (the polygon layer already renders the waterbody).
        Wetland keys are deliberately excluded so streams through wetlands
        are kept.
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

    # ------------------------------------------------------------------
    # Lookups & metadata
    # ------------------------------------------------------------------

    def expand_admin_reg_ids(self, base_ids: set) -> set:
        """Expand base synopsis regulation IDs to their ``_ruleN`` variants.

        The ``admin_area_reg_map`` stores base regulation IDs (e.g.
        ``reg_00618``) for synopsis-sourced regulations, but the
        ``regulations.json`` keys use per-rule suffixes
        (``reg_00618_rule0``, ``reg_00618_rule1``, …).  Provincial and
        zone IDs are already exact keys and pass through unchanged.
        """
        reg_details = self.pipeline_result.regulation_details
        expanded: set = set()
        for rid in base_ids:
            if rid in reg_details:
                expanded.add(rid)
            else:
                rule_keys = [k for k in reg_details if k.startswith(f"{rid}_rule")]
                if rule_keys:
                    expanded.update(rule_keys)
                else:
                    logger.error(
                        f"expand_admin_reg_ids: regulation '{rid}' not found in "
                        f"regulation_details and has no _ruleN variants — skipping"
                    )
        return expanded

    def get_reg_names(
        self, reg_ids: List[str], feature_ids: Optional[List[str]] = None
    ) -> List[str]:
        """Return human-readable regulation names for the given IDs.

        Provincial/admin regulations (``prov_``, ``zone_``) and admin-area
        synopsis matches are excluded — they aren't waterbody-specific names.
        """
        if not reg_ids:
            return []

        base_ids = {parse_base_regulation_id(r) for r in reg_ids}
        base_ids = {
            b
            for b in base_ids
            if not b.startswith("prov_") and not b.startswith("zone_")
        }
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

    # ------------------------------------------------------------------
    # Caching & geometry loading
    # ------------------------------------------------------------------

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

    def _load_all_stream_geometries(self) -> None:
        if self._stream_geometries is not None:
            return

        def _load() -> Dict[str, BaseGeometry]:
            geoms: Dict[str, BaseGeometry] = {}
            gdf = self.data_accessor.get_layer("streams")
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

        valid_poly_ids: set = set()
        for ftype_enum in POLYGON_LAYERS.values():
            if ftype_enum in self.gazetteer.metadata:
                valid_poly_ids.update(
                    str(k) for k in self.gazetteer.metadata[ftype_enum].keys()
                )

        def _load() -> Dict[str, BaseGeometry]:
            geoms: Dict[str, BaseGeometry] = {}
            for layer_name, ftype_enum in tqdm(POLYGON_LAYERS.items(), desc="Polygons"):
                gdf = self.data_accessor.get_layer(layer_name)
                if gdf.empty or "WATERBODY_POLY_ID" not in gdf.columns:
                    continue
                valid_ids_for_type: set = set()
                if ftype_enum in self.gazetteer.metadata:
                    valid_ids_for_type = {
                        str(k) for k in self.gazetteer.metadata[ftype_enum].keys()
                    }
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

    def _inject_ungazetted_geometries(self) -> None:
        """Inject ungazetted waterbody geometries into the polygon cache."""
        from .linking_corrections import UNGAZETTED_WATERBODIES

        if self._polygon_geometries is None:
            self._polygon_geometries = {}

        for uid, uw in UNGAZETTED_WATERBODIES.items():
            if uw.geometry_type == "point":
                key = f"{FeatureType.UNGAZETTED.value.upper()}_{uid}"
                self._polygon_geometries[key] = Point(uw.coordinates)

    # ------------------------------------------------------------------
    # Scoring & zoom thresholds
    # ------------------------------------------------------------------

    def _compute_blk_stats(self, keys_iterable: Iterable[str]) -> dict:
        stats: dict = defaultdict(
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

    @staticmethod
    def _calculate_score(
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
        if scores.size == 0:
            return [(0.0, z) for z in sorted(PERCENTILES.keys())]
        return [
            (np.percentile(scores, PERCENTILES[z]), z)
            for z in sorted(PERCENTILES.keys())
        ]

    def _get_synchronized_blk_zooms(self) -> dict:
        assert (
            self._stream_geometries is not None
        ), "_get_synchronized_blk_zooms called before stream geometries loaded"
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
        """Return the minimum zoom level for a given value using threshold lookup."""
        return next(
            (zoom for threshold, zoom in thresholds if value >= threshold),
            default,
        )

    # ------------------------------------------------------------------
    # Admin boundary helpers
    # ------------------------------------------------------------------

    def _get_all_admin_reg_ids(self) -> set:
        """Collect all regulation IDs sourced from admin polygon spatial matching.

        Aboriginal lands are excluded because their advisory is attached
        post-merge (no geometry splitting for advisory-only layers).
        """
        raw_ids: set = set()
        for layer_key, features_map in self.admin_area_reg_map.items():
            if layer_key == "aboriginal_lands":
                continue
            for reg_ids in features_map.values():
                raw_ids.update(reg_ids)
        if not raw_ids:
            return set()
        return self.expand_admin_reg_ids(raw_ids)

    def _get_admin_gdf(self, layer_key: str) -> Optional[gpd.GeoDataFrame]:
        """Load an admin layer GeoDataFrame, reusing caches when possible."""
        cache_key = f"{self.gpkg_path}_{layer_key}"

        cached = self.gazetteer._reprojected_admin_cache.get(cache_key)
        if cached is not None:
            return cached

        if cache_key in self._admin_gdf_cache:
            return self._admin_gdf_cache[cache_key]

        if layer_key not in self.data_accessor.list_layers():
            logger.warning(f"Admin layer '{layer_key}' not in GPKG")
            return None
        gdf = self.data_accessor.get_layer(layer_key).to_crs("EPSG:3005")

        # Merge overlapping aboriginal_lands polygons so each parcel
        # carries a single regulation instance.
        if layer_key == "aboriginal_lands":
            cfg = ADMIN_LAYER_CONFIG.get(layer_key, {})
            gdf = merge_overlapping_polygons(gdf, cfg["id_field"], cfg["name_field"])

        self._admin_gdf_cache[cache_key] = gdf
        return gdf

    def _build_admin_clip_union(self) -> Optional[BaseGeometry]:
        """Build a single union polygon of all admin areas that carry regulations.

        Aboriginal lands are excluded — their advisory is attached post-merge
        so they never cause geometry splitting.
        """
        all_geoms: list = []
        for layer_key, features_map in self.admin_area_reg_map.items():
            if layer_key == "aboriginal_lands":
                continue
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

        union = unary_union(all_geoms)
        logger.debug(
            f"Admin clip union built from {len(all_geoms)} polygon(s) "
            f"across {len(self.admin_area_reg_map)} layer(s)"
        )
        return union

    # ------------------------------------------------------------------
    # Aboriginal lands advisory (post-merge, non-splitting)
    # ------------------------------------------------------------------

    _ABORIGINAL_ADVISORY_REG_ID = "prov_aboriginal_lands_advisory"

    def _attach_aboriginal_advisory(self, features: List[dict]) -> List[dict]:
        """Append the aboriginal lands advisory to features that intersect.

        Unlike parks and eco reserves which cause geometry splits, aboriginal
        lands only add an informational notice to whichever merged features
        already exist.  This avoids creating extra sections while still
        informing anglers that the waterway passes through Indigenous
        territory.  Works for both streams and polygons.
        """
        ab_map = self.admin_area_reg_map.get("aboriginal_lands")
        if not ab_map:
            return features

        # Build a single union of all aboriginal lands polygons
        gdf = self._get_admin_gdf("aboriginal_lands")
        if gdf is None or gdf.empty:
            return features

        cfg = ADMIN_LAYER_CONFIG.get("aboriginal_lands", {})
        id_field = cfg.get("id_field", "osm_id")
        matched_ids = set(ab_map.keys())
        matched = gdf[gdf[id_field].isin(matched_ids)]
        if matched.empty:
            return features

        ab_union = unary_union(matched.geometry.tolist())
        ab_prep = shapely_prep(ab_union)

        reg_id = self._ABORIGINAL_ADVISORY_REG_ID
        per_instance_prefix = f"{reg_id}:"

        # ── Phase 1: strip per-instance aboriginal reg IDs ──────────
        # regulation_mapper already stamped IDs like
        #   prov_aboriginal_lands_advisory:aboriginal_lands:12288701
        # onto every group that intersected a territory.  Since we now
        # add a single shared advisory instead, remove those per-instance
        # IDs to avoid duplicate / confusing entries.
        stripped = 0
        for feat in features:
            old_ids = feat["regulation_ids"].split(",")
            new_ids = [r for r in old_ids if not r.startswith(per_instance_prefix)]
            if len(new_ids) < len(old_ids):
                stripped += 1
                feat["regulation_ids"] = ",".join(sorted(new_ids))
                feat["regulation_count"] = len(new_ids)
                feat["frontend_group_id"] = self._recompute_frontend_group_id(feat, new_ids)

        if stripped:
            logger.info(
                f"Stripped per-instance aboriginal reg IDs from {stripped} feature(s)"
            )

        # ── Phase 2: attach single advisory to intersecting features ──
        attached = 0
        for feat in features:
            geom = feat.get("geometry")
            if geom is None or geom.is_empty:
                continue
            if not ab_prep.intersects(geom):
                continue

            existing_ids = feat["regulation_ids"].split(",")
            if reg_id in existing_ids:
                continue

            new_ids = sorted(existing_ids + [reg_id])
            feat["regulation_ids"] = ",".join(new_ids)
            feat["regulation_count"] = len(new_ids)
            feat["frontend_group_id"] = self._recompute_frontend_group_id(feat, new_ids)
            attached += 1

        if attached:
            logger.info(
                f"Aboriginal lands advisory attached to {attached} feature(s)"
            )
        return features

    def _recompute_frontend_group_id(self, feat: dict, reg_ids: List[str]) -> str:
        """Recompute frontend_group_id after regulation_ids change."""
        ftype = feat.get("feature_type", "")
        if ftype == FeatureType.STREAM.value:
            gk = (feat.get("fwa_watershed_code") or "").split(",")[0].strip()
        else:
            gk = str(feat.get("waterbody_key") or "")
        if not gk:
            gk = feat.get("group_id", "")
        return self._compute_frontend_group_id(
            gk, feat.get("display_name", ""), tuple(reg_ids)
        )

    def _build_tidal_clip_union(self) -> Optional[BaseGeometry]:
        """Load tidal boundary polygon(s) from GPKG and return their union.

        The stored 'tidal_boundary' layer is already processed (holes filled,
        BC-masked, 5 m margin buffer, simplified) by
        data/process_tidal_boundary.py. No further transformation is applied
        here so the clipping geometry exactly matches what is displayed on the
        front end.

        Returns ``None`` when the layer is absent (data not yet available),
        allowing the pipeline to run unchanged until a tidal polygon is added.
        """
        if TIDAL_BOUNDARY_LAYER not in self.data_accessor.list_layers():
            logger.debug(
                f"'{TIDAL_BOUNDARY_LAYER}' layer not in GPKG — tidal clipping disabled"
            )
            return None
        gdf = self.data_accessor.get_layer(TIDAL_BOUNDARY_LAYER).to_crs("EPSG:3005")
        if gdf.empty:
            return None
        union = unary_union(gdf.geometry.tolist())
        logger.info(f"Tidal clip union built from {len(gdf)} polygon(s)")
        return union

    def get_tidal_boundary_gdf(self) -> Optional[gpd.GeoDataFrame]:
        """Return the raw tidal boundary GeoDataFrame (for the display layer)."""
        if TIDAL_BOUNDARY_LAYER not in self.data_accessor.list_layers():
            return None
        gdf = self.data_accessor.get_layer(TIDAL_BOUNDARY_LAYER).to_crs("EPSG:3005")
        return gdf if not gdf.empty else None

    @staticmethod
    def _build_tidal_tiles(
        tidal_union: BaseGeometry,
        target_cells: int = 2000,
    ) -> Tuple[List[BaseGeometry], Any]:
        """Segment the tidal polygon into a grid of small local tiles.

        The full tidal polygon covers all of coastal BC (113K vertices, 3300+
        holes). Running ``difference()`` against it for every crossing stream is
        expensive even with GEOS internals. This method slices it into a grid of
        small local sub-polygons (typically <2K vertices each) computed once at
        pipeline startup. Each crossing stream then only clips against the 1-4
        tiles that touch it, giving the same cut-point accuracy at a fraction of
        the cost.

        The grid cell size is derived automatically from ``target_cells`` and the
        polygon's bounding box so the tiles stay roughly square.

        Args:
            tidal_union: Full-resolution tidal polygon (any complexity).
            target_cells: Approximate grid size. More cells → smaller,
                faster-to-clip tiles; fewer → faster setup. 2000 gives
                ~25 km cells over the BC coast bbox.

        Returns:
            (tiles, STRtree-over-tiles)
        """
        import shapely as _shapely
        from shapely.strtree import STRtree
        from shapely.geometry import box

        minx, miny, maxx, maxy = tidal_union.bounds
        cell_size = (((maxx - minx) * (maxy - miny)) / target_cells) ** 0.5

        xs = np.arange(minx, maxx, cell_size)
        ys = np.arange(miny, maxy, cell_size)

        # Build all grid cells, then drop those whose bbox doesn't even touch
        # the tidal polygon bbox (saves intersection work on empty sea/land).
        all_cells = [
            box(x, y, min(x + cell_size, maxx), min(y + cell_size, maxy))
            for x in xs
            for y in ys
        ]
        cell_tree = STRtree(all_cells)
        candidate_idxs = cell_tree.query(tidal_union)  # bbox-level pre-filter
        candidate_cells = np.array([all_cells[i] for i in candidate_idxs], dtype=object)

        if len(candidate_cells) == 0:
            return [], STRtree([])

        # Batch-intersect in C via Shapely 2.x vectorised API:
        # tidal_union is broadcast against the array of cells in one C call.
        tile_geoms = _shapely.intersection(tidal_union, candidate_cells)
        valid_tiles = [g for g in tile_geoms if g is not None and not g.is_empty]

        logger.info(
            f"Tidal polygon tiled into {len(valid_tiles)} segments "
            f"(grid cell ≈ {cell_size / 1000:.0f} km, "
            f"{len(all_cells)} total cells checked)"
        )
        return valid_tiles, STRtree(valid_tiles)

    # ------------------------------------------------------------------
    # Tidal boundary clipping
    # ------------------------------------------------------------------

    @staticmethod
    def _clip_streams_at_tidal_boundary(
        stream_features: List[dict],
        tidal_union: BaseGeometry,
        tidal_prep: PreparedGeometry,
        tidal_tiles: List[BaseGeometry] = (),
        tile_tree: Any = None,
    ) -> List[dict]:
        """Clip stream features at the tidal boundary, keeping only freshwater portions.

        - Streams fully inside tidal: removed.
        - Streams crossing the boundary: split; only outside portion kept.
        - Streams fully outside: kept unchanged.

        Uses two spatial indexes:
        1. STRtree over all streams (predicate='intersects') to find tidal candidates
           in pure C — the vast majority of inland streams are eliminated here.
        2. STRtree over pre-tiled tidal sub-polygons so that difference() for each
           crossing stream runs against 1-4 small local tiles (~1-5 km²) rather than
           the full 113K-vertex coastline polygon. Accuracy is unchanged.
        """
        from shapely.strtree import STRtree

        use_tiles = bool(tidal_tiles)

        # Filter out null/empty geometries upfront
        indexed_feats = [
            f
            for f in stream_features
            if f.get("geometry") and not f["geometry"].is_empty
        ]
        indexed_geoms = [f["geometry"] for f in indexed_feats]

        if not indexed_feats:
            return []

        # STRtree.query with predicate='intersects' runs the full geometry
        # intersection test in C — only returns streams that actually intersect
        # the tidal polygon (not just bbox overlap).
        tree = STRtree(indexed_geoms)
        intersecting_idxs = set(tree.query(tidal_union, predicate="intersects"))

        result: List[dict] = []
        removed = 0
        clipped = 0

        for i, (feat, geom) in enumerate(zip(indexed_feats, indexed_geoms)):
            if i not in intersecting_idxs:
                # Confirmed non-intersecting in C — keep as-is
                result.append(feat)
                continue

            # Intersects tidal: check if fully inside or crossing
            if tidal_prep.contains(geom):
                removed += 1
                continue

            # Boundary crossing: keep only the freshwater (outside) portion.
            # For the difference() step, build a small local tidal polygon by
            # unioning only the pre-tiled segments that touch this stream. Each
            # tile covers ~25 km², so a typical crossing stream hits 1-4 tiles
            # with a few hundred vertices instead of the full 113K-vertex polygon.
            if use_tiles:
                tile_idxs = tile_tree.query(geom, predicate="intersects")
                local_tidal = (
                    unary_union([tidal_tiles[ti] for ti in tile_idxs])
                    if len(tile_idxs)
                    else tidal_union  # safety fallback
                )
            else:
                local_tidal = tidal_union

            outside = geom.difference(local_tidal)
            outside_parts = extract_line_components(outside)
            # extract_line_components already drops zero-length degenerate artefacts
            # (points, empty rings) that difference() can emit at tangent touches.
            # We do NOT apply an additional length threshold here — legitimate
            # freshwater remnants right at the river mouth can be very short.

            if outside_parts:
                clipped += 1
                merged = merge_lines(outside_parts)
                result.append({**feat, "geometry": merged, "length_m": merged.length})
            else:
                removed += 1

        logger.info(
            f"Tidal stream clipping: {len(intersecting_idxs):,} intersected "
            f"(of {len(indexed_feats):,} total), {clipped} split at boundary, "
            f"{removed} removed (fully tidal), {len(result):,} retained"
        )
        return result

    @staticmethod
    def _remove_tidal_wetlands(
        polygon_features: List[dict],
        tidal_union: BaseGeometry,
    ) -> List[dict]:
        """Remove wetland features that are inside the tidal boundary.

        Lakes and manmade waterbodies are kept unchanged.
        Wetlands are removed if they intersect the tidal polygon.
        Uses STRtree with predicate='intersects' for full C-speed filtering.
        """
        from shapely.strtree import STRtree

        # Non-wetlands pass through immediately — no geometry check needed
        non_wetlands = [
            f
            for f in polygon_features
            if f.get("feature_type") != FeatureType.WETLAND.value
        ]
        wetlands = [
            f
            for f in polygon_features
            if f.get("feature_type") == FeatureType.WETLAND.value
            and f.get("geometry")
            and not f["geometry"].is_empty
        ]

        if not wetlands:
            return non_wetlands

        wetland_geoms = [f["geometry"] for f in wetlands]
        tree = STRtree(wetland_geoms)
        # Full intersection test in C — returns only wetlands that actually intersect
        remove_idxs = set(tree.query(tidal_union, predicate="intersects"))

        kept_wetlands = [f for i, f in enumerate(wetlands) if i not in remove_idxs]
        removed = len(remove_idxs)

        if removed:
            logger.info(f"Tidal wetland removal: {removed} wetlands removed")
        return non_wetlands + kept_wetlands

    # ------------------------------------------------------------------
    # Canonical feature building (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_frontend_group_id(
        watershed_code: str,
        display_name: str,
        regulation_ids: tuple,
    ) -> str:
        """Compute a unique frontend group ID for highlighting."""
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
        """Build a single canonical feature dict."""
        rids = reg_ids if reg_ids is not None else group.regulation_ids
        display_name = group.display_name

        ftype = base_props["feature_type"]
        if ftype == FeatureType.STREAM.value:
            gk = (base_props.get("fwa_watershed_code") or "").split(",")[0].strip()
        else:
            gk = str(base_props.get("waterbody_key") or "")
        if not gk:
            gk = group.group_id
        frontend_group_id = self._compute_frontend_group_id(gk, display_name, rids)

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
            "waterbody_group": (
                # For streams: fwa_watershed_code groups all segments of the same physical channel.
                # For polygons: waterbody_key groups all features sharing the same waterbody.
                base_props.get("fwa_watershed_code") or ""
                if ftype == FeatureType.STREAM.value
                else str(base_props.get("waterbody_key") or "")
            ),
            "regulation_ids": ",".join(sorted(rids)),
            "regulation_count": len(rids),
            "length_m": length_m,
            "geometry": geometry,
        }

    def _build_group_base_props(self, group: MergedGroup) -> dict:
        """Build shared metadata dict from MergedGroup fields."""
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

    def _clip_group_at_admin_boundary(
        self,
        group: MergedGroup,
        geom_list: list,
        admin_clip_union: BaseGeometry,
        admin_prep: PreparedGeometry,
        admin_reg_ids: set,
        base_props: dict,
    ) -> Tuple[List[dict], str]:
        """Split a merged group's geometry at admin polygon boundaries."""
        MIN_LENGTH_M = 1.0

        merged_line = merge_lines(geom_list)
        non_admin_regs = tuple(
            r for r in group.regulation_ids if r not in admin_reg_ids
        )

        # Fast-path: entirely inside
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

        # Fast-path: entirely outside
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

        # Slow path: boundary crossing
        inside_parts = extract_line_components(
            merged_line.intersection(admin_clip_union)
        )
        outside_parts = extract_line_components(
            merged_line.difference(admin_clip_union)
        )

        results: List[dict] = []

        inside_parts = [g for g in inside_parts if g.length >= MIN_LENGTH_M]
        if inside_parts:
            results.append(
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_in",
                    reg_ids=group.regulation_ids,
                    geometry=merge_lines(inside_parts),
                )
            )

        outside_parts = [g for g in outside_parts if g.length >= MIN_LENGTH_M]
        if outside_parts:
            results.append(
                self._emit_group_feature(
                    group,
                    base_props,
                    "_admin_out",
                    reg_ids=non_admin_regs,
                    geometry=merge_lines(outside_parts),
                )
            )

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
        assert self._stream_geometries is not None
        geom_list: list = []
        max_order = 0

        for fid in group.feature_ids:
            geom = self._stream_geometries.get(fid)
            meta = self.gazetteer.get_stream_metadata(fid)
            if geom:
                geom_list.extend(extract_geoms(geom))
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

        merged_geom = merge_lines(geom_list)
        return [self._emit_group_feature(group, base_props, geometry=merged_geom)]

    def _build_polygon_canonical(self, group: MergedGroup) -> List[dict]:
        """Build canonical feature(s) for a polygon group."""
        assert self._polygon_geometries is not None
        ftype_enum = FeatureType(group.feature_type)
        prefix = f"{ftype_enum.value.upper()}_"

        geom_list: list = []
        for fid in group.feature_ids:
            geom = self._polygon_geometries.get(f"{prefix}{fid}")
            if geom:
                geom_list.extend(extract_geoms(geom))

        if not geom_list:
            return []

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

    # ------------------------------------------------------------------
    # Post-merge: combine polygon pieces with same waterbody_key
    # ------------------------------------------------------------------

    def _merge_same_waterbody_polygons(self, features: list) -> list:
        """Merge polygon features sharing the same waterbody_key.

        When a lake/wetland/manmade polygon straddles a zone boundary,
        upstream merging creates separate ``MergedGroup``s because their
        regulation_id sets differ (different zone regs).  This post-merge
        step reunifies them into a single feature per physical waterbody:

        * Geometry → ``unary_union`` of all constituent polygons
        * ``regulation_ids`` → union of all regulation sets
        * ``regulation_count`` → updated to match
        * ``frontend_group_id`` → recalculated from merged reg_ids
        * ``zones``, ``mgmt_units``, ``region_name`` → combined
        * ``area_sqm``, ``tippecanoe:minzoom`` → recalculated
        * ``feature_ids``, ``feature_count`` → combined

        Features with no ``waterbody_key`` pass through unchanged.
        """
        if not features:
            return features

        # Partition: features with a WBK are candidates; others pass through
        wbk_groups: Dict[str, list] = defaultdict(list)
        no_wbk: list = []
        for feat in features:
            wbk = feat.get("waterbody_key")
            if wbk:
                wbk_groups[wbk].append(feat)
            else:
                no_wbk.append(feat)

        merged: list = list(no_wbk)
        merge_count = 0
        for wbk, group_features in wbk_groups.items():
            if len(group_features) == 1:
                merged.append(group_features[0])
                continue

            # Union geometry
            all_geoms: list = []
            for feat in group_features:
                geom = feat["geometry"]
                if geom is not None:
                    all_geoms.extend(extract_geoms(geom))

            if not all_geoms:
                merged.append(group_features[0])
                continue

            merged_geom = unary_union(all_geoms)

            # Build merged template from first feature
            template = dict(group_features[0])
            template["geometry"] = merged_geom
            template["area_sqm"] = merged_geom.area
            template["tippecanoe:minzoom"] = self._calculate_minzoom(
                merged_geom.area, _LAKE_ZOOM_LOOKUP
            )

            # Union regulation_ids
            all_rids: set = set()
            for feat in group_features:
                for rid in feat["regulation_ids"].split(","):
                    if rid:
                        all_rids.add(rid)
            template["regulation_ids"] = ",".join(sorted(all_rids))
            template["regulation_count"] = len(all_rids)

            # Prefer a group_id without admin suffixes
            group_ids = [f["group_id"] for f in group_features]
            base_ids = [
                g for g in group_ids if not g.endswith(("_admin_in", "_admin_out"))
            ]
            template["group_id"] = base_ids[0] if base_ids else group_ids[0]

            # Combine zones
            all_zones: set = set()
            for feat in group_features:
                for z in feat.get("zones", "").split(","):
                    if z:
                        all_zones.add(z)
            template["zones"] = ",".join(sorted(all_zones))

            # Combine mgmt_units
            all_mu: set = set()
            for feat in group_features:
                for mu in feat.get("mgmt_units", "").split(","):
                    if mu:
                        all_mu.add(mu)
            template["mgmt_units"] = ",".join(sorted(all_mu))

            # Combine region_name
            all_rn: set = set()
            for feat in group_features:
                for rn in feat.get("region_name", "").split(","):
                    if rn:
                        all_rn.add(rn)
            template["region_name"] = ",".join(sorted(all_rn))

            # Combine feature_ids and feature_count
            all_fids: list = []
            for feat in group_features:
                for fid in feat.get("feature_ids", "").split(","):
                    if fid and fid not in all_fids:
                        all_fids.append(fid)
            template["feature_ids"] = ",".join(all_fids)
            template["feature_count"] = len(all_fids)

            # Recalculate frontend_group_id with merged regulation set
            display_name = template.get("display_name", "")
            gk = str(wbk)
            template["frontend_group_id"] = self._compute_frontend_group_id(
                gk, display_name, tuple(sorted(all_rids))
            )

            merged.append(template)
            merge_count += 1

        if merge_count:
            logger.info(
                f"Polygon post-merge: combined {merge_count} waterbody groups "
                f"({len(features)} → {len(merged)} features)"
            )
        return merged

    # ------------------------------------------------------------------
    # Post-merge: combine pieces with identical regulation sets
    # ------------------------------------------------------------------

    def _merge_same_regulation_features(self, features: list) -> list:
        """Merge features sharing the same physical waterbody and regulation_ids."""
        if not features:
            return features

        groups: Dict[tuple, list] = defaultdict(list)
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

        merged: list = []
        merge_count = 0
        for key, group_features in groups.items():
            if len(group_features) == 1:
                merged.append(group_features[0])
                continue

            all_geoms: list = []
            for feat in group_features:
                geom = feat["geometry"]
                if geom is not None:
                    all_geoms.extend(extract_geoms(geom))

            if not all_geoms:
                merged.append(group_features[0])
                continue

            template = dict(group_features[0])
            template["geometry"] = merge_lines(all_geoms)
            template["length_m"] = template["geometry"].length

            # Combine feature_ids from all merged features so that
            # downstream is_under_lake checks see every constituent edge.
            all_fids: list = []
            for feat in group_features:
                for fid in feat.get("feature_ids", "").split(","):
                    if fid and fid not in all_fids:
                        all_fids.append(fid)
            template["feature_ids"] = ",".join(all_fids)

            group_ids = [f["group_id"] for f in group_features]
            base_ids = [
                g for g in group_ids if not g.endswith(("_admin_in", "_admin_out"))
            ]
            template["group_id"] = base_ids[0] if base_ids else group_ids[0]

            all_mu: set = set()
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

    # ------------------------------------------------------------------
    # Admin layer access (for geo_exporter admin layer creation)
    # ------------------------------------------------------------------

    def get_admin_gdf(self, layer_key: str) -> Optional[gpd.GeoDataFrame]:
        """Public accessor — delegates to cached loader."""
        return self._get_admin_gdf(layer_key)
