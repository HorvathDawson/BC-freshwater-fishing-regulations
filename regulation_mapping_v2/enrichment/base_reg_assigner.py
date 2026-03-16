"""Phase 4 — Assign zone and provincial base regulations.

Loads base regulation definitions from JSON and applies them to atlas
features using spatial polygon intersection:

    - Zone-wide: dissolve WMU polygons for the target zone/MU → two-pass
      hysteresis for streams, buffered intersection for polygon waterbodies
    - Admin-targeted: same polygon_filter against atlas admin layers
    - Direct-match: resolve by ID (gnis_ids, blue_line_keys, etc.)

All stream matching uses the two-pass hysteresis from polygon_filter,
called per-WSC-group to preserve the guarantee that a stream must *enter*
the polygon before buffer leniency is applied to its siblings.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import geopandas as gpd
from tqdm import tqdm

from data.data_extractor import FWADataAccessor
from regulation_mapping_v2.atlas.freshwater_atlas import FreshWaterAtlas

from .models import AtlasMetadata, BaseRegulationDef, FeatureAssignment

logger = logging.getLogger(__name__)

_PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_REGS_PATH = _PACKAGE_DIR / "base_regulations.json"


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _load_base_regulations(path: Path) -> List[BaseRegulationDef]:
    """Load base regulation definitions from JSON, filtering disabled."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    all_regs = [BaseRegulationDef.from_dict(d) for d in data]
    active = [r for r in all_regs if not r.disabled]
    disabled_count = len(all_regs) - len(active)
    if disabled_count:
        logger.info("  Skipped %d disabled regulations", disabled_count)
    return active


# ---------------------------------------------------------------------------
# WMU pre-computation (run once, reuse for all regs)
# ---------------------------------------------------------------------------


def _load_wmu_polygons(gpkg_path: Path) -> gpd.GeoDataFrame:
    """Load WMU (Wildlife Management Units) polygons from GPKG.

    Uses FWADataAccessor for consistent column normalization
    (numeric IDs → str, etc.), then reprojects to EPSG:3005
    for meter-based spatial operations.
    """
    accessor = FWADataAccessor(gpkg_path)
    gdf = accessor.get_layer("wmu")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(epsg=3005)
    return gdf


def _precompute_zone_mu_map(
    wmu_gdf: gpd.GeoDataFrame,
) -> Dict[str, Set[str]]:
    """Build {zone_id: {mu_ids}} from WMU GeoDataFrame."""
    zone_mus: Dict[str, Set[str]] = defaultdict(set)
    for _, row in wmu_gdf.iterrows():
        zone_id = str(row.get("REGION_RESPONSIBLE_ID") or "")
        mu_id = str(row.get("WILDLIFE_MGMT_UNIT_ID") or "")
        if zone_id and mu_id:
            zone_mus[zone_id].add(mu_id)
    return dict(zone_mus)


def _precompute_mu_features(
    wmu_gdf: gpd.GeoDataFrame,
    wsc_groups: Dict[str, List[Tuple[str, Any]]],
    atlas: FreshWaterAtlas,
    buffer_m: float = 500.0,
) -> Dict[str, Tuple[Set[str], Set[str]]]:
    """Pre-compute {mu_id: (stream_fids, waterbody_keys)} for every MU.

    Uses **vectorized** STRtree.query() — all stream geometries are passed
    in one call so the spatial-index traversal + predicate test stays in
    C (shapely 2.x).  Avoids 2.3 M individual Python→C round-trips.
    """
    import numpy as np
    from shapely import STRtree

    # ── Build MU lookup + buffered polygons ──
    mu_ids_list: List[str] = []
    mu_buffered: List[Any] = []

    for _, row in wmu_gdf.iterrows():
        mu_id = str(row.get("WILDLIFE_MGMT_UNIT_ID") or "")
        if not mu_id:
            continue
        polygon = row.geometry
        if not polygon.is_valid:
            polygon = polygon.buffer(0)
        mu_ids_list.append(mu_id)
        mu_buffered.append(polygon.buffer(buffer_m))

    tree = STRtree(mu_buffered)

    mu_fids: Dict[str, Set[str]] = {mid: set() for mid in mu_ids_list}
    mu_wbks: Dict[str, Set[str]] = {mid: set() for mid in mu_ids_list}

    # ── Streams: flatten, then single vectorized query ──
    all_fids: List[str] = []
    all_geoms: List[Any] = []
    for pairs in wsc_groups.values():
        for fid, geom in pairs:
            all_fids.append(fid)
            all_geoms.append(geom)

    logger.info(
        "  Vectorized MU-stream query: %d streams × %d MUs …",
        len(all_geoms),
        len(mu_ids_list),
    )
    geom_arr = np.array(all_geoms, dtype=object)
    stream_idx, mu_idx = tree.query(geom_arr, predicate="intersects")
    logger.info("  → %d stream–MU pairs", len(stream_idx))

    for si, mi in zip(stream_idx.tolist(), mu_idx.tolist()):
        mu_fids[mu_ids_list[mi]].add(all_fids[si])

    # ── Polygon waterbodies: single vectorized query ──
    poly_wbks: List[str] = []
    poly_geoms: List[Any] = []
    for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
        for wbk, poly_rec in collection.items():
            poly_wbks.append(wbk)
            poly_geoms.append(poly_rec.geometry)

    if poly_geoms:
        logger.info(
            "  Vectorized MU-polygon query: %d polygons × %d MUs …",
            len(poly_geoms),
            len(mu_ids_list),
        )
        poly_arr = np.array(poly_geoms, dtype=object)
        p_idx, mu_p_idx = tree.query(poly_arr, predicate="intersects")
        logger.info("  → %d polygon–MU pairs", len(p_idx))
        for pi, mi in zip(p_idx.tolist(), mu_p_idx.tolist()):
            mu_wbks[mu_ids_list[mi]].add(poly_wbks[pi])

    return {mid: (mu_fids[mid], mu_wbks[mid]) for mid in mu_ids_list}


# ---------------------------------------------------------------------------
# Shared spatial matching
# ---------------------------------------------------------------------------


def _feature_type_matches(
    feature_type: str,
    allowed: Optional[Tuple[str, ...]],
) -> bool:
    """Check if a feature type string matches the allowed list."""
    if allowed is None:
        return True
    return feature_type in allowed


def _group_streams_by_wsc(
    atlas: FreshWaterAtlas,
) -> Dict[str, List[Tuple[str, Any]]]:
    """Group atlas stream fids by fwa_watershed_code.

    Returns {wsc: [(fid, geometry), ...]}.
    """
    groups: Dict[str, List[Tuple[str, Any]]] = defaultdict(list)
    for fid, stream in atlas.streams.items():
        wsc = stream.fwa_watershed_code
        if wsc:
            groups[wsc].append((fid, stream.geometry))
    return groups


def _filter_and_assign(
    fids: Set[str],
    wbks: Set[str],
    reg: BaseRegulationDef,
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
) -> int:
    """Shared pathway: filter by feature_type and bulk-assign.

    Both zone-wide and admin-targeted converge here after resolving
    their (fid_set, wbk_set) via different strategies.
    """
    count = 0
    if fids and _feature_type_matches("stream", reg.feature_types):
        count += assignments.assign_fids_bulk(fids, reg.reg_id, phase=4)
    if wbks:
        # Split wbks by polygon layer type for feature_type filtering
        if _feature_type_matches("lake", reg.feature_types):
            lake_wbks = wbks & atlas.lakes.keys()
            if lake_wbks:
                count += assignments.assign_wbks_bulk(lake_wbks, reg.reg_id, phase=4)
        if _feature_type_matches("wetland", reg.feature_types):
            wet_wbks = wbks & atlas.wetlands.keys()
            if wet_wbks:
                count += assignments.assign_wbks_bulk(wet_wbks, reg.reg_id, phase=4)
        if _feature_type_matches("manmade", reg.feature_types):
            man_wbks = wbks & atlas.manmade.keys()
            if man_wbks:
                count += assignments.assign_wbks_bulk(man_wbks, reg.reg_id, phase=4)
    return count


def _precompute_admin_features(
    admin_polygons: List[Any],
    stream_fids: List[str],
    stream_geoms: "np.ndarray",
    poly_wbks: List[str],
    poly_geoms: "np.ndarray",
    buffer_m: float = 500.0,
) -> Tuple[Set[str], Set[str]]:
    """Vectorised STRtree intersection of streams/polygons vs admin polygons.

    Builds a spatial index on buffered admin polygons and runs a single
    vectorised query for streams and another for waterbodies.
    """
    import numpy as np
    from shapely import STRtree

    # Buffer admin polygons and build tree
    buffered = []
    for p in admin_polygons:
        geom = p if not hasattr(p, "geometry") else p.geometry
        if not geom.is_valid:
            geom = geom.buffer(0)
        buffered.append(geom.buffer(buffer_m))

    tree = STRtree(buffered)

    # Streams
    fids: Set[str] = set()
    if len(stream_geoms) > 0:
        s_idx, _a_idx = tree.query(stream_geoms, predicate="intersects")
        for si in s_idx.tolist():
            fids.add(stream_fids[si])

    # Waterbody polygons
    wbks: Set[str] = set()
    if len(poly_geoms) > 0:
        p_idx, _a_idx = tree.query(poly_geoms, predicate="intersects")
        for pi in p_idx.tolist():
            wbks.add(poly_wbks[pi])

    return fids, wbks


# ---------------------------------------------------------------------------
# Zone-wide assignment (spatial)
# ---------------------------------------------------------------------------


def _resolve_mu_set(
    reg: BaseRegulationDef,
    mu_features: Dict[str, Tuple[Set[str], Set[str]]],
    zone_mu_map: Dict[str, Set[str]],
) -> Optional[Set[str]]:
    """Resolve a regulation to its target MU set. Returns None if empty."""
    if reg.mu_ids:
        target_mus = set(reg.mu_ids)
    elif reg.zone_ids:
        target_mus: Set[str] = set()
        for zid in reg.zone_ids:
            target_mus |= zone_mu_map.get(zid, set())
    else:
        target_mus = set(mu_features.keys())

    if reg.include_mu_ids:
        target_mus |= set(reg.include_mu_ids)
    if reg.exclude_mu_ids:
        target_mus -= set(reg.exclude_mu_ids)

    return target_mus or None


def _union_mu_features(
    target_mus: Set[str],
    mu_features: Dict[str, Tuple[Set[str], Set[str]]],
) -> Tuple[Set[str], Set[str]]:
    """Union pre-computed (fid_set, wbk_set) across a set of MUs."""
    all_fids: Set[str] = set()
    all_wbks: Set[str] = set()
    for mu_id in target_mus:
        mu_data = mu_features.get(mu_id)
        if mu_data:
            all_fids |= mu_data[0]
            all_wbks |= mu_data[1]
    return all_fids, all_wbks


def _assign_zone_wide(
    reg: BaseRegulationDef,
    mu_features: Dict[str, Tuple[Set[str], Set[str]]],
    zone_mu_map: Dict[str, Set[str]],
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
) -> int:
    """Assign a zone-wide regulation using pre-computed MU feature sets."""
    target_mus = _resolve_mu_set(reg, mu_features, zone_mu_map)
    if not target_mus:
        logger.warning("  %s: no MUs resolved", reg.reg_id)
        return 0

    all_fids, all_wbks = _union_mu_features(target_mus, mu_features)
    return _filter_and_assign(all_fids, all_wbks, reg, atlas, assignments)


# ---------------------------------------------------------------------------
# Admin-targeted assignment
# ---------------------------------------------------------------------------


_ADMIN_LAYER_MAP = {
    "parks_nat": "parks_nat",
    "parks_bc": "eco_reserves",
    "eco_reserves": "eco_reserves",
    "wma": "wma",
    "historic_sites": "historic_sites",
    "watersheds": "watersheds",
    "osm_admin_boundaries": "osm_admin",
    "aboriginal_lands": "aboriginal_lands",
}


def _assign_admin_targeted(
    reg: BaseRegulationDef,
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
    admin_cache: Dict[Tuple[str, Optional[str]], Tuple[Set[str], Set[str]]],
    stream_fids: List[str],
    stream_geoms: "np.ndarray",
    poly_wbks: List[str],
    poly_geoms: "np.ndarray",
) -> int:
    """Assign an admin-targeted regulation via vectorised STRtree.

    Uses the same _filter_and_assign pathway as zone-wide regs.
    Results are cached per (layer, feature_id).
    """
    if not reg.admin_targets:
        return 0

    count = 0
    for target_dict in reg.admin_targets:
        target = dict(target_dict)
        layer = target["layer"]
        feature_id = target.get("feature_id")

        cache_key = (layer, feature_id)
        if cache_key in admin_cache:
            cached_fids, cached_wbks = admin_cache[cache_key]
            count += _filter_and_assign(
                cached_fids, cached_wbks, reg, atlas, assignments
            )
            continue

        attr = _ADMIN_LAYER_MAP.get(layer)
        records = getattr(atlas, attr, None) if attr else None
        if records is None:
            logger.warning("Unknown admin layer %s for reg %s", layer, reg.reg_id)
            admin_cache[cache_key] = (set(), set())
            continue

        admin_list = (
            [records[feature_id]]
            if feature_id and feature_id in records
            else list(records.values())
        )

        # Filter by admin_type if type_filter is specified.
        # Used e.g. to target only ECOLOGICAL_RESERVE within the eco_reserves
        # dict (which also contains PROVINCIAL_PARK, PROTECTED_AREA, etc.).
        type_filter = target.get("type_filter")
        if type_filter:
            admin_list = [r for r in admin_list if r.admin_type == type_filter]

        if not admin_list:
            logger.warning(
                "No admin polygons for layer=%s id=%s (reg %s)",
                layer,
                feature_id,
                reg.reg_id,
            )
            admin_cache[cache_key] = (set(), set())
            continue

        # Vectorised spatial intersection
        target_fids, target_wbks = _precompute_admin_features(
            admin_list,
            stream_fids,
            stream_geoms,
            poly_wbks,
            poly_geoms,
            buffer_m=reg.buffer_m,
        )
        admin_cache[cache_key] = (target_fids, target_wbks)
        count += _filter_and_assign(target_fids, target_wbks, reg, atlas, assignments)

    return count


# ---------------------------------------------------------------------------
# Direct-match assignment (resolve by ID)
# ---------------------------------------------------------------------------


def _assign_direct_match(
    reg: BaseRegulationDef,
    atlas: FreshWaterAtlas,
    metadata: AtlasMetadata,
    assignments: FeatureAssignment,
) -> int:
    """Assign a direct-match regulation by resolving ID fields to atlas features."""
    count = 0

    # gnis_ids → stream fids via metadata
    if reg.gnis_ids:
        for gid in reg.gnis_ids:
            s_meta = metadata["streams"].get(str(gid))
            if s_meta and _feature_type_matches("stream", reg.feature_types):
                for fid in s_meta["edge_ids"]:
                    if fid in atlas.streams:
                        assignments.assign_fid(fid, reg.reg_id, phase=4)
                        count += 1
            # Also check polygon layers via _gnis_to_wbk
            gnis_to_wbk = metadata.get("_gnis_to_wbk", {})
            for wbk in gnis_to_wbk.get(str(gid), []):
                for layer_key in ("lakes", "wetlands", "manmade"):
                    layer = getattr(atlas, layer_key, {})
                    if wbk in layer:
                        assignments.assign_wbk(wbk, reg.reg_id, phase=4)
                        count += 1

    # blue_line_keys → stream fids
    if reg.blue_line_keys:
        for blk in reg.blue_line_keys:
            for fid, stream in atlas.streams.items():
                if stream.blk == blk:
                    if _feature_type_matches("stream", reg.feature_types):
                        assignments.assign_fid(fid, reg.reg_id, phase=4)
                        count += 1

    # fwa_watershed_codes → stream fids
    if reg.fwa_watershed_codes:
        wsc_set = set(reg.fwa_watershed_codes)
        for fid, stream in atlas.streams.items():
            if stream.fwa_watershed_code in wsc_set:
                if _feature_type_matches("stream", reg.feature_types):
                    assignments.assign_fid(fid, reg.reg_id, phase=4)
                    count += 1

    # waterbody_keys → polygon wbks
    if reg.waterbody_keys:
        for wbk in reg.waterbody_keys:
            for layer_key in ("lakes", "wetlands", "manmade"):
                layer = getattr(atlas, layer_key, {})
                if wbk in layer:
                    assignments.assign_wbk(wbk, reg.reg_id, phase=4)
                    count += 1

    # linear_feature_ids → stream fids
    if reg.linear_feature_ids:
        lfid_set = set(reg.linear_feature_ids)
        for fid in lfid_set:
            if fid in atlas.streams:
                if _feature_type_matches("stream", reg.feature_types):
                    assignments.assign_fid(fid, reg.reg_id, phase=4)
                    count += 1

    return count


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def assign_base_regulations(
    atlas: FreshWaterAtlas,
    metadata: AtlasMetadata,
    assignments: FeatureAssignment,
    gpkg_path: Optional[Path] = None,
    base_regs_path: Optional[Path] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
    """Apply zone/provincial base regs to all atlas features.

    Mutates ``assignments`` in-place (phase=4).

    Parameters
    ----------
    atlas : FreshWaterAtlas
        Loaded atlas with streams, lakes, etc.
    metadata : AtlasMetadata
        Graph-derived metadata (needed for direct-match ID resolution).
    assignments : FeatureAssignment
        Mutable accumulator.
    gpkg_path : Path, optional
        Path to bc_fisheries_data.gpkg for WMU polygon loading.
        Required for zone-wide regulations.
    base_regs_path : Path, optional
        Path to base_regulations.json.

    Returns
    -------
    Tuple of (regulations dict, reach_level_reg_ids set).
    """
    path = base_regs_path or DEFAULT_BASE_REGS_PATH
    base_regs = _load_base_regulations(path)
    logger.info("Phase 4: applying %d base regulations from %s", len(base_regs), path)

    # Pre-group streams by WSC for efficient polygon matching
    wsc_groups = _group_streams_by_wsc(atlas)
    logger.info(
        "  Grouped %d streams into %d WSC groups",
        len(atlas.streams),
        len(wsc_groups),
    )

    # ── Pre-compute MU → features (once for all zone/provincial regs) ──
    mu_features: Dict[str, Tuple[Set[str], Set[str]]] = {}
    zone_mu_map: Dict[str, Set[str]] = {}

    has_zone_wide = any(
        not r.admin_targets and not r.has_direct_target for r in base_regs
    )
    if has_zone_wide:
        if gpkg_path is None:
            raise ValueError(
                "gpkg_path is required for zone-wide base regulations. "
                "Set data_accessor.gpkg_path in config.yaml."
            )
        logger.info("  Loading WMU polygons from %s", gpkg_path)
        wmu_gdf = _load_wmu_polygons(gpkg_path)
        logger.info("  Loaded %d MU polygons", len(wmu_gdf))

        zone_mu_map = _precompute_zone_mu_map(wmu_gdf)
        logger.info(
            "  Zone → MU map: %d zones, %d total MUs",
            len(zone_mu_map),
            sum(len(v) for v in zone_mu_map.values()),
        )

        mu_features = _precompute_mu_features(wmu_gdf, wsc_groups, atlas)
        total_fids = sum(len(f) for f, _ in mu_features.values())
        total_wbks = sum(len(w) for _, w in mu_features.values())
        logger.info(
            "  MU features pre-computed: %d MUs → %d stream refs, %d wbk refs",
            len(mu_features),
            total_fids,
            total_wbks,
        )

    # ── Admin polygon cache (keyed by (layer, feature_id)) ───────────
    admin_cache: Dict[Tuple[str, Optional[str]], Tuple[Set[str], Set[str]]] = {}

    # ── Pre-flatten streams + waterbody polygons for vectorised admin queries ──
    import numpy as np

    flat_stream_fids: List[str] = []
    flat_stream_geoms_list: List[Any] = []
    for pairs in wsc_groups.values():
        for fid, geom in pairs:
            flat_stream_fids.append(fid)
            flat_stream_geoms_list.append(geom)
    flat_stream_geoms = np.array(flat_stream_geoms_list, dtype=object)
    del flat_stream_geoms_list  # free memory

    flat_poly_wbks: List[str] = []
    flat_poly_geoms_list: List[Any] = []
    for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
        for wbk, poly_rec in collection.items():
            flat_poly_wbks.append(wbk)
            flat_poly_geoms_list.append(poly_rec.geometry)
    flat_poly_geoms = np.array(flat_poly_geoms_list, dtype=object)
    del flat_poly_geoms_list

    logger.info(
        "  Pre-flattened %d streams, %d waterbody polygons for admin queries",
        len(flat_stream_fids),
        len(flat_poly_wbks),
    )

    regulations: Dict[str, Dict[str, Any]] = {}
    reach_level_reg_ids: Set[str] = set()

    for reg in tqdm(base_regs, desc="  Phase 4: base regs", leave=False):
        # Route to the correct assignment strategy
        if reg.has_direct_target:
            count = _assign_direct_match(reg, atlas, metadata, assignments)
        elif reg.admin_targets:
            count = _assign_admin_targeted(
                reg,
                atlas,
                assignments,
                admin_cache,
                flat_stream_fids,
                flat_stream_geoms,
                flat_poly_wbks,
                flat_poly_geoms,
            )
        else:
            count = _assign_zone_wide(reg, mu_features, zone_mu_map, atlas, assignments)

        logger.debug("  %s: %d features assigned", reg.reg_id, count)

        # Build regulation info for the index
        reg_info: Dict[str, Any] = {
            "raw_regs": reg.rule_text,
            "source": reg.source,
            "restriction": reg.restriction,
        }
        if reg.zone_ids:
            reg_info["zone_ids"] = list(reg.zone_ids)
        if reg.dates:
            reg_info["dates"] = list(reg.dates)
        if reg.scope_location:
            reg_info["scope_location"] = reg.scope_location
        if reg.notes:
            reg_info["notes"] = reg.notes

        regulations[reg.reg_id] = reg_info
        if reg.reach_level:
            reach_level_reg_ids.add(reg.reg_id)

    logger.info("Phase 4 complete: %s", assignments.summary())
    return regulations, reach_level_reg_ids
