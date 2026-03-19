"""Phase 2 — Resolve match entries to concrete atlas features.

Takes RegulationRecords from Phase 1 and resolves every ID type
(gnis_ids, waterbody_keys, fwa_watershed_codes, blue_line_keys,
linear_feature_ids, admin_targets) to atlas stream fids and
polygon waterbody_keys.

Also prepares seed lists for Phase 3 tributary enrichment.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from tqdm import tqdm

from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
from pipeline.utils.wsc import trim_wsc
from pipeline.matching.match_table import OverrideEntry

from .models import (
    AtlasMetadata,
    FeatureAssignment,
    RegulationRecord,
    ResolvedRegulation,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metadata building (gnis_id → edge_ids, wbk → gnis reverse map)
# ---------------------------------------------------------------------------


def build_metadata_from_graph(graph_path) -> AtlasMetadata:
    """Build atlas metadata from the graph pickle.

    This is the canonical way to get gnis_id → edge_ids mappings.
    Mirrors base_entry_builder._build_metadata() but returns only
    the lookup dicts without the spatial indexing.
    """
    import pickle
    from pathlib import Path

    path = Path(graph_path)
    logger.info("Loading graph for metadata from %s", path)

    with open(path, "rb") as f:
        graph_data = pickle.load(f)

    graph = graph_data["graph"]

    # Streams grouped by gnis_id.
    # Edges with their own gnis_id are grouped directly.
    # Unnamed edges (gnis_id="") with inherited_gnis_names are grouped
    # under the inherited gnis_id — this captures side channels and
    # unnamed tributaries that share a mainstem's watershed code.
    stream_groups: Dict[str, Dict[str, Any]] = {}
    inherited_count = 0
    for edge in graph.es:
        gnis_id = str(edge["gnis_id"] or "")
        fid = str(edge["linear_feature_id"])

        # Determine which gnis_id(s) to file this edge under
        group_ids: List[str] = []
        if gnis_id:
            group_ids.append(gnis_id)
        else:
            # Unnamed edge — check for inherited names from upstream BFS
            inherited = (
                edge["inherited_gnis_names"]
                if "inherited_gnis_names" in edge.attributes()
                else None
            )
            if inherited and len(inherited) == 1:
                inh_gid = str(inherited[0].get("gnis_id", "") or "")
                if inh_gid:
                    group_ids.append(inh_gid)
                    inherited_count += 1

        for gid in group_ids:
            if gid not in stream_groups:
                gnis_name = ""
                if gid == gnis_id:
                    gnis_name = edge["gnis_name"] or ""
                else:
                    # Inherited — pull name from the inherited record
                    gnis_name = inherited[0].get("gnis_name", "")
                stream_groups[gid] = {
                    "gnis_id": gid,
                    "gnis_name": gnis_name,
                    "edge_ids": [],
                    "fwa_watershed_codes": set(),
                    "blue_line_keys": set(),
                    "zones": set(),
                    "mgmt_units": set(),
                }
            grp = stream_groups[gid]
            grp["edge_ids"].append(fid)
            wsc = trim_wsc(edge["fwa_watershed_code"] or "")
            blk = edge["blue_line_key"] or ""
            if wsc:
                grp["fwa_watershed_codes"].add(wsc)
            if blk:
                grp["blue_line_keys"].add(blk)

    # Convert sets to sorted lists for StreamMetaEntry compatibility
    streams: Dict[str, Any] = {}
    for gid, grp in stream_groups.items():
        streams[gid] = {
            "gnis_id": gid,
            "gnis_name": grp["gnis_name"],
            "edge_ids": grp["edge_ids"],
            "fwa_watershed_codes": sorted(grp["fwa_watershed_codes"]),
            "blue_line_keys": sorted(grp["blue_line_keys"]),
            "zones": sorted(grp.get("zones", set())),
            "mgmt_units": sorted(grp.get("mgmt_units", set())),
        }

    logger.info(
        "  Metadata: %d stream GNIS groups (%d edges via inherited names)",
        len(streams),
        inherited_count,
    )

    # Polygon metadata: build wbk → [fid] for lake outlet seeds.
    # NOTE: gnis_to_wbk is NOT built here — under-lake stream edges carry
    # both a stream's gnis_id and a lake's waterbody_key, which would
    # falsely link tributary gnis_ids to the lake.  The correct mapping is
    # built later by enrich_metadata_with_polygons() from atlas polygon
    # records (where gnis_id belongs to the lake itself).
    gnis_to_wbk: Dict[str, Set[str]] = defaultdict(set)
    wbk_to_fids: Dict[str, List[str]] = defaultdict(list)

    for edge in graph.es:
        wbk = str(edge["waterbody_key"] or "")
        fid = str(edge["linear_feature_id"])
        if wbk:
            wbk_to_fids[wbk].append(fid)

    logger.info("  Metadata: %d waterbody_key groups", len(wbk_to_fids))

    return {
        "streams": streams,
        "lakes": {},  # filled by caller from atlas polygons
        "wetlands": {},
        "manmade": {},
        "_gnis_to_wbk": gnis_to_wbk,
        "_wbk_to_fids": wbk_to_fids,
    }


def enrich_metadata_with_polygons(
    metadata: AtlasMetadata, atlas: FreshWaterAtlas
) -> None:
    """Add gnis_id → waterbody_key mappings from atlas polygon records.

    build_metadata_from_graph() can only link gnis_ids to waterbody_keys
    when an under-lake stream edge carries both values.  Many lakes have
    no such edge, so their gnis_id→wbk mapping is missing.

    This function fills the gap by scanning atlas.lakes / wetlands / manmade
    and adding every PolygonRecord.gnis_id → waterbody_key pair.
    """
    gnis_to_wbk = metadata["_gnis_to_wbk"]
    added = 0
    for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
        for wbk, rec in collection.items():
            if rec.gnis_id:
                before = len(gnis_to_wbk.get(rec.gnis_id, set()))
                gnis_to_wbk.setdefault(rec.gnis_id, set()).add(wbk)
                if len(gnis_to_wbk[rec.gnis_id]) > before:
                    added += 1
    logger.info("  Polygon gnis_id enrichment: %d new gnis→wbk links", added)


# ---------------------------------------------------------------------------
# Internal resolution helpers
# ---------------------------------------------------------------------------


def _resolve_by_gnis_ids(
    gnis_ids: List[str],
    metadata: AtlasMetadata,
    atlas: FreshWaterAtlas,
) -> Tuple[Set[str], Set[str], List[str], Dict[str, List[str]]]:
    """Resolve gnis_ids → fids, waterbody_keys, and seed lists.

    Returns:
        (stream_fids, waterbody_keys, stream_seeds, lake_seeds)
    """
    stream_fids: Set[str] = set()
    waterbody_keys: Set[str] = set()
    stream_seeds: List[str] = []
    lake_seeds: Dict[str, List[str]] = {}

    gnis_to_wbk = metadata["_gnis_to_wbk"]
    wbk_to_fids = metadata["_wbk_to_fids"]

    for gid in gnis_ids:
        # Stream resolution
        s_meta = metadata["streams"].get(gid)
        if s_meta:
            fids = s_meta["edge_ids"]
            stream_fids.update(fids)
            stream_seeds.extend(fids)
        else:
            logger.debug("GNIS %s: no stream metadata", gid)

        # Polygon resolution (gnis_id → waterbody_keys)
        wbks = gnis_to_wbk.get(gid, set())
        for wbk in wbks:
            # Check which atlas collection has this wbk
            if wbk in atlas.lakes or wbk in atlas.wetlands or wbk in atlas.manmade:
                waterbody_keys.add(wbk)
                # Lake seeds: outlet streams for this waterbody
                outlet_fids = wbk_to_fids.get(wbk, [])
                if outlet_fids:
                    lake_seeds[wbk] = outlet_fids

    return stream_fids, waterbody_keys, stream_seeds, lake_seeds


def _resolve_by_waterbody_keys(
    wbks: List[str],
    atlas: FreshWaterAtlas,
    metadata: AtlasMetadata,
) -> Tuple[Set[str], Dict[str, List[str]]]:
    """Resolve explicit waterbody_keys → wbks and lake seeds."""
    waterbody_keys: Set[str] = set()
    lake_seeds: Dict[str, List[str]] = {}
    wbk_to_fids = metadata["_wbk_to_fids"]

    for wbk in wbks:
        if wbk in atlas.lakes or wbk in atlas.wetlands or wbk in atlas.manmade:
            waterbody_keys.add(wbk)
            outlet_fids = wbk_to_fids.get(wbk, [])
            if outlet_fids:
                lake_seeds[wbk] = outlet_fids
        else:
            logger.warning("waterbody_key %s not found in atlas", wbk)

    return waterbody_keys, lake_seeds


def _resolve_by_waterbody_poly_ids(
    poly_ids: List[str],
    atlas: FreshWaterAtlas,
    metadata: AtlasMetadata,
) -> Tuple[Set[str], Dict[str, List[str]]]:
    """Resolve waterbody_poly_ids → waterbody_keys via atlas index, then seeds."""
    resolved_wbks: List[str] = []
    for pid in poly_ids:
        wbk = atlas.poly_id_to_wbk.get(pid)
        if wbk:
            resolved_wbks.append(wbk)
        else:
            logger.warning("waterbody_poly_id %s not in poly_id_to_wbk index", pid)
    return _resolve_by_waterbody_keys(resolved_wbks, atlas, metadata)


def _resolve_by_fwa_watershed_codes(
    wscs: List[str],
    atlas: FreshWaterAtlas,
) -> Set[str]:
    """Resolve fwa_watershed_codes → stream fids by scanning atlas."""
    fids: Set[str] = set()
    wsc_set = set(wscs)
    matched_wscs: Set[str] = set()
    for fid, rec in atlas.streams.items():
        if rec.fwa_watershed_code in wsc_set:
            fids.add(fid)
            matched_wscs.add(rec.fwa_watershed_code)
    for fid, rec in atlas.under_lake_streams.items():
        if rec.fwa_watershed_code in wsc_set:
            fids.add(fid)
            matched_wscs.add(rec.fwa_watershed_code)
    for wsc in wscs:
        if wsc not in matched_wscs:
            logger.warning("fwa_watershed_code %s matched zero atlas features", wsc)
    return fids


def _resolve_by_blue_line_keys(
    blks: List[str],
    atlas: FreshWaterAtlas,
) -> Set[str]:
    """Resolve blue_line_keys → stream fids by scanning atlas."""
    fids: Set[str] = set()
    blk_set = set(blks)
    matched_blks: Set[str] = set()
    for fid, rec in atlas.streams.items():
        if rec.blk in blk_set:
            fids.add(fid)
            matched_blks.add(rec.blk)
    for fid, rec in atlas.under_lake_streams.items():
        if rec.blk in blk_set:
            fids.add(fid)
            matched_blks.add(rec.blk)
    for blk in blks:
        if blk not in matched_blks:
            logger.warning("blue_line_key %s matched zero atlas features", blk)
    return fids


def _resolve_by_linear_feature_ids(
    lfids: List[str],
    atlas: FreshWaterAtlas,
) -> Set[str]:
    """Resolve explicit linear_feature_ids → fids (direct lookup)."""
    fids: Set[str] = set()
    for fid in lfids:
        if fid in atlas.streams or fid in atlas.under_lake_streams:
            fids.add(fid)
        else:
            logger.warning("linear_feature_id %s not found in atlas", fid)
    return fids


def _resolve_by_admin_targets(
    admin_targets: List[Dict[str, str]],
    atlas: FreshWaterAtlas,
    stream_fid_index: List[str],
    stream_geom_index: List[Any],
    fid_to_wsc: Dict[str, str],
    poly_wbk_index: List[str],
    poly_geom_index: List[Any],
    bc_boundary: Optional[Any] = None,
    buffer_m: float = 500.0,
) -> Tuple[Set[str], Set[str]]:
    """Resolve admin polygon targets → fids + wbks via two-pass hysteresis.

    Uses match_features_to_polygons for the same two-pass rule as
    MU matching and zone pruning — streams that just graze the buffer
    but never enter the exact polygon are excluded.
    The buffer is clipped to the BC provincial boundary (WMU union)
    so it never extends outside the province.
    """
    from pipeline.enrichment.polygon_filter import match_features_to_polygons

    # Collect all admin polygons and compute their clipped buffers
    admin_polys: List[Any] = []
    admin_buffered: List[Any] = []

    for target in admin_targets:
        layer = target["layer"]
        feature_id = target.get("feature_id")

        admin_records = _get_admin_polygons(atlas, layer, feature_id)
        if not admin_records:
            logger.warning(
                "Admin target layer=%s feature_id=%s: no polygons found",
                layer,
                feature_id,
            )
            continue

        for admin_rec in admin_records:
            polygon = admin_rec.geometry
            if not polygon.is_valid:
                polygon = polygon.buffer(0)
            buf = polygon.buffer(buffer_m)
            # Clip to BC provincial boundary so buffer doesn't leak outside
            if bc_boundary is not None:
                buf = buf.intersection(bc_boundary)
                if buf.is_empty:
                    continue
            admin_polys.append(polygon)
            admin_buffered.append(buf)

    if not admin_polys:
        return set(), set()

    results = match_features_to_polygons(
        polygons=admin_polys,
        buffer_m=buffer_m,
        buffered=admin_buffered,
        stream_fids=stream_fid_index if stream_fid_index else None,
        stream_geoms=stream_geom_index if stream_geom_index else None,
        fid_to_wsc=fid_to_wsc if fid_to_wsc else None,
        waterbody_keys=poly_wbk_index if poly_wbk_index else None,
        waterbody_geoms=poly_geom_index if poly_geom_index else None,
    )

    # Union results across all admin polygons
    stream_fids: Set[str] = set()
    waterbody_keys: Set[str] = set()
    for matched_fids, matched_wbks in results:
        stream_fids |= matched_fids
        waterbody_keys |= matched_wbks

    return stream_fids, waterbody_keys


def _get_admin_polygons(atlas: FreshWaterAtlas, layer: str, feature_id: Optional[str]):
    """Look up admin polygons from atlas by layer name."""
    if layer == "parks_nat":
        records = atlas.parks_nat
    elif layer in ("parks_bc", "eco_reserves"):
        records = atlas.eco_reserves
    elif layer == "wma":
        records = atlas.wma
    elif layer == "historic_sites":
        records = atlas.historic_sites
    elif layer == "watersheds":
        records = atlas.watersheds
    elif layer == "osm_admin_boundaries":
        records = atlas.osm_admin
    elif layer == "aboriginal_lands":
        records = atlas.aboriginal_lands
    else:
        raise ValueError(f"Unknown admin layer: {layer}")

    if feature_id:
        rec = records.get(feature_id)
        return [rec] if rec else []
    return list(records.values())


# ---------------------------------------------------------------------------
# Zone pruning for only_within_zones
# ---------------------------------------------------------------------------


def _prune_features_by_zones(
    fids: Set[str],
    wbks: Set[str],
    only_within_zones: List[str],
    zone_polygons: Dict[str, Any],
    atlas: FreshWaterAtlas,
    buffer_m: float = 500.0,
) -> Tuple[Set[str], Set[str]]:
    """Filter stream fids and waterbody keys to those within target zones.

    Delegates to polygon_filter.match_features_to_polygons for the
    vectorized STRtree two-pass hysteresis — the same code path used
    by base_reg_assigner for MU matching.
    """
    from pipeline.enrichment.polygon_filter import match_features_to_polygons
    from shapely.ops import unary_union

    target_polys = [zone_polygons[z] for z in only_within_zones if z in zone_polygons]
    if not target_polys:
        logger.warning(
            "only_within_zones %s: none matched WMU zones %s — no pruning applied",
            only_within_zones,
            sorted(zone_polygons.keys()),
        )
        return fids, wbks

    target_exact = unary_union(target_polys)

    # Build parallel fid/geom/wsc lists for candidate streams
    stream_fids_list: List[str] = []
    stream_geoms_list: List[Any] = []
    fid_to_wsc: Dict[str, str] = {}
    for fid in fids:
        rec = atlas.streams.get(fid)
        if rec:
            stream_fids_list.append(fid)
            stream_geoms_list.append(rec.geometry)
            fid_to_wsc[fid] = rec.fwa_watershed_code or fid

    # Build parallel wbk/geom lists for candidate waterbodies
    wbk_list: List[str] = []
    wbk_geoms: List[Any] = []
    for wbk in wbks:
        for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
            rec = collection.get(wbk)
            if rec:
                wbk_list.append(wbk)
                wbk_geoms.append(rec.geometry)
                break

    (matched_fids, matched_wbks) = match_features_to_polygons(
        polygons=[target_exact],
        buffer_m=buffer_m,
        stream_fids=stream_fids_list if stream_fids_list else None,
        stream_geoms=stream_geoms_list if stream_geoms_list else None,
        fid_to_wsc=fid_to_wsc if fid_to_wsc else None,
        waterbody_keys=wbk_list if wbk_list else None,
        waterbody_geoms=wbk_geoms if wbk_geoms else None,
    )[0]
    return matched_fids, matched_wbks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_features(
    records: List[RegulationRecord],
    atlas: FreshWaterAtlas,
    metadata: AtlasMetadata,
) -> Tuple[List[ResolvedRegulation], FeatureAssignment]:
    """Resolve each RegulationRecord to atlas features.

    Returns:
        (resolved_list, assignments) where resolved_list has seed info
        for Phase 3, and assignments has fid→reg_ids / wbk→reg_ids
        populated for direct matches.
    """
    assignments = FeatureAssignment()
    resolved: List[ResolvedRegulation] = []

    # Build parallel feature arrays once for admin-target lookups.
    # match_features_to_polygons builds its own STRtrees internally.
    has_admin = any(
        isinstance(r.match_entry, OverrideEntry) and r.match_entry.admin_targets
        for r in records
    )
    stream_fid_index: List[str] = []
    stream_geom_index: List[Any] = []
    admin_fid_to_wsc: Dict[str, str] = {}
    poly_wbk_index: List[str] = []
    poly_geom_index: List[Any] = []

    if has_admin:
        # Streams + under-lake streams
        for fid, rec_s in atlas.streams.items():
            stream_fid_index.append(fid)
            stream_geom_index.append(rec_s.geometry)
            admin_fid_to_wsc[fid] = rec_s.fwa_watershed_code or fid
        for fid, rec_s in atlas.under_lake_streams.items():
            stream_fid_index.append(fid)
            stream_geom_index.append(rec_s.geometry)
            admin_fid_to_wsc[fid] = rec_s.fwa_watershed_code or fid
        logger.info(
            "  Built stream arrays for admin: %d geometries", len(stream_fid_index)
        )

        # Waterbody polygons
        for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
            for wbk, poly_rec in collection.items():
                poly_wbk_index.append(wbk)
                poly_geom_index.append(poly_rec.geometry)
        logger.info(
            "  Built polygon arrays for admin: %d geometries", len(poly_wbk_index)
        )

    # Build BC provincial boundary for admin buffer clipping
    bc_boundary: Optional[Any] = None
    if has_admin and atlas.wmu:
        from shapely.ops import unary_union as _union

        bc_boundary = _union([r.geometry for r in atlas.wmu.values()])
        if not bc_boundary.is_valid:
            bc_boundary = bc_boundary.buffer(0)
        logger.info("  Built BC boundary for admin buffer clipping")

    # Zone polygons from atlas (built during atlas construction from WMU REGION_RESPONSIBLE_ID)
    zone_polygons: Optional[Dict[str, Any]] = atlas.zone_polygons or None

    for rec in tqdm(records, desc="  Phase 2: resolving", leave=False):
        entry = rec.match_entry
        all_stream_fids: Set[str] = set()
        all_wbks: Set[str] = set()
        all_stream_seeds: List[str] = []
        all_lake_seeds: Dict[str, List[str]] = {}

        # 1. gnis_ids (both BaseEntry and OverrideEntry have this)
        if entry.gnis_ids:
            fids, wbks, seeds, lk_seeds = _resolve_by_gnis_ids(
                entry.gnis_ids, metadata, atlas
            )
            all_stream_fids |= fids
            all_wbks |= wbks
            all_stream_seeds.extend(seeds)
            all_lake_seeds.update(lk_seeds)

        # 2-6. Override-only ID types
        admin_fids: Set[str] = set()
        admin_wbks: Set[str] = set()
        if isinstance(entry, OverrideEntry):
            if entry.waterbody_keys:
                wbks, lk_seeds = _resolve_by_waterbody_keys(
                    entry.waterbody_keys, atlas, metadata
                )
                all_wbks |= wbks
                all_lake_seeds.update(lk_seeds)

            if entry.waterbody_poly_ids:
                wbks, lk_seeds = _resolve_by_waterbody_poly_ids(
                    entry.waterbody_poly_ids, atlas, metadata
                )
                all_wbks |= wbks
                all_lake_seeds.update(lk_seeds)

            if entry.fwa_watershed_codes:
                fids = _resolve_by_fwa_watershed_codes(entry.fwa_watershed_codes, atlas)
                all_stream_fids |= fids
                all_stream_seeds.extend(fids)

            if entry.blue_line_keys:
                fids = _resolve_by_blue_line_keys(entry.blue_line_keys, atlas)
                all_stream_fids |= fids
                all_stream_seeds.extend(fids)

            if entry.linear_feature_ids:
                fids = _resolve_by_linear_feature_ids(entry.linear_feature_ids, atlas)
                all_stream_fids |= fids
                all_stream_seeds.extend(fids)

            if entry.admin_targets:
                fids, wbks = _resolve_by_admin_targets(
                    entry.admin_targets,
                    atlas,
                    stream_fid_index,
                    stream_geom_index,
                    admin_fid_to_wsc,
                    poly_wbk_index,
                    poly_geom_index,
                    bc_boundary=bc_boundary,
                )
                all_stream_fids |= fids
                all_wbks |= wbks
                admin_fids = fids
                admin_wbks = wbks

        # Zone pruning: restrict features to only_within_zones if specified
        if (
            isinstance(entry, OverrideEntry)
            and entry.only_within_zones
            and zone_polygons
        ):
            before_fids = len(all_stream_fids)
            before_wbks = len(all_wbks)
            all_stream_fids, all_wbks = _prune_features_by_zones(
                all_stream_fids,
                all_wbks,
                entry.only_within_zones,
                zone_polygons,
                atlas,
            )
            # Re-filter seeds and lake seeds to match pruned sets
            all_stream_seeds = [f for f in all_stream_seeds if f in all_stream_fids]
            all_lake_seeds = {
                wbk: fids for wbk, fids in all_lake_seeds.items() if wbk in all_wbks
            }
            pruned_fids = before_fids - len(all_stream_fids)
            pruned_wbks = before_wbks - len(all_wbks)
            if pruned_fids or pruned_wbks:
                logger.info(
                    "  %s: only_within_zones=%s pruned %d fids, %d wbks",
                    rec.reg_id,
                    entry.only_within_zones,
                    pruned_fids,
                    pruned_wbks,
                )

        # Determine tributary status from parsed output, with symbol cross-check
        includes_tribs = False
        trib_only = False
        has_trib_symbol = "Incl. Tribs" in rec.symbols or "Tribs Only" in rec.symbols
        if rec.parsed:
            includes_tribs = rec.parsed.get("includes_tributaries", False) is True
            trib_only = rec.parsed.get("tributary_only", False) is True
            if trib_only:
                includes_tribs = True
        elif has_trib_symbol:
            # Parse failed but synopsis symbols indicate tributaries —
            # don’t silently drop the tributary expansion.
            includes_tribs = True
            trib_only = "Tribs Only" in rec.symbols
            logger.warning(
                "%s: parsed=None but symbols=%s — defaulting includes_tribs=%s trib_only=%s",
                rec.reg_id,
                rec.symbols,
                includes_tribs,
                trib_only,
            )

        # Register direct assignments
        # When tributary_only is True, the regulation applies only to
        # tributaries (Phase 3 BFS), not to the named waterbody itself.
        if not trib_only:
            for fid in all_stream_fids:
                assignments.assign_fid(
                    fid,
                    rec.reg_id,
                    phase=2,
                    is_admin=fid in admin_fids,
                )
            for wbk in all_wbks:
                assignments.assign_wbk(
                    wbk,
                    rec.reg_id,
                    phase=2,
                    is_admin=wbk in admin_wbks,
                )

        # Stream seeds exclude under-lake fids — those are virtual edges
        # inside a lake and belong to lake_outlet_fids, not stream seeds.
        clean_stream_seeds = tuple(
            f for f in all_stream_seeds if str(f) not in atlas.under_lake_streams
        )

        resolved.append(
            ResolvedRegulation(
                record=rec,
                matched_stream_fids=(
                    frozenset(all_stream_fids) if not trib_only else frozenset()
                ),
                matched_waterbody_keys=(
                    frozenset(all_wbks) if not trib_only else frozenset()
                ),
                includes_tributaries=includes_tribs,
                tributary_only=trib_only,
                tributary_stream_seeds=clean_stream_seeds,
                lake_outlet_fids=tuple(
                    (wbk, tuple(fids)) for wbk, fids in all_lake_seeds.items()
                ),
            )
        )

    no_match = sum(
        1
        for r in resolved
        if not r.tributary_only
        and not r.matched_stream_fids
        and not r.matched_waterbody_keys
    )
    logger.info(
        "Phase 2 complete: %d resolved (%d with no atlas match), %s",
        len(resolved),
        no_match,
        assignments.summary(),
    )
    return resolved, assignments
