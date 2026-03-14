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

import numpy as np
from shapely import STRtree
from tqdm import tqdm

from regulation_mapping_v2.atlas.freshwater_atlas import FreshWaterAtlas
from regulation_mapping_v2.atlas.models import StreamRecord
from regulation_mapping_v2.matching.match_table import BaseEntry, OverrideEntry

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


def build_atlas_metadata(atlas: FreshWaterAtlas) -> AtlasMetadata:
    """Build gnis_id → feature lookups from atlas collections.

    Scans all atlas streams/lakes/wetlands/manmade to create the same
    metadata structure that base_entry_builder produces, but directly
    from the atlas pickle (no re-reading GPKG).
    """
    # Streams: group by gnis_id (derived from display_name matching won't
    # work — we need the actual gnis_id.  Unfortunately atlas StreamRecord
    # doesn't carry gnis_id, only display_name.  We need to build the
    # reverse map from the graph or metadata pickle.)
    #
    # For now, we build a simpler mapping:
    #   display_name → [fid, ...]  for streams
    #   waterbody_key → fid mapping for lake-outlet seeds

    # This function will be called by the builder, which also has access
    # to the graph and metadata.  The actual implementation loads the
    # metadata from the graph pickle's edge attributes.
    raise NotImplementedError(
        "build_atlas_metadata requires graph edge attributes.  "
        "Use build_metadata_from_graph() instead."
    )


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

    # Streams grouped by gnis_id
    stream_groups: Dict[str, Dict[str, Any]] = {}
    for edge in graph.es:
        gnis_id = str(edge["gnis_id"] or "")
        if not gnis_id:
            continue
        fid = str(edge["linear_feature_id"])
        if gnis_id not in stream_groups:
            stream_groups[gnis_id] = {
                "gnis_id": gnis_id,
                "gnis_name": edge["gnis_name"] or "",
                "edge_ids": [],
                "fwa_watershed_codes": set(),
                "blue_line_keys": set(),
                "zones": set(),
                "mgmt_units": set(),
            }
        grp = stream_groups[gnis_id]
        grp["edge_ids"].append(fid)
        wsc = edge["fwa_watershed_code"] or ""
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

    logger.info("  Metadata: %d stream GNIS groups", len(streams))

    # Polygon metadata: we build a gnis_id → waterbody_key reverse map
    # by scanning the graph for edges with waterbody_key (these are
    # under-lake streams that tell us which gnis_id a lake belongs to).
    # Also build wbk → [fid] for lake outlet seeds.
    gnis_to_wbk: Dict[str, Set[str]] = defaultdict(set)
    wbk_to_fids: Dict[str, List[str]] = defaultdict(list)

    for edge in graph.es:
        wbk = str(edge["waterbody_key"] or "")
        gnis_id = str(edge["gnis_id"] or "")
        fid = str(edge["linear_feature_id"])
        if wbk:
            wbk_to_fids[wbk].append(fid)
            if gnis_id:
                gnis_to_wbk[gnis_id].add(wbk)

    logger.info("  Metadata: %d waterbody_key groups", len(wbk_to_fids))

    return {
        "streams": streams,
        "lakes": {},  # filled by caller from atlas polygons
        "wetlands": {},
        "manmade": {},
        # Extra lookups stored as module-level state
        "_gnis_to_wbk": gnis_to_wbk,  # type: ignore[typeddict-unknown-key]
        "_wbk_to_fids": wbk_to_fids,  # type: ignore[typeddict-unknown-key]
    }


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

    gnis_to_wbk = metadata.get("_gnis_to_wbk", {})  # type: ignore[typeddict-item]
    wbk_to_fids = metadata.get("_wbk_to_fids", {})  # type: ignore[typeddict-item]

    for gid in gnis_ids:
        # Stream resolution
        s_meta = metadata["streams"].get(gid)
        if s_meta:
            fids = s_meta["edge_ids"]
            stream_fids.update(fids)
            stream_seeds.extend(fids)

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
    wbk_to_fids = metadata.get("_wbk_to_fids", {})  # type: ignore[typeddict-item]

    for wbk in wbks:
        if wbk in atlas.lakes or wbk in atlas.wetlands or wbk in atlas.manmade:
            waterbody_keys.add(wbk)
            outlet_fids = wbk_to_fids.get(wbk, [])
            if outlet_fids:
                lake_seeds[wbk] = outlet_fids
        else:
            logger.warning("waterbody_key %s not found in atlas", wbk)

    return waterbody_keys, lake_seeds


def _resolve_by_fwa_watershed_codes(
    wscs: List[str],
    atlas: FreshWaterAtlas,
) -> Set[str]:
    """Resolve fwa_watershed_codes → stream fids by scanning atlas."""
    fids: Set[str] = set()
    wsc_set = set(wscs)
    for fid, rec in atlas.streams.items():
        if rec.fwa_watershed_code in wsc_set:
            fids.add(fid)
    for fid, rec in atlas.under_lake_streams.items():
        if rec.fwa_watershed_code in wsc_set:
            fids.add(fid)
    return fids


def _resolve_by_blue_line_keys(
    blks: List[str],
    atlas: FreshWaterAtlas,
) -> Set[str]:
    """Resolve blue_line_keys → stream fids by scanning atlas."""
    fids: Set[str] = set()
    blk_set = set(blks)
    for fid, rec in atlas.streams.items():
        if rec.blk in blk_set:
            fids.add(fid)
    for fid, rec in atlas.under_lake_streams.items():
        if rec.blk in blk_set:
            fids.add(fid)
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
    stream_tree: STRtree,
    stream_fid_index: List[str],
    poly_tree: STRtree,
    poly_wbk_index: List[str],
    buffer_m: float = 500.0,
) -> Tuple[Set[str], Set[str]]:
    """Resolve admin polygon targets → fids + wbks via STRtree queries.

    Uses pre-built spatial indices on stream and waterbody geometries.
    Each admin polygon is buffered and queried against the trees.
    """
    stream_fids: Set[str] = set()
    waterbody_keys: Set[str] = set()

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
            buffered = polygon.buffer(buffer_m)

            # Query streams
            hits = stream_tree.query(buffered, predicate="intersects")
            for idx in hits:
                stream_fids.add(stream_fid_index[idx])

            # Query waterbody polygons
            hits = poly_tree.query(buffered, predicate="intersects")
            for idx in hits:
                waterbody_keys.add(poly_wbk_index[idx])

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
    else:
        logger.warning("Unknown admin layer: %s", layer)
        return []

    if feature_id:
        rec = records.get(feature_id)
        return [rec] if rec else []
    return list(records.values())


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

    # Build spatial indices once for admin-target lookups
    has_admin = any(
        isinstance(r.match_entry, OverrideEntry) and r.match_entry.admin_targets
        for r in records
    )
    stream_tree: Optional[STRtree] = None
    stream_fid_index: List[str] = []
    poly_tree: Optional[STRtree] = None
    poly_wbk_index: List[str] = []

    if has_admin:
        # Streams + under-lake streams
        stream_geoms: List[Any] = []
        for fid, rec_s in atlas.streams.items():
            stream_fid_index.append(fid)
            stream_geoms.append(rec_s.geometry)
        for fid, rec_s in atlas.under_lake_streams.items():
            stream_fid_index.append(fid)
            stream_geoms.append(rec_s.geometry)
        stream_tree = STRtree(stream_geoms)
        logger.info("  Built stream STRtree: %d geometries", len(stream_geoms))
        del stream_geoms

        # Waterbody polygons
        poly_geoms: List[Any] = []
        for collection in (atlas.lakes, atlas.wetlands, atlas.manmade):
            for wbk, poly_rec in collection.items():
                poly_wbk_index.append(wbk)
                poly_geoms.append(poly_rec.geometry)
        poly_tree = STRtree(poly_geoms)
        logger.info("  Built polygon STRtree: %d geometries", len(poly_geoms))
        del poly_geoms

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
        if isinstance(entry, OverrideEntry):
            if entry.waterbody_keys:
                wbks, lk_seeds = _resolve_by_waterbody_keys(
                    entry.waterbody_keys, atlas, metadata
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
                    stream_tree,
                    stream_fid_index,
                    poly_tree,
                    poly_wbk_index,
                )
                all_stream_fids |= fids
                all_wbks |= wbks

        # Determine tributary status from parsed output
        includes_tribs = False
        if rec.parsed:
            includes_tribs = rec.parsed.get("includes_tributaries", False)

        # Register direct assignments
        for fid in all_stream_fids:
            assignments.assign_fid(fid, rec.reg_id, phase=2)
        for wbk in all_wbks:
            assignments.assign_wbk(wbk, rec.reg_id, phase=2)

        resolved.append(
            ResolvedRegulation(
                record=rec,
                matched_stream_fids=frozenset(all_stream_fids),
                matched_waterbody_keys=frozenset(all_wbks),
                includes_tributaries=includes_tribs,
                tributary_stream_seeds=tuple(all_stream_seeds),
                lake_outlet_fids=tuple(
                    (wbk, tuple(fids)) for wbk, fids in all_lake_seeds.items()
                ),
            )
        )

    no_match = sum(
        1
        for r in resolved
        if not r.matched_stream_fids and not r.matched_waterbody_keys
    )
    logger.info(
        "Phase 2 complete: %d resolved (%d with no atlas match), %s",
        len(resolved),
        no_match,
        assignments.summary(),
    )
    return resolved, assignments
