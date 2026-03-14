"""Phase 5 — Build regulation_index.json from feature assignments.

Groups atlas features into reaches (by WSC + regulation set for streams,
by waterbody_key for polygons), deduplicates regulation sets, and
assembles the 5-section output per REGULATION_INDEX_DESIGN.md.

Sections:
    regulations     — reg_id → regulation display info
    reg_sets        — dedup'd comma-joined reg_id strings
    reaches         — reach_id → reach metadata
    reach_segments  — reach_id → [fid, ...] (streams only)
    poly_reaches    — waterbody_key → reach_id (polygons only)
    search_index    — Fuse.js-compatible search entries
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from regulation_mapping_v2.atlas.freshwater_atlas import FreshWaterAtlas

from .models import FeatureAssignment, RegulationRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reach ID generation
# ---------------------------------------------------------------------------


def _reach_id(wsc: str, display_name: str, sorted_reg_ids: str) -> str:
    """Generate deterministic reach_id = md5(wsc|display_name|sorted_reg_ids)[:12]."""
    payload = f"{wsc}|{display_name}|{sorted_reg_ids}"
    return hashlib.md5(payload.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Synopsis regulation info builder
# ---------------------------------------------------------------------------


def _build_synopsis_regulations(
    records: List[RegulationRecord],
) -> Dict[str, Dict[str, Any]]:
    """Build regulation info entries for all synopsis records."""
    regulations: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        regulations[rec.reg_id] = {
            "water": rec.water,
            "region": rec.region,
            "mu": list(rec.mu),
            "raw_regs": rec.raw_regs,
            "symbols": list(rec.symbols),
            "source": rec.source,
            "page": rec.page,
            "image": rec.image,
        }
        if rec.parsed:
            regulations[rec.reg_id]["parsed"] = rec.parsed
    return regulations


# ---------------------------------------------------------------------------
# Reach grouping
# ---------------------------------------------------------------------------


def _group_stream_reaches(
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
) -> Dict[str, Dict[str, Any]]:
    """Group stream fids into reaches by (WSC, sorted reg_set).

    Returns reach_id → {display_name, wsc, fids, reg_set_str, feature_type, minzoom, regions}
    """
    # Group fids by (wsc, reg_set_str)
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    all_streams = {**atlas.streams, **atlas.under_lake_streams}

    for fid, reg_ids in assignments.fid_to_reg_ids.items():
        stream = all_streams.get(fid)
        if stream is None:
            continue

        reg_set_str = ",".join(sorted(reg_ids))
        wsc = stream.fwa_watershed_code or stream.blk  # fallback to BLK
        group_key = (wsc, reg_set_str)

        if group_key not in groups:
            groups[group_key] = {
                "wsc": wsc,
                "display_name": stream.display_name,
                "reg_set_str": reg_set_str,
                "fids": [],
                "minzoom": stream.minzoom,
                "feature_type": "stream",
                "name_variants": set(),
            }

        grp = groups[group_key]
        grp["fids"].append(fid)
        if stream.minzoom < grp["minzoom"]:
            grp["minzoom"] = stream.minzoom

    # Convert to reach_id → reach_data
    reaches: Dict[str, Dict[str, Any]] = {}
    for (wsc, reg_set_str), grp in groups.items():
        rid = _reach_id(wsc, grp["display_name"], reg_set_str)
        if rid in reaches:
            # Hash collision — append fids to existing reach
            reaches[rid]["fids"].extend(grp["fids"])
        else:
            reaches[rid] = grp

    return reaches


def _group_polygon_reaches(
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
) -> Dict[str, Dict[str, Any]]:
    """Group polygon waterbody_keys into reaches.

    Each (wbk, reg_set) = one reach.

    Returns reach_id → {display_name, wbk, reg_set_str, feature_type, minzoom}
    """
    reaches: Dict[str, Dict[str, Any]] = {}

    poly_collections = [
        ("lake", atlas.lakes),
        ("wetland", atlas.wetlands),
        ("manmade", atlas.manmade),
    ]

    for ft_name, collection in poly_collections:
        for wbk, reg_ids in assignments.wbk_to_reg_ids.items():
            poly_rec = collection.get(wbk)
            if poly_rec is None:
                continue

            reg_set_str = ",".join(sorted(reg_ids))
            rid = _reach_id(wbk, poly_rec.display_name, reg_set_str)

            reaches[rid] = {
                "wsc": wbk,  # Use wbk as the grouping key for polys
                "display_name": poly_rec.display_name,
                "reg_set_str": reg_set_str,
                "fids": [],  # Polys don't have fid lists
                "wbk": wbk,
                "minzoom": poly_rec.minzoom,
                "feature_type": ft_name,
                "name_variants": set(),
            }

    return reaches


# ---------------------------------------------------------------------------
# Reg set deduplication
# ---------------------------------------------------------------------------


def _dedup_reg_sets(
    reaches: Dict[str, Dict[str, Any]],
) -> Tuple[List[str], Dict[str, int]]:
    """Deduplicate regulation set strings.

    Returns (reg_sets_list, reg_set_str_to_index).
    """
    unique: Dict[str, int] = {}
    for reach in reaches.values():
        rs = reach["reg_set_str"]
        if rs not in unique:
            unique[rs] = len(unique)
    return list(unique.keys()), unique


# ---------------------------------------------------------------------------
# Search index builder
# ---------------------------------------------------------------------------


def _build_search_index(
    reaches: Dict[str, Dict[str, Any]],
    atlas: FreshWaterAtlas,
) -> List[Dict[str, Any]]:
    """Build Fuse.js-compatible search index.

    Groups reaches by display_name → one search entry per named waterbody.
    Unnamed features are excluded from search.
    """
    # Group: display_name → {reach_ids, feature_type, regions, minzoom, bbox}
    name_groups: Dict[str, Dict[str, Any]] = {}

    for rid, reach in reaches.items():
        dn = reach["display_name"]
        if not dn:
            continue

        if dn not in name_groups:
            name_groups[dn] = {
                "dn": dn,
                "nv": set(),
                "reaches": [],
                "ft": reach["feature_type"],
                "rg": set(),
                "mz": reach["minzoom"],
            }

        grp = name_groups[dn]
        grp["reaches"].append(rid)
        if reach["minzoom"] < grp["mz"]:
            grp["mz"] = reach["minzoom"]
        # Name variants
        grp["nv"] |= reach.get("name_variants", set())

    # Convert to list
    index: List[Dict[str, Any]] = []
    for dn, grp in sorted(name_groups.items()):
        entry: Dict[str, Any] = {
            "dn": grp["dn"],
            "nv": sorted(grp["nv"] - {grp["dn"]}),  # exclude primary name
            "reaches": grp["reaches"],
            "ft": grp["ft"],
            "rg": sorted(grp["rg"]),
            "mz": grp["mz"],
        }
        index.append(entry)

    return index


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_regulation_index(
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
    base_regulations: Dict[str, Dict[str, Any]],
    records: List[RegulationRecord],
) -> Dict[str, Any]:
    """Build the 5-section regulation_index.json.

    Args:
        atlas: FreshWaterAtlas with all feature collections
        assignments: final fid→reg_ids and wbk→reg_ids from phases 2-4
        base_regulations: reg_id→info from Phase 4 (zone + provincial)
        records: RegulationRecords from Phase 1 (for synopsis reg info)

    Returns:
        The complete regulation index dict ready for JSON serialization.
    """
    logger.info("Phase 5: building regulation index")

    # 1. Regulations dict (synopsis + base)
    regulations = _build_synopsis_regulations(records)
    regulations.update(base_regulations)
    logger.info(
        "  %d total regulations (%d synopsis, %d base)",
        len(regulations),
        len(records),
        len(base_regulations),
    )

    # 2. Group features into reaches
    stream_reaches = _group_stream_reaches(atlas, assignments)
    poly_reaches_data = _group_polygon_reaches(atlas, assignments)

    # Merge all reaches
    all_reaches = {**stream_reaches, **poly_reaches_data}
    logger.info(
        "  %d total reaches (%d stream, %d polygon)",
        len(all_reaches),
        len(stream_reaches),
        len(poly_reaches_data),
    )

    # 3. Dedup reg sets
    reg_sets_list, reg_set_index = _dedup_reg_sets(all_reaches)
    logger.info("  %d unique regulation sets", len(reg_sets_list))

    # 4. Build output sections
    reaches_out: Dict[str, Dict[str, Any]] = {}
    reach_segments_out: Dict[str, List[str]] = {}
    poly_reaches_out: Dict[str, str] = {}

    for rid, reach in all_reaches.items():
        ri = reg_set_index[reach["reg_set_str"]]
        reaches_out[rid] = {
            "dn": reach["display_name"],
            "ft": reach["feature_type"],
            "ri": ri,
            "wsc": reach["wsc"],
            "mz": reach["minzoom"],
        }

        # Stream reaches → reach_segments
        if reach["fids"]:
            reach_segments_out[rid] = reach["fids"]

        # Polygon reaches → poly_reaches
        if "wbk" in reach:
            poly_reaches_out[reach["wbk"]] = rid

    # 5. Search index
    search_index = _build_search_index(all_reaches, atlas)
    logger.info("  %d search index entries", len(search_index))

    result = {
        "regulations": regulations,
        "reg_sets": reg_sets_list,
        "reaches": reaches_out,
        "reach_segments": reach_segments_out,
        "poly_reaches": poly_reaches_out,
        "search_index": search_index,
    }

    logger.info(
        "Phase 5 complete: %d regulations, %d reg_sets, %d reaches, "
        "%d reach_segments, %d poly_reaches, %d search entries",
        len(regulations),
        len(reg_sets_list),
        len(reaches_out),
        len(reach_segments_out),
        len(poly_reaches_out),
        len(search_index),
    )

    return result
