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

from pyproj import Transformer
from shapely.geometry.base import BaseGeometry

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


# EPSG:3005 → WGS 84 transformer (cached once)
_TO_WGS84 = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)


def _bbox_wgs84(geom: BaseGeometry) -> List[float]:
    """Compute [minlon, minlat, maxlon, maxlat] from an EPSG:3005 geometry."""
    minx, miny, maxx, maxy = geom.bounds
    lon1, lat1 = _TO_WGS84.transform(minx, miny)
    lon2, lat2 = _TO_WGS84.transform(maxx, maxy)
    return [
        round(min(lon1, lon2), 5),
        round(min(lat1, lat2), 5),
        round(max(lon1, lon2), 5),
        round(max(lat1, lat2), 5),
    ]


def _union_bbox(a: List[float], b: List[float]) -> List[float]:
    """Merge two [minlon, minlat, maxlon, maxlat] bboxes."""
    return [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]


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
    reach_level_reg_ids: Set[str] = frozenset(),
) -> Dict[str, Dict[str, Any]]:
    """Group stream fids into reaches by (WSC, sorted reg_set).

    Regulations in ``reach_level_reg_ids`` are excluded from the grouping
    key so they never cause a reach to split.  After grouping, if ANY fid
    in a reach carries such a reg, the whole reach gets it.

    Returns reach_id → {display_name, wsc, fids, reg_set_str, feature_type, minzoom, regions}
    """
    # Group fids by (wsc, reg_set_str)
    # NOTE: under_lake_streams are excluded — they should not form reaches
    # or appear as "Sections" in the info panel.
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    all_streams = atlas.streams

    for fid, reg_ids in assignments.fid_to_reg_ids.items():
        stream = all_streams.get(fid)
        if stream is None:
            continue

        # Exclude reach-level regs from the grouping key
        grouping_ids = sorted(reg_ids - reach_level_reg_ids)
        reg_set_str = ",".join(grouping_ids)
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
                "_has_reach_level_regs": set(),
            }

        grp = groups[group_key]
        grp["fids"].append(fid)
        # Track which reach-level regs appear on any fid in this group
        grp["_has_reach_level_regs"].update(reg_ids & reach_level_reg_ids)
        if stream.minzoom < grp["minzoom"]:
            grp["minzoom"] = stream.minzoom
        # Prefer a named display_name — first fid may have been unnamed
        if not grp["display_name"] and stream.display_name:
            grp["display_name"] = stream.display_name

    # Promote reach-level regs into reg_set_str and regenerate reach_id
    for grp in groups.values():
        if grp["_has_reach_level_regs"]:
            all_ids = (
                set(grp["reg_set_str"].split(",")) if grp["reg_set_str"] else set()
            )
            all_ids.update(grp["_has_reach_level_regs"])
            all_ids.discard("")
            grp["reg_set_str"] = ",".join(sorted(all_ids))
        del grp["_has_reach_level_regs"]

    # Convert to reach_id → reach_data
    reaches: Dict[str, Dict[str, Any]] = {}
    for (wsc, _), grp in groups.items():
        rid = _reach_id(wsc, grp["display_name"], grp["reg_set_str"])
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
# Reach enrichment (bbox, lkm, rg, z, mu)
# ---------------------------------------------------------------------------


def _enrich_reaches(
    reaches: Dict[str, Dict[str, Any]],
    atlas: FreshWaterAtlas,
    regulations: Dict[str, Dict[str, Any]],
) -> None:
    """Mutate each reach dict in-place: add bbox, lkm, rg, z, mu.

    Must be called after reach grouping and reg deduplication.
    """
    all_streams = {**atlas.streams, **atlas.under_lake_streams}
    poly_lookup: Dict[str, BaseGeometry] = {}
    for coll in (atlas.lakes, atlas.wetlands, atlas.manmade):
        for wbk, rec in coll.items():
            poly_lookup[wbk] = rec.geometry

    for rid, reach in reaches.items():
        # --- bbox ---
        bbox: Optional[List[float]] = None

        if reach["fids"]:
            for fid in reach["fids"]:
                s = all_streams.get(fid)
                if s is None:
                    continue
                fb = _bbox_wgs84(s.geometry)
                bbox = _union_bbox(bbox, fb) if bbox else fb
        elif "wbk" in reach:
            geom = poly_lookup.get(reach["wbk"])
            if geom is not None:
                bbox = _bbox_wgs84(geom)

        reach["bbox"] = bbox

        # --- lkm (stream length in km; EPSG:3005 units are metres) ---
        if reach["fids"]:
            total_m = sum(
                all_streams[f].geometry.length
                for f in reach["fids"]
                if f in all_streams
            )
            reach["lkm"] = round(total_m / 1000.0, 2)
        else:
            reach["lkm"] = 0

        # --- rg (regions), z (zones), mu (management units) from regulations ---
        reg_ids = [r.strip() for r in reach["reg_set_str"].split(",") if r.strip()]
        regions: Set[str] = set()
        zones: Set[str] = set()
        mus: Set[str] = set()
        for reg_id in reg_ids:
            reg = regulations.get(reg_id, {})
            if reg.get("region"):
                regions.add(reg["region"])
            if reg.get("zone"):
                zones.add(reg["zone"])
            for mu_val in reg.get("mu", []):
                mus.add(mu_val)
        reach["rg"] = sorted(regions)
        reach["z"] = sorted(zones)
        reach["mu"] = sorted(mus)


# ---------------------------------------------------------------------------
# Search index builder
# ---------------------------------------------------------------------------


def _build_search_index(
    reaches: Dict[str, Dict[str, Any]],
    atlas: FreshWaterAtlas,
) -> List[Dict[str, Any]]:
    """Build Fuse.js-compatible search index.

    Groups reaches by wsc (watershed code for streams, waterbody_key for
    polygons) → one search entry per distinct geographic feature.
    Same-name features with different wsc get separate entries.
    Unnamed features are excluded from search.

    Reaches must be enriched (_enrich_reaches) before calling this.
    """
    # Group: wsc → aggregated metadata
    wsc_groups: Dict[str, Dict[str, Any]] = {}

    for rid, reach in reaches.items():
        dn = reach["display_name"]
        if not dn:
            continue

        wsc = reach["wsc"]
        if wsc not in wsc_groups:
            wsc_groups[wsc] = {
                "dn": dn,
                "nv": set(),
                "reaches": [],
                "ft": reach["feature_type"],
                "rg": set(),
                "z": set(),
                "mu": set(),
                "mz": reach["minzoom"],
                "bbox": None,
                "tlkm": 0.0,
            }

        grp = wsc_groups[wsc]
        grp["reaches"].append(rid)
        if reach["minzoom"] < grp["mz"]:
            grp["mz"] = reach["minzoom"]
        # Name variants
        grp["nv"] |= reach.get("name_variants", set())
        # Regions, zones, management units (from enriched reaches)
        grp["rg"].update(reach.get("rg", []))
        grp["z"].update(reach.get("z", []))
        grp["mu"].update(reach.get("mu", []))
        # Accumulate bbox
        if reach.get("bbox"):
            grp["bbox"] = (
                _union_bbox(grp["bbox"], reach["bbox"])
                if grp["bbox"]
                else reach["bbox"]
            )
        # Accumulate stream length
        grp["tlkm"] += reach.get("lkm", 0)

    # Convert to list
    index: List[Dict[str, Any]] = []
    for wsc, grp in sorted(wsc_groups.items(), key=lambda kv: kv[1]["dn"]):
        entry: Dict[str, Any] = {
            "dn": grp["dn"],
            "nv": sorted(grp["nv"] - {grp["dn"]}),
            "reaches": grp["reaches"],
            "ft": grp["ft"],
            "rg": sorted(grp["rg"]),
            "mz": grp["mz"],
            "bbox": grp["bbox"],
            "wbg": wsc,
            "z": sorted(grp["z"]),
            "mu": sorted(grp["mu"]),
            "tlkm": round(grp["tlkm"], 2) if grp["tlkm"] else 0,
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
    *,
    reach_level_reg_ids: Set[str] = frozenset(),
) -> Dict[str, Any]:
    """Build the 5-section regulation_index.json.

    Args:
        atlas: FreshWaterAtlas with all feature collections
        assignments: final fid→reg_ids and wbk→reg_ids from phases 2-4
        base_regulations: reg_id→info from Phase 4 (zone + provincial)
        records: RegulationRecords from Phase 1 (for synopsis reg info)
        reach_level_reg_ids: reg IDs that propagate at reach level

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
    stream_reaches = _group_stream_reaches(atlas, assignments, reach_level_reg_ids)
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

    # 3b. Enrich reaches with bbox, lkm, rg, z, mu
    _enrich_reaches(all_reaches, atlas, regulations)
    logger.info("  Reaches enriched (bbox, lkm, rg, z, mu)")

    # 4. Build output sections
    reaches_out: Dict[str, Dict[str, Any]] = {}
    reach_segments_out: Dict[str, List[str]] = {}
    poly_reaches_out: Dict[str, str] = {}

    for rid, reach in all_reaches.items():
        ri = reg_set_index[reach["reg_set_str"]]
        reaches_out[rid] = {
            "dn": reach["display_name"],
            "nv": sorted(reach.get("name_variants", set())),
            "ft": reach["feature_type"],
            "ri": ri,
            "wsc": reach["wsc"],
            "mz": reach["minzoom"],
            "rg": reach.get("rg", []),
            "bbox": reach.get("bbox"),
            "lkm": reach.get("lkm", 0),
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
