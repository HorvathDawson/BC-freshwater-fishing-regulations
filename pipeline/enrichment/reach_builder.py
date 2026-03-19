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
from typing import Any, Dict, List, Optional, Set, Tuple

from pyproj import Transformer
from shapely.geometry.base import BaseGeometry

from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
from pipeline.matching.display_name_resolver import DisplayNameResolver
from pipeline.matching.match_table import (
    FEATURE_DISPLAY_NAMES_PATH,
    OVERRIDES_PATH,
)

from .models import FeatureAssignment, RegulationRecord

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Title-casing helper
# ---------------------------------------------------------------------------

import re as _re

_MC_RE = _re.compile(r"\bMc([a-z])")


def _title_case(s: str) -> str:
    """Title-case with Mc/O' prefix handling (McNaughton, O'Clock)."""
    tc = s.title()
    tc = _MC_RE.sub(lambda m: f"Mc{m.group(1).upper()}", tc)
    return tc


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
        entry: Dict[str, Any] = {
            "water": rec.water,
            "region": rec.region,
            "mu": list(rec.mu),
            "raw_regs": rec.raw_regs,
            "symbols": list(rec.symbols),
            "source": rec.source,
            "page": rec.page,
            "image": rec.image,
        }
        if rec.match_entry is not None:
            entry["match_type"] = rec.match_entry.match_type
        else:
            logger.warning(
                "No match_entry for %s (%s) — setting match_type=unmatched",
                rec.reg_id,
                rec.water,
            )
            entry["match_type"] = "unmatched"
        if rec.parsed:
            entry["parsed"] = rec.parsed
        regulations[rec.reg_id] = entry
    return regulations


# ---------------------------------------------------------------------------
# Reach grouping
# ---------------------------------------------------------------------------


def _group_stream_reaches(
    atlas: FreshWaterAtlas,
    assignments: FeatureAssignment,
    reach_level_reg_ids: Set[str] = frozenset(),
    reg_id_variants: Optional[Dict[str, Set[str]]] = None,
    admin_reg_ids: Optional[Set[str]] = None,
    resolver: Optional[DisplayNameResolver] = None,
    reg_water_lookup: Optional[Dict[str, str]] = None,
    trib_reg_ids_all: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Group stream fids into reaches by (WSC, display_name, sorted reg_set).

    Regulations in ``reach_level_reg_ids`` are excluded from the grouping
    key so they never cause a reach to split.  After grouping, if ANY fid
    in a reach carries such a reg, the whole reach gets it.

    Display name is resolved via the shared ``DisplayNameResolver``:
    feature_display_names → GNIS → direct-match regulation name → empty.
    Tributary regulation names are excluded from the display name fallback.

    Returns reach_id → {display_name, wsc, fids, reg_set_str, feature_type, minzoom, regions}
    """
    groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    _variants = reg_id_variants or {}
    _reg_water = reg_water_lookup or {}
    _trib_all = trib_reg_ids_all or set()
    _admin = admin_reg_ids or set()

    all_streams = atlas.streams

    for fid, reg_ids in assignments.fid_to_reg_ids.items():
        stream = all_streams.get(fid)
        if stream is None:
            continue

        # Resolve display name via shared resolver
        direct_reg_name = ""
        if resolver and not stream.display_name:
            # Only use reg name as fallback for unnamed features
            # Exclude tributary-sourced regs — they name the parent, not this feature
            # Exclude admin-sourced regs — they name the admin area, not this feature
            fid_tribs = assignments.fid_to_trib_reg_ids.get(fid, set())
            fid_admins = assignments.fid_to_admin_reg_ids.get(fid, set())
            for rid in sorted(reg_ids - fid_tribs - _trib_all - _admin - fid_admins):
                rn = _reg_water.get(rid, "")
                if rn:
                    direct_reg_name = rn
                    break

        if resolver:
            display_name = resolver.resolve_stream(
                stream.blk,
                stream.display_name,
                direct_reg_name,
                fid,
            )
        else:
            display_name = stream.display_name

        # Exclude reach-level regs from the grouping key
        grouping_ids = sorted(reg_ids - reach_level_reg_ids)
        reg_set_str = ",".join(grouping_ids)
        wsc = stream.fwa_watershed_code or stream.blk  # fallback to BLK
        group_key = (wsc, display_name, reg_set_str)

        if group_key not in groups:
            groups[group_key] = {
                "wsc": wsc,
                "display_name": display_name,
                "reg_set_str": reg_set_str,
                "fids": [],
                "minzoom": stream.minzoom,
                "feature_type": "stream",
                "_nv_direct": set(),  # names from direct regs
                "_nv_trib_only": set(),  # names ONLY from tributary regs
                "_nv_admin_only": set(),  # names ONLY from admin regs
                "_has_reach_level_regs": set(),
                "_trib_intersection": None,
            }

        grp = groups[group_key]
        grp["fids"].append(fid)
        # Collect name_variants, tracking source provenance.
        # Priority: direct > tributary > admin
        fid_tribs = assignments.fid_to_trib_reg_ids.get(fid, set())
        fid_admins = assignments.fid_to_admin_reg_ids.get(fid, set())
        for rid in reg_ids:
            nv_set = _variants.get(rid, set())
            if not nv_set:
                continue
            if rid in _admin or rid in fid_admins:
                grp["_nv_admin_only"].update(nv_set)
            elif rid in fid_tribs:
                grp["_nv_trib_only"].update(nv_set)
            else:
                grp["_nv_direct"].update(nv_set)
                # Direct wins — remove from trib/admin-only
                grp["_nv_trib_only"] -= nv_set
                grp["_nv_admin_only"] -= nv_set
        # Track which reach-level regs appear on any fid in this group
        grp["_has_reach_level_regs"].update(reg_ids & reach_level_reg_ids)
        # Running intersection of tributary reg_ids across all fids
        if grp["_trib_intersection"] is None:
            grp["_trib_intersection"] = fid_tribs.copy()
        else:
            grp["_trib_intersection"] &= fid_tribs
        if stream.minzoom < grp["minzoom"]:
            grp["minzoom"] = stream.minzoom
        # Prefer a named display_name — first fid may have been unnamed
        if not grp["display_name"] and display_name:
            grp["display_name"] = display_name

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

        # Finalize tributary intersection: restrict to actual reach reg_ids
        trib = grp.pop("_trib_intersection", None) or set()
        reach_reg_ids = set(grp["reg_set_str"].split(",")) - {""}
        grp["_trib_reg_ids"] = sorted(trib & reach_reg_ids)

        # Finalize structured name_variants with source provenance
        direct_names = grp.pop("_nv_direct")
        trib_only_names = grp.pop("_nv_trib_only") - direct_names
        admin_only_names = grp.pop("_nv_admin_only") - direct_names - trib_only_names
        structured_nv: List[Dict[str, Any]] = []
        for name in sorted(direct_names):
            structured_nv.append({"name": name, "source": "direct"})
        for name in sorted(trib_only_names):
            structured_nv.append({"name": name, "source": "tributary"})
        for name in sorted(admin_only_names):
            structured_nv.append({"name": name, "source": "admin"})
        grp["name_variants"] = structured_nv

    # Convert to reach_id → reach_data
    reaches: Dict[str, Dict[str, Any]] = {}
    for (wsc, _dn, _rs), grp in groups.items():
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
    reg_id_variants: Optional[Dict[str, Set[str]]] = None,
    admin_reg_ids: Optional[Set[str]] = None,
    resolver: Optional[DisplayNameResolver] = None,
    reg_water_lookup: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Group polygon waterbody_keys into reaches.

    Each (wbk, reg_set) = one reach.

    Returns reach_id → {display_name, wbk, reg_set_str, feature_type, minzoom}
    """
    reaches: Dict[str, Dict[str, Any]] = {}
    _variants = reg_id_variants or {}
    _reg_water = reg_water_lookup or {}
    _admin = admin_reg_ids or set()

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

            # Resolve display name via shared resolver
            direct_reg_name = ""
            if resolver and not poly_rec.display_name:
                # Exclude admin-sourced regs — they name the admin area, not this feature
                wbk_admins = assignments.wbk_to_admin_reg_ids.get(wbk, set())
                for rid in sorted(reg_ids - _admin - wbk_admins):
                    rn = _reg_water.get(rid, "")
                    if rn:
                        direct_reg_name = rn
                        break
            if resolver:
                display_name = resolver.resolve_polygon(
                    wbk,
                    poly_rec.display_name,
                    direct_reg_name,
                )
            else:
                display_name = poly_rec.display_name

            reg_set_str = ",".join(sorted(reg_ids))
            rid = _reach_id(wbk, display_name, reg_set_str)

            # Collect name_variants with source provenance
            wbk_admins = assignments.wbk_to_admin_reg_ids.get(wbk, set())
            nv_direct: Set[str] = set()
            nv_admin: Set[str] = set()
            for r in reg_ids:
                nv_set = _variants.get(r, set())
                if not nv_set:
                    continue
                if r in _admin or r in wbk_admins:
                    nv_admin.update(nv_set)
                else:
                    nv_direct.update(nv_set)
                    nv_admin -= nv_set

            structured_nv: List[Dict[str, Any]] = []
            for n in sorted(nv_direct):
                structured_nv.append({"name": n, "source": "direct"})
            for n in sorted(nv_admin - nv_direct):
                structured_nv.append({"name": n, "source": "admin"})

            reaches[rid] = {
                "wsc": wbk,  # Use wbk as the grouping key for polys
                "display_name": display_name,
                "reg_set_str": reg_set_str,
                "fids": [],  # Polys don't have fid lists
                "wbk": wbk,
                "minzoom": poly_rec.minzoom,
                "feature_type": ft_name,
                "name_variants": structured_nv,
            }

    return reaches


# ---------------------------------------------------------------------------
# Ungazetted waterbody reaches
# ---------------------------------------------------------------------------


def _build_ungazetted_reaches(
    records: List["RegulationRecord"],
) -> Dict[str, Dict[str, Any]]:
    """Build synthetic reaches for ungazetted waterbodies.

    These are fishing spots that don't exist in the FWA atlas — no polygon
    or stream geometry.  They appear in search and display regulations but
    have no tile footprint on the map.

    Each override with ``ungazetted_waterbody_id`` AND ``ungazetted_location``
    produces one reach keyed by the ungazetted ID.
    """
    from pipeline.matching.match_table import OverrideEntry

    reaches: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        entry = rec.match_entry
        if not isinstance(entry, OverrideEntry):
            continue
        if not entry.ungazetted_waterbody_id or not entry.ungazetted_location:
            continue
        loc = entry.ungazetted_location
        if not isinstance(loc, list) or len(loc) != 2:
            continue

        uid = entry.ungazetted_waterbody_id
        # Avoid duplicates if multiple regs share the same ungazetted_waterbody_id
        if uid in reaches:
            # Append this reg_id to the existing reach
            existing_regs = (
                set(reaches[uid]["reg_set_str"].split(","))
                if reaches[uid]["reg_set_str"]
                else set()
            )
            existing_regs.add(rec.reg_id)
            existing_regs.discard("")
            reaches[uid]["reg_set_str"] = ",".join(sorted(existing_regs))
            continue

        # Build name variants from the override (title-cased for display)
        nv: List[Dict[str, str]] = []
        for name in entry.name_variants:
            tc = _title_case(name.replace('"', '').strip())
            if tc != rec.water:
                nv.append({"name": tc, "source": "direct"})

        reaches[uid] = {
            "wsc": uid,  # Use ungazetted ID as the grouping key
            "display_name": rec.water,
            "reg_set_str": rec.reg_id,
            "fids": [],
            "minzoom": 13,
            "feature_type": "ungazetted",
            "name_variants": nv,
            "bbox": [loc[0], loc[1], loc[0], loc[1]],  # point bbox
            "lkm": 0,
            "rg": [entry.criteria.region] if entry.criteria.region else [],
            "z": [],
            "mu": list(entry.criteria.mus) if entry.criteria.mus else [],
        }

    if reaches:
        logger.info("  %d ungazetted reaches created", len(reaches))
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
        # Ungazetted reaches are pre-enriched — skip them
        if reach.get("feature_type") == "ungazetted":
            continue

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

    Groups reaches by (wsc, display_name) → one search entry per distinct
    named geographic feature.  Features with different display names on the
    same watershed code (e.g. side channels) get separate entries.
    Unnamed features are excluded from search.

    Reaches must be enriched (_enrich_reaches) before calling this.
    """
    # Group: (wsc, display_name) → aggregated metadata
    wsc_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for rid, reach in reaches.items():
        dn = reach["display_name"]
        if not dn:
            continue

        wsc = reach["wsc"]
        search_key = (wsc, dn)
        if search_key not in wsc_groups:
            wsc_groups[search_key] = {
                "display_name": dn,
                "name_variants": {},  # name → source (direct > tributary > admin)
                "reaches": [],
                "feature_type": reach["feature_type"],
                "regions": set(),
                "zones": set(),
                "management_units": set(),
                "min_zoom": reach["minzoom"],
                "bbox": None,
                "total_length_km": 0.0,
            }

        grp = wsc_groups[search_key]
        grp["reaches"].append(rid)
        if reach["minzoom"] < grp["min_zoom"]:
            grp["min_zoom"] = reach["minzoom"]
        # Name variants: merge structured [{name, source}] across reaches.
        # Most specific source wins: direct > tributary > admin.
        _SOURCE_PRIORITY = {"direct": 0, "tributary": 1, "admin": 2}
        for nv_entry in reach.get("name_variants", []):
            name = nv_entry["name"]
            src = nv_entry["source"]
            existing = grp["name_variants"].get(name)
            if existing is None or _SOURCE_PRIORITY.get(src, 9) < _SOURCE_PRIORITY.get(
                existing, 9
            ):
                grp["name_variants"][name] = src
        # Regions, zones, management units (from enriched reaches)
        grp["regions"].update(reach.get("rg", []))
        grp["zones"].update(reach.get("z", []))
        grp["management_units"].update(reach.get("mu", []))
        # Accumulate bbox
        if reach.get("bbox"):
            grp["bbox"] = (
                _union_bbox(grp["bbox"], reach["bbox"])
                if grp["bbox"]
                else reach["bbox"]
            )
        # Accumulate stream length
        grp["total_length_km"] += reach.get("lkm", 0)

    # Convert to list
    index: List[Dict[str, Any]] = []
    for (wsc, dn), grp in sorted(
        wsc_groups.items(), key=lambda kv: kv[1]["display_name"]
    ):
        # Build structured name_variants list, excluding display_name
        nv_list = [
            {"name": n, "source": src}
            for n, src in sorted(grp["name_variants"].items())
            if n != grp["display_name"]
        ]
        entry: Dict[str, Any] = {
            "display_name": grp["display_name"],
            "name_variants": nv_list,
            "reaches": grp["reaches"],
            "feature_type": grp["feature_type"],
            "regions": sorted(grp["regions"]),
            "min_zoom": grp["min_zoom"],
            "bbox": grp["bbox"],
            "waterbody_group": wsc,
            "zones": sorted(grp["zones"]),
            "management_units": sorted(grp["management_units"]),
            "total_length_km": (
                round(grp["total_length_km"], 2) if grp["total_length_km"] else 0
            ),
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
    match_table_path: Optional[str] = None,
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

    # 1b. Build reg_id → display names for reach name_variants.
    # Combines canonical_name (powers "Tributary of X" / "In X") with
    # actual name_variants (alternative names like "Jones Lake").
    # All names are title-cased for display; matching uses ALL-CAPS originals.
    # Variants that just duplicate the regulation's own water name (stripped
    # parentheticals) are excluded — they exist only for match-table lookup.
    reg_id_variants: Dict[str, Set[str]] = {}
    for rec in records:
        me = rec.match_entry
        names: Set[str] = set()
        # The water name itself (title-cased) — used to filter out redundant variants
        water_tc = _title_case(rec.water) if rec.water else ""
        if me:
            if hasattr(me, "canonical_name") and me.canonical_name:
                names.add(me.canonical_name)
            for nv in me.name_variants:
                tc = _title_case(nv.replace('"', '').strip())
                # Skip variants that are just the water name itself
                if tc and tc != water_tc:
                    names.add(tc)
        if not names and water_tc:
            names.add(water_tc)
        if names:
            reg_id_variants[rec.reg_id] = names

    # 1c. Build reg_id → water name lookup (for display name fallback)
    reg_water_lookup: Dict[str, str] = {}
    for rec in records:
        if rec.water:
            reg_water_lookup[rec.reg_id] = rec.water

    # 1d. Collect all tributary reg_ids across all features
    trib_reg_ids_all: Set[str] = set()
    for trib_set in assignments.fid_to_trib_reg_ids.values():
        trib_reg_ids_all.update(trib_set)

    # 1e. Collect all admin reg_ids across all features
    admin_reg_ids: Set[str] = set()
    for admin_set in assignments.fid_to_admin_reg_ids.values():
        admin_reg_ids.update(admin_set)
    for admin_set in assignments.wbk_to_admin_reg_ids.values():
        admin_reg_ids.update(admin_set)

    # 1f. Build shared DisplayNameResolver
    from pathlib import Path as _Path

    mt_path = _Path(match_table_path) if match_table_path else None
    resolver = DisplayNameResolver(
        feature_dn_path=FEATURE_DISPLAY_NAMES_PATH,
        match_table_path=mt_path if mt_path and mt_path.exists() else None,
        overrides_path=OVERRIDES_PATH,
    )
    logger.info("  DisplayNameResolver ready")

    # 2. Group features into reaches
    stream_reaches = _group_stream_reaches(
        atlas,
        assignments,
        reach_level_reg_ids,
        reg_id_variants,
        admin_reg_ids,
        resolver,
        reg_water_lookup,
        trib_reg_ids_all,
    )
    poly_reaches_data = _group_polygon_reaches(
        atlas,
        assignments,
        reg_id_variants,
        admin_reg_ids,
        resolver,
        reg_water_lookup,
    )

    # Merge all reaches
    ungaz_reaches = _build_ungazetted_reaches(records)
    all_reaches = {**stream_reaches, **poly_reaches_data, **ungaz_reaches}
    logger.info(
        "  %d total reaches (%d stream, %d polygon, %d ungazetted)",
        len(all_reaches),
        len(stream_reaches),
        len(poly_reaches_data),
        len(ungaz_reaches),
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
        # name_variants is already structured [{name, source}];
        # filter out display_name.
        nv = [
            e
            for e in reach.get("name_variants", [])
            if e["name"] != reach["display_name"]
        ]
        reaches_out[rid] = {
            "display_name": reach["display_name"],
            "name_variants": nv,
            "feature_type": reach["feature_type"],
            "reg_set_index": ri,
            "watershed_code": reach["wsc"],
            "min_zoom": reach["minzoom"],
            "regions": reach.get("rg", []),
            "bbox": reach.get("bbox"),
            "length_km": reach.get("lkm", 0),
            "tributary_reg_ids": reach.get("_trib_reg_ids", []),
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
