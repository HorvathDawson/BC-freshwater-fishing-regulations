"""
waterbody_matcher.py — self-contained FWA lake index + override lookup for
this directory's own matching scripts (match_fwa_gazette.py,
match_fwa_identifier.py, match_fwa_override.py, match_wbid_gazette.py).

This is a trimmed, standalone copy of the pieces of
`live-data/common/waterbody_matcher.py` this folder's scripts actually use
(`LakeIndex`/`build_lake_index()`, `locate_at_point()`, `_confirmed_by_name()`,
`OverrideCandidate`/`build_override_index()`) — not an import of that module.
waterbody_db/ is meant to be self-contained within `live-data/` (it already
replicates `stocking/fetch_stocking.py` and `bathymetry/fetch_bathymetry.py`
rather than importing them — see this directory's own README), so its
matching scripts shouldn't reach into `common/` either. The full T1-T5
identifier/override cascade in the original (`match_row_t1()`,
`match_records()`, etc.) isn't reproduced here — this folder's own scripts
each build their own tier logic on top of just the FWA index + override
lookup, not the whole cascade.

Everything below still depends on the main pipeline (`pipeline/`, `data/`)
the same way the original does — that's the production matching/gazetteer
code this whole project shares, not a `live-data/` sibling.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import geopandas as gpd
from pyproj import Transformer
from shapely.geometry import Point
from shapely.prepared import prep
from shapely.strtree import STRtree

from data.data_extractor import FWADataAccessor
from pipeline.matching.base_entry_builder import _add_name, _natural_search
from pipeline.matching.bathymetry_matcher import normalize_name
from pipeline.matching.match_table import OverrideEntry

logger = logging.getLogger(__name__)

OVERRIDES_PATH = Path(__file__).resolve().parents[2] / "pipeline" / "matching" / "overrides.json"

# The three FWA polygon layers a "lake" can physically live in — natural
# lakes, wetlands, and man-made waterbodies (dammed lakes, reservoirs, urban
# ponds like Green Timbers) all carry the identical attribute schema
# (WATERBODY_KEY, GNIS_NAME_1/2, WATERBODY_KEY_GROUP_CODE_50K,
# WATERSHED_CODE_50K), so a wbid or name search restricted to `lakes` alone
# silently misses anything FWA classifies as wetland or man-made even though
# the source data itself may call it a lake.
POLYGON_LAYERS = ("lakes", "wetlands", "manmade")


# ---------------------------------------------------------------------------
# FWA lake index — name, group-code identifier, and watershed code, all from
# a single FWADataAccessor.get_layers() read across POLYGON_LAYERS.
# ---------------------------------------------------------------------------
@dataclass
class LakeIndex:
    key_names: Dict[str, Set[str]]        # WATERBODY_KEY -> {normalised GNIS names 1 & 2}
    key_wsc: Dict[str, str]               # WATERBODY_KEY -> WATERSHED_CODE_50K (45-char, zero-padded)
    group_code_map: Dict[str, Set[str]]   # WATERBODY_KEY_GROUP_CODE_50K (== 50K wbid scheme) -> {WATERBODY_KEY}
    name_index: Dict[str, Set[str]]       # normalised name -> {WATERBODY_KEY} (province-wide)
    display_name: Dict[str, str]          # WATERBODY_KEY -> GNIS name for display
    source_layer: Dict[str, str]          # WATERBODY_KEY -> which POLYGON_LAYERS layer it came from
    key_to_group: Dict[str, str]          # WATERBODY_KEY -> its WATERBODY_KEY_GROUP_CODE_50K (reverse of group_code_map)
    watershed_group_code: Dict[str, str]  # WATERBODY_KEY -> WATERSHED_GROUP_CODE_50K (4-letter mnemonic, e.g. "OKAN"). 50K-specific only.
    name_variant_index: Dict[str, Set[str]]  # generic-suffix-stripped name variant -> {WATERBODY_KEY} (see _name_variants)
    gazetteer_index: Dict[str, List[dict]]   # title-cased GNIS name -> [{"waterbody_key": key}, ...] for _natural_search()
    geoms: List[Any]                      # polygon geometries (EPSG:3005), parallel to geom_keys
    geom_preps: List[Any]                 # shapely.prepared versions of geoms, for fast .contains()
    geom_keys: List[str]                  # WATERBODY_KEY parallel to geoms/geom_preps
    strtree: Any                          # STRtree over geoms — positions index into geom_keys/geom_preps
    key_geom: Dict[str, Any]              # WATERBODY_KEY -> geometry (EPSG:3005)


# A source's own marker/survey titles can drop the generic water-body-type
# word a full gazetted name always carries (confirmed live in stocking:
# gofishbc marker title "Deka" for FIDQ's "DEKA LAKE") — normalize_name() is
# deliberately built to keep that word (so "Goose Lake" never collides with
# "Goose Creek" when comparing two *full* names), so name matching alone
# can't bridge a title that drops it. name_variant_index exists for exactly
# that case.
_GENERIC_WATERBODY_SUFFIXES = (" LAKES", " LAKE", " RESERVOIR", " PONDS", " POND")
_GENERIC_WATERBODY_PREFIXES = ("LAKE ",)


def _strip_generic_suffix(name: str) -> str:
    for suffix in _GENERIC_WATERBODY_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _strip_generic_prefix(name: str) -> str:
    for prefix in _GENERIC_WATERBODY_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def _name_variants(name: str) -> Set[str]:
    """A normalized name plus its generic-type-word-stripped forms (trailing
    "LAKE"/"LAKES"/etc., or a leading "LAKE " as in "LAKE ENID")."""
    if not name:
        return set()
    variants = {name, _strip_generic_suffix(name), _strip_generic_prefix(name)}
    variants.discard("")
    return variants


def build_lake_index(gpkg_path: Path) -> LakeIndex:
    accessor = FWADataAccessor(gpkg_path)
    cols = [
        "WATERBODY_KEY", "GNIS_NAME_1", "GNIS_NAME_2",
        "WATERBODY_KEY_GROUP_CODE_50K", "WATERSHED_CODE_50K", "WATERSHED_GROUP_CODE_50K",
    ]
    polys = accessor.get_layers(list(POLYGON_LAYERS), columns=cols)

    key_names: Dict[str, Set[str]] = defaultdict(set)
    name_index: Dict[str, Set[str]] = defaultdict(set)
    name_variant_index: Dict[str, Set[str]] = defaultdict(set)
    key_wsc: Dict[str, str] = {}
    group_code_map: Dict[str, Set[str]] = defaultdict(set)
    key_to_group: Dict[str, str] = {}
    watershed_group_code: Dict[str, str] = {}
    display_name: Dict[str, str] = {}
    source_layer: Dict[str, str] = {}
    gazetteer_index: Dict[str, List[dict]] = {}
    geoms: List[Any] = []
    geom_keys: List[str] = []
    key_geom: Dict[str, Any] = {}

    for key, nm1, nm2, group, wsc, wsg, layer, geom in zip(
        polys["WATERBODY_KEY"], polys["GNIS_NAME_1"], polys["GNIS_NAME_2"],
        polys["WATERBODY_KEY_GROUP_CODE_50K"], polys["WATERSHED_CODE_50K"],
        polys["WATERSHED_GROUP_CODE_50K"], polys["_source_layer"], polys.geometry,
    ):
        key = str(key)
        key_wsc[key] = str(wsc).strip() if isinstance(wsc, str) and wsc.strip() else ""
        source_layer[key] = layer
        # Deliberately 50K-specific only — no fallback to the plain
        # WATERSHED_GROUP_CODE (the two can genuinely diverge).
        if isinstance(wsg, str) and wsg.strip():
            watershed_group_code[key] = wsg.strip()
        if isinstance(group, str) and group.strip():
            g = group.strip()
            group_code_map[g].add(key)
            key_to_group[key] = g
        feature = {"waterbody_key": key}
        for raw_name in (nm1, nm2):
            norm = normalize_name(raw_name)
            if not norm:
                continue
            key_names[key].add(norm)
            name_index[norm].add(key)
            for variant in _name_variants(norm):
                name_variant_index[variant].add(key)
            if key not in display_name:
                display_name[key] = raw_name.strip()
            # gazetteer_index feeds base_entry_builder._natural_search() —
            # the same title-cased name index the main regulations pipeline
            # builds for its own gazetteer search, reused here.
            _add_name(gazetteer_index, raw_name, feature)
        if geom is not None and not geom.is_empty:
            geoms.append(geom)
            geom_keys.append(key)
            key_geom[key] = geom

    geom_preps = [prep(g) for g in geoms]
    strtree = STRtree(geoms)

    return LakeIndex(
        dict(key_names), key_wsc, dict(group_code_map), dict(name_index),
        display_name, source_layer, key_to_group, watershed_group_code,
        dict(name_variant_index), gazetteer_index,
        geoms, geom_preps, geom_keys, strtree, key_geom,
    )


# BC Albers (EPSG:3005) is what every FWA polygon layer is stored in;
# caller coordinates (e.g. a gofishbc marker's lat/lng) arrive as WGS84
# (EPSG:4326).
_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)

# How far (metres) a point may sit from the nearest FWA polygon edge and
# still count as "at" that waterbody — some source points sit at a
# dock/shoreline access point rather than the lake's interior, so an exact
# point-in-polygon test alone would miss those.
GEO_SNAP_METERS = 250.0


@dataclass
class GeoHit:
    waterbody_key: Optional[str]  # FWA WATERBODY_KEY located at/near the point; None if nothing was within GEO_SNAP_METERS
    group_code: Optional[str]     # its WATERBODY_KEY_GROUP_CODE_50K; None if the polygon has none, or hits tie on >1 code


def _locate_at_projected_point(index: LakeIndex, point: Point) -> GeoHit:
    """Point-in-polygon lookup against the FWA polygon layers, given a point
    already in the layers' own EPSG:3005 — pure geolocation, independent of
    the 1:50K identifier scheme. Falls back to the nearest polygon within
    GEO_SNAP_METERS when the point doesn't land exactly inside one."""
    hit_idxs = [i for i in index.strtree.query(point) if index.geom_preps[i].contains(point)]
    if not hit_idxs:
        nearest_i = index.strtree.nearest(point)
        if nearest_i is not None and index.geoms[nearest_i].distance(point) <= GEO_SNAP_METERS:
            hit_idxs = [nearest_i]
    if not hit_idxs:
        return GeoHit(None, None)

    keys = {index.geom_keys[i] for i in hit_idxs}
    key = sorted(keys)[0]
    groups = {index.key_to_group.get(k) for k in keys}
    groups.discard(None)
    group_code = next(iter(groups)) if len(groups) == 1 else None
    return GeoHit(key, group_code)


def locate_at_point(index: LakeIndex, lat: float, lng: float) -> GeoHit:
    """_locate_at_projected_point(), but starting from WGS84 lat/lng (e.g. a
    gofishbc marker's coordinates) instead of an already-projected point."""
    x, y = _TO_ALBERS.transform(lng, lat)
    return _locate_at_projected_point(index, Point(x, y))


def _confirmed_by_name(index: LakeIndex, raw_name: str, pool: Set[str]) -> bool:
    """Independent corroboration check for an identifier-based candidate
    pool: True if ``raw_name`` is blank (nothing to check against), OR a
    gazetteer natural-language search of ``raw_name`` returns no results
    (inconclusive: absence of gazetteer coverage for this name isn't
    evidence *against* the identifier), OR it returns at least one
    candidate that's actually in ``pool``. False only when the name search
    finds real candidates and *none* of them are in pool — the identifier's
    own answer and an independent name search actively disagree."""
    if not raw_name.strip():
        return True
    refs, _ = _natural_search(index.gazetteer_index, raw_name, None, [])
    if not refs:
        return True
    found = {r["waterbody_key"] for r in refs}
    return bool(found & pool)


@dataclass(frozen=True)
class OverrideCandidate:
    waterbody_key: str
    # 4-letter WATERSHED_GROUP_CODE_50K mnemonic (e.g. "PARK"), when the
    # override entry was curated with one. Not part of OverrideEntry's own
    # schema — read directly off the raw JSON entry.
    watershed_group_code: Optional[str] = None


def build_override_index(path: Path = OVERRIDES_PATH) -> Dict[str, List[OverrideCandidate]]:
    """normalised name_verbatim -> [OverrideCandidate, ...], from the main
    regulations pipeline's own curated corrections table
    (pipeline/matching/overrides.json, OverrideEntry — see match_table.py).
    Only entries with `waterbody_keys` are useful here — this project
    matches on FWA WATERBODY_KEY, not GNIS_ID, which most other overrides
    carry instead."""
    if not path.exists():
        return {}
    with open(path) as f:
        raw_entries = json.load(f)
    index: Dict[str, List[OverrideCandidate]] = defaultdict(list)
    for raw in raw_entries:
        if raw.get("type") != "override":
            continue
        entry = OverrideEntry.from_dict(raw)
        if entry.skip or not entry.waterbody_keys:
            continue
        name = normalize_name(entry.criteria.name_verbatim)
        if not name:
            continue
        group_code = raw.get("watershed_group_code")
        group_code = group_code.strip().upper() if isinstance(group_code, str) and group_code.strip() else None
        index[name].extend(OverrideCandidate(k, group_code) for k in entry.waterbody_keys)
    return dict(index)
