"""
Regulation resolvers — pure functions and constants for feature resolution.

Extracted from ``regulation_mapper.py`` to break the monolith into
three layers: **Types** (regulation_types), **Resolvers** (this file),
and **Orchestrator** (RegulationMapper).

All functions here are stateless — they take explicit arguments and
return results without mutating any shared state.  Both the
``RegulationMapper`` orchestrator and CLI test scripts consume them.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import re

from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from .admin_target import AdminTarget
from .regulation_types import FeatureIndex, DirectMatchTarget, ZoneWideTarget
from .logger_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default feature types covering every FWA waterbody category.
ALL_FWA_TYPES: List[FeatureType] = [
    FeatureType.STREAM,
    FeatureType.LAKE,
    FeatureType.WETLAND,
    FeatureType.MANMADE,
    FeatureType.UNGAZETTED,
]

# Zone ID → human-readable region name (used for display in exports/UI).
ZONE_REGION_NAMES: Dict[str, str] = {
    "1": "REGION 1 - Vancouver Island",
    "2": "REGION 2 - Lower Mainland",
    "3": "REGION 3 - Thompson-Nicola",
    "4": "REGION 4 - Kootenay",
    "5": "REGION 5 - Cariboo",
    "6": "REGION 6 - Skeena",
    "7A": "REGION 7A - Omineca",
    "7B": "REGION 7B - Peace",
    "8": "REGION 8 - Okanagan",
}


# ---------------------------------------------------------------------------
# Name inheritance detection (pure function)
# ---------------------------------------------------------------------------


def is_regulation_inherited(
    base_regulation_id: str,
    regulation_ids: tuple,
    group_fids: set,
    feature_to_regs: Dict[str, List[str]],
    tributary_assignments: Dict[str, Set[str]],
) -> bool:
    """Check if a regulation is entirely inherited via tributary enrichment.

    A regulation is "inherited" when EVERY ``(feature, rule)`` mapping for
    the given ``base_regulation_id`` within the group came from tributary
    enrichment — meaning no feature was directly matched.

    This is a pure function: all state is passed explicitly as arguments.

    Args:
        base_regulation_id: The base regulation ID (e.g. ``"reg_00042"``).
        regulation_ids: All rule IDs assigned to the merged group.
        group_fids: Feature IDs in the group.
        feature_to_regs: Global feature → rule-ID index.
        tributary_assignments: Per-feature set of rule IDs assigned via
            tributary enrichment.

    Returns:
        True if the regulation is entirely inherited (tributary-only).
    """
    rule_ids = [
        r for r in regulation_ids if parse_base_regulation_id(r) == base_regulation_id
    ]
    if not rule_ids:
        return False
    has_any_mapping = False
    for fid in group_fids:
        fid_rules = set(feature_to_regs.get(fid, []))
        trib_rules = tributary_assignments.get(fid, set())
        for rid in rule_ids:
            if rid in fid_rules:
                has_any_mapping = True
                if rid not in trib_rules:
                    return False  # Direct match found
    return has_any_mapping  # True only if all mappings were tributary


# ---------------------------------------------------------------------------
# General-purpose feature-set helpers
# ---------------------------------------------------------------------------


def collect_features_from_index(
    index: FeatureIndex,
    keys: List[str],
    feature_types: List[FeatureType],
) -> set:
    """Collect feature IDs from an index for the given keys and feature types.

    Works with any index shape (zone, MU, admin, etc.).
    """
    fids: set = set()
    for key in keys:
        bucket = index.get(key, {})
        for ftype in feature_types:
            fids.update(bucket.get(ftype, {}).keys())
    return fids


def exclude_features_from_index(
    fids: set,
    index: FeatureIndex,
    keys: List[str],
    feature_types: List[FeatureType],
) -> set:
    """Return *fids* minus any features found under *keys* in *index*."""
    return fids - collect_features_from_index(index, keys, feature_types)


def include_features_from_index(
    fids: set,
    index: FeatureIndex,
    keys: List[str],
    feature_types: List[FeatureType],
) -> set:
    """Return *fids* plus any features found under *keys* in *index*."""
    return fids | collect_features_from_index(index, keys, feature_types)


# ---------------------------------------------------------------------------
# Shared resolution functions
# ---------------------------------------------------------------------------
# These are the **single source of truth** for resolving features from
# admin targets, zone indexes, and direct-match ID fields.  Both the
# RegulationMapper and the CLI test scripts use them.


def extend_boundary_hysteresis(
    base_fids: Set[str],
    buffered_fids: Set[str],
    stream_meta: Dict[str, dict],
) -> Tuple[Set[str], int]:
    """Extend a base feature set with boundary-straddling streams.

    Used by both zone-wide resolution and admin-polygon resolution.

    From *buffered_fids*, keep stream features whose
    ``fwa_watershed_code`` matches any WSC already present in
    *base_fids*.  This captures both the mainstem continuation
    across a boundary **and** braided side channels (different BLK,
    same WSC) in a single pass — since all segments sharing a BLK
    also share a WSC, WSC matching is a strict superset of BLK matching.

    Non-stream features in *base_fids* pass through unchanged.

    Args:
        base_fids: Feature IDs from the unbuffered / exact resolution.
        buffered_fids: Feature IDs from the buffered resolution (superset
            of *base_fids*).
        stream_meta: ``gazetteer.metadata[FeatureType.STREAM]`` dict.

    Returns:
        ``(extended_fids, count_of_newly_added)``.
    """
    # Collect exact WSCs from the base (unbuffered) set.
    base_wscs: Set[str] = set()
    for fid in base_fids:
        if meta := stream_meta.get(fid):
            if wsc := meta.get("fwa_watershed_code"):
                base_wscs.add(wsc)

    extended: Set[str] = set(base_fids)
    added = 0
    for fid in buffered_fids - base_fids:
        if meta := stream_meta.get(fid):
            if wsc := meta.get("fwa_watershed_code"):
                if wsc in base_wscs:
                    extended.add(fid)
                    added += 1

    return extended, added


def lookup_admin_targets(
    gazetteer: MetadataGazetteer,
    gpkg_path: Path,
    admin_targets: List[AdminTarget],
    feature_types: Optional[List[FeatureType]],
    buffer_m: float = 0,
) -> Tuple[List[FWAFeature], List[Tuple[str, FWAFeature]]]:
    """Resolve FWA features that spatially intersect admin polygon targets.

    Groups targets by ``(layer, code_filter)`` for efficient batching, then
    calls ``search_admin_layer`` + ``find_features_in_admin_area`` per group.

    When ``buffer_m > 0``, hysteresis is applied: the exact (unbuffered)
    intersection is computed first, then the buffered intersection.  Features
    captured *only* by the buffer are kept only if their ``blue_line_key``
    appears in the exact set — this ensures extensions of boundary-straddling
    streams are included while distant unrelated streams are not.

    Args:
        gazetteer: Loaded FWA metadata gazetteer.
        gpkg_path: Path to the GPKG file (must exist).
        admin_targets: List of AdminTarget specifying polygons to intersect.
        feature_types: Restrict intersection to these types (None = all).
        buffer_m: Optional buffer (metres) to expand admin polygons before
            intersection.  0 = exact intersection only.

    Returns:
        (matched_features, admin_entries) where admin_entries is
        ``[(layer_key, admin_feature), ...]``.
    """
    if not admin_targets:
        return [], []

    grouped: Dict[Tuple[str, Optional[str]], List[str]] = defaultdict(list)
    for target in admin_targets:
        key = (target.layer, target.code_filter)
        if target.feature_id:
            grouped[key].append(target.feature_id)
        else:
            grouped.setdefault(key, [])

    all_matched: List[FWAFeature] = []
    admin_entries: List[Tuple[str, FWAFeature]] = []

    for (layer_key, code_filter), feature_ids in grouped.items():
        admin_features = gazetteer.search_admin_layer(
            layer_key=layer_key,
            feature_ids=feature_ids or None,
            code_filter=[code_filter] if code_filter else None,
        )
        if not admin_features:
            logger.error(f"Admin lookup returned no features (layer: {layer_key})")
            continue

        if buffer_m > 0:
            # --- Hysteresis: two-pass intersection via shared helper ---
            # Pass 1: exact boundary
            exact = gazetteer.find_features_in_admin_area(
                admin_features=admin_features,
                layer_key=layer_key,
                feature_types=feature_types,
                gpkg_path=gpkg_path,
                buffer_m=0,
            )
            exact_ids = {f.fwa_id for f in exact}

            # Pass 2: buffered boundary
            buffered = gazetteer.find_features_in_admin_area(
                admin_features=admin_features,
                layer_key=layer_key,
                feature_types=feature_types,
                gpkg_path=gpkg_path,
                buffer_m=buffer_m,
            )
            buffered_ids = {f.fwa_id for f in buffered}

            # Use the shared WSC hysteresis function
            stream_meta = gazetteer.metadata.get(FeatureType.STREAM, {})
            extended_ids, n_hysteresis = extend_boundary_hysteresis(
                exact_ids, buffered_ids, stream_meta,
            )
            if n_hysteresis:
                logger.info(
                    f"  Admin hysteresis: {n_hysteresis} buffer-only features "
                    f"kept via WSC extension ({layer_key})"
                )

            # Filter buffered FWAFeature list to the extended ID set
            matched = [f for f in buffered if f.fwa_id in extended_ids]
        else:
            matched = gazetteer.find_features_in_admin_area(
                admin_features=admin_features,
                layer_key=layer_key,
                feature_types=feature_types,
                gpkg_path=gpkg_path,
                buffer_m=0,
            )

        all_matched.extend(matched)
        admin_entries.extend((layer_key, af) for af in admin_features)

    return all_matched, admin_entries


def build_feature_index(
    gazetteer: MetadataGazetteer,
    feature_types: Optional[List[FeatureType]] = None,
) -> Tuple[FeatureIndex, FeatureIndex, FeatureIndex, FeatureIndex]:
    """Build zone and MU feature indexes in a single pass over gazetteer metadata.

    Two sets of indexes are built:

    * **Unbuffered** (``zones_unbuffered`` / ``mgmt_units_unbuffered``) — exact
      zone/MU boundaries.  Used to determine which streams are *actually*
      inside a zone.
    * **Buffered** (``zones`` / ``mgmt_units``) — 500m‐buffered boundaries.
      Used to capture additional segments of streams that straddle a boundary.

    The two-pass strategy in ``_resolve_zone_wide`` resolves the unbuffered
    set first, then extends it with the buffered set **only for streams whose
    ``blue_line_key`` already appears in the unbuffered set**.  This prevents
    entirely new streams from being pulled in.

    Args:
        gazetteer: Loaded FWA metadata gazetteer.
        feature_types: Feature types to index (defaults to ALL_FWA_TYPES).

    Returns:
        ``(zone_index, mu_index, zone_index_buffered, mu_index_buffered)``
        — all keyed as ``{id → FeatureType → {feature_id: metadata_dict}}``.

    Raises:
        KeyError: If the expected zone metadata keys are missing from a
            feature record (e.g. metadata pickle was built before unbuffered
            fields were added).
    """
    ftypes = feature_types if feature_types is not None else ALL_FWA_TYPES
    zone_index: Dict[str, Dict[FeatureType, Dict[str, dict]]] = {}
    mu_index: Dict[str, Dict[FeatureType, Dict[str, dict]]] = {}
    zone_index_buffered: Dict[str, Dict[FeatureType, Dict[str, dict]]] = {}
    mu_index_buffered: Dict[str, Dict[FeatureType, Dict[str, dict]]] = {}

    _missing_checked = False
    for ftype in ftypes:
        type_metadata = gazetteer.metadata.get(ftype, {})
        for fid, meta in type_metadata.items():
            if not _missing_checked:
                if "zones_unbuffered" not in meta:
                    raise KeyError(
                        f"Metadata field 'zones_unbuffered' not found on "
                        f"feature '{fid}' ({ftype.value}). Re-run "
                        f"'python -m fwa_pipeline.metadata_builder' to "
                        f"regenerate the metadata pickle with unbuffered "
                        f"zone fields."
                    )
                _missing_checked = True

            # Unbuffered indexes (exact zone/MU boundaries)
            for zone_id in meta.get("zones_unbuffered", []):
                zone_index.setdefault(zone_id, {}).setdefault(ftype, {})[fid] = meta
            for mu_id in meta.get("mgmt_units_unbuffered", []):
                mu_index.setdefault(mu_id, {}).setdefault(ftype, {})[fid] = meta

            # Buffered indexes (500m-buffered zone/MU boundaries)
            # Use the full buffered zones/MUs so boundary-straddling
            # features appear in the neighboring zone's candidate set.
            # Protection against false zone assignment happens at the
            # extension level (_extend_boundary_streams) via WSC matching,
            # not at the index level.
            for zone_id in meta.get("zones", []):
                zone_index_buffered.setdefault(zone_id, {}).setdefault(ftype, {})[
                    fid
                ] = meta
            for mu_id in meta.get("mgmt_units", []):
                mu_index_buffered.setdefault(mu_id, {}).setdefault(ftype, {})[
                    fid
                ] = meta

    return zone_index, mu_index, zone_index_buffered, mu_index_buffered


def resolve_direct_match_features(
    gazetteer: MetadataGazetteer, reg: DirectMatchTarget
) -> List[FWAFeature]:
    """Resolve FWA features from a regulation's direct-match ID fields.

    Supports: ``gnis_ids``, ``waterbody_poly_ids``, ``fwa_watershed_codes``,
    ``waterbody_keys``, ``linear_feature_ids``, ``blue_line_keys``,
    ``sub_polygon_ids``, ``ungazetted_waterbody_id``.

    *reg* must expose those fields (e.g. a ``ZoneRegulation`` or ``DirectMatch``).

    Returns the actual FWAFeature objects. Use ``resolve_direct_match_ids``
    when only the ID set is needed.
    """
    features: List[FWAFeature] = []

    if getattr(reg, "gnis_ids", None):
        for gnis_id in reg.gnis_ids:
            features.extend(gazetteer.search_by_gnis_id(str(gnis_id)))

        # Auto-expand: also include unnamed streams that inherit any of these
        # GNIS IDs (e.g. unnamed side channels of a named river).
        # TODO: In the future, limit inherited expansion to features within
        # the regulation's target zone.  Fraser River (zones 3/5/7A) is the
        # main case — side channels should only attach to their own zone.
        # But a naive zone filter would break cross-boundary streams that
        # carry a single regulation across zones (e.g. Similkameen River),
        # so this needs careful design.
        seen_ids = {f.fwa_id for f in features}
        for gnis_id in reg.gnis_ids:
            for inherited_feat in gazetteer.search_unnamed_by_inherited_gnis_id(
                str(gnis_id)
            ):
                if inherited_feat.fwa_id not in seen_ids:
                    seen_ids.add(inherited_feat.fwa_id)
                    features.append(inherited_feat)

    if getattr(reg, "waterbody_poly_ids", None):
        for poly_id in reg.waterbody_poly_ids:
            feat = gazetteer.get_polygon_by_id(str(poly_id))
            if feat:
                features.append(feat)

    if getattr(reg, "fwa_watershed_codes", None):
        for wsc in reg.fwa_watershed_codes:
            features.extend(gazetteer.search_by_watershed_code(wsc))

    if getattr(reg, "waterbody_keys", None):
        for wbk in reg.waterbody_keys:
            features.extend(gazetteer.get_waterbody_by_key(str(wbk)))

    if getattr(reg, "linear_feature_ids", None):
        for lf_id in reg.linear_feature_ids:
            feat = gazetteer.get_stream_by_id(str(lf_id))
            if feat:
                features.append(feat)

    if getattr(reg, "blue_line_keys", None):
        for blk in reg.blue_line_keys:
            features.extend(gazetteer.search_by_blue_line_key(blk))

    if getattr(reg, "sub_polygon_ids", None):
        for sp_id in reg.sub_polygon_ids:
            feat = gazetteer.get_polygon_by_id(sp_id)
            if feat:
                features.append(feat)

    if getattr(reg, "ungazetted_waterbody_id", None):
        feat = gazetteer.get_ungazetted_by_id(reg.ungazetted_waterbody_id)
        if feat:
            features.append(feat)

    return features


def resolve_direct_match_ids(
    gazetteer: MetadataGazetteer, reg: DirectMatchTarget
) -> set:
    """Resolve FWA feature IDs from a regulation's direct-match ID fields.

    Thin wrapper around ``resolve_direct_match_features`` that returns
    only the ID set.
    """
    return {f.fwa_id for f in resolve_direct_match_features(gazetteer, reg)}


def resolve_zone_wide_ids(
    reg: ZoneWideTarget,
    zone_index: FeatureIndex,
    mu_index: FeatureIndex,
    all_feature_types: Optional[List[FeatureType]] = None,
) -> set:
    """Resolve zone-wide feature IDs with MU filtering.

    Supports three MU modifiers (all optional, combinable):

    - ``mu_ids``: only include features in these MUs
    - ``exclude_mu_ids``: remove features in these MUs
    - ``include_mu_ids``: add features from these MUs (can be outside zone)

    *reg* must expose: ``zone_ids``, ``feature_types``, ``mu_ids``,
    ``exclude_mu_ids``, ``include_mu_ids``.
    """
    ftypes = all_feature_types if all_feature_types is not None else ALL_FWA_TYPES
    target_ftypes = reg.feature_types if reg.feature_types else ftypes

    # 1. Collect features from target zones
    if reg.mu_ids:
        zone_fids = collect_features_from_index(zone_index, reg.zone_ids, target_ftypes)
        mu_fids = collect_features_from_index(mu_index, reg.mu_ids, target_ftypes)
        matched_ids = zone_fids & mu_fids
    else:
        matched_ids = collect_features_from_index(
            zone_index, reg.zone_ids, target_ftypes
        )

    # 2. Exclude features belonging to specific MUs
    if reg.exclude_mu_ids:
        matched_ids = exclude_features_from_index(
            matched_ids, mu_index, reg.exclude_mu_ids, target_ftypes
        )

    # 3. Add features from extra MUs (can be outside the zone)
    if reg.include_mu_ids:
        matched_ids = include_features_from_index(
            matched_ids, mu_index, reg.include_mu_ids, target_ftypes
        )

    return matched_ids


# --- Standalone Helper Functions ---


def parse_region(raw: str) -> Optional[str]:
    """Normalise a raw region string to ``"Region 7A"`` form.

    Handles both formats encountered in the data:

    * ``"REGION 7A - Omineca"`` → ``"Region 7A"``
    * ``"Region 4"`` → ``"Region 4"`` (returned unchanged)

    Returns ``None`` for empty / unparseable input.
    """
    if not raw:
        return None
    if raw.startswith("REGION"):
        m = re.match(r"REGION\s+(\d+[A-Za-z]?)\s*[-–]", raw)
        if m:
            return f"Region {m.group(1).upper()}"
        # Fallback: extract digits+letters before first hyphen
        token = raw.split("-")[0].strip()
        parts = token.split()
        if len(parts) >= 2:
            return f"Region {parts[-1].upper()}"
        return None
    # Already normalised (e.g. "Region 4")
    if raw.startswith("Region "):
        return raw
    return None


def generate_regulation_id(regulation_idx: int) -> str:
    """Generate a consistent regulation ID from its index."""
    return f"reg_{regulation_idx:05d}"


def generate_rule_id(regulation_idx: int, rule_idx: int) -> str:
    """Generate a consistent rule ID from regulation and rule indices."""
    return f"{generate_regulation_id(regulation_idx)}_rule{rule_idx}"


def parse_base_regulation_id(rule_id: str) -> str:
    """Extract the base regulation ID from a rule ID.

    Inverse of ``generate_rule_id``:
    ``"reg_00042_rule0"`` → ``"reg_00042"``

    Every place that needs to associate a rule ID with its parent regulation
    should call this function instead of inline ``rsplit("_rule", 1)[0]``.
    """
    return rule_id.rsplit("_rule", 1)[0]


# ---------------------------------------------------------------------------
# Pure utility functions (extracted from RegulationMapper)
# ---------------------------------------------------------------------------


def title_case_name(name: str) -> str:
    """Title-case a waterbody name: first letter of each word uppercase, rest lower.

    Handles apostrophes (Pete's → Pete's not Pete'S), quoted strings,
    and parenthetical content correctly.
    """
    if not name:
        return name

    def _title_word(m: re.Match) -> str:
        word = m.group(0)
        lower = word.lower()
        for i, ch in enumerate(lower):
            if ch.isalpha():
                return lower[:i] + ch.upper() + lower[i + 1 :]
        return lower  # no alpha chars (e.g. pure digits)

    return re.sub(r"[A-Za-z0-9']+", _title_word, name)


def get_feature_type(feature: FWAFeature) -> FeatureType:
    """Extract and guarantee a FeatureType enum return.

    Raises:
        ValueError: If the feature has an unrecognized feature_type value
            or FeatureType.UNKNOWN.  Data integrity issues must halt the
            build, not produce incomplete maps.
    """
    ftype = feature.feature_type
    if isinstance(ftype, FeatureType):
        if ftype == FeatureType.UNKNOWN:
            raise ValueError(
                f"Feature {feature.fwa_id!r} has feature_type=UNKNOWN. "
                f"This indicates a metadata build issue — re-run metadata_builder."
            )
        return ftype
    if ftype is not None:
        try:
            result = FeatureType(ftype)
        except ValueError:
            raise ValueError(
                f"Unrecognized feature type {ftype!r} on feature "
                f"{feature.fwa_id!r}. If this is a new type, add it to "
                f"FeatureType enum in metadata_builder.py."
            )
        if result == FeatureType.UNKNOWN:
            raise ValueError(
                f"Feature {feature.fwa_id!r} has feature_type=UNKNOWN. "
                f"This indicates a metadata build issue — re-run metadata_builder."
            )
        return result
    raise ValueError(
        f"Feature {feature.fwa_id!r} has no feature_type set. "
        f"This indicates a metadata build issue — re-run metadata_builder."
    )


def get_parent_watershed_codes(watershed_code: str) -> Set[str]:
    """Return the set of parent watershed codes for a given FWA watershed code.

    Walks the hyphen-separated sections right-to-left, zeroing each
    non-zero section to produce progressively broader parent codes.

    Example::

        "100-123456-000000" → {"100-000000-000000"}
    """
    if not watershed_code:
        return set()
    parents, sections = set(), watershed_code.split("-")
    for i in range(len(sections) - 1, 0, -1):
        if sections[i] != "000000":
            parent_code = "-".join(sections[:i] + ["000000"] * (len(sections) - i))
            parents.add(parent_code)
            sections[i] = "000000"
    return parents


def resolve_group_inherited_names(
    features_data: List[Tuple[str, FWAFeature]], has_gnis_name: bool
) -> Tuple[str, list]:
    """Resolve inherited GNIS names for unnamed groups.

    ``inherited_gnis_names`` comes from graph context — only present on
    unnamed stream edges.  Collects unique (name, id) pairs across all
    features in the group.

    Returns:
        ``(inherited_gnis_name, inherited_name_variants)`` where:
        - Single inherited name → ``(title_cased_name, [])``
        - Multiple inherited names → ``("", [variant_dicts...])``
        - Named group or no inherited names → ``("", [])``
    """
    if has_gnis_name:
        return "", []

    inherited_set: set = set()
    for _, feat in features_data:
        inh = feat.inherited_gnis_names
        if isinstance(inh, list):
            for entry in inh:
                iname = entry.get("gnis_name", "")
                iid = entry.get("gnis_id", "")
                if iname:
                    inherited_set.add((iname, iid))

    if len(inherited_set) == 1:
        iname, _ = next(iter(inherited_set))
        return title_case_name(iname), []

    if len(inherited_set) > 1:
        variants = [
            {"name": title_case_name(iname), "from_tributary": False}
            for iname, _ in sorted(inherited_set)
        ]
        return "", variants

    return "", []
