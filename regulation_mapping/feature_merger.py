"""
Feature merger — groups FWA features by physical identity and regulation set.

Extracted from ``RegulationMapper`` to isolate the merge/grouping logic.
All functions are stateless: they receive the data they need via explicit
keyword-only parameters rather than accessing ``self``.
"""

from typing import Dict, List, Set, Any, Tuple
from collections import defaultdict
import logging

from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from .regulation_types import MergedGroup
from .regulation_resolvers import (
    is_regulation_inherited,
    parse_base_regulation_id,
    title_case_name,
    get_feature_type,
    resolve_group_inherited_names,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: physical grouping key
# ---------------------------------------------------------------------------


def build_physical_grouping_key(
    feature: FWAFeature,
    feature_id: str,
    *,
    linked_waterbody_keys: Set[str],
) -> str:
    """Build a grouping key from physical identifiers.

    Key construction rules:

    - Streams with both BLK and linked WBK:
      ``{ftype}_blue_line_{blk}_waterbody_{wbk}``
    - Named streams (has gnis_id):
      ``{ftype}_blue_line_{blk}_gnis_{gnis_id}``
    - Unnamed streams:
      ``{ftype}_blue_line_{blk}``
    - Polygon features with linked WBK:
      ``{ftype}_waterbody_{wbk}``
    - Features without physical IDs:
      ``{ftype}_feature_{feature_id}`` (no merging)
    """
    ftype_val = get_feature_type(feature).value
    blk = feature.blue_line_key
    wbk = feature.waterbody_key
    use_wbk = wbk and str(wbk) in linked_waterbody_keys

    if blk and use_wbk:
        return f"{ftype_val}_blue_line_{blk}_waterbody_{wbk}"
    if blk:
        if feature.gnis_id:
            return f"{ftype_val}_blue_line_{blk}_gnis_{feature.gnis_id}"
        return f"{ftype_val}_blue_line_{blk}"
    if use_wbk:
        return f"{ftype_val}_waterbody_{wbk}"
    return f"{ftype_val}_feature_{feature_id}"


# ---------------------------------------------------------------------------
# Helper: aggregate group metadata
# ---------------------------------------------------------------------------


def aggregate_group_metadata(
    features_data: List[Tuple[str, FWAFeature]],
    *,
    linked_waterbody_keys: Set[str],
) -> Dict[str, Any]:
    """Aggregate zone/MU/identifier metadata across all features in a group.

    Returns a dict with:
    - ``zones``: sorted tuple of zone IDs
    - ``mgmt_units``: sorted tuple of MU IDs
    - ``region_names``: tuple of names paired with zones
    - ``waterbody_key``: single WBK if group shares exactly one, else None
    - ``gnis_name``: single GNIS name if group shares exactly one, else ""
    - ``blue_line_key``: single BLK if group shares exactly one, else None
    - ``fwa_watershed_code``: first watershed code found, or None
    """
    all_mu: set = set()
    all_wbks: set = set()
    zone_to_name: dict = {}
    gnis_names: set = set()
    blks: set = set()
    watershed_codes: set = set()

    for _, feat in features_data:
        # Zones and region names (positionally paired)
        feat_zones = feat.zones or []
        feat_names = feat.region_names or []
        for z, n in zip(feat_zones, feat_names):
            zone_to_name[z] = n
        for z in feat_zones:
            if z not in zone_to_name:
                zone_to_name[z] = ""

        # Management units
        all_mu.update(feat.mgmt_units or [])

        # Waterbody key (only for linked polygons)
        if feat.waterbody_key and str(feat.waterbody_key) in linked_waterbody_keys:
            all_wbks.add(str(feat.waterbody_key))

        # Identifiers
        if feat.gnis_name:
            gnis_names.add(feat.gnis_name)
        if feat.blue_line_key:
            blks.add(str(feat.blue_line_key))
        if feat.fwa_watershed_code:
            watershed_codes.add(str(feat.fwa_watershed_code))

    sorted_zones = sorted(zone_to_name.keys())
    return {
        "zones": tuple(sorted_zones),
        "mgmt_units": tuple(sorted(all_mu)),
        "region_names": tuple(zone_to_name[z] for z in sorted_zones),
        "waterbody_key": next(iter(all_wbks)) if len(all_wbks) == 1 else None,
        "gnis_name": next(iter(gnis_names)) if len(gnis_names) == 1 else "",
        "blue_line_key": next(iter(blks)) if len(blks) == 1 else None,
        "fwa_watershed_code": (
            next(iter(watershed_codes)) if watershed_codes else None
        ),
    }


# ---------------------------------------------------------------------------
# Helper: name variant builder
# ---------------------------------------------------------------------------


def build_name_variants_for_group(
    features_data: list,
    regulation_ids: tuple,
    *,
    admin_regulation_ids: set,
    regulation_names: Dict[str, str],
    feature_to_regs: Dict[str, List[str]],
    tributary_assignments: Dict[str, set],
    regulation_parent_gnis: Dict[str, set],
    feature_to_aliases: Dict[str, set],
) -> tuple:
    """Build name variants for a single merged group.

    Collects names from:
    1. All gnis_name and gnis_name_2 values from features
    2. Waterbody-specific regulation names (excluding admin/provincial/zone)
    3. Name variation aliases resolved to specific feature_ids

    Returns tuple of dicts with 'name' and 'from_tributary' keys.
    Names from tributary-enriched features are tagged as from_tributary=True.
    All names are title-cased for consistent display.
    """
    # Track names with their tributary status: name -> from_tributary
    name_to_tributary: dict = {}

    # Collect group feature IDs and their tributary rule sets
    group_fids = {fid for fid, _ in features_data}

    def add_name(name: str, is_tributary: bool) -> None:
        if not name:
            return
        # Normalize to title case so "ALICE LAKE" and "Alice Lake" merge
        name = title_case_name(name)
        # If name already exists as non-tributary, keep it as non-tributary
        if name in name_to_tributary:
            if not is_tributary:
                name_to_tributary[name] = False
        else:
            name_to_tributary[name] = is_tributary

    # 1. GNIS names from features — always the feature's own identity,
    #    never marked as from_tributary.
    for fid, feat in features_data:
        if feat.gnis_name:
            add_name(feat.gnis_name, False)
        if feat.gnis_name_2:
            add_name(feat.gnis_name_2, False)

    # 2. Regulation names (waterbody-specific only).
    #    For inherited regulations (from tributary enrichment),
    #    use the parent stream's GNIS name with from_tributary=True
    #    so the UI shows "Tributary of <clean GNIS name>".
    #    For direct regulations, add the regulation name normally.
    base_ids = {parse_base_regulation_id(r) for r in regulation_ids}
    base_ids = {
        b for b in base_ids if not b.startswith("prov_") and not b.startswith("zone_")
    }
    base_ids -= admin_regulation_ids

    for bid in base_ids:
        inherited = is_regulation_inherited(
            bid,
            regulation_ids,
            group_fids,
            feature_to_regs,
            tributary_assignments,
        )
        if inherited:
            parent_names = regulation_parent_gnis.get(bid, set())
            if parent_names:
                for pname in parent_names:
                    add_name(pname, True)
            else:
                reg_name = regulation_names.get(bid, "")
                if reg_name:
                    add_name(reg_name, True)
        else:
            reg_name = regulation_names.get(bid, "")
            if reg_name:
                add_name(reg_name, False)

    # 3. Name variation aliases — never from_tributary.
    for fid, _ in features_data:
        for alias in feature_to_aliases.get(fid, set()):
            add_name(alias, False)

    # Build sorted list of dicts
    result = tuple(
        {"name": name, "from_tributary": is_trib}
        for name, is_trib in sorted(name_to_tributary.items())
    )
    return result


# ---------------------------------------------------------------------------
# Main merge function
# ---------------------------------------------------------------------------


def merge_features(
    feature_to_regs: Dict[str, List[str]],
    *,
    gazetteer: MetadataGazetteer,
    linked_waterbody_keys: Set[str],
    admin_regulation_ids: set,
    regulation_names: Dict[str, str],
    feature_to_regs_full: Dict[str, List[str]],
    tributary_assignments: Dict[str, set],
    regulation_parent_gnis: Dict[str, set],
    feature_to_aliases: Dict[str, set],
    feature_display_name_overrides: Dict[str, str],
    progress_wrapper: Any = None,
) -> Dict[str, MergedGroup]:
    """Group features with identical regulation sets into ``MergedGroup`` instances.

    Groups individual FWA features by ``(physical_grouping_key, regulation_set)``.
    See ``build_physical_grouping_key`` for the key construction rules.

    Args:
        feature_to_regs: Mapping of feature_id → [regulation_ids] to merge.
        gazetteer: For looking up feature objects by ID.
        linked_waterbody_keys: WBKs of polygon features (for grouping key).
        admin_regulation_ids: Regulation IDs from admin area matches (excluded from names).
        regulation_names: regulation_id → display name.
        feature_to_regs_full: Full feature→regs mapping (for inheritance checks).
        tributary_assignments: feature_id → {rule_ids assigned via tributaries}.
        regulation_parent_gnis: base_reg_id → {parent GNIS names}.
        feature_to_aliases: feature_id → {alias names}.
        feature_display_name_overrides: feature_id → override display name.
        progress_wrapper: Optional callable to wrap iterables with progress bars.
    """
    logger.info(f"Merging {len(feature_to_regs)} features into groups...")

    items = feature_to_regs.items()
    if progress_wrapper is not None:
        items = progress_wrapper(items, "Grouping features", "feature")

    # Step 1: Build groups keyed by (physical_key, regulation_set)
    group_map: Dict[tuple, list] = defaultdict(list)
    for feature_id, reg_ids in items:
        reg_set = frozenset(reg_ids)
        feature = gazetteer.get_feature_by_id(feature_id)
        if not feature:
            logger.debug(f"Feature {feature_id} not found in gazetteer — skipping")
            continue

        grouping_key = build_physical_grouping_key(
            feature,
            feature_id,
            linked_waterbody_keys=linked_waterbody_keys,
        )
        group_map[(grouping_key, reg_set)].append((feature_id, feature))

    # Step 2: Build MergedGroup for each (physical_key, reg_set) bucket
    merged_groups: Dict[str, MergedGroup] = {}
    grouping_key_counter: Dict[str, int] = defaultdict(int)

    for (grouping_key, reg_set), features_data in group_map.items():
        unique_key = f"{grouping_key}_{grouping_key_counter[grouping_key]}"
        grouping_key_counter[grouping_key] += 1
        reg_ids_tuple = tuple(sorted(reg_set))

        # Aggregate zone/MU/identifier metadata across all features
        meta = aggregate_group_metadata(
            features_data,
            linked_waterbody_keys=linked_waterbody_keys,
        )

        # Resolve inherited GNIS names for unnamed groups
        inherited_name, inherited_variants = resolve_group_inherited_names(
            features_data, has_gnis_name=bool(meta["gnis_name"])
        )

        # Build name_variants from gnis, regulation names, and aliases
        name_variants = build_name_variants_for_group(
            features_data=features_data,
            regulation_ids=reg_ids_tuple,
            admin_regulation_ids=admin_regulation_ids,
            regulation_names=regulation_names,
            feature_to_regs=feature_to_regs_full,
            tributary_assignments=tributary_assignments,
            regulation_parent_gnis=regulation_parent_gnis,
            feature_to_aliases=feature_to_aliases,
        )

        # Resolve display_name_override from FeatureNameVariation.
        display_override = ""
        for fid, _ in features_data:
            override = feature_display_name_overrides.get(fid)
            if override:
                display_override = override
                break

        # If override exists AND a single inherited name was resolved,
        # demote the inherited name to a variant instead of display.
        final_inherited = inherited_name
        if display_override and inherited_name:
            inherited_variants.append({"name": inherited_name, "from_tributary": False})
            final_inherited = ""

        # Merge inherited variants into name_variants (dedup by name)
        if inherited_variants:
            existing_names = {nv["name"] for nv in name_variants}
            extra = tuple(
                nv for nv in inherited_variants if nv["name"] not in existing_names
            )
            name_variants = tuple(
                sorted(
                    name_variants + extra,
                    key=lambda nv: nv["name"],
                )
            )

        merged_groups[unique_key] = MergedGroup(
            group_id=unique_key,
            feature_ids=tuple(fid for fid, _ in features_data),
            regulation_ids=reg_ids_tuple,
            feature_type=get_feature_type(features_data[0][1]).value,
            gnis_name=meta["gnis_name"],
            display_name_override=display_override,
            inherited_gnis_name=final_inherited,
            name_variants=name_variants,
            waterbody_key=meta["waterbody_key"],
            blue_line_key=meta["blue_line_key"],
            fwa_watershed_code=meta["fwa_watershed_code"],
            feature_count=len(features_data),
            zones=meta["zones"],
            mgmt_units=meta["mgmt_units"],
            region_names=meta["region_names"],
        )

    logger.info(
        f"merge_features: {len(group_map)} groups from {len(feature_to_regs)} features"
    )
    return merged_groups
