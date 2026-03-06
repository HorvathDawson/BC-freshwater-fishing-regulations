"""
Regulation types — shared dataclasses, protocols, and type aliases.

Extracted from ``regulation_mapper.py`` to break the monolith into
three layers: **Types** (this file), **Resolvers** (pure functions),
and **Orchestrator** (RegulationMapper).

External consumers (geo_exporter, canonical_store, search_exporter,
regulation_pipeline, etc.) should import from here for:
  - ``MergedGroup``
  - ``PipelineResult``
  - ``RegulationMappingStats``
  - ``FeatureIndex``
  - ``DirectMatchTarget`` / ``ZoneWideTarget`` protocols
  - ``ZoneScopeClassification`` / ``ZoneScopeOptimizer``
  - ``LinkedRegulation`` / ``AdminResolutionResult``
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple, runtime_checkable

from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from .zone_base_regulations import ZoneRegulation
from .logger_config import get_logger

logger = get_logger(__name__)

# Shared type alias for feature indexes keyed by zone/MU/admin ID.
# Structure: {group_id → {FeatureType → {feature_id: metadata_dict}}}
FeatureIndex = Dict[str, Dict[FeatureType, Dict[str, dict]]]


# ---------------------------------------------------------------------------
# Structural protocols for regulation targets
# ---------------------------------------------------------------------------


@runtime_checkable
class DirectMatchTarget(Protocol):
    """Structural interface for regulations with direct-match ID fields.

    Satisfied by ``DirectMatch`` and ``ZoneRegulation``.  Any object
    exposing these optional ID attributes can be passed to
    ``resolve_direct_match_features`` / ``resolve_direct_match_ids``.
    """

    gnis_ids: Optional[List[str]]
    waterbody_poly_ids: Optional[List[str]]
    fwa_watershed_codes: Optional[List[str]]
    waterbody_keys: Optional[List[str]]
    linear_feature_ids: Optional[List[str]]
    blue_line_keys: Optional[List[str]]
    sub_polygon_ids: Optional[List[str]]
    ungazetted_waterbody_id: Optional[str]


@runtime_checkable
class ZoneWideTarget(Protocol):
    """Structural interface for zone-wide regulation scope fields.

    Satisfied by ``ZoneRegulation``.  Any object exposing these
    attributes can be passed to ``resolve_zone_wide_ids``.
    """

    zone_ids: List[str]
    feature_types: Optional[List[FeatureType]]
    mu_ids: Optional[List[str]]
    exclude_mu_ids: Optional[List[str]]
    include_mu_ids: Optional[List[str]]


# ---------------------------------------------------------------------------
# Zone scope optimizer — groups zone regulations by identical scope
# ---------------------------------------------------------------------------


@dataclass
class ZoneScopeClassification:
    """Classified zone regulations, split by resolution strategy.

    Produced by ``ZoneScopeOptimizer.classify`` so that the mapper can
    process each category with the appropriate resolution method.
    """

    zone_wide_groups: Dict[tuple, List[ZoneRegulation]] = field(default_factory=dict)
    admin_regs: List[ZoneRegulation] = field(default_factory=list)
    direct_regs: List[ZoneRegulation] = field(default_factory=list)


class ZoneScopeOptimizer:
    """Groups zone regulations by identical scope for efficient resolution.

    Zone-wide regulations are pre-grouped by
    ``(zone_ids, feature_types, mu_ids, exclude_mu_ids, include_mu_ids)``
    so that spatial resolution runs once per unique scope instead of once
    per regulation.
    """

    @staticmethod
    def _make_scope_key(reg: ZoneRegulation) -> tuple:
        """Build a hashable key from the 5 fields that determine a zone-wide feature set.

        Uses ``frozenset`` for order-independent comparison — two regulations
        with ``[STREAM, LAKE]`` vs ``[LAKE, STREAM]`` produce the same key.
        """
        return (
            frozenset(reg.zone_ids),
            frozenset(reg.feature_types or []),
            frozenset(reg.mu_ids or []),
            frozenset(reg.exclude_mu_ids or []),
            frozenset(reg.include_mu_ids or []),
        )

    @staticmethod
    def classify(regs: List[ZoneRegulation]) -> ZoneScopeClassification:
        """Classify zone regulations by resolution strategy.

        Returns a ``ZoneScopeClassification`` with three categories:
        - ``zone_wide_groups``: keyed by scope key, for batch resolution
        - ``admin_regs``: admin-match regulations (spatial intersection)
        - ``direct_regs``: direct-match regulations (ID-based lookup)
        """
        result = ZoneScopeClassification()
        zone_wide: Dict[tuple, List[ZoneRegulation]] = defaultdict(list)

        for reg in regs:
            if reg.admin_targets:
                result.admin_regs.append(reg)
            elif reg.has_direct_target():
                result.direct_regs.append(reg)
            else:
                zone_wide[ZoneScopeOptimizer._make_scope_key(reg)].append(reg)

        result.zone_wide_groups = dict(zone_wide)
        return result


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


@dataclass
class RegulationMappingStats:
    """Statistics from regulation mapping process."""

    total_regulations: int = 0
    linked_regulations: int = 0
    failed_to_link_regulations: int = 0
    bad_regulation: int = 0
    total_rules_processed: int = 0
    total_rule_to_feature_mappings: int = 0
    unique_features_with_rules: int = 0
    link_status_counts: Counter = field(default_factory=Counter)


# ---------------------------------------------------------------------------
# Resolution result types
# ---------------------------------------------------------------------------


@dataclass
class AdminResolutionResult:
    """Result of resolving admin targets to FWA features.

    Returned by ``_resolve_admin_rule_set`` — a pure lookup with no
    side-effects.  The caller decides how to apply the results (e.g.
    pack into ``Pass1Result`` for synopsis, or write to ``self`` for
    provincial/zone).
    """

    matched_features: List[FWAFeature] = field(default_factory=list)
    feature_ids: List[str] = field(default_factory=list)
    admin_entries: List[Tuple[str, FWAFeature]] = field(default_factory=list)
    waterbody_keys: Set[str] = field(default_factory=set)

    @property
    def empty(self) -> bool:
        return not self.feature_ids


@dataclass
class LinkedRegulation:
    """A single regulation linked to its FWA features (Pass 1 output).

    Named replacement for the opaque 5-tuple previously used in
    ``Pass1Result.linked_cache``.  Self-documenting and safe from
    positional errors.

    ``additional_info`` carries linking-correction notes that should be
    injected as a synthetic "Note" rule during Pass 2.  Stored here
    instead of mutating the input ``regulation`` dict, so that the
    input data remains read-only.
    """

    idx: int
    regulation: Dict
    regulation_id: str
    base_features: List[FWAFeature]
    is_admin_match: bool
    additional_info: Optional[str] = None


# ---------------------------------------------------------------------------
# Output types (frozen — used by exporters)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MergedGroup:
    """Merged group of features with identical regulation sets.

    Created by the **feature merging** stage (``merge_features``), which
    groups individual FWA features that share the same physical identity
    (BLK / WBK / GNIS) *and* identical regulation set into one group.

    ``zones`` and ``region_names`` are positionally paired — index *i* in
    ``region_names`` is the name for index *i* in ``zones``.  Both are
    sorted by zone ID.

    ``gnis_name`` is the primary FWA GNIS name (canonical name from the
    Freshwater Atlas).  ``display_name_override`` is set by
    ``FeatureNameVariation`` corrections and **takes priority** over
    ``gnis_name`` for display and for **search grouping** (stage 2).
    ``name_variants`` contains all searchable names: gnis_name, gnis_name_2,
    regulation name_verbatim values, and NameVariationLink aliases.
    Each entry is a dict with 'name' and 'from_tributary' keys.
    """

    group_id: str
    feature_ids: tuple[str, ...]
    regulation_ids: tuple[str, ...]
    feature_type: str = ""  # FeatureType.value (e.g., "stream", "lake")
    gnis_name: str = ""
    display_name_override: str = (
        ""  # From FeatureNameVariation — takes priority over gnis_name
    )
    inherited_gnis_name: str = (
        ""  # From graph context for unnamed streams (single inherited name)
    )
    name_variants: tuple[dict, ...] = ()  # All searchable names with tributary flag
    waterbody_key: Optional[str] = None
    blue_line_key: Optional[str] = None  # For streams: physical channel ID
    fwa_watershed_code: Optional[str] = None  # For streams: unique stream identifier
    feature_count: int = 0
    zones: tuple[str, ...] = ()  # REGION_RESPONSIBLE_ID values
    mgmt_units: tuple[str, ...] = ()
    region_names: tuple[str, ...] = ()  # Paired with zones

    @property
    def display_name(self) -> str:
        """Effective display name: override > gnis_name > inherited_gnis_name > first name_variant.

        Used by the **search grouping** stage (``_build_waterbodies_list``)
        to determine which groups merge into one search entry.  Features
        with different display names stay as separate search entries.
        """
        if self.display_name_override:
            return self.display_name_override
        if self.gnis_name:
            return self.gnis_name
        if self.inherited_gnis_name:
            return self.inherited_gnis_name
        for nv in self.name_variants:
            if not nv.get("from_tributary") and nv.get("name"):
                return nv["name"]
        return ""


@dataclass(frozen=True)
class PipelineResult:
    """Result from full regulation processing pipeline. Contains all state needed for export."""

    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    feature_to_linked_regulation: Dict[str, Set[str]] = field(default_factory=dict)
    gazetteer: Optional[MetadataGazetteer] = None
    stats: Optional[RegulationMappingStats] = None
    # Provincial regulation support: maps regulation_id → list of matched FWA feature_ids
    provincial_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    # Zone regulation support: maps regulation_id → list of matched FWA feature_ids
    zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    # Admin area feature mapping: maps admin regulation name → list of matched FWA feature_ids
    admin_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    # Synopsis regulation IDs that were admin area matches (e.g. "Liard River Watershed")
    # Excluded from waterbody-specific display names in search/tile exports
    admin_regulation_ids: set = field(default_factory=set)
    # Admin area → regulation IDs: layer_key → {admin_feature_id: {regulation_ids}}
    # Used by exporter to create admin boundary layers with matched regulation info
    admin_area_reg_map: Dict[str, Dict[str, set]] = field(default_factory=dict)
    # Centralized regulation metadata for export — populated by the mapper from
    # all sources (synopsis, provincial, zone).  Keyed by rule_id.
    # The exporter writes this directly to regulations.json without needing
    # to import source-specific modules.
    regulation_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
