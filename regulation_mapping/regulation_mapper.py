"""
RegulationMapper - Orchestrates the full pipeline: Link -> Scope -> Enrich -> Map

Creates inverted index: feature_id -> regulation_ids
Handles all regulation sources: synopsis, admin area matches, and provincial base regulations.
"""

from typing import Dict, List, Optional, Set, Any, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path
import json
import re

from .linker import WaterbodyLinker, LinkStatus
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .provincial_base_regulations import (
    PROVINCIAL_BASE_REGULATIONS,
    ProvincialRegulation,
)
from .admin_target import AdminTarget
from .zone_base_regulations import ZoneRegulation
from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from fwa_pipeline.metadata_builder import ADMIN_BOUNDARY_BUFFER_M
from .regulation_types import (
    FeatureIndex,
    DirectMatchTarget,
    ZoneWideTarget,
    ZoneScopeClassification,
    ZoneScopeOptimizer,
    RegulationMappingStats,
    AdminResolutionResult,
    LinkedRegulation,
    MergedGroup,
    PipelineResult,
)
from .regulation_resolvers import (
    ALL_FWA_TYPES,
    ZONE_REGION_NAMES,
    is_regulation_inherited,
    collect_features_from_index,
    exclude_features_from_index,
    include_features_from_index,
    lookup_admin_targets,
    build_feature_index,
    resolve_direct_match_features,
    resolve_direct_match_ids,
    resolve_zone_wide_ids,
    parse_region,
    generate_regulation_id,
    generate_rule_id,
    parse_base_regulation_id,
    title_case_name,
    get_feature_type,
    get_parent_watershed_codes,
    resolve_group_inherited_names,
)
from .feature_merger import merge_features as _merge_features_impl
from .logger_config import get_logger

logger = get_logger(__name__)


@dataclass
class Pass1Result:
    """Immutable output from the pre-linking pass.

    Each field is data *produced* by Pass 1 — nothing is mutated on the
    mapper class.  Call ``merge_into(mapper)`` to apply results.
    """

    linked_cache: List[LinkedRegulation] = field(default_factory=list)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    feature_to_linked_regulation: Dict[str, Set[str]] = field(default_factory=dict)
    linked_waterbody_keys_of_polygon: Set[str] = field(default_factory=set)
    admin_regulation_ids: Set[str] = field(default_factory=set)
    admin_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    admin_area_reg_map: Dict[str, Dict[str, set]] = field(default_factory=dict)
    pending_name_variation_aliases: Dict[str, List[tuple]] = field(default_factory=dict)
    stats_linked: int = 0
    stats_failed: int = 0
    stats_bad: int = 0
    link_status_counts: Counter = field(default_factory=Counter)

    def merge_into(self, mapper: "RegulationMapper") -> None:
        """Apply all Pass 1 outputs into the mapper's mutable state."""
        mapper.regulation_names.update(self.regulation_names)
        for fid, reg_ids in self.feature_to_linked_regulation.items():
            mapper.feature_to_linked_regulation[fid].update(reg_ids)
        mapper.linked_waterbody_keys_of_polygon.update(
            self.linked_waterbody_keys_of_polygon
        )
        mapper.admin_regulation_ids.update(self.admin_regulation_ids)
        mapper.admin_feature_map.update(self.admin_feature_map)
        for layer_key, area_map in self.admin_area_reg_map.items():
            for admin_fid, reg_ids in area_map.items():
                mapper.admin_area_reg_map[layer_key][admin_fid].update(reg_ids)
        for primary, aliases in self.pending_name_variation_aliases.items():
            mapper._pending_name_variation_aliases[primary].extend(aliases)
        mapper.stats.linked_regulations += self.stats_linked
        mapper.stats.failed_to_link_regulations += self.stats_failed
        mapper.stats.bad_regulation += self.stats_bad
        mapper.stats.link_status_counts.update(self.link_status_counts)


@dataclass
class Pass2Result:
    """Immutable output from the rule-mapping pass.

    Each field is data *produced* by Pass 2 — nothing is mutated on the
    mapper class.  Call ``merge_into(mapper)`` to apply results.
    """

    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    regulation_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    regulation_parent_gnis: Dict[str, Set[str]] = field(default_factory=dict)
    tributary_assignments: Dict[str, Set[str]] = field(default_factory=dict)
    stats_rules_processed: int = 0
    stats_rule_to_feature_mappings: int = 0

    def merge_into(self, mapper: "RegulationMapper") -> None:
        """Apply all Pass 2 outputs into the mapper's mutable state."""
        for fid, rule_ids in self.feature_to_regs.items():
            mapper.feature_to_regs.setdefault(fid, []).extend(rule_ids)
        mapper.regulation_details.update(self.regulation_details)
        for reg_id, gnis_names in self.regulation_parent_gnis.items():
            mapper.regulation_parent_gnis[reg_id].update(gnis_names)
        for fid, trib_rules in self.tributary_assignments.items():
            mapper.tributary_assignments[fid].update(trib_rules)
        mapper.stats.total_rules_processed += self.stats_rules_processed
        mapper.stats.total_rule_to_feature_mappings += (
            self.stats_rule_to_feature_mappings
        )


class RegulationMapper:
    """Orchestrates the full regulation mapping pipeline."""

    # Default feature types for resolving None → all waterbody types
    # Default feature types — delegates to module-level constant.
    _ALL_FWA_TYPES = ALL_FWA_TYPES

    def __init__(
        self,
        linker: WaterbodyLinker,
        scope_filter: ScopeFilter,
        tributary_enricher: TributaryEnricher,
        gpkg_path: Optional[Path] = None,
    ):
        self.linker = linker
        self.scope_filter = scope_filter
        self.tributary_enricher = tributary_enricher
        self.gazetteer = linker.gazetteer
        self.gpkg_path = gpkg_path
        self.stats = RegulationMappingStats()

        self.feature_to_regs = {}
        self.merged_groups = {}
        self.regulation_names = {}
        self.feature_to_linked_regulation = defaultdict(set)
        self.linked_waterbody_keys_of_polygon = set()
        # Regulation IDs that came from admin area matches (synopsis entries like
        # "Liard River Watershed") — these apply to many features within the area
        # and should not appear as waterbody-specific names in search results.
        self.admin_regulation_ids: set = set()
        # Admin area feature mapping: regulation name → list of matched FWA feature_ids
        self.admin_feature_map: Dict[str, List[str]] = {}
        # Tracks which admin polygons matched which regulation IDs
        # layer_key → {admin_feature_id → {regulation_ids}}
        self.admin_area_reg_map: Dict[str, Dict[str, set]] = defaultdict(
            lambda: defaultdict(set)
        )
        # Centralized regulation metadata for export (rule_id → entry dict)
        self.regulation_details: Dict[str, Dict[str, Any]] = {}
        # Pending name variation aliases collected during linking: primary_name.upper() → [(alias_name, zone_id)]
        # Resolved to feature_to_aliases after linking completes.
        self._pending_name_variation_aliases: Dict[str, List[tuple]] = defaultdict(list)
        # Resolved feature_id → {alias_names} — built by _resolve_name_variation_aliases
        self.feature_to_aliases: Dict[str, Set[str]] = defaultdict(set)
        # Per-feature tracking of which rule_ids were assigned via tributary
        # enrichment.  Maps feature_id → {rule_ids assigned as tributary}.
        # Used to decide which regulation names are "inherited" in name_variants.
        self.tributary_assignments: Dict[str, Set[str]] = defaultdict(set)
        # GNIS names of the directly-matched (non-tributary) features per
        # base regulation.  Used so that tributary groups show
        # "Tributary of <parent GNIS>" rather than the synopsis reg name.
        self.regulation_parent_gnis: Dict[str, Set[str]] = defaultdict(set)
        # Feature-level display name overrides from FeatureNameVariation.
        # Maps feature_id → assigned display name.  Propagated to
        # MergedGroup.display_name_override so the search grouping stage
        # keeps these features as separate search entries.
        self.feature_display_name_overrides: Dict[str, str] = {}

        # Reverse index: waterbody_key → [linear_feature_ids] for O(1) lookup
        # Eagerly built so _get_stream_seeds_for_waterbody is a pure dict lookup.
        self._wb_key_to_stream_ids: Dict[str, List[str]] = self._build_wb_key_index()

    # --- Core Pipeline ---

    def _process_synopsis_regulations(
        self, regulations: List[Dict]
    ) -> Dict[str, List[str]]:
        """Process synopsis regulations — creates feature-to-rule index.

        Orchestrates two passes over the regulation list, using immutable
        result objects to make data flow explicit:

        1. **Pass 1 (pre-linking)**: Link each regulation to FWA features,
           resolve admin matches, track polygon waterbody keys.
           Returns ``Pass1Result``.
        2. **Pass 2 (rule mapping)**: Apply scope + tributary enrichment,
           map individual rules onto features, store regulation details.
           Returns ``Pass2Result``.
        3. **Merge**: Explicitly merge stage results into mapper state.

        Returns:
            ``feature_to_regs``: mapping of feature_id → [rule_ids].
        """
        # Reset mutable state for this run
        self.feature_to_regs = {}
        self.feature_to_linked_regulation = defaultdict(set)
        self.linked_waterbody_keys_of_polygon = set()
        self.admin_feature_map = {}
        self.admin_regulation_ids = set()
        self.stats.total_regulations = len(regulations)

        logger.info(f"Processing {len(regulations)} regulations...")

        # --- Stage 1: Pre-link (returns immutable result) ---
        pass1 = self._pass1_prelink(regulations)
        pass1.merge_into(self)

        # --- Stage 2: Map rules (returns immutable result) ---
        pass2 = self._pass2_map_rules(pass1.linked_cache)
        pass2.merge_into(self)

        # Sort rule lists and resolve aliases
        for feature_id in self.feature_to_regs:
            self.feature_to_regs[feature_id].sort()
        self._resolve_name_variation_aliases()

        self.stats.unique_features_with_rules = len(self.feature_to_regs)
        return self.feature_to_regs

    def _pass1_prelink(self, regulations: List[Dict]) -> Pass1Result:
        """Pass 1: Link regulations to FWA features and build lookup caches.

        Returns an immutable ``Pass1Result`` containing all data produced by
        this stage.  The caller (``_process_synopsis_regulations``) explicitly
        merges the result into mapper state — this method does NOT mutate ``self``.

        For each regulation:
        - Validate and skip errored regulations.
        - Link via ``WaterbodyLinker`` (name → FWA features).
        - Resolve admin direct-match regulations inline (spatial intersection).
        - Record name-variation aliases for later resolution.
        - Track polygon waterbody keys for grouping.
        - Carry ``additional_info`` on ``LinkedRegulation`` for Pass 2 injection.

        The input ``regulations`` list is treated as **read-only** — no
        dictionaries are mutated.
        """
        result = Pass1Result()
        linked_cache: List[LinkedRegulation] = []

        for idx, regulation in enumerate(
            self._with_progress(regulations, "Pre-linking regulations", "reg")
        ):
            regulation_id = generate_regulation_id(idx)
            identity = regulation.get("identity", {})
            name_verbatim = identity.get("name_verbatim", "")

            # 0: prelim check for regulation errors before linking.
            reg_error = regulation.get("error", None)
            if reg_error:
                logger.warning(
                    f"Regulation {name_verbatim} has error flag: {reg_error}"
                )
                result.stats_bad += 1
                continue

            if name_verbatim:
                result.regulation_names[regulation_id] = name_verbatim

            # 1. Normalize Region and Link
            region = parse_region(regulation.get("region"))

            link_result = self.linker.link_waterbody(
                region=region,
                mgmt_units=regulation.get("mu", []),
                name_verbatim=name_verbatim,
            )

            result.link_status_counts[link_result.status.value] += 1

            # Handle admin direct matches: resolve spatial intersection inline
            is_admin_match = False
            if (
                link_result.link_method == "admin_direct_match"
                and link_result.admin_match
            ):
                is_admin_match = True
                result.stats_linked += 1
                result.admin_regulation_ids.add(regulation_id)
                admin_match = link_result.admin_match

                admin_result = self._resolve_admin_rule_set(
                    admin_match.admin_targets,
                    admin_match.feature_types,
                    regulation_id,
                )
                if admin_result.empty:
                    logger.warning(
                        f"  No FWA features found in admin area for '{name_verbatim}'"
                    )
                    continue

                # Pack admin bookkeeping into Pass1Result (no self-mutation)
                base_features = admin_result.matched_features
                result.admin_feature_map[name_verbatim] = admin_result.feature_ids
                result.linked_waterbody_keys_of_polygon.update(
                    admin_result.waterbody_keys
                )
                for layer_key, admin_feat in admin_result.admin_entries:
                    result.admin_area_reg_map.setdefault(layer_key, {}).setdefault(
                        admin_feat.fwa_id, set()
                    ).add(regulation_id)

                logger.debug(
                    f"  Admin match '{name_verbatim}': {len(admin_result.feature_ids)} FWA features"
                )

            elif link_result.status == LinkStatus.NAME_VARIATION:
                # Alternate name for an already-linked waterbody — don’t link features,
                # but record the alias (with zone) for later resolution to feature_ids.
                primary_name = link_result.matched_name
                if primary_name:
                    # Extract zone number from region string (e.g. "Region 2" → "2")
                    zone_id = region.replace("Region ", "") if region else ""
                    result.pending_name_variation_aliases.setdefault(
                        primary_name.upper(), []
                    ).append((name_verbatim, zone_id))
                    logger.debug(
                        f"Name variation '{name_verbatim}' → primary '{primary_name}' (zone {zone_id})"
                    )
                continue

            elif (
                link_result.status != LinkStatus.SUCCESS
                or not link_result.matched_features
            ):
                result.stats_failed += 1
                continue

            else:
                result.stats_linked += 1
                base_features = link_result.matched_features

            # 2. Track keys and polygon waterbody identifiers for grouping.
            # Admin matches: _register_admin_results already handled wb_key backfill.
            # All matches: track feature_to_linked_regulation here (synopsis' sole writer).
            for feature in base_features:
                result.feature_to_linked_regulation.setdefault(
                    feature.fwa_id, set()
                ).add(regulation_id)

            if not is_admin_match:
                for feature in base_features:
                    if feature.feature_type in (
                        FeatureType.LAKE,
                        FeatureType.WETLAND,
                        FeatureType.MANMADE,
                    ):
                        if feature.waterbody_key:
                            result.linked_waterbody_keys_of_polygon.add(
                                str(feature.waterbody_key)
                            )

            linked_cache.append(
                LinkedRegulation(
                    idx=idx,
                    regulation=regulation,
                    regulation_id=regulation_id,
                    base_features=base_features,
                    is_admin_match=is_admin_match,
                    additional_info=link_result.additional_info,
                )
            )

        logger.info(
            f"Pre-step complete. Found {len(result.linked_waterbody_keys_of_polygon)} linked polygon waterbodies."
        )
        result.linked_cache = linked_cache
        return result

    def _pass2_map_rules(
        self,
        linked_cache: List[LinkedRegulation],
    ) -> Pass2Result:
        """Pass 2: Scope, enrich, and map individual rules to features.

        Returns an immutable ``Pass2Result`` containing all data produced by
        this stage.  The caller (``_process_synopsis_regulations``) explicitly
        merges the result into mapper state.

        For each linked regulation:
        - Collect GNIS names from base features (for tributary display).
        - Apply global scope + tributary enrichment.
        - Map each rule to its final set of features.
        - Store regulation details for export.
        """
        result = Pass2Result()

        for entry in self._with_progress(
            linked_cache, "Mapping rules to features", "reg"
        ):
            idx = entry.idx
            regulation_id = entry.regulation_id
            base_features = entry.base_features
            is_admin_match = entry.is_admin_match
            identity = entry.regulation.get("identity", {})

            # Build rule list — inject synthetic "Note" from additional_info
            # without mutating the original regulation dict.
            rules = list(entry.regulation.get("rules", []))
            if entry.additional_info:
                rules.append(
                    {
                        "rule_text_verbatim": entry.additional_info,
                        "restriction": {
                            "type": "Note",
                            "details": entry.additional_info,
                        },
                        "scope": {},
                    }
                )

            # Collect GNIS names from base (directly-matched) features
            # so tributary groups can show "Tributary of <parent GNIS>".
            for bf in base_features:
                if bf.gnis_name:
                    result.regulation_parent_gnis.setdefault(regulation_id, set()).add(
                        bf.gnis_name
                    )

            # 3. Apply Global Scope
            global_scope = identity.get("global_scope", {})

            # Admin area regulations should never use tributary enrichment.
            # The admin polygon already delimits the spatial extent — enriching
            # with tributaries is redundant and extremely expensive.
            if is_admin_match:
                has_trib_flag = global_scope.get("includes_tributaries") or any(
                    r.get("scope", {}).get("includes_tributaries") for r in rules
                )
                if has_trib_flag:
                    name_verbatim = identity.get("name_verbatim", "")
                    logger.warning(
                        f"Admin area regulation '{name_verbatim}' has includes_tributaries=true "
                        "— ignoring. Tributary enrichment is not applicable for admin area matches."
                    )

            # Force skip tributaries for admin matches
            skip_tribs = is_admin_match

            globally_scoped_features = self.apply_scope_and_enrich(
                base_features,
                scope=global_scope,
                global_scope=global_scope,
                skip_tributary_enrichment=skip_tribs,
            )

            if (
                not globally_scoped_features
                and global_scope.get("type") != "TRIBUTARIES_ONLY"
            ):
                globally_scoped_features = base_features

            # 4. Map Rules to Features
            for rule_idx, rule in enumerate(rules):
                result.stats_rules_processed += 1
                rule_scope = rule.get("scope", {})

                final_features = self.apply_scope_and_enrich(
                    globally_scoped_features,
                    rule_scope,
                    global_scope,
                    skip_tributary_enrichment=skip_tribs,
                )

                rule_id = generate_rule_id(idx, rule_idx)
                for feature in final_features:
                    fid = feature.fwa_id
                    result.feature_to_regs.setdefault(fid, []).append(rule_id)
                    result.stats_rule_to_feature_mappings += 1
                    # Track which (feature, rule) pairs came from tributary enrichment
                    if feature.matched_via == "tributary_enrichment":
                        result.tributary_assignments.setdefault(fid, set()).add(rule_id)

                # Store regulation details for export (synopsis source)
                rest = rule.get("restriction", {})
                scope_d = rule.get("scope", {})
                raw_exclusions = identity.get("exclusions", [])
                result.regulation_details[rule_id] = self._build_regulation_detail(
                    source="synopsis",
                    waterbody_name=identity.get("name_verbatim"),
                    lookup_name=identity.get("lookup_name")
                    or identity.get("waterbody_key"),
                    region=entry.regulation.get("region"),
                    management_units=entry.regulation.get("mu", []),
                    rule_text=rule.get("rule_text_verbatim"),
                    restriction_type=rest.get("type"),
                    restriction_details=rest.get("details"),
                    dates=rest.get("dates"),
                    scope_type=scope_d.get("type"),
                    scope_location=scope_d.get("location_verbatim"),
                    includes_tributaries=scope_d.get("includes_tributaries"),
                    source_image=entry.regulation.get("image"),
                    exclusions=raw_exclusions if raw_exclusions else None,
                )

        return result

    def apply_scope_and_enrich(
        self,
        base_features: List[FWAFeature],
        scope: Dict,
        global_scope: Dict,
        skip_tributary_enrichment: bool = False,
    ) -> List[FWAFeature]:
        """Apply spatial filter + tributary enrichment.

        Args:
            skip_tributary_enrichment: If True, tributary enrichment is skipped entirely.
                Used for admin area matches where the admin polygon already delimits the
                spatial extent — enriching with tributaries is not applicable.
        """

        scope_type = scope.get("type", "WHOLE_SYSTEM")

        if scope_type == "TRIBUTARIES_ONLY":
            if skip_tributary_enrichment:
                # Admin area — no tributary enrichment, and TRIBUTARIES_ONLY means
                # "return only tributaries." With no enrichment there are none.
                return []
            if not scope.get("includes_tributaries"):
                logger.warning(
                    "TRIBUTARIES_ONLY scope should have includes_tributaries=true."
                )
            return self._enrich_with_tributaries(base_features)

        scoped_features = self.scope_filter.apply_scope(base_features, scope)

        if skip_tributary_enrichment:
            return scoped_features

        # Check if we should enrich with tributaries based on rule scope or global scope.
        # Rule scope takes precedence; fall back to global scope when absent.
        val = scope.get("includes_tributaries")
        should_enrich = (
            val if val is not None else global_scope.get("includes_tributaries", False)
        )

        if should_enrich:
            tributaries = self._enrich_with_tributaries(scoped_features)
            return scoped_features + tributaries

        return scoped_features

    def _resolve_name_variation_aliases(self) -> None:
        """Resolve pending name variation aliases to specific feature_ids.

        Called at the end of _process_synopsis_regulations after all linking is complete.
        Builds feature_to_aliases by:
        1. Finding regulation_ids whose name matches each pending alias's primary_name
        2. Finding feature_ids linked to those regulation_ids
        3. Assigning the alias names to those feature_ids

        This ensures aliases only apply to the actual features that were linked
        by the primary regulation, not to any waterbody with a matching name.
        """
        if not self._pending_name_variation_aliases:
            return

        # Build reverse index: regulation_name.upper() → [regulation_ids]
        reg_name_to_ids: Dict[str, List[str]] = defaultdict(list)
        for reg_id, reg_name in self.regulation_names.items():
            if reg_name:
                reg_name_to_ids[reg_name.upper()].append(reg_id)

        # Build reverse index: regulation_id → {feature_ids}
        # (feature_to_linked_regulation is feature_id → {regulation_ids})
        reg_id_to_features: Dict[str, Set[str]] = defaultdict(set)
        for fid, reg_ids in self.feature_to_linked_regulation.items():
            for reg_id in reg_ids:
                reg_id_to_features[reg_id].add(fid)

        resolved_count = 0
        for primary_name_upper, aliases in self._pending_name_variation_aliases.items():
            # Find regulation_ids matching this primary_name
            matching_reg_ids = reg_name_to_ids.get(primary_name_upper, [])
            if not matching_reg_ids:
                logger.warning(
                    f"Name variation alias for '{primary_name_upper}' found no matching "
                    f"regulation — aliases {[a[0] for a in aliases]} will be orphaned"
                )
                continue

            # Find feature_ids linked to those regulation_ids
            target_feature_ids: Set[str] = set()
            for reg_id in matching_reg_ids:
                target_feature_ids.update(reg_id_to_features.get(reg_id, set()))

            if not target_feature_ids:
                logger.warning(
                    f"Name variation alias for '{primary_name_upper}' matched regulations "
                    f"{matching_reg_ids} but found no linked features"
                )
                continue

            # Assign alias names to those feature_ids (respecting zone restrictions)
            for alias_name, alias_zone in aliases:
                if alias_zone:
                    # Zone-restricted: only assign to features in that zone
                    for fid in target_feature_ids:
                        feat = self.gazetteer.get_feature_by_id(fid)
                        if feat:
                            if alias_zone in (feat.zones or []):
                                self.feature_to_aliases[fid].add(alias_name)
                                resolved_count += 1
                else:
                    # No zone restriction: assign to all target features
                    for fid in target_feature_ids:
                        self.feature_to_aliases[fid].add(alias_name)
                        resolved_count += 1

        logger.info(
            f"Resolved {len(self._pending_name_variation_aliases)} name variation aliases "
            f"to {len(self.feature_to_aliases)} features ({resolved_count} assignments)"
        )

    def _resolve_feature_name_variations(self) -> None:
        """Resolve FeatureNameVariation corrections to feature_ids.

        Looks up features by blue_line_keys or waterbody_keys and assigns the
        display name to:
        - ``feature_display_name_overrides``: propagated to
          ``MergedGroup.display_name_override`` so the search grouping stage
          keeps these features as separate search entries.
        - ``feature_to_aliases``: makes the name searchable in the front-end
          search index and visible in name_variants.

        Called from ``run()`` after all linking is complete but before the
        feature merging stage.  Only processes features that are already in
        ``feature_to_regs`` (i.e., features with at least one regulation).

        Raises:
            AssertionError: If called before any regulation source has
                populated ``feature_to_regs``.
        """
        assert self.feature_to_regs, (
            "_resolve_feature_name_variations must be called after all "
            "regulation sources have populated feature_to_regs"
        )
        corrections = self.linker.corrections
        all_fnv = corrections.get_all_feature_name_variations()
        if not all_fnv:
            return

        resolved_count = 0
        for fnv in all_fnv:
            matched_fids: Set[str] = set()

            if fnv.blue_line_keys:
                # Use reverse index for O(1) lookup per BLK
                for blk in fnv.blue_line_keys:
                    for fid, ftype in self.gazetteer.blue_line_key_index.get(
                        str(blk), []
                    ):
                        if ftype == FeatureType.STREAM:
                            matched_fids.add(fid)

            if fnv.waterbody_keys:
                # Use reverse index for O(1) lookup per WBK
                for wbk in fnv.waterbody_keys:
                    for fid, ftype in self.gazetteer.waterbody_key_index.get(
                        str(wbk), []
                    ):
                        matched_fids.add(fid)

            # Only assign to features already in the regulation index
            active_fids = matched_fids & set(self.feature_to_regs.keys())

            if not active_fids:
                logger.warning(
                    f"FeatureNameVariation '{fnv.name}' (blks={fnv.blue_line_keys}, "
                    f"wbks={fnv.waterbody_keys}) matched no regulated features"
                )
                continue

            for fid in active_fids:
                self.feature_display_name_overrides[fid] = fnv.name
                self.feature_to_aliases[fid].add(fnv.name)
                resolved_count += 1

        if resolved_count:
            logger.info(
                f"Resolved {len(all_fnv)} feature name variations "
                f"to {resolved_count} regulated features"
            )

    def run(
        self,
        regulations: List[Dict],
        include_zone_regulations: bool = False,
    ) -> PipelineResult:
        """Full pipeline: Link -> Scope -> Enrich -> Map (all sources) -> Merge.

        Processes all regulation sources before merging so that merged groups
        contain the complete set of regulation IDs per feature:
          1. Synopsis + admin area regulations (resolved inline during linking)
          2. Provincial base regulations (blanket rules for admin boundaries)
          2.5. Zone base regulations (opt-in, skipped by default)
          3. Merge features into groups (with ALL regulation sources present)

        Args:
            regulations: Parsed synopsis regulations (list of dicts).
            include_zone_regulations: If True, process zone-level default
                regulations. Defaults to False to keep test runs fast (zone
                regs can touch millions of features).
        """
        # Phase 1: Synopsis + admin area regulations (fully resolved inline)
        self._process_synopsis_regulations(regulations)

        # Phase 2: Provincial base regulations
        provincial_feature_map = self._process_provincial_regulations()

        # Phase 2.5: Zone base regulations (zone membership lookup)
        zone_feature_map: Dict[str, List[str]] = {}
        if include_zone_regulations:
            zone_feature_map = self._process_zone_regulations()
        else:
            logger.info("Zone regulations skipped (include_zone_regulations=False)")

        # Phase 2.9: Resolve FeatureNameVariation corrections (assigns display
        # names to specific features by BLK/WBK).  Must run after all regulation
        # sources are processed so feature_to_regs is fully populated.
        self._resolve_feature_name_variations()

        # Phase 3: Merge with ALL regulation sources present
        # (name_variants are built inline during merge_features)
        self.merged_groups = self.merge_features(self.feature_to_regs)

        logger.info("Processing complete")

        return PipelineResult(
            feature_to_regs=self.feature_to_regs,
            merged_groups=self.merged_groups,
            regulation_names=self.regulation_names,
            feature_to_linked_regulation=dict(self.feature_to_linked_regulation),
            gazetteer=self.gazetteer,
            stats=self.stats,
            provincial_feature_map=provincial_feature_map,
            zone_feature_map=zone_feature_map,
            admin_feature_map=self.admin_feature_map,
            admin_regulation_ids=self.admin_regulation_ids,
            admin_area_reg_map=dict(self.admin_area_reg_map),
            regulation_details=self.regulation_details,
        )

    # --- Admin & Provincial Resolution ---

    def _process_provincial_regulations(self) -> Dict[str, List[str]]:
        """
        Process provincial base regulations.

        Supports two scope types:
        - admin_targets set → spatial intersection with admin boundary polygons
        - feature_types set (no admin_targets) → all FWA features of those types

        Provincial regulations use the prov_* ID namespace and are added to
        feature_to_regs so they participate in merged group formation.
        """
        provincial_feature_map: Dict[str, List[str]] = {}

        active_regulations = [r for r in PROVINCIAL_BASE_REGULATIONS if not r._disabled]
        if not active_regulations:
            logger.info("No active provincial base regulations to process")
            return provincial_feature_map

        logger.info(
            f"Processing {len(active_regulations)} provincial base regulation(s)..."
        )

        for prov_reg in active_regulations:
            if prov_reg.admin_targets and prov_reg.per_instance_ids:
                # Per-instance path: each admin polygon gets its own regulation ID
                # so streams through multiple distinct reserves keep separate
                # frontend_group_ids (e.g. Tsitika River in two eco reserves).
                instance_map = self._process_provincial_per_instance(prov_reg)
                provincial_feature_map.update(instance_map)
                continue

            if prov_reg.admin_targets:
                # Admin boundary scope — unified resolution path
                admin_result = self._resolve_admin_rule_set(
                    prov_reg.admin_targets,
                    prov_reg.feature_types,
                    prov_reg.regulation_id,
                )
                feature_ids = admin_result.feature_ids
                if not admin_result.empty:
                    self._register_admin_results(
                        admin_result.feature_ids,
                        admin_result.admin_entries,
                        prov_reg.regulation_id,
                    )
            elif prov_reg.feature_types:
                # Feature type scope — all features of specified types
                feature_ids = self._resolve_provincial_feature_types(prov_reg)
            else:
                logger.debug(
                    f"  Skipping '{prov_reg.regulation_id}' (no admin_targets or feature_types)"
                )
                continue

            if not feature_ids:
                continue

            provincial_feature_map[prov_reg.regulation_id] = feature_ids

            # Add provincial regulation to regulation_names for display in exports
            self.regulation_names[prov_reg.regulation_id] = prov_reg.rule_text

            # Store regulation details for export (provincial source)
            self.regulation_details[prov_reg.regulation_id] = (
                self._build_regulation_detail(
                    source="provincial",
                    waterbody_name=prov_reg.regulation_id.replace("_", " ").title(),
                    rule_text=prov_reg.rule_text,
                    restriction_type=(
                        prov_reg.restriction.get("type")
                        if prov_reg.restriction
                        else None
                    ),
                    restriction_details=(
                        prov_reg.restriction.get("details")
                        if prov_reg.restriction
                        else None
                    ),
                    dates=(
                        prov_reg.restriction.get("dates")
                        if prov_reg.restriction
                        else None
                    ),
                    scope_type=prov_reg.scope_type,
                    scope_location=(
                        ", ".join(sorted({t.layer for t in prov_reg.admin_targets}))
                        if prov_reg.admin_targets
                        else None
                    ),
                )
            )

            self._register_features(feature_ids, prov_reg.regulation_id)

            logger.debug(
                f"  Provincial '{prov_reg.regulation_id}': {len(feature_ids)} FWA features"
            )

        return provincial_feature_map

    def _process_provincial_per_instance(
        self, prov_reg: "ProvincialRegulation"
    ) -> Dict[str, List[str]]:
        """Process a provincial admin regulation with per-polygon regulation IDs.

        Instead of sharing one ``regulation_id`` across all matched admin
        polygons, generates ``"<base_id>:<layer>:<admin_fid>"`` for each
        individual admin polygon.  This ensures that streams passing through
        multiple distinct reserves (e.g. two adjacent ecological reserves)
        end up in separate ``MergedGroup`` instances with distinct
        ``frontend_group_id`` values, so the map can highlight each
        independently.

        Returns mapping of ``instance_regulation_id → [feature_ids]``.
        """
        base_id = prov_reg.regulation_id
        instance_map: Dict[str, List[str]] = {}

        if not self.gpkg_path or not self.gpkg_path.exists():
            raise FileNotFoundError(
                f"GPKG required for '{base_id}' (per-instance) "
                f"but not found at {self.gpkg_path}"
            )

        # Build regulation detail template once—same content for all instances.
        detail = self._build_regulation_detail(
            source="provincial",
            waterbody_name=base_id.replace("_", " ").title(),
            rule_text=prov_reg.rule_text,
            restriction_type=(
                prov_reg.restriction.get("type") if prov_reg.restriction else None
            ),
            restriction_details=(
                prov_reg.restriction.get("details") if prov_reg.restriction else None
            ),
            dates=(prov_reg.restriction.get("dates") if prov_reg.restriction else None),
            scope_type=prov_reg.scope_type,
            scope_location=(
                ", ".join(sorted({t.layer for t in prov_reg.admin_targets}))
                if prov_reg.admin_targets
                else None
            ),
        )

        for admin_target in prov_reg.admin_targets:
            layer_key = admin_target.layer
            admin_features = self.gazetteer.search_admin_layer(
                layer_key=layer_key,
                feature_ids=(
                    [admin_target.feature_id] if admin_target.feature_id else None
                ),
                code_filter=(
                    [admin_target.code_filter] if admin_target.code_filter else None
                ),
            )
            if not admin_features:
                logger.warning(
                    f"  '{base_id}': no admin features in layer '{layer_key}'"
                )
                continue

            logger.info(
                f"  '{base_id}': processing {len(admin_features)} admin polygon(s) "
                f"individually for per-instance IDs"
            )

            for admin_feat in admin_features:
                matched = self.gazetteer.find_features_in_admin_area(
                    admin_features=[admin_feat],
                    layer_key=layer_key,
                    feature_types=prov_reg.feature_types,
                    gpkg_path=self.gpkg_path,
                    buffer_m=ADMIN_BOUNDARY_BUFFER_M,
                )
                if not matched:
                    continue

                feature_ids = [f.fwa_id for f in matched]
                instance_id = f"{base_id}:{layer_key}:{admin_feat.fwa_id}"

                self.regulation_names[instance_id] = prov_reg.rule_text
                self.regulation_details[instance_id] = detail
                self.admin_area_reg_map[layer_key][admin_feat.fwa_id].add(instance_id)

                self._backfill_waterbody_keys(set(feature_ids))
                self._register_features(feature_ids, instance_id)

                instance_map[instance_id] = feature_ids

        total_instances = len(instance_map)
        total_assignments = sum(len(v) for v in instance_map.values())
        logger.info(
            f"  Provincial '{base_id}' (per-instance): "
            f"{total_instances} reserve(s) matched, "
            f"{total_assignments} total feature assignments"
        )
        return instance_map

    def _register_features(self, feature_ids, regulation_id: str) -> None:
        """Register feature IDs against a regulation in both indexes.

        Updates ``feature_to_regs`` (feature → [rule/reg IDs]) and
        ``feature_to_linked_regulation`` (feature → {regulation IDs}).
        """
        for fid in feature_ids:
            self.feature_to_regs.setdefault(fid, []).append(regulation_id)
            self.feature_to_linked_regulation[fid].add(regulation_id)

    def _register_admin_results(
        self,
        feature_ids: List[str],
        admin_entries: List[Tuple[str, Any]],
        regulation_id: str,
    ) -> None:
        """Register shared state for admin-resolved features.

        Single mutation point for all admin match bookkeeping, regardless of
        whether the regulation source is synopsis, provincial, or zone.
        Handles:

        * ``linked_waterbody_keys_of_polygon`` — via ``_backfill_waterbody_keys``
        * ``admin_area_reg_map`` — tracks which admin polygons → regulation IDs

        **Does NOT write** ``feature_to_regs`` or ``feature_to_linked_regulation``
        — those are the caller's responsibility (synopsis uses Pass 2;
        provincial/zone use ``_register_features``).
        """
        self._backfill_waterbody_keys(set(feature_ids))
        for layer_key, admin_feat in admin_entries:
            self.admin_area_reg_map[layer_key][admin_feat.fwa_id].add(regulation_id)

    def _resolve_admin_rule_set(
        self,
        admin_targets: List[AdminTarget],
        feature_types: Optional[List[FeatureType]],
        regulation_id: str,
    ) -> AdminResolutionResult:
        """Unified admin target → feature resolution (pure lookup).

        Single source of truth for resolving features from admin polygons.
        Used by all three regulation sources (synopsis, provincial, zone).

        Performs spatial lookup via ``_lookup_admin_targets`` and computes
        waterbody keys for polygon features.  Does **not** mutate ``self``
        — the caller decides how to apply the results:

        * Synopsis (``_pass1_prelink``): packs into ``Pass1Result``
        * Provincial / Zone: calls ``_register_admin_results``

        Returns:
            ``AdminResolutionResult`` with matched features, IDs,
            admin entries, and waterbody keys.  All lists empty if no matches.
        """
        matched, admin_entries = self._lookup_admin_targets(
            admin_targets, feature_types, regulation_id
        )
        if not matched:
            return AdminResolutionResult()

        feature_ids = [f.fwa_id for f in matched]

        # Compute waterbody keys from matched polygon features
        wb_keys: Set[str] = set()
        for feat in matched:
            if feat.feature_type in (
                FeatureType.LAKE,
                FeatureType.WETLAND,
                FeatureType.MANMADE,
            ):
                if feat.waterbody_key:
                    wb_keys.add(str(feat.waterbody_key))

        return AdminResolutionResult(
            matched_features=matched,
            feature_ids=feature_ids,
            admin_entries=admin_entries,
            waterbody_keys=wb_keys,
        )

    @staticmethod
    def _build_regulation_detail(
        *,
        source: str,
        waterbody_name: str = "",
        lookup_name: Optional[str] = None,
        region: Optional[str] = None,
        management_units: Optional[List[str]] = None,
        rule_text: str = "",
        restriction_type: Optional[str] = None,
        restriction_details: Optional[str] = None,
        dates: Optional[Any] = None,
        scope_type: Optional[str] = None,
        scope_location: Optional[str] = None,
        includes_tributaries: Optional[bool] = None,
        source_image: Optional[str] = None,
        zone_ids: Optional[List[str]] = None,
        feature_types: Optional[List[str]] = None,
        is_direct_match: Optional[bool] = None,
        exclusions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Build a regulation detail entry with a consistent schema.

        Every key is always present in the output dict (defaulting to None).
        Source-specific fields (e.g. ``zone_ids``, ``source_image``) are only
        non-None for their respective sources.
        """
        return {
            "waterbody_name": waterbody_name or None,
            "lookup_name": lookup_name,
            "region": region,
            "management_units": management_units or [],
            "rule_text": rule_text or None,
            "restriction_type": restriction_type,
            "restriction_details": restriction_details,
            "dates": dates,
            "scope_type": scope_type,
            "scope_location": scope_location,
            "includes_tributaries": includes_tributaries,
            "source": source,
            "source_image": source_image,
            "zone_ids": zone_ids,
            "feature_types": feature_types,
            "is_direct_match": is_direct_match,
            "exclusions": exclusions or None,
        }

    def _lookup_admin_targets(
        self,
        admin_targets: List[AdminTarget],
        feature_types: Optional[List[FeatureType]],
        regulation_id: str,
    ) -> Tuple[List[FWAFeature], List[Tuple[str, FWAFeature]]]:
        """Validate GPKG availability, then delegate to ``lookup_admin_targets``."""
        if not admin_targets:
            return [], []
        if not self.gpkg_path or not self.gpkg_path.exists():
            raise FileNotFoundError(
                f"GPKG required for admin target resolution "
                f"(regulation '{regulation_id}') but not found at {self.gpkg_path}"
            )
        return lookup_admin_targets(
            self.gazetteer,
            self.gpkg_path,
            admin_targets,
            feature_types,
            buffer_m=ADMIN_BOUNDARY_BUFFER_M,
        )

    def _resolve_provincial_feature_types(
        self, prov_reg: ProvincialRegulation
    ) -> List[str]:
        """
        Resolve features for a provincial regulation via feature type scope.

        Iterates all FWA features of the specified types and collects their IDs.
        """
        target_types = (
            prov_reg.feature_types
            if prov_reg.feature_types is not None
            else self._ALL_FWA_TYPES
        )
        matched_ids: List[str] = []

        for ftype in target_types:
            type_metadata = self.gazetteer.metadata.get(ftype, {})
            for fid in type_metadata:
                matched_ids.append(fid)

        self._backfill_waterbody_keys(set(matched_ids))
        return matched_ids

    # --- Zone Resolution ---

    def _process_zone_regulations(self) -> Dict[str, List[str]]:
        """
        Process zone-level default regulations.

        Zone regulations apply either to all FWA features of specified types
        within a zone (zone-wide mode), or to specific waterbodies identified
        by direct-match ID fields (direct-match mode), or via admin polygon
        intersection (admin-match mode).

        Zone-wide regulations are **pre-grouped by scope** so that spatial
        resolution (the expensive part) runs once per unique scope instead
        of once per regulation.  The 5 fields that determine the feature set
        are: ``zone_ids``, ``feature_types``, ``mu_ids``, ``exclude_mu_ids``,
        ``include_mu_ids``.

        Returns:
            Dict mapping regulation_id → list of matched feature IDs.
        """
        from .zone_base_regulations import ZONE_BASE_REGULATIONS

        zone_feature_map: Dict[str, List[str]] = {}

        active_regs = [r for r in ZONE_BASE_REGULATIONS if not r._disabled]
        if not active_regs:
            logger.info("No active zone base regulations to process")
            return zone_feature_map

        # Build zone + MU feature indexes (unbuffered + 500m-buffered).
        # The two-pass buffer strategy extends streams already partially inside
        # a zone/MU boundary without pulling in entirely new streams.
        zone_index, mu_index, zone_index_buf, mu_index_buf = (
            self._build_zone_feature_index()
        )

        # --- Phase 1: Classify regulations by resolution strategy ---
        classified = ZoneScopeOptimizer.classify(active_regs)

        logger.info(
            f"Processing {len(active_regs)} zone regs: "
            f"{len(classified.zone_wide_groups)} unique zone-wide scopes, "
            f"{len(classified.direct_regs)} direct-match, {len(classified.admin_regs)} admin-match"
        )

        # --- Phase 2: Resolve each unique zone-wide scope once ---
        for scope_key, regs_in_scope in classified.zone_wide_groups.items():
            # Any regulation in the group can serve as the representative —
            # all share identical scope fields.
            representative = regs_in_scope[0]
            resolved_ids = self._resolve_zone_wide(
                representative, zone_index, mu_index, zone_index_buf, mu_index_buf
            )
            matched_ids = list(resolved_ids)

            if not matched_ids:
                for reg in regs_in_scope:
                    logger.warning(f"  Zone '{reg.regulation_id}': no features matched")
                continue

            # Log once per scope group
            reg_names = [r.regulation_id for r in regs_in_scope]
            logger.info(
                f"  Scope zones={sorted(representative.zone_ids)} "
                f"types={[ft.value for ft in representative.feature_types] if representative.feature_types else 'ALL'}: "
                f"{len(matched_ids):,} features — "
                f"shared by {len(regs_in_scope)} reg(s): "
                f"{', '.join(reg_names[:3])}"
                f"{'...' if len(reg_names) > 3 else ''}"
            )

            # Fan out: register metadata + features for each regulation
            for reg in regs_in_scope:
                zone_feature_map[reg.regulation_id] = matched_ids
                self._register_zone_reg_metadata(reg)
                self._register_features(matched_ids, reg.regulation_id)

        # --- Phase 3: Direct-match regulations (no grouping) ---
        for reg in classified.direct_regs:
            matched_ids = list(self._resolve_zone_direct_match(reg))
            if not matched_ids:
                logger.warning(f"  Zone '{reg.regulation_id}': no features matched")
                continue
            zone_feature_map[reg.regulation_id] = matched_ids
            self._register_zone_reg_metadata(reg)
            self._register_features(matched_ids, reg.regulation_id)
            logger.info(
                f"  Zone '{reg.regulation_id}' (direct-match): "
                f"{len(matched_ids):,} features across zones {reg.zone_ids}"
            )

        # --- Phase 4: Admin-match regulations (unified resolution) ---
        for reg in classified.admin_regs:
            admin_result = self._resolve_admin_rule_set(
                reg.admin_targets, reg.feature_types, reg.regulation_id
            )
            if admin_result.empty:
                logger.warning(f"  Zone '{reg.regulation_id}': no features matched")
                continue
            self._register_admin_results(
                admin_result.feature_ids,
                admin_result.admin_entries,
                reg.regulation_id,
            )
            zone_feature_map[reg.regulation_id] = admin_result.feature_ids
            self._register_zone_reg_metadata(reg)
            self._register_features(admin_result.feature_ids, reg.regulation_id)
            logger.info(
                f"  Zone '{reg.regulation_id}' (admin-match): "
                f"{len(admin_result.feature_ids):,} features across zones {reg.zone_ids}"
            )

        return zone_feature_map

    def _register_zone_reg_metadata(self, zone_reg: ZoneRegulation) -> None:
        """Register regulation name and detail for a single zone regulation.

        Extracted from the loop body so all three resolution paths
        (zone-wide, direct-match, admin-match) share the same bookkeeping.
        """
        self.regulation_names[zone_reg.regulation_id] = zone_reg.rule_text

        display_name = (
            zone_reg.regulation_id.replace("zone_", "").replace("_", " ").title()
        )
        region_str = ", ".join(
            ZONE_REGION_NAMES.get(z, f"Region {z}") for z in zone_reg.zone_ids
        )
        self.regulation_details[zone_reg.regulation_id] = self._build_regulation_detail(
            source="zone",
            waterbody_name=display_name,
            region=region_str,
            rule_text=zone_reg.rule_text,
            restriction_type=zone_reg.restriction.get("type", ""),
            restriction_details=zone_reg.restriction.get("details", ""),
            dates=zone_reg.dates,
            scope_location=zone_reg.scope_location,
            zone_ids=zone_reg.zone_ids,
            feature_types=(
                [ft.value for ft in zone_reg.feature_types]
                if zone_reg.feature_types
                else None
            ),
            is_direct_match=zone_reg.has_direct_target(),
        )

    def _build_zone_feature_index(
        self,
    ) -> Tuple[FeatureIndex, FeatureIndex, FeatureIndex, FeatureIndex]:
        """Delegate to ``build_feature_index`` and log summary."""
        zone_index, mu_index, zone_index_buf, mu_index_buf = build_feature_index(
            self.gazetteer
        )
        total = sum(
            len(features)
            for zones in zone_index.values()
            for features in zones.values()
        )
        logger.info(
            f"  Zone feature index: {len(zone_index)} zones, "
            f"{len(mu_index)} MUs, {total:,} entries "
            f"(buffered: {len(zone_index_buf)} zones, "
            f"{len(mu_index_buf)} MUs)"
        )
        return zone_index, mu_index, zone_index_buf, mu_index_buf

    def _resolve_zone_wide(
        self,
        zone_reg: ZoneRegulation,
        zone_index: FeatureIndex,
        mu_index: FeatureIndex,
        zone_index_buf: FeatureIndex,
        mu_index_buf: FeatureIndex,
    ) -> set:
        """Resolve zone-wide regulation with two-pass boundary buffer.

        1. Resolve with **unbuffered** indexes → base set (truly inside).
        2. Resolve with **buffered** indexes → wider candidate set.
        3. From the wider set, keep only stream features whose
           ``blue_line_key`` already appears in the base set.  This extends
           boundary-straddling streams without pulling in new ones.
        """
        base_ids = resolve_zone_wide_ids(zone_reg, zone_index, mu_index)
        buffered_ids = resolve_zone_wide_ids(zone_reg, zone_index_buf, mu_index_buf)
        extended, newly_added = self._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=zone_reg.mu_ids
        )
        if newly_added:
            logger.info(
                f"    Buffer extended scope "
                f"zones={sorted(zone_reg.zone_ids)} by "
                f"{newly_added} boundary segments "
                f"(base: {len(base_ids):,}, total: {len(extended):,})"
            )
        self._backfill_waterbody_keys(extended)
        return extended

    def _extend_boundary_streams(
        self,
        base_ids: set,
        buffered_ids: set,
        target_mu_ids: Optional[List[str]] = None,
    ) -> Tuple[set, int]:
        """Extend a base feature set with buffered features for boundary streams.

        Two extension passes:

        1. **BLK pass** — from *buffered_ids*, keep stream features whose
           ``blue_line_key`` is already represented in *base_ids*.  This
           extends boundary-straddling streams without pulling in new ones.

        2. **WSC pass** (only when *target_mu_ids* is set) — for side
           channels sharing the exact ``fwa_watershed_code`` of a feature
           already in the extended set, include them if their raw
           (buffered) ``mgmt_units`` overlap with *target_mu_ids*.  This
           captures braided side channels near an MU boundary that have
           different BLKs from the mainstem but belong to the same river
           corridor.  Only features within the 500m buffer distance of
           the target MU boundary are included.

        Non-stream features pass through from the base set unchanged.

        Returns:
            Tuple of (extended feature ID set, count of newly added segments).
        """
        stream_meta = self.gazetteer.metadata.get(FeatureType.STREAM, {})

        # --- Pass 1: BLK extension (existing behaviour) ---

        # Collect blue_line_keys already present in the base (unbuffered) set
        base_blks: set = set()
        for fid in base_ids:
            if meta := stream_meta.get(fid):
                if blk := meta.get("blue_line_key"):
                    base_blks.add(str(blk))

        # From the buffered-only extras, keep streams whose BLK is in base
        extended_ids: set = set(base_ids)
        newly_added = 0
        for fid in buffered_ids - base_ids:
            if meta := stream_meta.get(fid):
                if blk := meta.get("blue_line_key"):
                    if str(blk) in base_blks:
                        extended_ids.add(fid)
                        newly_added += 1

        # --- Pass 2: WSC side-channel extension ---
        # Include side channels (different BLK, same exact WSC) whose
        # buffered MU overlaps the regulation's target MUs.  This handles
        # braiding near MU boundaries where side channels are just outside
        # the unbuffered boundary but within the 500m buffer.
        if target_mu_ids and hasattr(self.gazetteer, "watershed_code_index"):
            target_mu_set = set(target_mu_ids)

            # Collect exact WSCs from the already-extended set
            extended_wscs: set = set()
            for fid in extended_ids:
                if meta := stream_meta.get(fid):
                    if wsc := meta.get("fwa_watershed_code"):
                        extended_wscs.add(wsc)

            # For each WSC, find candidate features via the reverse index
            for wsc in extended_wscs:
                for candidate_fid in self.gazetteer.watershed_code_index.get(wsc, []):
                    if candidate_fid in extended_ids:
                        continue
                    candidate_meta = stream_meta.get(candidate_fid, {})
                    # Check raw (buffered) MU overlap with regulation targets
                    buffered_mus = set(candidate_meta.get("mgmt_units", []))
                    if buffered_mus & target_mu_set:
                        extended_ids.add(candidate_fid)
                        newly_added += 1

        return extended_ids, newly_added

    def _backfill_waterbody_keys(self, fids: set) -> None:
        """Add waterbody_key values to linked_waterbody_keys_of_polygon for polygon features."""
        for fid in fids:
            for ftype in (FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE):
                type_meta = self.gazetteer.metadata.get(ftype, {})
                if meta := type_meta.get(fid):
                    if wb_key := meta.get("waterbody_key"):
                        self.linked_waterbody_keys_of_polygon.add(str(wb_key))
                    break

    def _resolve_zone_direct_match(self, zone_reg: ZoneRegulation) -> set:
        """Delegate to ``resolve_direct_match_ids`` and backfill waterbody keys."""
        matched_ids = resolve_direct_match_ids(self.gazetteer, zone_reg)
        self._backfill_waterbody_keys(matched_ids)
        return matched_ids

    def _get_lake_only_excluded_wb_keys(self) -> Set[str]:
        """Return the subset of linked polygon wb_keys that belong to lakes.

        Used by lake-seeded tributary enrichment so that "tributaries of X Lake"
        stops at the next regulated lake upstream, but wetlands and manmade
        features are transparent to traversal.
        """
        lake_meta = self.gazetteer.metadata.get(FeatureType.LAKE, {})
        all_lake_wb_keys: Set[str] = set()
        for meta in lake_meta.values():
            if wb_key := meta.get("waterbody_key"):
                all_lake_wb_keys.add(str(wb_key))
        return self.linked_waterbody_keys_of_polygon & all_lake_wb_keys

    # --- Enrichment & Grouping ---

    def _enrich_with_tributaries(self, features: List[FWAFeature]) -> List[FWAFeature]:
        """Enrich features with upstream tributaries.

        Performance notes:
        - Uses reverse index for O(1) wb_key → stream_id lookup (see _get_stream_seeds_for_waterbody)
        """
        if (
            not self.tributary_enricher
            or not self.tributary_enricher.graph
            or not features
        ):
            return []

        stream_seeds, polygon_seeds_by_type, skipped_features = (
            [],
            defaultdict(list),
            [],
        )

        for feature in features:
            feature_type = get_feature_type(feature)
            linear_id = feature.fwa_id

            if feature_type == FeatureType.STREAM:
                if linear_id:
                    stream_seeds.append(linear_id)
                else:
                    skipped_features.append(feature.gnis_name or "unnamed")

            elif feature_type in (
                FeatureType.LAKE,
                FeatureType.WETLAND,
                FeatureType.MANMADE,
            ):
                if linear_id:
                    try:
                        raw_metadata = self.gazetteer.metadata.get(
                            feature_type, {}
                        ).get(linear_id, {})

                        if wb_key := raw_metadata.get("waterbody_key"):
                            if connected_streams := self._get_stream_seeds_for_waterbody(
                                wb_key
                            ):
                                polygon_seeds_by_type[feature_type].extend(
                                    connected_streams
                                )
                    except ValueError as exc:
                        raise ValueError(
                            f"Failed to resolve tributaries for feature "
                            f"{linear_id!r} ({feature_type.value}): {exc}"
                        ) from exc

        all_tributaries_dict = {}
        if stream_seeds:
            excluded_codes = self._get_watershed_codes_for_streams(stream_seeds)
            # Stream-seeded: traverse through all waterbodies (lakes, wetlands,
            # manmade) freely.  The stream is the regulatory entity — waterbodies
            # the stream passes through should not block reachability.
            for trib in self.tributary_enricher.enrich_with_tributaries(
                stream_seeds,
                excluded_watershed_codes=excluded_codes,
            ):
                all_tributaries_dict[trib.fwa_id] = trib

        if polygon_seeds_by_type:
            # Lake/polygon-seeded: stop traversal at the next regulated lake.
            # This prevents "tributaries of X Lake" from climbing past another
            # regulated lake upstream.  Only LAKE wb_keys act as barriers;
            # wetlands and manmade features are pass-through.
            lake_only_excluded = self._get_lake_only_excluded_wb_keys()
            for ftype, seeds in polygon_seeds_by_type.items():
                for trib in self.tributary_enricher.enrich_with_tributaries(
                    seeds,
                    excluded_waterbody_keys=lake_only_excluded or None,
                ):
                    all_tributaries_dict[trib.fwa_id] = trib

        return list(all_tributaries_dict.values())

    def merge_features(
        self, feature_to_regs: Dict[str, List[str]]
    ) -> Dict[str, MergedGroup]:
        """Delegate to ``feature_merger.merge_features`` with mapper state."""
        return _merge_features_impl(
            feature_to_regs,
            gazetteer=self.gazetteer,
            linked_waterbody_keys=self.linked_waterbody_keys_of_polygon,
            admin_regulation_ids=self.admin_regulation_ids,
            regulation_names=self.regulation_names,
            feature_to_regs_full=self.feature_to_regs,
            tributary_assignments=self.tributary_assignments,
            regulation_parent_gnis=self.regulation_parent_gnis,
            feature_to_aliases=self.feature_to_aliases,
            feature_display_name_overrides=self.feature_display_name_overrides,
            progress_wrapper=self._with_progress,
        )

    # --- Property & Metadata Helpers ---

    def _build_wb_key_index(self) -> Dict[str, List[str]]:
        """Build waterbody_key → [linear_feature_ids] reverse index.

        Called once during ``__init__`` so that ``_get_stream_seeds_for_waterbody``
        is a simple dict lookup with no lazy-init side effects.
        """
        index: Dict[str, List[str]] = defaultdict(list)
        if not self.gazetteer:
            return index
        for lin_id, meta in self.gazetteer.metadata.get(FeatureType.STREAM, {}).items():
            if wb_key := meta.get("waterbody_key"):
                index[str(wb_key)].append(lin_id)
        logger.debug(f"Built wb_key reverse index: {len(index)} unique waterbody keys")
        return dict(index)

    def _get_stream_seeds_for_waterbody(self, waterbody_key: str) -> List[str]:
        """Get stream IDs connected to a waterbody via reverse index (O(1) lookup)."""
        return self._wb_key_to_stream_ids.get(str(waterbody_key), [])

    def _get_watershed_codes_for_streams(
        self, linear_feature_ids: List[str]
    ) -> Set[str]:
        codes = set()
        for lin_id in linear_feature_ids:
            if meta := (
                self.gazetteer.get_stream_metadata(str(lin_id))
                if self.gazetteer
                else None
            ):
                if ws_code := meta.get("fwa_watershed_code"):
                    codes.add(ws_code)
                    codes.update(get_parent_watershed_codes(ws_code))
        return codes

    def _with_progress(self, iterable: Any, desc: str, unit: str) -> Any:
        try:
            from tqdm import tqdm

            return tqdm(iterable, desc=desc, unit=unit)
        except ImportError:
            logger.debug("tqdm not available, progress bars disabled")
            return iterable

    def get_stats(self) -> RegulationMappingStats:
        return self.stats
