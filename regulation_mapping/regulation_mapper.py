"""
RegulationMapper - Orchestrates the full pipeline: Link -> Scope -> Enrich -> Map

Creates inverted index: feature_id -> regulation_ids
Handles all regulation sources: synopsis, admin area matches, and provincial base regulations.
"""

from typing import Dict, List, Optional, Set, Any
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
from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from .logger_config import get_logger

logger = get_logger(__name__)


# --- Standalone Helper Functions ---
def generate_regulation_id(regulation_idx: int) -> str:
    """Generate a consistent regulation ID from its index."""
    return f"reg_{regulation_idx:05d}"


def generate_rule_id(regulation_idx: int, rule_idx: int) -> str:
    """Generate a consistent rule ID from regulation and rule indices."""
    return f"{generate_regulation_id(regulation_idx)}_rule{rule_idx}"


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


@dataclass(frozen=True)
class MergedGroup:
    """Merged group of features with identical regulation sets.

    ``zones`` and ``region_names`` are positionally paired — index *i* in
    ``region_names`` is the name for index *i* in ``zones``.  Both are
    sorted by zone ID.
    """

    group_id: str
    feature_ids: tuple[str, ...]
    regulation_ids: tuple[str, ...]
    waterbody_key: Optional[str] = None
    feature_count: int = 0
    zones: tuple[str, ...] = ()  # REGION_RESPONSIBLE_ID values
    mgmt_units: tuple[str, ...] = ()
    region_names: tuple[str, ...] = ()  # Paired with zones


@dataclass(frozen=True)
class PipelineResult:
    """Result from full regulation processing pipeline. Contains all state needed for export."""

    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    feature_to_linked_regulation: Dict[str, Set[str]] = field(default_factory=dict)
    gazetteer: Optional[MetadataGazetteer] = None
    stats: Optional[RegulationMappingStats] = None
    # Provincial regulation support: maps feature_id → list of provincial regulation_ids
    provincial_feature_map: Dict[str, List[str]] = field(default_factory=dict)
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
    # Name variation aliases: maps primary regulation name_verbatim → list of alternate names.
    # Populated from NameVariationLink entries.  The exporter injects these into
    # the search index name_variants for discoverability.
    name_variation_aliases: Dict[str, List[str]] = field(default_factory=dict)


class RegulationMapper:
    """Orchestrates the full regulation mapping pipeline."""

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
        # Name variation aliases: primary_name_verbatim → [alternate names]
        self.name_variation_aliases: Dict[str, List[str]] = defaultdict(list)

        # Reverse index: waterbody_key → [linear_feature_ids] for O(1) lookup
        # Replaces O(5M) linear scan in _get_stream_seeds_for_waterbody
        self._wb_key_to_stream_ids: Optional[Dict[str, List[str]]] = None

    # --- Core Pipeline ---

    # regulation_id
    def regulation_id(self, regulation_idx: int) -> str:
        return generate_regulation_id(regulation_idx)

    # rule id
    def rule_id(self, regulation_idx: int, rule_idx: int) -> str:
        """Generate rule ID using the standalone helper function."""
        return generate_rule_id(regulation_idx, rule_idx)

    def process_all_regulations(self, regulations: List[Dict]) -> Dict[str, List[str]]:
        """Main processing loop - creates feature to rule index."""
        self.feature_to_regs = {}
        self.feature_to_linked_regulation = defaultdict(set)
        self.linked_waterbody_keys_of_polygon = set()
        self.admin_feature_map = {}
        self.admin_regulation_ids = set()
        self.stats.total_regulations = len(regulations)

        logger.info(f"Processing {len(regulations)} regulations...")

        # Cache linked results to avoid re-processing in Pass 2
        # Format: tuple of (original_index, regulation_dict, regulation_id, base_features, is_admin_match)
        linked_regulations_cache = []

        # ==========================================
        # PASS 1: Pre-calculate Links and Lookups
        # ==========================================
        for idx, regulation in enumerate(
            self._with_progress(regulations, "Pre-linking regulations", "reg")
        ):
            regulation_id = self.regulation_id(idx)
            identity = regulation.get("identity", {})
            name_verbatim = identity.get("name_verbatim", "")

            # 0: prelim check for regulation errors before linking.
            reg_error = regulation.get("error", None)
            if reg_error:
                logger.warning(
                    f"Regulation {name_verbatim} has error flag: {reg_error}"
                )
                self.stats.bad_regulation += 1
                continue

            if name_verbatim:
                self.regulation_names[regulation_id] = name_verbatim

            # 1. Normalize Region and Link
            region = regulation.get("region")
            if region and region.startswith("REGION"):
                # Extract region number+letter from e.g. "REGION 7A - Omineca" → "7A"
                _m = re.match(r"REGION\s+(\d+[A-Za-z]?)\s*[-–]", region)
                if _m:
                    region = f"Region {_m.group(1).upper()}"
                else:
                    region_num = "".join(c for c in region.split("-")[0] if c.isdigit())
                    region = f"Region {region_num}" if region_num else None

            link_result = self.linker.link_waterbody(
                region=region,
                mgmt_units=regulation.get("mu", []),
                name_verbatim=name_verbatim,
            )

            self.stats.link_status_counts[link_result.status.value] += 1

            # Handle admin direct matches: resolve spatial intersection inline
            is_admin_match = False
            if (
                link_result.link_method == "admin_direct_match"
                and link_result.admin_match
            ):
                is_admin_match = True
                self.stats.linked_regulations += 1
                self.admin_regulation_ids.add(regulation_id)
                admin_match = link_result.admin_match

                if not self.gpkg_path or not self.gpkg_path.exists():
                    logger.warning(
                        f"GPKG not available - skipping admin match for '{name_verbatim}'"
                    )
                    continue

                admin_features = self.gazetteer.search_admin_layer(
                    layer_key=admin_match.admin_layer,
                    feature_ids=admin_match.feature_ids,
                    feature_names=admin_match.feature_names,
                    code_filter=admin_match.code_filter,
                )
                if not admin_features:
                    logger.error(
                        f"  Skipping '{name_verbatim}' "
                        f"- admin feature lookup failed (layer: {admin_match.admin_layer})"
                    )
                    continue

                base_features = self.gazetteer.find_features_in_admin_area(
                    admin_features=admin_features,
                    layer_key=admin_match.admin_layer,
                    include_streams=admin_match.include_streams,
                    include_lakes=admin_match.include_lakes,
                    include_wetlands=admin_match.include_wetlands,
                    include_manmade=admin_match.include_manmade,
                    gpkg_path=self.gpkg_path,
                )
                if not base_features:
                    logger.warning(
                        f"  No FWA features found in admin area for '{name_verbatim}'"
                    )
                    continue

                self.admin_feature_map[name_verbatim] = [
                    f.fwa_id for f in base_features
                ]

                logger.info(
                    f"  Admin match '{name_verbatim}': {len(base_features)} FWA features"
                )

                # Track admin polygon → regulation IDs for admin boundary export
                for admin_feat in admin_features:
                    self.admin_area_reg_map[admin_match.admin_layer][
                        admin_feat.fwa_id
                    ].add(regulation_id)

            elif link_result.status == LinkStatus.NAME_VARIATION:
                # Alternate name for an already-linked waterbody — don't link features,
                # but record the alias so the search index can include it.
                primary_name = link_result.matched_name
                if primary_name:
                    self.name_variation_aliases[primary_name].append(name_verbatim)
                    logger.debug(
                        f"Name variation '{name_verbatim}' → primary '{primary_name}'"
                    )
                continue

            elif (
                link_result.status != LinkStatus.SUCCESS
                or not link_result.matched_features
            ):
                self.stats.failed_to_link_regulations += 1
                continue

            else:
                self.stats.linked_regulations += 1
                base_features = link_result.matched_features

            # 2. Track Keys and Polygons for Grouping (Pre-step)
            for feature in base_features:
                fid = self._get_feature_id(feature)
                self.feature_to_linked_regulation[fid].add(regulation_id)

                if self._get_feature_type(feature) in (
                    FeatureType.LAKE,
                    FeatureType.WETLAND,
                    FeatureType.MANMADE,
                ):
                    if wb_key := self._get_prop(feature, ["waterbody_key"]):
                        self.linked_waterbody_keys_of_polygon.add(str(wb_key))

            # Store successful links for Pass 2
            # Inject additional_info from linking corrections as a synthetic "Note" rule
            if link_result.additional_info:
                regulation.setdefault("rules", []).append(
                    {
                        "rule_text_verbatim": link_result.additional_info,
                        "restriction": {
                            "type": "Note",
                            "details": link_result.additional_info,
                        },
                        "scope": {},
                    }
                )
            linked_regulations_cache.append(
                (idx, regulation, regulation_id, base_features, is_admin_match)
            )

        logger.info(
            f"Pre-step complete. Found {len(self.linked_waterbody_keys_of_polygon)} linked polygon waterbodies."
        )

        # ==========================================
        # PASS 2: Scope, Enrich, and Map Rules
        # ==========================================
        for (
            idx,
            regulation,
            regulation_id,
            base_features,
            is_admin_match,
        ) in self._with_progress(
            linked_regulations_cache, "Mapping rules to features", "reg"
        ):
            identity = regulation.get("identity", {})

            # 3. Apply Global Scope
            global_scope = identity.get("global_scope", {})

            # Admin area regulations should never use tributary enrichment.
            # The admin polygon already delimits the spatial extent — enriching
            # with tributaries is redundant and extremely expensive.
            if is_admin_match:
                has_trib_flag = global_scope.get("includes_tributaries") or any(
                    r.get("scope", {}).get("includes_tributaries")
                    for r in regulation.get("rules", [])
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
            for rule_idx, rule in enumerate(regulation.get("rules", [])):
                self.stats.total_rules_processed += 1
                rule_scope = rule.get("scope", {})

                final_features = self.apply_scope_and_enrich(
                    globally_scoped_features,
                    rule_scope,
                    global_scope,
                    skip_tributary_enrichment=skip_tribs,
                )

                rule_id = self.rule_id(idx, rule_idx)
                for feature in final_features:
                    self.feature_to_regs.setdefault(
                        self._get_feature_id(feature), []
                    ).append(rule_id)
                    self.stats.total_rule_to_feature_mappings += 1

                # Store regulation details for export (synopsis source)
                rest = rule.get("restriction", {})
                scope_d = rule.get("scope", {})
                self.regulation_details[rule_id] = {
                    "waterbody_name": identity.get("name_verbatim"),
                    "waterbody_key": identity.get("waterbody_key"),
                    "region": regulation.get("region"),
                    "management_units": regulation.get("mu", []),
                    "rule_text": rule.get("rule_text_verbatim"),
                    "restriction_type": rest.get("type"),
                    "restriction_details": rest.get("details"),
                    "dates": rest.get("dates"),
                    "scope_type": scope_d.get("type"),
                    "scope_location": scope_d.get("location_verbatim"),
                    "includes_tributaries": scope_d.get("includes_tributaries"),
                    "source": "synopsis",
                }

        # Sort indices
        for feature_id in self.feature_to_regs:
            self.feature_to_regs[feature_id].sort()

        self.stats.unique_features_with_rules = len(self.feature_to_regs)
        return self.feature_to_regs

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

        # Check if we should enrich with tributaries based on rule scope or global scope
        should_enrich = (
            scope.get("includes_tributaries", False)
            if scope.get("includes_tributaries", False) is not None
            else global_scope.get("includes_tributaries", False)
        )

        if should_enrich:
            tributaries = self._enrich_with_tributaries(scoped_features)
            return scoped_features + tributaries

        return scoped_features

    def run(self, regulations: List[Dict]) -> PipelineResult:
        """Full pipeline: Link -> Scope -> Enrich -> Map (all sources) -> Merge.

        Processes all regulation sources before merging so that merged groups
        contain the complete set of regulation IDs per feature:
          1. Synopsis + admin area regulations (resolved inline during linking)
          2. Provincial base regulations (blanket rules for admin boundaries)
          3. Merge features into groups (with ALL regulation sources present)
        """
        # Phase 1: Synopsis + admin area regulations (fully resolved inline)
        self.process_all_regulations(regulations)

        # Phase 2: Provincial base regulations
        provincial_feature_map = self._process_provincial_regulations()

        # Phase 3: Merge with ALL regulation sources present
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
            admin_feature_map=self.admin_feature_map,
            admin_regulation_ids=self.admin_regulation_ids,
            admin_area_reg_map=dict(self.admin_area_reg_map),
            regulation_details=self.regulation_details,
            name_variation_aliases=dict(self.name_variation_aliases),
        )

    # --- Admin & Provincial Resolution ---

    def _process_provincial_regulations(self) -> Dict[str, List[str]]:
        """
        Process provincial base regulations (blanket rules for admin areas).

        Provincial regulations apply to all FWA features within admin boundaries
        (e.g., "All National Parks are closed to fishing"). These use their own
        regulation ID namespace (prov_*) and are added to feature_to_regs so
        they participate in merged group formation.
        """
        provincial_feature_map: Dict[str, List[str]] = {}

        active_regulations = [
            r for r in PROVINCIAL_BASE_REGULATIONS if not getattr(r, "_disabled", False)
        ]
        if not active_regulations:
            logger.info("No active provincial base regulations to process")
            return provincial_feature_map

        if not self.gpkg_path or not self.gpkg_path.exists():
            logger.warning("GPKG not available - skipping provincial regulations")
            return provincial_feature_map

        logger.info(
            f"Processing {len(active_regulations)} provincial base regulation(s)..."
        )

        for prov_reg in active_regulations:
            if not prov_reg.admin_layer:
                logger.debug(f"  Skipping '{prov_reg.regulation_id}' (no admin_layer)")
                continue

            # Find admin features from pickle metadata
            admin_features = self.gazetteer.search_admin_layer(
                layer_key=prov_reg.admin_layer,
                feature_ids=prov_reg.feature_ids,
                feature_names=prov_reg.feature_names,
                code_filter=prov_reg.code_filter,
            )

            if not admin_features:
                logger.error(
                    f"  Skipping '{prov_reg.regulation_id}' "
                    f"- admin feature lookup failed (layer: {prov_reg.admin_layer})"
                )
                continue

            # Determine include flags from provincial regulation
            include_streams = (
                "stream" in prov_reg.feature_types
                if prov_reg.feature_types
                else prov_reg.include_streams
            )
            include_lakes = (
                "lake" in prov_reg.feature_types
                if prov_reg.feature_types
                else prov_reg.include_lakes
            )
            include_wetlands = (
                "wetland" in prov_reg.feature_types
                if prov_reg.feature_types
                else prov_reg.include_wetlands
            )
            include_manmade = (
                "manmade" in prov_reg.feature_types
                if prov_reg.feature_types
                else prov_reg.include_manmade
            )

            # Spatial intersection with FWA features
            matched_features = self.gazetteer.find_features_in_admin_area(
                admin_features=admin_features,
                layer_key=prov_reg.admin_layer,
                include_streams=include_streams,
                include_lakes=include_lakes,
                include_wetlands=include_wetlands,
                include_manmade=include_manmade,
                gpkg_path=self.gpkg_path,
            )

            feature_ids = [f.fwa_id for f in matched_features]
            provincial_feature_map[prov_reg.regulation_id] = feature_ids

            # Backfill linked_waterbody_keys_of_polygon for polygon features so they
            # are grouped correctly during merge_features
            for feat in matched_features:
                if feat.feature_type in (
                    FeatureType.LAKE,
                    FeatureType.WETLAND,
                    FeatureType.MANMADE,
                ):
                    if wb_key := getattr(feat, "waterbody_key", None):
                        self.linked_waterbody_keys_of_polygon.add(str(wb_key))

            # Add provincial regulation to regulation_names for display in exports
            self.regulation_names[prov_reg.regulation_id] = prov_reg.rule_text

            # Store regulation details for export (provincial source)
            self.regulation_details[prov_reg.regulation_id] = {
                "waterbody_name": prov_reg.regulation_id.replace("_", " ").title(),
                "waterbody_key": None,
                "region": None,
                "management_units": [],
                "rule_text": prov_reg.rule_text,
                "restriction_type": (
                    prov_reg.restriction.get("type") if prov_reg.restriction else None
                ),
                "restriction_details": (
                    prov_reg.restriction.get("details")
                    if prov_reg.restriction
                    else None
                ),
                "dates": (
                    prov_reg.restriction.get("dates") if prov_reg.restriction else None
                ),
                "scope_type": prov_reg.scope_type,
                "scope_location": prov_reg.admin_layer,
                "includes_tributaries": None,
                "source": "provincial",
            }

            # Add to feature_to_regs and feature_to_linked_regulation
            for fid in feature_ids:
                self.feature_to_regs.setdefault(fid, []).append(prov_reg.regulation_id)
                self.feature_to_linked_regulation[fid].add(prov_reg.regulation_id)

            logger.info(
                f"  Provincial '{prov_reg.regulation_id}': {len(feature_ids)} FWA features"
            )

            # Track admin polygon → regulation IDs for admin boundary export
            for admin_feat in admin_features:
                self.admin_area_reg_map[prov_reg.admin_layer][admin_feat.fwa_id].add(
                    prov_reg.regulation_id
                )

        return provincial_feature_map

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
            feature_type = self._get_feature_type(feature)
            linear_id = self._get_prop(feature, ["linear_feature_id", "fwa_id"])

            if feature_type in (FeatureType.STREAM, FeatureType.UNKNOWN):
                if linear_id:
                    stream_seeds.append(linear_id)
                else:
                    skipped_features.append(
                        self._get_prop(feature, ["gnis_name", "name"], "unnamed")
                    )

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
                    except ValueError:
                        continue

        all_tributaries_dict = {}
        if stream_seeds:
            excluded_codes = self._get_watershed_codes_for_streams(stream_seeds)
            for trib in self.tributary_enricher.enrich_with_tributaries(
                stream_seeds,
                excluded_watershed_codes=excluded_codes,
                excluded_waterbody_keys=self.linked_waterbody_keys_of_polygon,  # NOTE: I am not sure if this is right but I dont think we want tributary of lake stream segments since it is not really the river here. it is the lake which might have its own tributary regulations.
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib

        for ftype, seeds in polygon_seeds_by_type.items():
            for trib in self.tributary_enricher.enrich_with_tributaries(
                seeds,
                excluded_waterbody_keys=self.linked_waterbody_keys_of_polygon,
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib

        return list(all_tributaries_dict.values())

    def merge_features(
        self, feature_to_regs: Dict[str, List[str]]
    ) -> Dict[str, MergedGroup]:
        """Merge features with identical regulation sets into groups.

        Grouping key is (feature_type, gnis_id, reg_set):
        - Named features (gnis_id present) group by GNIS — all segments/polygons
          of the same named waterbody with the same regulations become one group.
        - Unnamed features (no gnis_id) with a linked waterbody_key group by that key.
        - Remaining unnamed features pool together per (type, reg_set), so e.g. all
          unnamed streams inside an eco reserve that share the same regulation set
          become a single group rather than hundreds of individual entries.
        """
        logger.info(f"Merging {len(feature_to_regs)} features into groups...")
        group_map = defaultdict(list)

        for feature_id, reg_ids in self._with_progress(
            feature_to_regs.items(), "Grouping features", "feature"
        ):
            reg_set = frozenset(reg_ids)
            feature = self.gazetteer.get_feature_by_id(feature_id)

            if not feature:
                continue

            feature_type_val = self._get_feature_type(feature).value
            blk = self._get_prop(feature, ["blue_line_key"])
            wbk = self._get_prop(feature, ["waterbody_key"])
            use_wbk = wbk and str(wbk) in self.linked_waterbody_keys_of_polygon

            if blk and use_wbk:
                grouping_key = f"{feature_type_val}_blue_line_{blk}_waterbody_{wbk}"
            elif blk:
                # Named streams group by blk+gnis so segments of the same named stream
                # stay together. Unnamed streams fall back to blk alone — all segments
                # of the same physical stream will share the same blk, and the reg_set
                # in the group_map key already ensures only identical-regulation streams merge.
                gnis_id = self._get_prop(feature, ["gnis_id"])
                if gnis_id:
                    grouping_key = f"{feature_type_val}_blue_line_{blk}_gnis_{gnis_id}"
                else:
                    grouping_key = f"{feature_type_val}_blue_line_{blk}"
            elif use_wbk:
                grouping_key = f"{feature_type_val}_waterbody_{wbk}"
            else:
                grouping_key = f"{feature_type_val}_feature_{feature_id}"

            group_map[(grouping_key, reg_set)].append((feature_id, feature))

        merged_groups = {}
        grouping_key_counter: Dict[str, int] = defaultdict(int)
        for (grouping_key, reg_set), features_data in group_map.items():
            all_mu: set = set()
            all_wbks: set = set()
            zone_to_name: dict = {}  # zone_id → region_name (maintains pairing)

            for _, feat in features_data:
                feat_zones = self._get_prop(feat, ["zones"], [])
                feat_names = self._get_prop(feat, ["region_names"], [])
                for z, n in zip(feat_zones, feat_names):
                    zone_to_name[z] = n
                # Also pick up any zones without paired names
                for z in feat_zones:
                    if z not in zone_to_name:
                        zone_to_name[z] = ""
                all_mu.update(self._get_prop(feat, ["mgmt_units"], []))
                wbk = self._get_prop(feat, ["waterbody_key"])
                if wbk and str(wbk) in self.linked_waterbody_keys_of_polygon:
                    all_wbks.add(str(wbk))

            sorted_zones = sorted(zone_to_name.keys())

            # Only carry a waterbody_key when the whole group shares exactly one.
            group_wbk = next(iter(all_wbks)) if len(all_wbks) == 1 else None

            unique_key = f"{grouping_key}_{grouping_key_counter[grouping_key]}"
            grouping_key_counter[grouping_key] += 1

            merged_groups[unique_key] = MergedGroup(
                group_id=unique_key,
                feature_ids=tuple(fid for fid, _ in features_data),
                regulation_ids=tuple(sorted(reg_set)),
                waterbody_key=group_wbk,
                feature_count=len(features_data),
                zones=tuple(sorted_zones),
                mgmt_units=tuple(sorted(all_mu)),
                region_names=tuple(zone_to_name[z] for z in sorted_zones),
            )

        logger.info(
            f"merge_features: {len(group_map)} groups from {len(feature_to_regs)} features"
        )
        return merged_groups

    # --- Property & Metadata Helpers ---

    def _get_prop(self, feature: Any, keys: List[str], default: Any = None) -> Any:
        """Safely extract properties from both dataclasses and dictionaries."""
        for key in keys:
            if hasattr(feature, key) and getattr(feature, key) is not None:
                return getattr(feature, key)
            if isinstance(feature, dict) and feature.get(key) is not None:
                return feature.get(key)
        return default

    def _get_feature_type(self, feature: Any) -> FeatureType:
        """Extract and guarantee a FeatureType enum return."""
        ftype = self._get_prop(feature, ["feature_type"])
        if isinstance(ftype, FeatureType):
            return ftype
        if ftype:
            try:
                return FeatureType(ftype)
            except ValueError:
                pass
        return FeatureType.UNKNOWN

    def _get_feature_id(self, feature: Any) -> str:
        """Extract purely the Primary Key (fwa_id)."""
        if fid := self._get_prop(
            feature, ["fwa_id", "linear_feature_id", "waterbody_poly_id"]
        ):
            return str(fid)
        return str(feature)

    def _get_stream_seeds_for_waterbody(self, waterbody_key: str) -> List[str]:
        """Get stream IDs connected to a waterbody via reverse index (O(1) lookup).

        Lazily builds a waterbody_key → [linear_feature_ids] reverse index on first call,
        replacing the previous O(5M) linear scan of all stream metadata per call.
        """
        if not self.gazetteer:
            return []

        # Lazy-build reverse index on first call
        if self._wb_key_to_stream_ids is None:
            self._wb_key_to_stream_ids = defaultdict(list)
            for lin_id, meta in self.gazetteer.metadata.get(
                FeatureType.STREAM, {}
            ).items():
                if wb_key := meta.get("waterbody_key"):
                    self._wb_key_to_stream_ids[str(wb_key)].append(lin_id)
            logger.debug(
                f"Built wb_key reverse index: {len(self._wb_key_to_stream_ids)} unique waterbody keys"
            )

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
                    codes.update(self._get_parent_watershed_codes(ws_code))
        return codes

    def _get_parent_watershed_codes(self, watershed_code: str) -> Set[str]:
        if not watershed_code:
            return set()
        parents, sections = set(), watershed_code.split("-")
        for i in range(len(sections) - 1, 0, -1):
            if sections[i] != "000000":
                parent_code = "-".join(sections[:i] + ["000000"] * (len(sections) - i))
                parents.add(parent_code)
                sections[i] = "000000"
        return parents

    def _with_progress(self, iterable, desc: str, unit: str):
        try:
            from tqdm import tqdm

            return tqdm(iterable, desc=desc, unit=unit)
        except ImportError:
            return iterable

    def get_stats(self) -> RegulationMappingStats:
        return self.stats

    def reset_stats(self):
        self.stats = RegulationMappingStats()
        self.feature_to_regs = {}
        self.merged_groups = {}
        self.regulation_names = {}
        self.admin_feature_map = {}
        self._wb_key_to_stream_ids = None
