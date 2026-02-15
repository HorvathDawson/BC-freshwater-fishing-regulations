"""
RegulationMapper - Orchestrates the full pipeline: Link -> Scope -> Enrich -> Map

Creates inverted index: feature_id -> regulation_ids

Processing flow:
1. Link regulation to FWA features (WaterbodyLinker)
2. Apply global spatial scope (ScopeFilter)
3. For each rule:
   - Apply rule-specific scope (ScopeFilter)
   - Enrich with tributaries if needed (TributaryEnricher)
   - Map rule to features
4. Return feature -> regulation index
"""

from typing import Dict, List, Optional, Set, Any
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path
import json

from .linker import WaterbodyLinker, LinkStatus
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .metadata_gazetteer import FWAFeature, MetadataGazetteer, FeatureType
from .logger_config import get_logger

logger = get_logger(__name__)


@dataclass
class RegulationMappingStats:
    """Statistics from regulation mapping process.

    Note: A regulation (ParsedWaterbody) can have multiple rules (RuleGroup).
    Each rule is mapped independently to features.
    """

    total_regulations: int = 0  # Number of ParsedWaterbody objects
    linked_regulations: int = 0  # Regulations successfully linked to features
    failed_to_link_regulations: int = 0  # Regulations that couldn't be linked
    total_rules_processed: int = 0  # Number of RuleGroup objects processed
    total_rule_to_feature_mappings: int = 0  # Individual rule -> feature mappings
    unique_features_with_rules: int = 0  # Unique features that have at least one rule

    link_status_counts: Counter = None  # Breakdown of LinkStatus values

    def __post_init__(self):
        if self.link_status_counts is None:
            self.link_status_counts = Counter()


@dataclass(frozen=True)
class MergedGroup:
    """Merged group of features with identical regulation sets."""

    group_id: str
    feature_ids: tuple[str, ...]  # Tuple for immutability
    regulation_ids: tuple[str, ...]  # Tuple for immutability
    gnis_id: Optional[str] = None
    gnis_name: Optional[str] = None
    feature_type: Optional[str] = None
    watershed_code: Optional[str] = None
    waterbody_key: Optional[str] = None
    feature_count: int = 0
    zones: tuple[str, ...] = ()  # All zones this group passes through
    mgmt_units: tuple[str, ...] = ()  # All management units this group passes through


@dataclass(frozen=True)
class PipelineResult:
    """Result from full regulation processing pipeline."""

    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    stats: Optional[RegulationMappingStats] = None
    exported_files: Optional[Dict[str, Path]] = None


class RegulationMapper:
    """
    Orchestrates the full regulation mapping pipeline.

    Terminology:
    - Regulation (ParsedWaterbody): A waterbody entry with identity and multiple rules
    - Rule (RuleGroup): An atomic regulation unit (one restriction + scope)
    - One regulation can have multiple rules that apply to different feature sets

    Workflow:
    1. Link regulation identity to FWA features
    2. Apply global spatial scope (fallback: WHOLE_SYSTEM if no landmark data)
    3. For each rule in the regulation:
       - Apply rule-specific spatial scope (fallback: use global scope if no landmark data)
       - Enrich with tributaries if includes_tributaries is true
       - Map this rule to the resulting features
    4. Return feature -> [rule_ids] index

    Fallback behavior:
    - Missing landmarks -> ScopeFilter returns all input features
    - Scope type not implemented -> WHOLE_SYSTEM
    - includes_tributaries always applied (just determines enrichment)

    "MVP" reality: MVP isn't a separate mode - it's just the system running with
    incomplete landmark/polygon data. Many scopes will fall back to WHOLE_SYSTEM.
    """

    def __init__(
        self,
        linker: WaterbodyLinker,
        scope_filter: ScopeFilter,
        tributary_enricher: TributaryEnricher,
    ):
        self.linker = linker
        self.scope_filter = scope_filter
        self.tributary_enricher = tributary_enricher
        self.gazetteer = linker.gazetteer  # Get gazetteer from linker
        self.stats = RegulationMappingStats()
        # Store gazetteer reference for feature metadata lookup
        self.gazetteer = linker.gazetteer

    def apply_scope_and_enrich(
        self,
        base_features: List,
        scope: Dict,
        global_scope: Dict,
        includes_tributaries: Optional[bool] = None,
    ) -> List:
        """
        Helper: Apply spatial filter + tributary enrichment.

        Args:
            base_features: Initial feature set (from linking)
            scope: Spatial scope constraint
            global_scope: Global scope for fallback
            includes_tributaries: Whether to enrich with tributaries

        Returns:
            Filtered and enriched feature list
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        scope_type = scope.get("type", "WHOLE_SYSTEM")

        # TRIBUTARIES_ONLY: Return ONLY tributaries (exclude base features)
        if scope_type == "TRIBUTARIES_ONLY":
            # Validation: TRIBUTARIES_ONLY should have includes_tributaries=true
            if not scope.get("includes_tributaries"):
                logger.warning(
                    f"TRIBUTARIES_ONLY scope should have includes_tributaries=true. "
                    f"Proceeding anyway."
                )

            # Get ONLY tributaries of base features (base excluded)
            tributaries = self._enrich_with_tributaries(base_features)
            return tributaries

        # Normal flow: Apply spatial scope (with fallback to WHOLE_SYSTEM)
        scoped_features = self.scope_filter.apply_scope(base_features, scope)

        # If scope filter returned nothing (error case), fall back to base features
        if not scoped_features:
            scoped_features = base_features

        # Determine if we should enrich with tributaries
        # Tri-state logic: True (explicit yes), False (explicit no), None (inherit from global)
        should_enrich = includes_tributaries
        if should_enrich is None:
            # Inherit from global scope
            should_enrich = global_scope.get("includes_tributaries", False)

        # Enrich with tributaries if requested
        if should_enrich:
            # Get tributaries (returns ONLY tributaries, excludes scoped_features)
            tributaries = self._enrich_with_tributaries(scoped_features)
            # Combine base features with tributaries
            return scoped_features + tributaries

        return scoped_features

    def _enrich_with_tributaries(self, features: List) -> List:
        """
        Enrich features with upstream tributaries.

        Orchestrates tributary enrichment based on feature type:
        - Streams: Use linear_feature_id as seeds, exclude mainstem
        - Polygons: Convert waterbody_key to stream seeds, include ALL upstream

        Args:
            features: List of FWAFeature objects to enrich

        Returns:
            List of tributary FWAFeature objects (excludes input features)
        """
        if not self.tributary_enricher or not self.tributary_enricher.graph:
            return []

        if not features:
            return []

        # Separate features by type and collect seeds
        stream_seeds = []
        polygon_seeds_by_type = {}  # type -> list of seeds
        skipped_features = []

        for feature in features:
            feature_type = self._get_feature_type(feature)

            if feature_type == "stream" or feature_type is None:
                # Stream: use linear_feature_id directly
                linear_id = self._get_feature_linear_id(feature)
                if linear_id:
                    stream_seeds.append(linear_id)
                else:
                    # No linear_id found for this feature
                    skipped_features.append(self._get_feature_name(feature))
                    if feature_type is None:
                        # Warn about unknown type
                        logger.warning(
                            f"Feature {self._get_feature_name(feature)} has feature_type=None. "
                            f"Cannot determine if stream or polygon. Skipping."
                        )
            elif feature_type in ["lake", "wetland", "manmade", "unmarked"]:
                # Polygon: convert waterbody_key to stream seeds using metadata
                wb_key = self._get_feature_waterbody_key(feature)
                if wb_key:
                    # Get connected stream segments from metadata
                    connected_streams = self._get_stream_seeds_for_waterbody(wb_key)
                    if connected_streams:
                        if feature_type not in polygon_seeds_by_type:
                            polygon_seeds_by_type[feature_type] = []
                        polygon_seeds_by_type[feature_type].extend(connected_streams)
                    else:
                        logger.debug(
                            f"{feature_type.capitalize()} {self._get_feature_name(feature)} "
                            f"has no connected streams (waterbody_key={wb_key})"
                        )
            else:
                logger.warning(
                    f"Feature {self._get_feature_name(feature)} has unknown feature_type='{feature_type}'. "
                    f"Skipping tributary enrichment."
                )

        # Log if features were skipped
        if skipped_features:
            logger.warning(
                f"Skipped {len(skipped_features)} features without linear_id: "
                f"{skipped_features[:5]}{'...' if len(skipped_features) > 5 else ''}"
            )

        # Use set to avoid duplicates when merging results
        all_tributaries_set = set()

        # Process streams: exclude mainstem (same watershed code)
        if stream_seeds:
            # Get watershed codes for these streams to exclude them (mainstem)
            excluded_codes = self._get_watershed_codes_for_streams(stream_seeds)
            tributaries = self.tributary_enricher.enrich_with_tributaries(
                stream_seeds,
                excluded_watershed_codes=excluded_codes,
                parent_features=features,
            )
            # Add to set using fwa_id as key
            for trib in tributaries:
                all_tributaries_set.add((self._get_feature_id(trib), trib))
            logger.debug(
                f"Stream enrichment: {len(stream_seeds)} seeds -> {len(tributaries)} tributaries "
                f"(excluded {len(excluded_codes)} watershed codes)"
            )

        # Process polygons by type: include ALL upstream (no mainstem exclusion)
        for ftype, seeds in polygon_seeds_by_type.items():
            # No watershed code exclusions for polygons - pass empty set
            tributaries = self.tributary_enricher.enrich_with_tributaries(
                seeds, excluded_watershed_codes=set(), parent_features=features
            )
            # Add to set using fwa_id as key
            for trib in tributaries:
                all_tributaries_set.add((self._get_feature_id(trib), trib))
            logger.debug(
                f"{ftype.capitalize()} enrichment: {len(seeds)} seeds -> {len(tributaries)} tributaries"
            )

        # Convert back to list (removing duplicate keys)
        all_tributaries = [trib for _, trib in all_tributaries_set]

        return all_tributaries

    def _get_feature_type(self, feature) -> str:
        """Get feature_type from feature object or dict."""
        if hasattr(feature, "feature_type"):
            return getattr(feature, "feature_type", None)
        elif isinstance(feature, dict):
            return feature.get("feature_type")
        return None

    def _get_feature_linear_id(self, feature) -> str:
        """Get linear_feature_id or fwa_id from feature."""
        if hasattr(feature, "linear_feature_id"):
            return getattr(feature, "linear_feature_id", None)
        elif hasattr(feature, "fwa_id"):
            return getattr(feature, "fwa_id", None)
        elif isinstance(feature, dict):
            return feature.get("linear_feature_id") or feature.get("fwa_id")
        return None

    def _get_feature_waterbody_key(self, feature) -> str:
        """Get waterbody_key from feature."""
        if hasattr(feature, "waterbody_key"):
            return getattr(feature, "waterbody_key", None)
        elif isinstance(feature, dict):
            return feature.get("waterbody_key")
        return None

    def _get_feature_name(self, feature) -> str:
        """Get name from feature for logging."""
        if hasattr(feature, "gnis_name"):
            return getattr(feature, "gnis_name", "unnamed")
        elif hasattr(feature, "name"):
            return getattr(feature, "name", "unnamed")
        elif isinstance(feature, dict):
            return feature.get("gnis_name") or feature.get("name") or "unnamed"
        return "unnamed"

    def _get_stream_seeds_for_waterbody(self, waterbody_key: str) -> List[str]:
        """
        Get linear_feature_ids of streams connected to a waterbody.

        Uses metadata to find all stream segments with matching waterbody_key.

        Args:
            waterbody_key: The waterbody_key of the lake/polygon

        Returns:
            List of linear_feature_ids (stream segments connected to waterbody)
        """
        if not self.gazetteer:
            return []

        # Get all stream metadata entries with this waterbody_key
        connected_streams = []

        # Iterate through metadata to find streams with matching waterbody_key
        # Note: This could be optimized with an index if performance is an issue
        for linear_id, metadata in self.gazetteer.stream_metadata.items():
            if metadata.get("waterbody_key") == str(waterbody_key):
                connected_streams.append(linear_id)

        return connected_streams

    def _get_watershed_codes_for_streams(
        self, linear_feature_ids: List[str]
    ) -> Set[str]:
        """
        Get watershed codes for stream linear_feature_ids using metadata.

        Also includes all parent watershed codes to prevent traversing into
        what appears upstream but is actually downstream in braided areas.

        Example: For watershed code "100-077501-094860-000000-...",
        also exclude parents:
        - "100-077501-000000-000000-..."
        - "100-000000-000000-000000-..."

        Args:
            linear_feature_ids: List of stream linear_feature_ids

        Returns:
            Set of watershed codes including parents (excludes None/empty)
        """
        if not self.gazetteer:
            return set()

        watershed_codes = set()
        for linear_id in linear_feature_ids:
            metadata = self.gazetteer.get_stream_metadata(str(linear_id))
            if metadata:
                watershed_code = metadata.get("fwa_watershed_code")
                if watershed_code:
                    watershed_codes.add(watershed_code)
                    # Add all parent watershed codes
                    watershed_codes.update(
                        self._get_parent_watershed_codes(watershed_code)
                    )

        return watershed_codes

    def _get_parent_watershed_codes(self, watershed_code: str) -> Set[str]:
        """
        Generate all parent watershed codes by progressively zeroing sections.

        Example: "100-077501-094860-000000-..." produces:
        - "100-077501-000000-000000-..."
        - "100-000000-000000-000000-..."

        Args:
            watershed_code: FWA watershed code (dash-separated)

        Returns:
            Set of parent watershed codes (excluding the original)
        """
        if not watershed_code:
            return set()

        parents = set()
        sections = watershed_code.split("-")

        # Start from the right, find non-zero sections and zero them out
        for i in range(len(sections) - 1, 0, -1):  # Don't zero out the first section
            if sections[i] != "000000":
                # Create parent by zeroing this section
                parent_sections = sections[:i] + ["000000"] * (len(sections) - i)
                parent_code = "-".join(parent_sections)
                parents.add(parent_code)
                # Update sections for next iteration
                sections[i] = "000000"

        return parents

    def process_regulation(
        self,
        regulation: Dict,
        regulation_id: str,
        feature_to_regs: Dict[str, List[str]],
    ) -> bool:
        """
        Process a single regulation and update feature_to_regs mapping.

        Args:
            regulation: Parsed regulation object (ParsedWaterbody)
            regulation_id: Unique identifier for this regulation
            feature_to_regs: Dict to update with mappings

        Returns:
            True if successfully processed, False if failed to link
        """
        identity = regulation["identity"]
        rules = regulation.get("rules", [])

        # Extract linking parameters from identity
        waterbody_key = identity.get("waterbody_key", "")
        name_verbatim = identity.get("name_verbatim", "")

        # Extract region and management units from regulation
        region = regulation.get("region")  # e.g., "REGION 4 - Kootenay"
        mgmt_units = regulation.get("mu", [])  # e.g., ["4-15", "4-16"]

        # Normalize region to "Region X" format if needed
        if region and region.startswith("REGION"):
            # "REGION 4 - Kootenay" -> "Region 4"
            region_num = "".join(c for c in region.split("-")[0] if c.isdigit())
            region = f"Region {region_num}" if region_num else None

        # STEP 1: Link regulation to FWA features
        link_result = self.linker.link_waterbody(
            waterbody_key=waterbody_key,
            region=region,
            mgmt_units=mgmt_units,
            name_verbatim=name_verbatim,
        )

        # Track linking status
        self.stats.link_status_counts[link_result.status.value] += 1

        # Handle linking failures
        if link_result.status != LinkStatus.SUCCESS:
            self.stats.failed_to_link_regulations += 1
            return False

        self.stats.linked_regulations += 1

        # Get matched features (base feature set for this regulation)
        base_features = link_result.matched_features
        if not base_features and link_result.matched_feature:
            base_features = [link_result.matched_feature]

        if not base_features:
            self.stats.failed_to_link_regulations += 1
            return False

        # STEP 2: Apply global spatial scope (applies to all rules in this regulation)
        global_scope = identity.get("global_scope", {})
        globally_scoped_features = self.scope_filter.apply_scope(
            base_features, global_scope
        )

        # Fallback if scope filtering failed
        if not globally_scoped_features:
            globally_scoped_features = base_features

        # STEP 3: Process each rule in this regulation
        for rule_idx, rule in enumerate(rules):
            self.stats.total_rules_processed += 1

            rule_scope = rule.get("scope", {})
            includes_tributaries = rule_scope.get("includes_tributaries")

            # Apply rule-specific scope and enrichment
            final_features = self.apply_scope_and_enrich(
                globally_scoped_features,
                rule_scope,
                global_scope,
                includes_tributaries,
            )

            # Create rule ID: regulation_id + rule index
            rule_id = f"{regulation_id}_rule{rule_idx}"

            # Map this rule to all final features
            for feature in final_features:
                feature_id = self._get_feature_id(feature)
                if feature_id not in feature_to_regs:
                    feature_to_regs[feature_id] = []
                feature_to_regs[feature_id].append(rule_id)
                self.stats.total_rule_to_feature_mappings += 1

        return True

    def _sort_feature_regulation_lists(self, feature_to_regs: Dict[str, List[str]]):
        """Sort regulation lists for each feature for consistent ordering."""
        for feature_id in feature_to_regs:
            feature_to_regs[feature_id] = sorted(feature_to_regs[feature_id])

    def _get_feature_metadata(self, feature_id: str) -> Dict[str, Any]:
        """
        Get feature metadata from gazetteer for grouping.

        Args:
            feature_id: Feature identifier (linear_feature_id or composite key)

        Returns:
            Dict with feature metadata (gnis_id, watershed_code, etc.)
        """
        # Determine feature type using unified helper
        feature_type_enum = self.gazetteer.get_feature_type_from_id(feature_id)
        feature_type = feature_type_enum.value

        # Handle streams (numeric IDs or STREAM_ prefix)
        if feature_type_enum == FeatureType.STREAM:
            # Extract numeric ID if composite format
            stream_id = feature_id.split("_", 1)[1] if "_" in feature_id else feature_id
            stream = self.gazetteer.metadata.get("streams", {}).get(stream_id)
            if stream:
                return {
                    "gnis_id": stream.get("gnis_id"),
                    "gnis_name": stream.get("gnis_name"),
                    "watershed_code": stream.get("fwa_watershed_code"),
                    "waterbody_key": stream.get("waterbody_key"),
                    "blue_line_key": stream.get("blue_line_key"),
                    "feature_type": feature_type,
                    "zones": stream.get("zones", []),
                    "mgmt_units": stream.get("mgmt_units", []),
                }

        # Handle polygons (lakes, wetlands, manmade)
        elif feature_type_enum in [
            FeatureType.LAKE,
            FeatureType.WETLAND,
            FeatureType.MANMADE,
        ]:
            # Extract key from composite format or use as-is
            key = feature_id.split("_", 1)[1] if "_" in feature_id else feature_id

            # Get collection name (plural form)
            collection_map = {
                FeatureType.LAKE: "lakes",
                FeatureType.WETLAND: "wetlands",
                FeatureType.MANMADE: "manmade",
            }
            collection_name = collection_map[feature_type_enum]

            polygon = self.gazetteer.metadata.get(collection_name, {}).get(key)
            if polygon:
                return {
                    "gnis_id": polygon.get("gnis_id"),
                    "gnis_name": polygon.get("gnis_name"),
                    "waterbody_key": key,
                    "feature_type": feature_type,
                    "watershed_code": None,
                    "blue_line_key": polygon.get("blue_line_key"),
                    "zones": polygon.get("zones", []),
                    "mgmt_units": polygon.get("mgmt_units", []),
                }

        # Handle unmarked waterbodies
        elif feature_type_enum == FeatureType.UNMARKED:
            return {
                "gnis_id": None,
                "gnis_name": None,
                "waterbody_key": None,
                "feature_type": feature_type,
                "watershed_code": None,
                "blue_line_key": None,
                "zones": [],
                "mgmt_units": [],
            }

        # Fallback: return minimal metadata and LOG the failure
        logger.warning(
            f"Metadata lookup failed for feature_id '{feature_id}' - "
            f"feature type: {feature_type}. "
            f"This feature has regulations but missing metadata."
        )
        return {
            "gnis_id": None,
            "gnis_name": None,
            "watershed_code": None,
            "waterbody_key": None,
            "blue_line_key": None,
            "feature_type": feature_type,
            "zones": [],
            "mgmt_units": [],
        }

    def _get_feature_id(self, feature) -> str:
        """
        Extract unique identifier from FWA feature.

        Handles both FWAFeature objects and tributary feature dicts.
        """
        # Try dict access first (tributary features from graph)
        if isinstance(feature, dict):
            # Tributary features have linear_feature_id
            if "linear_feature_id" in feature:
                return str(feature["linear_feature_id"])
            # Fallback to waterbody_poly_id
            feature_type = feature.get("feature_type", "UNKNOWN")
            poly_id = feature.get("waterbody_poly_id", "UNKNOWN")
            return f"{feature_type}_{poly_id}"

        # FWAFeature objects
        # Try fwa_id first (primary key)
        if hasattr(feature, "fwa_id") and feature.fwa_id:
            return str(feature.fwa_id)

        # Try waterbody_poly_id
        if hasattr(feature, "waterbody_poly_id"):
            feature_type = getattr(feature, "feature_type", "UNKNOWN")
            return f"{feature_type}_{feature.waterbody_poly_id}"

        # Last resort: str representation
        return str(feature)

    def process_all_regulations(self, regulations: List[Dict]) -> Dict[str, List[str]]:
        """
        Main processing loop - creates feature to rule index.

        Args:
            regulations: List of parsed regulation objects (ParsedWaterbody)

        Returns:
            Dict mapping feature_id to list of rule_ids
            (Note: rule_ids are formatted as "reg_XXXX_ruleY")
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        feature_to_regs = {}

        self.stats.total_regulations = len(regulations)

        logger.info(f"Processing {len(regulations)} regulations...")

        for idx, regulation in enumerate(regulations):
            # Generate regulation ID (could use actual ID from data)
            regulation_id = f"reg_{idx:04d}"

            # Process this regulation (which may have multiple rules)
            self.process_regulation(regulation, regulation_id, feature_to_regs)

        # Sort regulation lists for consistent ordering
        self._sort_feature_regulation_lists(feature_to_regs)

        # Update final stats
        self.stats.unique_features_with_rules = len(feature_to_regs)

        return feature_to_regs

    def get_stats(self) -> RegulationMappingStats:
        """Return processing statistics."""
        return self.stats

    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = RegulationMappingStats()

    def merge_features(
        self, feature_to_regs: Dict[str, List[str]]
    ) -> Dict[str, MergedGroup]:
        """
        Merge features with identical regulation sets into groups.

        Reduces approximately 500,000 individual features to 50,000 groups for optimized UI rendering.

        Grouping strategy:
        - Group by (feature_type, blue_line_key OR waterbody_key, regulation_set)
        - Feature type ensures lakes and streams are never mixed in the same group
        - For lakes: group by feature_type + waterbody_key (multiple polygons = one lake)
        - For streams: group by feature_type + blue_line_key (multiple segments = one stream)
        - For features with neither: group individually by feature_id

        Args:
            feature_to_regs: Dict mapping feature_id -> list of rule_ids

        Returns:
            Dict mapping group_id -> MergedGroup
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        logger.info(f"Merging {len(feature_to_regs)} features into groups...")

        # Build groups: (grouping_key, regulation_set) -> [(feature_id, metadata), ...]
        group_map = defaultdict(list)

        # Use tqdm for progress if available
        try:
            from tqdm import tqdm

            iterator = tqdm(
                feature_to_regs.items(), desc="Grouping features", unit="feature"
            )
        except ImportError:
            iterator = feature_to_regs.items()

        for feature_id, reg_ids in iterator:
            # Create frozen set for regulation set (hashable, order-independent)
            # frozenset(['a','b','c']) == frozenset(['c','a','b']) is True
            reg_set = frozenset(reg_ids)

            # Get feature metadata from gazetteer
            feature = self._get_feature_metadata(feature_id)

            # Determine grouping key based on feature type
            # Include feature_type to prevent mixing lakes and streams in the same group
            feature_type = feature.get("feature_type", "unknown")
            grouping_key = None
            if feature.get("blue_line_key"):
                # Group by Blue Line Key (all features - streams, lakes, wetlands with same blue line)
                grouping_key = f"{feature_type}_blue_line_{feature['blue_line_key']}"
            elif feature.get("waterbody_key"):
                # Group by waterbody key (fallback for polygons without blue_line_key)
                grouping_key = f"{feature_type}_waterbody_{feature['waterbody_key']}"
            else:
                # No grouping possible - use feature_id directly
                grouping_key = f"{feature_type}_feature_{feature_id}"

            # Combine grouping_key + regulation_set
            full_key = (grouping_key, reg_set)
            group_map[full_key].append((feature_id, feature))

        # Convert to output format
        merged_groups = {}
        for idx, ((grouping_key, reg_set), features_data) in enumerate(
            group_map.items()
        ):
            # Use first feature's metadata for group (all should be same waterbody)
            _, first_feature = features_data[0]

            # Aggregate all zones and mgmt_units from all features in this group
            all_zones = set()
            all_mgmt_units = set()
            for _, feature in features_data:
                all_zones.update(feature.get("zones", []))
                all_mgmt_units.update(feature.get("mgmt_units", []))

            group_id = f"group_{idx:06d}"
            merged_groups[group_id] = MergedGroup(
                group_id=group_id,
                feature_ids=tuple(fid for fid, _ in features_data),
                regulation_ids=tuple(sorted(reg_set)),
                gnis_id=first_feature.get("gnis_id"),
                gnis_name=first_feature.get("gnis_name"),
                feature_type=first_feature.get("feature_type"),
                watershed_code=first_feature.get("watershed_code"),
                waterbody_key=first_feature.get("waterbody_key"),
                feature_count=len(features_data),
                zones=tuple(sorted(all_zones)),
                mgmt_units=tuple(sorted(all_mgmt_units)),
            )

        logger.info(
            f"Merged {len(feature_to_regs)} features into {len(merged_groups)} groups"
        )

        return merged_groups

    def build_index(
        self,
        feature_to_regs: Dict[str, List[str]],
        merged_groups: Dict[str, MergedGroup],
        output_dir: Path,
    ) -> Dict[str, Path]:
        """
        Write query-ready indices to disk.

        Creates two JSON files:
        1. feature_to_regs.json - Individual feature lookup (O(1) queries)
        2. merged_features.json - Grouped geometries (optimized for UI rendering)

        Args:
            feature_to_regs: Dict mapping feature_id -> list of rule_ids
            merged_groups: Dict mapping group_id -> MergedGroup
            output_dir: Directory to write JSON files

        Returns:
            Dict with paths to created files
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write feature_to_regs.json
        logger.info(
            f"Writing feature_to_regs.json ({len(feature_to_regs)} features)..."
        )
        feature_index_path = output_dir / "feature_to_regs.json"
        with open(feature_index_path, "w") as f:
            json.dump(feature_to_regs, f, indent=2)

        logger.info(
            f"Wrote feature index: {feature_index_path} ({len(feature_to_regs)} features)"
        )

        # Write merged_features.json - convert MergedGroup to dict
        logger.info(f"Converting {len(merged_groups)} merged groups to JSON...")

        # Use tqdm for progress if available
        try:
            from tqdm import tqdm

            iterator = tqdm(
                merged_groups.items(), desc="Converting groups", unit="group"
            )
        except ImportError:
            iterator = merged_groups.items()

        merged_dict = {
            gid: {
                "group_id": group.group_id,
                "feature_ids": list(group.feature_ids),
                "regulation_ids": list(group.regulation_ids),
                "gnis_id": group.gnis_id,
                "gnis_name": group.gnis_name,
                "feature_type": group.feature_type,
                "watershed_code": group.watershed_code,
                "waterbody_key": group.waterbody_key,
                "feature_count": group.feature_count,
            }
            for gid, group in iterator
        }

        logger.info(f"Writing merged_features.json ({len(merged_groups)} groups)...")
        merged_index_path = output_dir / "merged_features.json"
        with open(merged_index_path, "w") as f:
            json.dump(merged_dict, f, indent=2)

        logger.info(
            f"Wrote merged groups: {merged_index_path} ({len(merged_groups)} groups)"
        )

        return {
            "feature_to_regs": feature_index_path,
            "merged_features": merged_index_path,
        }

    def process_and_export(
        self, regulations: List[Dict], output_dir: Optional[Path] = None
    ) -> PipelineResult:
        """
        Full pipeline: Link -> Scope -> Enrich -> Map -> Merge -> Export.

        This is the main entry point for complete regulation processing.

        Args:
            regulations: List of parsed regulation objects
            output_dir: Optional directory to write JSON indices (if None, skip export)

        Returns:
            PipelineResult with all processing results
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        logger.info(f"Processing {len(regulations)} regulations...")

        # Step 1: Create feature -> regulation index
        feature_to_regs = self.process_all_regulations(regulations)

        # Step 2: Merge features into groups
        merged_groups = self.merge_features(feature_to_regs)

        # Step 3: Export to JSON (optional)
        exported_files = None
        if output_dir:
            exported_files = self.build_index(
                feature_to_regs, merged_groups, output_dir
            )

        logger.info("Processing complete")

        return PipelineResult(
            feature_to_regs=feature_to_regs,
            merged_groups=merged_groups,
            stats=self.stats,
            exported_files=exported_files,
        )
