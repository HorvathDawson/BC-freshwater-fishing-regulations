"""
RegulationMapper - Orchestrates the full pipeline: Link → Scope → Enrich → Map

Creates inverted index: feature_id → regulation_ids

Processing flow:
1. Link regulation to FWA features (WaterbodyLinker)
2. Apply global spatial scope (ScopeFilter)
3. For each rule:
   - Apply rule-specific scope (ScopeFilter)
   - Enrich with tributaries if needed (TributaryEnricher)
   - Map rule to features
4. Return feature → regulation index
"""

from typing import Dict, List, Optional, Set, Any
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path
import json

from .linker import WaterbodyLinker, LinkStatus
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .metadata_gazetteer import FWAFeature, MetadataGazetteer


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
    total_rule_to_feature_mappings: int = 0  # Individual rule → feature mappings
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
    4. Return feature → [rule_ids] index

    Fallback behavior:
    - Missing landmarks → ScopeFilter returns all input features
    - Scope type not implemented → WHOLE_SYSTEM
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
            tributaries = self.tributary_enricher.enrich_with_tributaries(
                base_features, scope, global_scope
            )
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
            tributaries = self.tributary_enricher.enrich_with_tributaries(
                scoped_features, scope, global_scope
            )
            # Combine base features with tributaries
            return scoped_features + tributaries

        return scoped_features

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
        # Try to find feature in gazetteer by linear_feature_id (direct dict lookup - O(1))
        if feature_id.isdigit():
            # Linear feature ID (stream segment)
            stream = self.gazetteer.metadata.get("streams", {}).get(feature_id)
            if stream:
                return {
                    "gnis_id": stream.get("gnis_id"),
                    "gnis_name": stream.get("gnis_name"),
                    "watershed_code": stream.get("fwa_watershed_code"),
                    "waterbody_key": stream.get("waterbody_key"),
                    "feature_type": "stream",
                }

        # Try composite key (LAKE_123, STREAM_123) - direct dict lookup
        if "_" in feature_id:
            feature_type, key = feature_id.split("_", 1)
            # Search in appropriate collection
            if feature_type.upper() in ["LAKE", "LAKES"]:
                lake = self.gazetteer.metadata.get("lakes", {}).get(key)
                if lake:
                    return {
                        "gnis_id": lake.get("gnis_id"),
                        "gnis_name": lake.get("gnis_name"),
                        "waterbody_key": key,
                        "feature_type": "lake",
                        "watershed_code": None,
                    }
            elif feature_type.upper() == "WETLAND":
                wetland = self.gazetteer.metadata.get("wetlands", {}).get(key)
                if wetland:
                    return {
                        "gnis_id": wetland.get("gnis_id"),
                        "gnis_name": wetland.get("gnis_name"),
                        "waterbody_key": key,
                        "feature_type": "wetland",
                        "watershed_code": None,
                    }
            elif feature_type.upper() == "MANMADE":
                manmade = self.gazetteer.metadata.get("manmade", {}).get(key)
                if manmade:
                    return {
                        "gnis_id": manmade.get("gnis_id"),
                        "gnis_name": manmade.get("gnis_name"),
                        "waterbody_key": key,
                        "feature_type": "manmade",
                        "watershed_code": None,
                    }

        # Fallback: return minimal metadata
        return {
            "gnis_id": None,
            "gnis_name": None,
            "watershed_code": None,
            "waterbody_key": None,
            "feature_type": None,
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
        Main processing loop - creates feature → rule index.

        Args:
            regulations: List of parsed regulation objects (ParsedWaterbody)

        Returns:
            Dict mapping feature_id → list of rule_ids
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

        Reduces ~500k individual features to ~50k groups for optimized UI rendering.

        Grouping strategy:
        - Group by (gnis_id OR watershed_code, regulation_set)
        - For lakes: group by gnis_id (multiple polygons = one lake)
        - For streams: group by watershed_code (multiple segments = one stream)
        - For features with neither: group individually

        Args:
            feature_to_regs: Dict mapping feature_id → list of rule_ids

        Returns:
            Dict mapping group_id → MergedGroup
        """
        from .logger_config import get_logger

        logger = get_logger(__name__)

        logger.info(f"Merging {len(feature_to_regs)} features into groups...")

        # Build groups: (grouping_key, regulation_set) → [(feature_id, metadata), ...]
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
            grouping_key = None
            if feature.get("gnis_id"):
                # Group by GNIS ID (lakes/wetlands/manmade with same GNIS)
                grouping_key = f"gnis_{feature['gnis_id']}"
            elif feature.get("watershed_code"):
                # Group by watershed code (stream segments)
                grouping_key = f"watershed_{feature['watershed_code']}"
            elif feature.get("waterbody_key"):
                # Group by waterbody key (fallback for features without GNIS)
                grouping_key = f"waterbody_{feature['waterbody_key']}"
            else:
                # No grouping possible - use feature_id directly
                grouping_key = f"feature_{feature_id}"

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
            feature_to_regs: Dict mapping feature_id → list of rule_ids
            merged_groups: Dict mapping group_id → MergedGroup
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
        Full pipeline: Link → Scope → Enrich → Map → Merge → Export.

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

        # Step 1: Create feature → regulation index
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
