"""
RegulationMapper - Orchestrates the full pipeline: Link -> Scope -> Enrich -> Map

Creates inverted index: feature_id -> regulation_ids
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
    """Statistics from regulation mapping process."""

    total_regulations: int = 0
    linked_regulations: int = 0
    failed_to_link_regulations: int = 0
    total_rules_processed: int = 0
    total_rule_to_feature_mappings: int = 0
    unique_features_with_rules: int = 0
    link_status_counts: Counter = field(default_factory=Counter)


@dataclass(frozen=True)
class MergedGroup:
    """Merged group of features with identical regulation sets."""

    group_id: str
    feature_ids: tuple[str, ...]
    regulation_ids: tuple[str, ...]
    waterbody_key: Optional[str] = None
    feature_count: int = 0
    zones: tuple[str, ...] = ()
    mgmt_units: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineResult:
    """Result from full regulation processing pipeline."""

    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    stats: Optional[RegulationMappingStats] = None


class RegulationMapper:
    """Orchestrates the full regulation mapping pipeline."""

    def __init__(
        self,
        linker: WaterbodyLinker,
        scope_filter: ScopeFilter,
        tributary_enricher: TributaryEnricher,
    ):
        self.linker = linker
        self.scope_filter = scope_filter
        self.tributary_enricher = tributary_enricher
        self.gazetteer = linker.gazetteer
        self.stats = RegulationMappingStats()

        self.feature_to_regs = {}
        self.merged_groups = {}
        self.regulation_names = {}
        self.feature_to_linked_regulation = defaultdict(set)
        self.linked_waterbody_keys_of_polygon = set()

    # --- Core Pipeline ---

    # regulation_id
    def regulation_id(self, regulation_idx: int) -> str:
        return f"reg_{regulation_idx:05d}"

    # rule id
    def rule_id(self, regulation_idx: int, rule_idx: int) -> str:
        return f"{self.regulation_id(regulation_idx)}_rule{rule_idx}"

    def process_all_regulations(self, regulations: List[Dict]) -> Dict[str, List[str]]:
        """Main processing loop - creates feature to rule index."""
        self.feature_to_regs = {}
        self.stats.total_regulations = len(regulations)

        logger.info(f"Processing {len(regulations)} regulations...")

        for idx, regulation in enumerate(
            self._with_progress(regulations, "Processing regulations", "reg")
        ):
            regulation_id = self.regulation_id(idx)
            identity = regulation.get("identity", {})
            name_verbatim = identity.get("name_verbatim", "")

            if name_verbatim:
                self.regulation_names[regulation_id] = name_verbatim

            # 1. Normalize Region and Link
            region = regulation.get("region")
            if region and region.startswith("REGION"):
                region_num = "".join(c for c in region.split("-")[0] if c.isdigit())
                region = f"Region {region_num}" if region_num else None

            link_result = self.linker.link_waterbody(
                region=region,
                mgmt_units=regulation.get("mu", []),
                name_verbatim=name_verbatim,
            )

            self.stats.link_status_counts[link_result.status.value] += 1

            if (
                link_result.status != LinkStatus.SUCCESS
                or not link_result.matched_features
            ):
                self.stats.failed_to_link_regulations += 1
                continue

            self.stats.linked_regulations += 1
            base_features = link_result.matched_features

            # 2. Track Keys and Polygons for Grouping
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

            # 3. Apply Global Scope
            global_scope = identity.get("global_scope", {})
            globally_scoped_features = self.apply_scope_and_enrich(
                base_features,
                scope=global_scope,
                global_scope=global_scope,
                includes_tributaries=global_scope.get("includes_tributaries"),
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
                    rule_scope.get("includes_tributaries"),
                )

                rule_id = self.rule_id(idx, rule_idx)
                for feature in final_features:
                    self.feature_to_regs.setdefault(
                        self._get_feature_id(feature), []
                    ).append(rule_id)
                    self.stats.total_rule_to_feature_mappings += 1

        # Sort indices
        for feature_id in self.feature_to_regs:
            self.feature_to_regs[feature_id].sort()

        self.stats.unique_features_with_rules = len(self.feature_to_regs)
        logger.info(
            f"Found {len(self.linked_waterbody_keys_of_polygon)} linked polygon waterbodies."
        )
        return self.feature_to_regs

    def apply_scope_and_enrich(
        self,
        base_features: List,
        scope: Dict,
        global_scope: Dict,
        includes_tributaries: Optional[bool] = None,
    ) -> List:
        """Apply spatial filter + tributary enrichment."""
        scope_type = scope.get("type", "WHOLE_SYSTEM")

        if scope_type == "TRIBUTARIES_ONLY":
            if not scope.get("includes_tributaries"):
                logger.warning(
                    "TRIBUTARIES_ONLY scope should have includes_tributaries=true."
                )
            return self._enrich_with_tributaries(base_features)

        scoped_features = (
            self.scope_filter.apply_scope(base_features, scope) or base_features
        )

        should_enrich = (
            includes_tributaries
            if includes_tributaries is not None
            else global_scope.get("includes_tributaries", False)
        )

        if should_enrich:
            tributaries = self._enrich_with_tributaries(scoped_features)
            return scoped_features + tributaries

        return scoped_features

    def process_and_export(
        self, regulations: List[Dict], output_dir: Optional[Path] = None
    ) -> PipelineResult:
        """Full pipeline: Link -> Scope -> Enrich -> Map -> Merge -> Export."""
        self.process_all_regulations(regulations)
        self.merged_groups = self.merge_features(self.feature_to_regs)

        logger.info("Processing complete")
        return PipelineResult(
            feature_to_regs=self.feature_to_regs,
            merged_groups=self.merged_groups,
            regulation_names=self.regulation_names,
            stats=self.stats,
        )

    # --- Enrichment & Grouping ---

    def _enrich_with_tributaries(self, features: List) -> List:
        """Enrich features with upstream tributaries."""
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
                stream_seeds, excluded_codes, features
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib

        for ftype, seeds in polygon_seeds_by_type.items():
            for trib in self.tributary_enricher.enrich_with_tributaries(
                seeds, set(), features
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib

        return list(all_tributaries_dict.values())

    def merge_features(
        self, feature_to_regs: Dict[str, List[str]]
    ) -> Dict[str, MergedGroup]:
        """Merge features with identical regulation sets into groups."""
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

            # Lookups are clean now that FWAFeature stores these natively
            blk = self._get_prop(feature, ["blue_line_key"])
            wbk = self._get_prop(feature, ["waterbody_key"])
            use_wbk = wbk and str(wbk) in self.linked_waterbody_keys_of_polygon

            if blk and use_wbk:
                grouping_key = f"{feature_type_val}_blue_line_{blk}_waterbody_{wbk}"
            elif blk:
                grouping_key = f"{feature_type_val}_blue_line_{blk}"
            elif use_wbk:
                grouping_key = f"{feature_type_val}_waterbody_{wbk}"
            else:
                grouping_key = f"{feature_type_val}_feature_{feature_id}"

            group_map[(grouping_key, reg_set)].append((feature_id, feature))

        merged_groups = {}
        for idx, ((_, reg_set), features_data) in enumerate(group_map.items()):
            _, first_feature = features_data[0]
            all_zones, all_mu = set(), set()

            for _, feat in features_data:
                all_zones.update(self._get_prop(feat, ["zones"], []))
                all_mu.update(self._get_prop(feat, ["mgmt_units"], []))

            group_id = f"group_{idx:06d}"
            wbk = self._get_prop(first_feature, ["waterbody_key"])
            merged_groups[group_id] = MergedGroup(
                group_id=group_id,
                feature_ids=tuple(fid for fid, _ in features_data),
                regulation_ids=tuple(sorted(reg_set)),
                waterbody_key=(
                    wbk
                    if wbk and str(wbk) in self.linked_waterbody_keys_of_polygon
                    else None
                ),
                feature_count=len(features_data),
                zones=tuple(sorted(all_zones)),
                mgmt_units=tuple(sorted(all_mu)),
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
        if not self.gazetteer:
            return []
        return [
            lin_id
            for lin_id, meta in self.gazetteer.metadata.get(
                FeatureType.STREAM, {}
            ).items()
            if meta.get("waterbody_key") == str(waterbody_key)
        ]

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
