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
    gnis_id: Optional[str] = None
    gnis_name: Optional[str] = None
    feature_type: Optional[str] = None
    watershed_code: Optional[str] = None
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
    exported_files: Optional[Dict[str, Path]] = None


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
        self.linked_waterbody_keys = set()

    # --- Core Pipeline Methods ---

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
                    "TRIBUTARIES_ONLY scope should have includes_tributaries=true. Proceeding anyway."
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

    def process_regulation(
        self,
        regulation: Dict,
        regulation_id: str,
        feature_to_regs: Dict[str, List[str]],
    ) -> bool:
        """Process a single regulation and update feature_to_regs mapping."""
        identity = regulation["identity"]
        rules = regulation.get("rules", [])

        name_verbatim = identity.get("name_verbatim", "")
        if name_verbatim:
            self.regulation_names[regulation_id] = name_verbatim

        # Normalize region string
        region = regulation.get("region")
        if region and region.startswith("REGION"):
            region_num = "".join(c for c in region.split("-")[0] if c.isdigit())
            region = f"Region {region_num}" if region_num else None

        # Link regulation
        link_result = self.linker.link_waterbody(
            waterbody_key=identity.get("waterbody_key", ""),
            region=region,
            mgmt_units=regulation.get("mu", []),
            name_verbatim=name_verbatim,
        )

        self.stats.link_status_counts[link_result.status.value] += 1

        if link_result.status != LinkStatus.SUCCESS:
            self.stats.failed_to_link_regulations += 1
            return False

        self.stats.linked_regulations += 1

        base_features = link_result.matched_features or (
            [link_result.matched_feature] if link_result.matched_feature else []
        )
        if not base_features:
            self.stats.failed_to_link_regulations += 1
            return False

        # Track keys for later grouping
        for feature in base_features:
            self.feature_to_linked_regulation[self._get_feature_id(feature)].add(
                regulation_id
            )
            if self._get_feature_type(feature) in ("lake", "wetland", "manmade"):
                if wb_key := self._get_prop(feature, ["waterbody_key"]):
                    self.linked_waterbody_keys.add(str(wb_key))

        # Apply global scope
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

        # Process rules
        for rule_idx, rule in enumerate(rules):
            self.stats.total_rules_processed += 1
            rule_scope = rule.get("scope", {})

            final_features = self.apply_scope_and_enrich(
                globally_scoped_features,
                rule_scope,
                global_scope,
                rule_scope.get("includes_tributaries"),
            )

            rule_id = f"{regulation_id}_rule{rule_idx}"
            for feature in final_features:
                feature_to_regs.setdefault(self._get_feature_id(feature), []).append(
                    rule_id
                )
                self.stats.total_rule_to_feature_mappings += 1

        return True

    def process_all_regulations(self, regulations: List[Dict]) -> Dict[str, List[str]]:
        """Main processing loop - creates feature to rule index."""
        self.feature_to_regs = {}
        self.stats.total_regulations = len(regulations)

        logger.info(f"Processing {len(regulations)} regulations...")

        for idx, regulation in enumerate(regulations):
            self.process_regulation(regulation, f"reg_{idx:04d}", self.feature_to_regs)

        for feature_id in self.feature_to_regs:
            self.feature_to_regs[feature_id].sort()

        self.stats.unique_features_with_rules = len(self.feature_to_regs)
        logger.info(
            f"Found {len(self.linked_waterbody_keys)} linked polygon waterbodies (will be used for grouping)"
        )

        return self.feature_to_regs

    def process_and_export(
        self, regulations: List[Dict], output_dir: Optional[Path] = None
    ) -> PipelineResult:
        """Full pipeline: Link -> Scope -> Enrich -> Map -> Merge -> Export."""
        logger.info(f"Processing {len(regulations)} regulations...")
        self.process_all_regulations(regulations)
        self.merged_groups = self.merge_features(self.feature_to_regs)

        exported_files = (
            self.build_index(self.feature_to_regs, self.merged_groups, output_dir)
            if output_dir
            else None
        )

        logger.info("Processing complete")
        return PipelineResult(
            feature_to_regs=self.feature_to_regs,
            merged_groups=self.merged_groups,
            regulation_names=self.regulation_names,
            stats=self.stats,
            exported_files=exported_files,
        )

    # --- Enrichment & Grouping Logic ---

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

            if feature_type in ("stream", None):
                if linear_id:
                    stream_seeds.append(linear_id)
                else:
                    skipped_features.append(
                        self._get_prop(feature, ["gnis_name", "name"], "unnamed")
                    )
                    if feature_type is None:
                        logger.warning(
                            f"Feature {skipped_features[-1]} has feature_type=None. Skipping."
                        )

            elif feature_type in ("lake", "wetland", "manmade", "unmarked"):
                if linear_id:
                    metadata_key = {"unmarked": "lakes"}.get(
                        feature_type, f"{feature_type}s"
                    )
                    raw_metadata = self.gazetteer.metadata.get(metadata_key, {}).get(
                        linear_id, {}
                    )

                    if wb_key := raw_metadata.get("waterbody_key"):
                        if connected_streams := self._get_stream_seeds_for_waterbody(
                            wb_key
                        ):
                            polygon_seeds_by_type[feature_type].extend(
                                connected_streams
                            )
                        else:
                            name = raw_metadata.get(
                                "gnis_name",
                                self._get_prop(feature, ["gnis_name", "name"]),
                            )
                            logger.debug(
                                f"{feature_type.capitalize()} {name} has no connected streams (waterbody_key={wb_key})"
                            )

        if skipped_features:
            logger.warning(
                f"Skipped {len(skipped_features)} features without linear_id: {skipped_features[:5]}..."
            )

        all_tributaries_dict = {}

        if stream_seeds:
            excluded_codes = self._get_watershed_codes_for_streams(stream_seeds)
            for trib in self.tributary_enricher.enrich_with_tributaries(
                stream_seeds, excluded_codes, features
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib
            logger.debug(
                f"Stream enrichment: {len(stream_seeds)} seeds -> {len(all_tributaries_dict)} tributaries"
            )

        for ftype, seeds in polygon_seeds_by_type.items():
            for trib in self.tributary_enricher.enrich_with_tributaries(
                seeds, set(), features
            ):
                all_tributaries_dict[self._get_feature_id(trib)] = trib
            logger.debug(
                f"{ftype.capitalize()} enrichment: {len(seeds)} seeds -> processed."
            )

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
            feature = self._get_feature_metadata(feature_id)

            feature_type = feature.get("feature_type", "unknown")
            blk = feature.get("blue_line_key")
            wbk = feature.get("waterbody_key")

            use_wbk = wbk and str(wbk) in self.linked_waterbody_keys

            if blk and use_wbk:
                grouping_key = f"{feature_type}_blue_line_{blk}_waterbody_{wbk}"
            elif blk:
                grouping_key = f"{feature_type}_blue_line_{blk}"
            elif use_wbk:
                grouping_key = f"{feature_type}_waterbody_{wbk}"
            else:
                grouping_key = f"{feature_type}_feature_{feature_id}"

            group_map[(grouping_key, reg_set)].append((feature_id, feature))

        merged_groups = {}
        for idx, ((_, reg_set), features_data) in enumerate(group_map.items()):
            _, first_feature = features_data[0]

            all_zones, all_mgmt_units = set(), set()
            for _, feat in features_data:
                all_zones.update(feat.get("zones", []))
                all_mgmt_units.update(feat.get("mgmt_units", []))

            wbk = first_feature.get("waterbody_key")
            group_id = f"group_{idx:06d}"
            merged_groups[group_id] = MergedGroup(
                group_id=group_id,
                feature_ids=tuple(fid for fid, _ in features_data),
                regulation_ids=tuple(sorted(reg_set)),
                gnis_id=first_feature.get("gnis_id"),
                gnis_name=first_feature.get("gnis_name"),
                feature_type=first_feature.get("feature_type"),
                watershed_code=first_feature.get("watershed_code"),
                waterbody_key=(
                    wbk if (wbk and str(wbk) in self.linked_waterbody_keys) else None
                ),
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
        """Write query-ready indices to disk."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        feature_index_path = output_dir / "feature_to_regs.json"
        with open(feature_index_path, "w") as f:
            json.dump(feature_to_regs, f, indent=2)
        logger.info(
            f"Wrote feature index: {feature_index_path} ({len(feature_to_regs)} features)"
        )

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
            for gid, group in self._with_progress(
                merged_groups.items(), "Converting groups", "group"
            )
        }

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

    # --- Property & Metadata Utilities ---

    def _get_prop(self, feature: Any, keys: List[str], default: Any = None) -> Any:
        """Unified helper to safely extract properties from objects or dictionaries."""
        for key in keys:
            if hasattr(feature, key) and getattr(feature, key) is not None:
                return getattr(feature, key)
            if isinstance(feature, dict) and feature.get(key) is not None:
                return feature.get(key)
        return default

    def _get_feature_type(self, feature) -> Optional[str]:
        if ftype := self._get_prop(feature, ["feature_type"]):
            return ftype
        gtype = self._get_prop(feature, ["geometry_type"])
        if gtype in ("multilinestring", "linestring"):
            return "stream"
        if gtype == "polygon":
            return "lake"
        if gtype == "point":
            return "point"
        return None

    def _get_feature_id(self, feature) -> str:
        if lin_id := self._get_prop(feature, ["linear_feature_id", "fwa_id"]):
            return str(lin_id)
        if poly_id := self._get_prop(feature, ["waterbody_poly_id"]):
            return f"{self._get_prop(feature, ['feature_type'], 'UNKNOWN')}_{poly_id}"
        return str(feature)

    def _get_feature_metadata(self, feature_id: str) -> Dict[str, Any]:
        """Get feature metadata from gazetteer for grouping."""
        feature_type_enum = self.gazetteer.get_feature_type_from_id(feature_id)
        key = feature_id.split("_", 1)[1] if "_" in feature_id else feature_id

        meta = {
            "gnis_id": None,
            "gnis_name": None,
            "watershed_code": None,
            "waterbody_key": None,
            "blue_line_key": None,
            "feature_type": feature_type_enum.value,
            "zones": [],
            "mgmt_units": [],
        }

        if feature_type_enum == FeatureType.UNMARKED:
            return meta

        if feature_type_enum == FeatureType.STREAM:
            data = self.gazetteer.metadata.get("streams", {}).get(key)
        else:
            col_map = {
                FeatureType.LAKE: "lakes",
                FeatureType.WETLAND: "wetlands",
                FeatureType.MANMADE: "manmade",
            }
            data = self.gazetteer.metadata.get(
                col_map.get(feature_type_enum, ""), {}
            ).get(key)

        if not data:
            logger.warning(
                f"Metadata lookup failed for feature_id '{feature_id}' - type: {feature_type_enum.value}."
            )
            return meta

        meta.update(
            {
                "gnis_id": data.get("gnis_id"),
                "gnis_name": data.get("gnis_name"),
                "waterbody_key": data.get(
                    "waterbody_key",
                    key if feature_type_enum != FeatureType.STREAM else None,
                ),
                "blue_line_key": data.get("blue_line_key"),
                "zones": data.get("zones", []),
                "mgmt_units": data.get("mgmt_units", []),
            }
        )

        if feature_type_enum == FeatureType.STREAM:
            meta["watershed_code"] = data.get("fwa_watershed_code")

        return meta

    def _get_stream_seeds_for_waterbody(self, waterbody_key: str) -> List[str]:
        if not self.gazetteer:
            return []
        return [
            lin_id
            for lin_id, meta in self.gazetteer.metadata.get("streams", {}).items()
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
        """Helper to wrap an iterable with tqdm if available."""
        try:
            from tqdm import tqdm

            return tqdm(iterable, desc=desc, unit=unit)
        except ImportError:
            return iterable

    # Stats utilities
    def get_stats(self) -> RegulationMappingStats:
        return self.stats

    def reset_stats(self):
        self.__init__(self.linker, self.scope_filter, self.tributary_enricher)
