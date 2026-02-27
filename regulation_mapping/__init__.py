"""
Waterbody Linking Package

Links parsed fishing regulations to FWA (Freshwater Atlas) features.

Debug Mode:
    Set FWA_LINKING_DEBUG=1 environment variable to enable debug output,
    or call enable_debug() programmatically.
"""

from .linker import (
    WaterbodyLinker,
    LinkingResult,
    LinkStatus,
)
from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, FWAFeature
from .linking_corrections import (
    DirectMatch,
    AdminDirectMatch,
    NameVariationLink,
    ADMIN_DIRECT_MATCHES,
    NAME_VARIATION_LINKS,
    ManualCorrections,
)
from .admin_target import AdminTarget
from .regulation_mapper import (
    RegulationMapper,
    RegulationMappingStats,
    MergedGroup,
    PipelineResult,
    # Shared resolution functions (used by mapper + CLI tests)
    ALL_FWA_TYPES,
    FeatureIndex,
    collect_features_from_index,
    exclude_features_from_index,
    include_features_from_index,
    lookup_admin_targets,
    build_feature_index,
    resolve_direct_match_features,
    resolve_direct_match_ids,
    resolve_zone_wide_ids,
    parse_region,
)
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .zone_base_regulations import ZoneRegulation, ZONE_BASE_REGULATIONS
from .logger_config import get_logger, enable_debug, disable_debug

__all__ = [
    "WaterbodyLinker",
    "FWAFeature",
    "LinkingResult",
    "LinkStatus",
    "MetadataGazetteer",
    "DirectMatch",
    "AdminDirectMatch",
    "AdminTarget",
    "ADMIN_DIRECT_MATCHES",
    "ManualCorrections",
    "RegulationMapper",
    "RegulationMappingStats",
    "MergedGroup",
    "PipelineResult",
    "ALL_FWA_TYPES",
    "FeatureIndex",
    "collect_features_from_index",
    "exclude_features_from_index",
    "include_features_from_index",
    "lookup_admin_targets",
    "build_feature_index",
    "resolve_direct_match_features",
    "resolve_direct_match_ids",
    "resolve_zone_wide_ids",
    "parse_region",
    "ScopeFilter",
    "TributaryEnricher",
    "ZoneRegulation",
    "ZONE_BASE_REGULATIONS",
    "get_logger",
    "enable_debug",
    "disable_debug",
]
