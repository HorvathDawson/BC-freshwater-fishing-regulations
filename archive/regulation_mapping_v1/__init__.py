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
    DirectMatchError,
)
from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, FWAFeature
from .linking_corrections import (
    DirectMatch,
    AdminDirectMatch,
    NameVariationLink,
    FeatureNameVariation,
    ADMIN_DIRECT_MATCHES,
    NAME_VARIATION_LINKS,
    FEATURE_NAME_VARIATIONS,
    ManualCorrections,
)
from .admin_target import AdminTarget
from .regulation_mapper import (
    RegulationMapper,
    Pass1Result,
    Pass2Result,
)
from .regulation_types import (
    RegulationMappingStats,
    MergedGroup,
    PipelineResult,
    LinkedRegulation,
    AdminResolutionResult,
    ZoneScopeOptimizer,
    ZoneScopeClassification,
    DirectMatchTarget,
    ZoneWideTarget,
    FeatureIndex,
)
from .regulation_resolvers import (
    is_regulation_inherited,
    parse_base_regulation_id,
    ALL_FWA_TYPES,
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
from .canonical_store import CanonicalDataStore
from .geo_exporter import GeoArtifactGenerator
from .search_exporter import SearchIndexBuilder
from .geometry_utils import (
    round_coords,
    merge_lines,
    geoms_to_wgs84_bbox,
    extract_line_components,
    extract_geoms,
)
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
    "FeatureNameVariation",
    "ADMIN_DIRECT_MATCHES",
    "FEATURE_NAME_VARIATIONS",
    "ManualCorrections",
    "RegulationMapper",
    "RegulationMappingStats",
    "MergedGroup",
    "PipelineResult",
    "Pass1Result",
    "Pass2Result",
    "LinkedRegulation",
    "AdminResolutionResult",
    "ZoneScopeOptimizer",
    "ZoneScopeClassification",
    "DirectMatchTarget",
    "ZoneWideTarget",
    "is_regulation_inherited",
    "parse_base_regulation_id",
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
    "CanonicalDataStore",
    "GeoArtifactGenerator",
    "SearchIndexBuilder",
    "round_coords",
    "merge_lines",
    "geoms_to_wgs84_bbox",
    "extract_line_components",
    "extract_geoms",
    "get_logger",
    "enable_debug",
    "disable_debug",
]
