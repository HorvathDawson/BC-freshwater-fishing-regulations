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
    ADMIN_DIRECT_MATCHES,
    ManualCorrections,
)
from .regulation_mapper import (
    RegulationMapper,
    RegulationMappingStats,
    MergedGroup,
    PipelineResult,
)
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .logger_config import get_logger, enable_debug, disable_debug

__all__ = [
    "WaterbodyLinker",
    "FWAFeature",
    "LinkingResult",
    "LinkStatus",
    "MetadataGazetteer",
    "DirectMatch",
    "AdminDirectMatch",
    "ADMIN_DIRECT_MATCHES",
    "ManualCorrections",
    "RegulationMapper",
    "RegulationMappingStats",
    "MergedGroup",
    "PipelineResult",
    "ScopeFilter",
    "TributaryEnricher",
    "get_logger",
    "enable_debug",
    "disable_debug",
]
