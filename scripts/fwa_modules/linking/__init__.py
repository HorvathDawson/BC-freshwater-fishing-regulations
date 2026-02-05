"""
Waterbody Linking Package

Links parsed fishing regulations to FWA (Freshwater Atlas) features.
"""

from .linker import (
    WaterbodyLinker,
    LinkingResult,
    LinkStatus,
)
from .metadata_gazetteer import MetadataGazetteer, FWAFeature
from .name_variations import (
    NameVariation,
    NAME_VARIATIONS,
    DirectMatch,
    ManualCorrections,
)

__all__ = [
    "WaterbodyLinker",
    "FWAFeature",
    "LinkingResult",
    "LinkStatus",
    "MetadataGazetteer",
    "NameVariation",
    "NAME_VARIATIONS",
    "DirectMatch",
    "ManualCorrections",
]
