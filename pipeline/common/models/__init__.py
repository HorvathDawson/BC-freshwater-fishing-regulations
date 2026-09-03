"""Data structures for the v2 section pipeline.

Frozen dataclasses + str-valued enums; tuples (not lists) on frozen classes for hashability.
JSON-authored classes (``SplitDef``/``SplitAnchor``) carry ``from_dict`` and normalize ids on load.
Geometry is typed ``Any`` to keep the module import-light; concrete builders use shapely.

Split into submodules by concern (enums · names · chains · graph · splits · sections · regs ·
registry) and re-exported here, so ``from pipeline.common.models import X`` keeps working unchanged.
"""

from __future__ import annotations

from pipeline.common.models.enums import (AnchorType, BoundaryKind, NameSource, NodeKind,
                                   WATERBODY_KINDS)
from pipeline.common.models.names import NameTuple
from pipeline.common.models.chains import BlkChain, FidSpan, WaterbodyRun
from pipeline.common.models.sections import Section, SectionBoundary
from pipeline.common.models.graph import FlowEdge, StreamGraph, StreamNode
from pipeline.common.models.splits import SplitAnchor, SplitDef, SplitPoint
from pipeline.common.models.regs import SectionRegs
from pipeline.common.models.registry import RegistryBoundary, RegistryItem

__all__ = [
    "WATERBODY_KINDS",
    # enums
    "NameSource", "NodeKind", "BoundaryKind", "AnchorType",
    # names
    "NameTuple",
    # blk-chains
    "FidSpan", "WaterbodyRun", "BlkChain",
    # graph
    "StreamNode", "FlowEdge", "StreamGraph",
    # splits
    "SplitAnchor", "SplitDef", "SplitPoint",
    # sections
    "SectionBoundary", "Section",
    # matching output
    "SectionRegs",
    # registry
    "RegistryBoundary", "RegistryItem",
]
