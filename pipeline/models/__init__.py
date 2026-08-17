"""Data structures for the v2 section pipeline.

Frozen dataclasses + str-valued enums; tuples (not lists) on frozen classes for hashability.
JSON-authored classes (``SplitDef``/``SplitAnchor``) carry ``from_dict`` and normalize ids on load.
Geometry is typed ``Any`` to keep the module import-light; concrete builders use shapely.

Split into submodules by concern (enums · names · chains · graph · splits · sections · regs ·
registry) and re-exported here, so ``from pipeline.models import X`` keeps working unchanged.
"""

from __future__ import annotations

from pipeline.models.enums import AnchorType, BoundaryKind, NameSource, NodeKind
from pipeline.models.names import NameTuple
from pipeline.models.chains import BlkChain, FidSpan, WaterbodyRun
from pipeline.models.sections import Section, SectionBoundary
from pipeline.models.graph import FlowEdge, StreamGraph, StreamNode
from pipeline.models.splits import SplitAnchor, SplitDef, SplitPoint
from pipeline.models.regs import SectionRegs
from pipeline.models.registry import RegistryBoundary, RegistryItem

__all__ = [
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
