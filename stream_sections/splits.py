"""Load splits.json, resolve anchors, and write back splits.resolved.json (04).

Authored splits live at pipeline/matching/splits.json (next to overrides.json). Resolved
(blk, route_measure, fid) is written to pipeline/matching/splits.resolved.json for
deterministic, human-reviewable builds (mirrors the overrides.json "divide ~= LFID ..." note).
"""

from __future__ import annotations

from .models import BlkChain, SplitDef, SplitPoint


def load_split_defs(path: str) -> list[SplitDef]:
    raise NotImplementedError


def resolve_splits(defs: list[SplitDef], chains: list[BlkChain], context: dict) -> list[SplitPoint]:
    raise NotImplementedError


def write_resolved(points: list[SplitPoint], path: str) -> None:
    raise NotImplementedError
