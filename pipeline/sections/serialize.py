"""Artifact IO + partial-rerun cache detection (05).

GeoParquet for bulk-geometry artifacts (blk_chains, topology_segments, sections) —
memory-mappable, column/row-group pruning, reproducible bytes at multi-GB scale. Pickle for
the small graph containers (Topology w/o geometry, SectionGraph). Each artifact writes a
sibling ``*.meta.json`` {input_hashes, row_count, build_ts, git_sha} so a step re-runs only
when an input changed.
"""

from __future__ import annotations

from typing import Any


def write_artifact(obj: Any, path: str, input_hashes: dict[str, str]) -> None:
    raise NotImplementedError


def read_artifact(path: str) -> Any:
    raise NotImplementedError


def is_stale(path: str, input_hashes: dict[str, str]) -> bool:
    """True if the artifact is missing or its recorded input hashes differ."""
    raise NotImplementedError
