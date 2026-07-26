"""Step entrypoints, wired into pipeline/__main__.py as _step_blk_chains / _step_topology /
_step_sections (same _step_*(cfg) pattern; canonical order blk-chains -> topology -> sections).

Each step reads its typed input artifact(s), runs the builder, writes the output artifact +
*.meta.json, and skips when serialize.is_stale() is False (cheap partial reruns, 05).
Outputs go under output/pipeline/v2/ (parallel to prod until cutover).
"""

from __future__ import annotations


def build_blk_chains(cfg) -> None:
    raise NotImplementedError


def build_topology(cfg) -> None:
    raise NotImplementedError


def build_sections(cfg) -> None:
    raise NotImplementedError
