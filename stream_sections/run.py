"""Step entrypoints, to be wired into pipeline/__main__.py as _step_*(cfg) (canonical order
blk-chains -> graph -> sections).

Each step reads its typed input artifact(s), runs the builder, writes the output artifact +
*.meta.json, and skips when serialize.is_stale() is False (cheap partial reruns, 05).
Outputs go under output/pipeline/v2/ (parallel to prod until cutover). See build.py for the
current end-to-end validation entrypoint.
"""

from __future__ import annotations


def build_blk_chains(cfg) -> None:
    raise NotImplementedError


def build_graph(cfg) -> None:
    raise NotImplementedError


def build_sections(cfg) -> None:
    raise NotImplementedError
