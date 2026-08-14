"""pipeline — the BC freshwater regulations → sections pipeline.

The reg content chain plus the stream/section model (merged here from the old
``stream_sections/`` package during the regs→sections redesign):

    extraction/  Synopsis PDF/rows → raw reg text.
    parsing/     Raw reg text → frozen ParsedEntry structures.
    utils/       Shared helpers (wsc, etc.).
    (root)       blk-chains → graph/topology → sections; curated splits; serialize/export.

Build a stream section model with ``python -m pipeline.build`` (see ``build.py``).
Design lives in ``pipeline/docs/`` — start with ``DESIGN-regs-to-sections.md``.

The old matching / atlas / enrichment / tiles / graph / deploy / agent_parsing /
recurring subpackages were archived to ``archive/pipeline/`` (reference only).
"""
