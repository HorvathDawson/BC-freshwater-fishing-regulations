"""pipeline — the BC freshwater regulations → sections pipeline.

The reg content chain plus the stream/section model (merged here from the old
``stream_sections/`` package during the regs→sections redesign):

    extraction/  Synopsis PDF/rows → raw reg text.
    parsing/     Raw reg text → frozen ParsedEntry structures.
    utils/       Shared helpers (wsc, etc.).
    (root)       blk-chains → graph/topology → sections; curated splits; serialize/export.

Build a stream section model with ``python -m pipeline.atlas.build`` (see ``build.py``).
Design lives in ``pipeline/docs/`` — start with ``00-README.md``.

The old matching / atlas / enrichment / tiles / graph / deploy / agent_parsing /
recurring subpackages were archived to ``archive/pipeline/``, removed in T335 (``git show d1b36ca44:archive/pipeline/…``).
"""

import os as _os

if _os.environ.get("PIPELINE_TRACE"):          # T522: the build's input-keyed caches record what this process read
    from pipeline._trace import install as _install_trace
    _install_trace(_os.environ["PIPELINE_TRACE"])
