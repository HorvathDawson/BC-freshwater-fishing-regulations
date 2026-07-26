"""stream_sections — clean-slate stream model: BLK chains, contracted topology, sections.

Lives OUTSIDE ``pipeline/`` so it never touches prod code. It reuses prod modules read-only
(``data.data_extractor.FWADataAccessor``, ``pipeline.utils.wsc``). Run everything with the
project venv: ``.venv/bin/python``.

Replaces the per-micro-segment graph + regulation-derived reaches with a small, stable
**section** model:

    blk-chains  ->  topology  ->  sections  ->  (match, bundle, deploy)

Design lives in ``stream_sections/docs/`` (00-11). Nothing here ships until the v2 build is
validated and cut over. See ``docs/09-data-structures.md`` for the schema and
``docs/11-implementation-plan.md`` for build order.
"""
