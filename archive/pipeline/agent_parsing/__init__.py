"""Agent-driven synopsis parsing — an alternative to the Gemini parser.

When Google API quota is exhausted (or for stubborn residual failures), the
remaining pending rows can be parsed by LLM subagents (e.g. Opus 4.8) instead
of Gemini.  This package is a thin, file-based workflow around the *existing*
``pipeline.parsing`` schema and checkpoint — it never introduces a second
output format or a parallel session:

    batch_exporter  session pending rows  ->  batches/ (+ rendered prompts)
    (main agent dispatches each batch to a subagent, reviews the output)
    ingest          agent JSON  ->  validate  ->  shared session_state.json

All entries are validated through ``pipeline.parsing.models.ParsedEntry`` and
written into the same ``output/pipeline/parsing/session_state.json`` the Gemini
parser uses, so downstream steps stay engine-agnostic.
"""
