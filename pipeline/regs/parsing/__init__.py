"""parsing — the frozen-parse layer.

`entry_models.py` defines the checked-in `Entry`/`Rule`/`Extent` shape (the curation surface);
`rows.py` is the shared synopsis-row loader. The parse itself is driven agentically through Claude
Code (batch export → subagent → review → ingest), emitting `pipeline/regs/parsing/entries/region-N.json`.
The former Gemini batch parser (parser.py/models.py/session.py/api_manager.py) has been removed.
"""
