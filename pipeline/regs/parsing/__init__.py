"""parsing — the frozen-parse layer.

`catalogue.py` defines the checked-in entry and rule shape (`CatalogueEntry`/`CatalogueRule`);
`entry_models.py` keeps only the op + split `Extent` the DFO locations bind with; `rows.py` is the
shared synopsis-row loader. The parse is driven agentically through Claude Code (batch export →
subagent → review → `ingest_catalogue`), writing `data/curated/regulations/entries/catalogue/`.
The former Gemini batch parser (parser.py/models.py/session.py/api_manager.py) has been removed.
"""
