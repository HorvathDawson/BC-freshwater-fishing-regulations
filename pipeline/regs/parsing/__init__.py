"""parsing — the frozen-parse layer.

`catalogue.py` defines the checked-in entry and rule shape (`CatalogueEntry`/`CatalogueRule`);
`entry_models.py` keeps only the op + split `Extent` the DFO locations bind with; `rows.py` is the
shared synopsis-row loader. The parse is driven agentically through Claude Code (batch export →
subagent → `ingest_catalogue`), writing `data/curated/regulations/entries/catalogue/`. A review
pass writes its findings to the work dir, never onto an entry; `repass` re-parses what it flagged.
"""
