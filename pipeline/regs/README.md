# pipeline/regs: reading the regulations

Everything about getting the rules out of the books and into the curated corpus. How the book is READ is
[`pipeline/docs/RULINGS.md`](../docs/RULINGS.md), which wins over any other note.

| folder / file | what | entry |
|---|---|---|
| `extraction/` | the synopsis PDF to rows (`extract_synopsis.py`), row boxes, passage boxes, the extractor gate (`baseline.py`) | `python -m pipeline.regs.extraction.extract_synopsis` |
| `parsing/` | rows to validated catalogue entries through the LLM (HUMAN-ONLY, spends credits, AGENTS A1); the catalogue model and gate | `bash pipeline/regs/parsing/run_parse.sh <subcommand>` (README) |
| `matching/` | synopsis rows to registry items before the parse (`matcher.py`) | called by the batch exporter |
| `review/` | the review app's library: marks, apply paths, queue, pre-checks, vintage | `python -m pipeline.regs.review.apply` (`tools/curation-review/README.md`) |
| `dfo_salmon/` | federal salmon rules: fetch, parse, locate, type, live feed | its README |
| `inseason/` | the province's in-season notices (BC Fish and Wildlife, not DFO) matched to waters (`matches.py`, `resolve.py`) and formatted as the `inseason-links` file the app reads; the notices themselves are fetched by the feeds worker | `python -m pipeline.regs.inseason.publish` |
| `steelhead/` | the known steelhead waters list; `accept` prints added / dropped and `--write`s it | `python -m pipeline.regs.steelhead.accept` (AGENTS 54) |
| `classified_waters.py` | the book's classified waters held to the corpus and the bundle | `--check`, `--bundle`, `--write-pins` (`classified_pins.json`) |
| `local_closures.py` | waters a local authority closes (`data/curated/regulations/local_closures/`), a second entry source beside the synopsis | `python -m pipeline.regs.local_closures --check` |

- Reads `data/source/fishing_synopsis.pdf` and `data/curated/regulations/`; writes `data/curated/regulations/synopsis/region-*.json`
  (the corpus, single source of truth; back them up before writing, AGENTS A2) and work files under `data/generated/regs/`.
- Rules: the corpus is authored/reviewed data, never regenerated to check something (AGENTS 38); the owner's review marks are
  never touched (AGENTS A8); a repair pass runs after ingest and is never wired into `run_parse.sh`.
- Tests: `pipeline/tests/test_catalogue*.py`, `test_ingest*.py`, `test_extract*.py`, `test_review_*.py`, `test_dfo_*.py`, `test_inseason*.py`.
