# pipeline/regs/parsing: synopsis rows to validated catalogue entries

Turns each row of the synopsis's water-specific tables into a `CatalogueEntry` (`catalogue.py`): typed rules bound to the water's cut-points,
plus its `licensing` list. The checked-in region files `data/curated/regulations/synopsis/region-*.json` are the single source of truth; curator
edits are written straight back to them. The format: `pipeline/docs/18-how-regulations-are-stored.md`; the agent's invocation: `prompts/CHAT_INVOCATION.md`.

**HUMAN-ONLY (AGENTS A1): `all`, `parse`, `review` and `repass` spend the owner's credits. An agent hands over the command, never runs it.**
`parse-dry`, `ingest`, `seed-ledger` and `status` are local and free.

- Entry (one shell script; the Python modules are its building blocks): `bash pipeline/regs/parsing/run_parse.sh <all|parse|parse-dry|review|repass|ingest|seed-ledger|status>`.
  Env knobs: `REGISTRY`, `BATCH_SIZE`, `MODEL`, `REVIEW_MODEL`, `ESCALATE_MODEL`, `CONCURRENCY`, `CLAUDE_BIN`.
- Flow: rows (`rows.py`, matched by `regs/matching`) to `batch_exporter.py` batches (one item per row, `entry_id = r{region}:{name}@{MUs}`), `dispatch.py`
  (the `claude` CLI) to responses, `ingest_catalogue.py` (the gate: row facts from the batch, validate, write, record the ledger), then an independent review
  (`review_exporter.py`) whose findings feed a `repass` of the flagged entries only. Everything but the region files lives in the work dir `data/generated/regs/parse/`.
- Modules: `catalogue.py` + `catalogue_parts/` (the model and its generated labels), `io.py` (the one home for region-file IO; `write_entryfile` is atomic and
  validates what it wrote), `entry_models.py` (the op + split `Extent`), `validate_catalogue.py` (the checks, also a CLI), `parse_context.py` (the parse prompt),
  `backfill_matched.py` / `remap_boundaries.py` / `drop_on_parts.py` (local repairs after a registry change; AGENTS: repair passes run after ingest and are never wired into `run_parse.sh`).
- Reviews are never written onto an entry; a review is a finding about one parse of one batch, joined to the batch item's `entry_id`.
- A re-parse never overwrites a curator's edit: ingest records a digest of every entry it writes (`ingested.json`, `io.entry_digest`) and replaces an entry only if the
  file's copy is still the one it wrote; an edited one is reported KEPT (`--replace-entry <id>` one at a time, `--replace-edited` all, the curator's placement carried
  by `carry_curation`). **An entry that carries an owner mark is never replaced**: the run prints `REFUSED` naming the mark and exits 1 (T628). Re-seeding the ledger
  (`seed-ledger`) declares every entry on disk unedited; do it only after the work dir was lost or a new book was adopted.
- Back up `data/curated/regulations/synopsis/region-*.json` before any write to them (AGENTS A2).
- Tests: `pipeline/tests/test_catalogue*.py`, `test_ingest*.py`, `test_validate_catalogue*.py`, `test_batch*.py`.
