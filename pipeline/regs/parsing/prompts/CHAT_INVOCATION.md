# Parsing the synopsis with a Claude agent

The parser is a **Claude coding-agent** flow (not a programmatic API call): the registry is the source
of truth, each waterbody + its bindable boundaries is exported as a self-contained prompt, an Opus
subagent parses it into the `Entry` schema, and `ingest` validates and writes the frozen EntryFiles.
The agent may run the repo's own Python helpers to get it right.

```
 registry.json ──build_registry (in the build)      synopsis_raw_data.json
        │                                                     │  load_synopsis_rows
        ▼            matcher (row → registry id)              ▼
   batch_exporter ───────────────────────────────► batches/batch_NNN.json + .prompt.txt
        │                                                     │  parse (Opus subagent, per batch)
        │                    validate.py (self-check)         ▼
        │◄──────────────────────────────────────── responses/batch_NNN.json
        ▼            ingest (Entry + split-id gate)
   pipeline/regs/parsing/entries/region-N.json   ← frozen, `locked` entries never overwritten
```

## Prerequisites
- A build has run, producing `registry.json` (the build writes it next to the graph artifacts).
- Run everything with the venv + `PYTHONPATH="$PWD"`.

## 1. Export batches
```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.batch_exporter --batch-size 40
```
Writes `batches/batch_NNN.prompt.txt` (self-contained: rules + examples + each row's constrained menu +
the `{index, entry}` envelope) and `batch_NNN.json` (carries each item's `raw_regs` + `bindable_ids`).
Unmatched/ambiguous rows are reported in `manifest.json` and excluded — add them to a matcher
`overrides.json` (`{"<normalized water name>": "<registry id>"}` or `{"row:<index>": "<id>"}`) and re-export.

## 2. Parse each batch — two ways

**Automated (CLI subagents, parallel):**
```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.dispatch --model opus --concurrency 3 --review
```
Each batch is dispatched to its own headless `claude -p` subagent (fanned out `--concurrency` at a time);
`--review` runs a second, independent reviewer subagent per batch. Set `CLAUDE_BIN` if `claude` isn't on
PATH.

**By hand / inside a chat:** open `batch_NNN.prompt.txt`, give it to an Opus subagent, save its JSON
reply to `responses/batch_NNN.json`.

Either way the parsing agent should self-check before saving:
```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate \
    batches/batch_000.json responses/batch_000.json
```
and may use helpers like `resolve_species_phrase("char") → ['SLV']` from `pipeline.regs.parsing.species`.

## 3. Ingest (validation gate → frozen EntryFiles)
```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.ingest responses/*.json --dry-run   # validate only
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.ingest responses/*.json             # write
```
Ingest re-injects `regs_verbatim` from the batch (never trusts the model's copy), runs the full `Entry`
validators + split-id check, reports unused-boundary advisories, and writes
`pipeline/regs/parsing/entries/region-N.json`. A `locked` entry already on disk is preserved (human freeze).

## Safety
- **Same gate everywhere.** `validate` and `ingest` run the identical `Entry` model + split-id check.
- **No silent acceptance.** An entry that fails validation is reported and left out.
- **No overwrite of `locked`.** Curated/frozen entries survive a re-parse (full merge tool: later).
- **Drift guard.** The manifest's `rows_digest` catches synopsis changes between export and ingest.
