# Parsing the synopsis with a Claude agent

The parser is a **Claude coding-agent** flow, not a programmatic API call. The registry is the
source of truth; each waterbody plus its bindable boundaries is exported as a self-contained
prompt; an agent parses it into the catalogue format; and `ingest_catalogue` validates and writes.

```
 registry.json ──┐
                 ├─ batch_exporter ──▶ batches/batch_NNN.prompt.txt   (self-contained)
 raw synopsis ───┘                     batches/batch_NNN.json         (items + raw_regs)
                                              │
                                       dispatch (claude CLI)
                                              │
                                      responses/batch_NNN.json
                                              │
                                       ingest_catalogue  ──▶ entries/catalogue/region-N.json
```

## The format

A rule is a **type** plus named **conditions**. The displayed line is generated from those by
`catalogue.py`; the agent never writes a label. See `pipeline/docs/18-how-regulations-are-stored.md`
and `prompts/CATALOGUE_PARSE_PROMPT.md`.

## Running it

```bash
# no credits — export batches and read one before spending anything
bash pipeline/regs/parsing/run_parse.sh parse-dry

# the parse itself (HUMAN-ONLY: spends credits)
bash pipeline/regs/parsing/run_parse.sh parse

# an agent second pass against the strict checklist, then re-parse what it flags
bash pipeline/regs/parsing/run_parse.sh review
bash pipeline/regs/parsing/run_parse.sh repass

# local, no credits
bash pipeline/regs/parsing/run_parse.sh status
```

Knobs: `REGISTRY`, `BATCH_SIZE`, `MODEL`, `REVIEW_MODEL`, `ESCALATE_MODEL`, `CONCURRENCY`.

## What the agent must satisfy

It runs the same gate ingest runs, and should iterate until clean before submitting:

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue batch.json candidate.json
```

Three layers, and the third is the one that matters:

1. the **model** — types, conditions, enums, arithmetic
2. the **entry** — each rule's `verbatim` inside its own `regs_verbatim`
3. the **source** — `regs_verbatim` against the printed row the agent was handed

Layer 2 alone is not chain of custody: the agent writes both sides, so an invented sentence passes.
Two did. `ingest_catalogue` therefore takes `regs_verbatim` from the **batch** and discards whatever
the model supplied.

## What it must never do

* write a label — `details` does not exist; the line is derived
* reword `regs_verbatim`, or stitch it across a sentence boundary
* state a number that is not in that rule's own `verbatim`
* put several restrictions in one rule (see the parse prompt's check 6)
* guess a location — set `needs_review` with a reason that names what is missing
