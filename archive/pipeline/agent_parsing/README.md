# Agent parsing

An **alternative** to the Gemini synopsis parser (`python -m pipeline --step
parse`). When Google API quota is exhausted — or for a handful of stubborn
rows Gemini keeps failing — this workflow hands the remaining pending rows to
LLM subagents (e.g. **Opus 4.8**) instead.

It writes into the **same** checkpoint the Gemini parser uses
(`output/pipeline/parsing/session_state.json`) and validates every entry with
the **same** `ParsedEntry` schema, so downstream steps (`enrich`, etc.) neither
know nor care which engine produced a given row.

## Why this exists

The Gemini parser is the primary path. Google API keys have daily quotas; once
all keys are exhausted the run stalls with pending rows. Rather than wait,
export those pending rows and parse them with a different model. This is a
**mop-up / fallback** tool, not a replacement.

## How it fits together

```
  ┌────────────────────┐      batch_exporter       ┌──────────────────────┐
  │ session_state.json │ ───────────────────────►  │ batches/batch_NNN.*  │
  │ (shared checkpoint)│   (pending rows only)      │  .json + .prompt.txt │
  └────────────────────┘                            └──────────┬───────────┘
            ▲                                                   │  dispatch each
            │                ingest                             ▼  prompt to an
            │        (validate → apply)             ┌──────────────────────┐
            └────────────────────────────────────── │ responses/batch_NNN. │
                                                     │ json (agent output)  │
                                                     └──────────────────────┘
```

Rows are joined by **global row index**, shared via
`pipeline/parsing/rows.py::load_synopsis_rows` (the single source of truth for
row ordering, used by both engines).

## Prerequisites

- The Gemini parser has run at least once, so `session_state.json` exists.
  (This tool refuses to run against a missing/misaligned session.)
- Run everything with the venv + `PYTHONPATH`:

  ```sh
  PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.batch_exporter
  ```

## Recipe

### 1. Export pending rows into batches

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.batch_exporter \
    --batch-size 40
```

Writes, under `output/pipeline/agent_parsing/`:

- `batches/batch_NNN.json` — the indexed rows for that batch (+ `rows_digest`).
- `batches/batch_NNN.prompt.txt` — a **self-contained** prompt (canonical rules
  + examples + the batch's rows + output envelope). Hand this to a subagent.
- `manifest.json` — batch layout, `rows_digest`, pending count.

**Batch size / large context:** with an **Opus 4.8 + 1M-token** context you can
raise `--batch-size` substantially (e.g. 100–200) — the whole prompt (rules +
examples + rows) fits, so fewer round trips. Start smaller if you want tighter
review granularity.

### 2. Dispatch each batch to a subagent

See [`prompts/PARSE_INSTRUCTIONS.md`](prompts/PARSE_INSTRUCTIONS.md). For each
batch, send `batch_NNN.prompt.txt` to an Opus 4.8 subagent and save its raw
JSON reply to `responses/batch_NNN.json`.

### 2.5. Review each batch (recommended)

A passing dry-run only proves the output is *structurally* valid. To also catch
*content* errors and cross-batch inconsistency, render a review prompt and hand
it to an independent reviewer subagent:

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.review_exporter \
    output/pipeline/agent_parsing/responses/batch_000.json
```

This writes `batches/batch_000.review.prompt.txt` (the same canonical spec +
rows + the candidate output + a review checklist). The reviewer confirms the
`.validate()` dry-run passes, audits content correctness in the spirit of the
validators, checks cross-row consistency, and returns a `{"verdict", "issues"}`
report. Apply any fixes to the response and re-run the dry-run until clean, then
ingest. See [`prompts/CHAT_INVOCATION.md`](prompts/CHAT_INVOCATION.md) "Step
2.5" for the reviewer template + fix loop.

### 3. Review + ingest

Always dry-run first — it validates and reports without touching the session:

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
    output/pipeline/agent_parsing/responses/batch_000.json --dry-run
```

Then apply for real (backs up `session_state.json` first):

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
    output/pipeline/agent_parsing/responses/batch_000.json
```

Ingest multiple at once:

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
    output/pipeline/agent_parsing/responses/*.json
```

### 4. Finalize (optional)

Write `synopsis_parsed.json` from the current session (or just let the next
Gemini `--step parse --resume` / downstream step do it):

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
    output/pipeline/agent_parsing/responses/*.json --finalize
```

### 5. Compare against Gemini (optional)

If you ran the agent into an isolated session, diff it against Gemini's parse:

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.compare \
    --agent-session-dir output/pipeline/agent_parsing_full
```

Reports coverage (rows only one engine parsed), agreement, and a per-row
breakdown of every difference — rule count, tributary flags, restriction
types, and dates. Narrow with `--category restriction_types` (or `dates`,
`rule_count`, `includes_tributaries`, `tributary_only`, `echo`), show all rows
with `--limit 0`, or emit `--json`.

## Progress / resume

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.batch_exporter --status
```

Re-running the exporter always reflects the **current** session — rows that
became `success` (via Gemini or a prior ingest) drop out of the next export.
You can freely interleave: run Gemini until quota dies, export + agent-parse
the remainder, then let a later Gemini `--resume` pick up anything still
pending.

## Safety guarantees

- **Same validators.** Every entry passes the exact `ParsedEntry` schema and the
  verbatim `regs_verbatim == raw_regs` echo check the Gemini parser enforces.
  Invalid entries are **rejected and left pending** — never silently accepted.
- **Fail loud.** Missing/mismatched session or a changed `rows_digest` (data
  drift between export and ingest) aborts rather than corrupting indices.
- **No overwrite by default.** Rows already `success` are skipped unless
  `--force`.
- **Backups.** `session_state.json` is copied to `session_state.backup-<ts>.json`
  before every write. To roll back, restore that file.
- **Duplicate indices** across responses are rejected.

## Mixing Gemini and agent runs

Both engines write the same checkpoint keyed by the same row indices, so mixing
is supported and expected. The only rule: don't hand-edit `session_state.json`
between an export and its ingest, and don't change the underlying synopsis data
in that window (the `rows_digest` guard will catch the latter and abort).

## Generated files

`batches/`, `responses/`, `manifest.json`, and `*.backup-*.json` are
git-ignored — they are all regenerable from the session + raw data.
