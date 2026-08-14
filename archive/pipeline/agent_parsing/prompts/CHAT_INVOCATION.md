# Invoking agent parsing from a new chat session

This guide is for a **fresh Copilot chat** (or any coding agent). Paste the
relevant block, adjust paths, and the agent can drive the whole workflow. The
agent parses the remaining synopsis rows with LLM subagents (Opus 4.8) and
writes into an **isolated** session so it never disturbs the live Gemini run.

> All Python runs as: `PYTHONPATH="$PWD" .venv/bin/python ...` from the repo root.

---

## TL;DR — one message to start a run

Paste this into a new chat (edit the two values in **bold**):

> Run the agent-parsing workflow in `pipeline/agent_parsing`, writing to an
> **isolated** session dir **`output/pipeline/agent_parsing_full`** (do NOT
> touch the live Gemini session at `output/pipeline/parsing`). Use batch size
> **150**. Steps:
> 1. If the isolated session doesn't exist yet, create a fresh all-pending one
>    for all synopsis rows (see "Create an isolated session" below).
> 2. Export batches with `batch_exporter` into that dir.
> 3. For each `batch_NNN.prompt.txt`, dispatch an **Opus 4.8 subagent** using
>    the "Subagent batch prompt" template below; save each reply to
>    `responses/batch_NNN.json`.
> 4. **Review each batch**: render a review prompt with `review_exporter` and
>    dispatch an independent **reviewer subagent**; apply any fixes it reports
>    and re-run the batch's dry-run until it is clean.
> 5. Ingest each response with `--dry-run` first, then for real.
> 6. When all batches are applied, run `compare` against the Gemini session and
>    summarize the differences.

---

## Prerequisites

- The Python venv exists (`.venv/`) and the pipeline imports work.
- `output/pipeline/extraction/synopsis_raw_data.json` exists (the raw rows).
- A session checkpoint to work against:
  - **Mop-up mode** — parse only what Gemini hasn't: point `--session-dir` at
    the live session `output/pipeline/parsing`. Only pending rows export.
  - **Full independent run** — parse everything into a clean session for
    comparison: create a fresh isolated session (below).

### Create an isolated session (full independent run)

```sh
PYTHONPATH="$PWD" .venv/bin/python - <<'PY'
from pathlib import Path
from pipeline.parsing.rows import load_synopsis_rows
from pipeline.parsing.session import ParsingSession
d = Path("output/pipeline/agent_parsing_full")
d.mkdir(parents=True, exist_ok=True)
rows = load_synopsis_rows()
s = ParsingSession(session_dir=d, total_rows=len(rows))
s.record_indexed({})            # write a fresh all-pending checkpoint
print("created", d, s.summary())
PY
```

---

## Step 1 — export batches

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.batch_exporter \
  --batch-size 150 \
  --session-dir output/pipeline/agent_parsing_full \
  --out-dir     output/pipeline/agent_parsing_full
```

Writes `batches/batch_NNN.json` + `batches/batch_NNN.prompt.txt` + `manifest.json`.
Only not-yet-`success` rows are exported. Bigger `--batch-size` (100–200 with
Opus 4.8 + 1M context) = fewer batches. Re-running clears stale batch files.

Check progress anytime:

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.batch_exporter \
  --session-dir output/pipeline/agent_parsing_full \
  --out-dir     output/pipeline/agent_parsing_full --status
```

---

## Step 2 — dispatch a subagent per batch

For each `batch_NNN.prompt.txt`, launch an **Opus 4.8** subagent with the
template below (substitute `NNN`). The critical trick: the subagent must set
`regs_verbatim` **programmatically from the batch file**, never retype it —
otherwise the exact-echo check fails.

### Subagent batch prompt (template)

> You are parsing BC freshwater fishing regulations into a strict schema. Repo
> root: `/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations`.
>
> Inputs (BATCH **NNN**):
> - Rules + examples + rows (self-contained, READ FIRST):
>   `output/pipeline/agent_parsing_full/batches/batch_NNN.prompt.txt`
> - Machine-readable rows (each has integer `index` + exact `raw_regs`):
>   `output/pipeline/agent_parsing_full/batches/batch_NNN.json`
>
> Task: produce `output/pipeline/agent_parsing_full/responses/batch_NNN.json` —
> a JSON array of `{"index": <copied exactly>, "entry": <ParsedEntry>}`.
>
> CRITICAL — guarantee the echo: WRITE A PYTHON SCRIPT that loads the batch
> JSON, sets `entry["regs_verbatim"] = item["raw_regs"]` directly from the
> loaded data, and attaches your authored parse (rules, tributary flags, dates,
> locations) keyed by index. Every `rule_text` / `location_text` / date must be
> a contiguous substring of `raw_regs` after normalization (strip `**`, collapse
> whitespace, lowercase) — slice from `raw_regs` to guarantee it.
>
> CRITICAL — DO NOT SPLIT RULES ON NEWLINES. `raw_regs` wraps mid-sentence, so a
> single rule frequently spans one or more `\n`. Never cut a `rule_text` at a
> newline and never emit a continuation fragment (e.g. "to signs on the E. side
> of the lake", "approximately 250 m downstream", "year-round", a lone place
> name) as its own `note` rule. Segment into rules by MEANING (distinct
> restrictions), not by physical lines: normalize `\n` to spaces first, then
> decide rule boundaries. Each rule must carry its OWN complete
> `location_text`, `dates`, `exception`, and tributary flag. When one line
> holds two closures (e.g. "downstream of X ... July 1-Sept 30, upstream of X
> ... Dec 1-Sept 30"), emit TWO rules — never merge them or cross-attach a date
> to the wrong reach. Preserve every quantitative/qualifying detail verbatim in
> `details` (speed limits like "8 km/h", species qualifiers like "wild rainbow
> trout over 50 cm", carve-outs like "except Quinsam River"); never flatten
> "Speed restriction (8 km/h)" to "Speed restriction".
>
> CRITICAL — COMPOUND SENTENCES SHARE ONE `rule_text`. When a single sentence
> bundles multiple restrictions separated by commas, semicolons, or "and"
> (e.g. "Bait ban, single barbless hook"; "Rainbow trout daily quota = 2;
> single hook"; "No Ice Fishing; trout/char daily quota = 2; bait ban"), emit
> one rule per restriction but give EVERY rule from that sentence the SAME full
> sentence as its `rule_text` — never a fragment. The differentiation lives in
> `details` (the specific restriction), NOT in `rule_text`. Only a NEWLINE that
> separates two genuinely distinct statements starts a new `rule_text`; commas
> and semicolons within one continuous statement do not. Also strip ALL `**`
> bold markers from every stored field (e.g. store "No Fishing", never "No
> Fishing**"), and let `location_text` span newlines the same way `rule_text`
> does — never truncate it at a `\n`.
>
> CRITICAL — TRIBUTARY MARKER IS A SCOPE BOUNDARY (overrides the merge rule).
> An embedded per-restriction tributary marker — "[Includes Tributaries]" or
> "including tributaries" attached to ONE restriction inside a sentence (e.g.
> "Class II water[Includes Tributaries] when open") — must NOT be merged into
> its siblings' `rule_text`. Split the shared `rule_text` at that boundary: the
> marked restriction keeps the marker in its `rule_text` and sets
> `includes_tributaries=true`; each sibling restriction (e.g. a "No Fishing"
> closure on the same line) keeps its OWN `rule_text` WITHOUT the marker and
> does NOT get its flag forced to true — it inherits the entry-level tributary
> flag (leave the per-rule flag null/absent). Only set a per-rule
> `includes_tributaries=true` when that rule's OWN text carries the marker or
> the phrase "including tributaries"; only set it explicitly `false` when the
> rule is scoped "mainstem only" while the entry is tributary-inclusive.
>
> Validate: iterate until this dry-run reports `failed : 0` and `accepted`
> equals the item count:
> `PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest output/pipeline/agent_parsing_full/responses/batch_NNN.json --session-dir output/pipeline/agent_parsing_full --out-dir output/pipeline/agent_parsing_full --dry-run`
>
> Do NOT run ingest without `--dry-run`; do NOT touch `output/pipeline/parsing/`.
> Use `.venv/bin/python` with `PYTHONPATH="$PWD"`. Report the final counts and
> confirm the response file path. Remove any scratch scripts you created.

---

## Step 2.5 — review each batch (independent reviewer subagent)

A passing dry-run only proves the output is *structurally* valid (schema +
verbatim echo + keyword coverage). It does NOT prove the content is *correct*.
So every batch gets a second pass: an independent reviewer subagent audits the
candidate output for content correctness and cross-row consistency, in the
spirit of the validators, and reports fixes for the parsing agent to apply.

Render the review prompt from the batch's response (pairs the rows with the
candidate output):

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.review_exporter \
  output/pipeline/agent_parsing_full/responses/batch_NNN.json \
  --out-dir output/pipeline/agent_parsing_full
```

This writes `batches/batch_NNN.review.prompt.txt`. Dispatch it to a **fresh**
Opus 4.8 subagent (not the one that authored the batch).

### Reviewer subagent prompt (template)

> You are an independent REVIEW agent for BC freshwater fishing regulation
> parsing. Repo root:
> `/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations`.
>
> Read `output/pipeline/agent_parsing_full/batches/batch_NNN.review.prompt.txt`
> — it contains the full parsing spec, the batch rows, the candidate output to
> audit, and the review checklist/output contract. Follow it exactly.
>
> First confirm the ".validate()" gate: run the dry-run and require `failed : 0`:
> `PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest output/pipeline/agent_parsing_full/responses/batch_NNN.json --session-dir output/pipeline/agent_parsing_full --out-dir output/pipeline/agent_parsing_full --dry-run`
>
> Then audit content correctness and cross-row consistency per the checklist and
> return ONLY the review JSON (`{"verdict": ..., "issues": [...]}`). Do NOT edit
> the response file yourself. Do NOT touch `output/pipeline/parsing/`.

### Fix loop

- If the reviewer returns `{"verdict": "pass", "issues": []}`, proceed to ingest.
- Otherwise, hand the issues back to the **parsing** agent for that batch. It
  applies each `fix`, re-runs the dry-run until `failed : 0`, and re-saves
  `responses/batch_NNN.json`. Re-render + re-review until the verdict is `pass`
  (or only `nit`-severity issues remain and you accept them).

---

## Step 3 — ingest for real

Dry-run to review, then apply (a session backup is written automatically):

```sh
# review
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
  output/pipeline/agent_parsing_full/responses/batch_000.json \
  --session-dir output/pipeline/agent_parsing_full \
  --out-dir     output/pipeline/agent_parsing_full --dry-run

# apply all batches + write synopsis_parsed.json
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \
  output/pipeline/agent_parsing_full/responses/batch_*.json \
  --session-dir output/pipeline/agent_parsing_full \
  --out-dir     output/pipeline/agent_parsing_full --finalize
```

Invalid entries are rejected and left pending (never silently accepted). Rows
already `success` are skipped unless `--force`.

---

## Step 4 — compare against Gemini

```sh
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.compare \
  --agent-session-dir output/pipeline/agent_parsing_full
```

Reports coverage, agreement, and a per-row breakdown of every difference
(rule count, tributary flags, restriction types, dates, echo). Narrow it with
`--category restriction_types` (or `dates`, `rule_count`, ...), raise
`--limit 0` to see all rows, or add `--json` for a machine-readable report.

---

## Safety recap

- `--session-dir` isolation means the Gemini session is never touched.
- Same validators as Gemini (exact echo + `ParsedEntry` schema).
- `session_state.json` is backed up before every write.
- `batches/`, `responses/`, `manifest.json`, `*.backup-*.json` are git-ignored.
