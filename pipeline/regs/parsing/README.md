# pipeline/regs/parsing — synopsis rows → validated `Entry` records

Turns each BC fishing-synopsis row into an `Entry` (rules bound to a waterbody's boundaries), reviews
those parses, and keeps the checked-in `entries/region-*.json` as the **single source of truth**
(curator edits from the review app are written straight back here).

## The one entry point

Everything runs through the shell script — the Python modules are building blocks it calls:

```bash
bash pipeline/regs/parsing/run_parse.sh <parse|review|repass|prune|status> [rereview]
```

| subcommand | what it does | credits? |
|---|---|---|
| `parse`  | export un-parsed rows → parse (tiered cascade) → escalate → review → ingest | **yes** (claude CLI) |
| `review` | review EVERY current entry in place (locked + not) → stamp `parse_review` | **yes** (claude CLI) |
| `repass` | re-parse ONLY the review-flagged entries (MODEL-selectable) with reviewer hints | **yes** (claude CLI) |
| `prune`  | drop stale bare `item_id` entries superseded by per-row `item_id#reach` entries | no |
| `status` | entry counts, `parse_review` verdicts, repass-candidate count | no |

⛔ **HUMAN-ONLY**: `parse` / `review` / `repass` spend the user's credits — Claude/agents must hand over
the command, never run it. `prune` / `status` are local and safe.

Env knobs: `REGISTRY`, `BATCH_SIZE`, `MODEL` (parse), `REVIEW_MODEL`, `ESCALATE_MODEL`, `CONCURRENCY`,
`CLAUDE_BIN`. Model choice on a repass: `MODEL=opus bash pipeline/regs/parsing/run_parse.sh repass`.

## Dataflow

```
synopsis rows ──match──> batches/            (export)      one batch item per ROW; entry_id = item_id,
   (rows.py)             batch_NNN.json                     or item_id#<reach> for multi-row waterbodies
                              │
                     claude CLI (dispatch)
                              ▼
                        responses/            parsed {index, entry} JSON per batch
                              │
                   review (dispatch --review)
                              ▼
                        reviews/              {verdict, issues} per batch
                              │
                          ingest
                              ▼
                    entries/region-*.json     the SINGLE SOURCE OF TRUTH (validated Entry records,
                                              parse_review stamped in, locked entries preserved)
```

`review` reuses this by rebuilding `responses/` from the *current* entries (`synth_responses.py`) so the
reviewer critiques what's on disk, then `ingest --apply-reviews` writes ONLY `parse_review` back (no
content change — safe on locked/curated entries). `repass` re-exports just the flagged entries
(`export --flagged`) with the reviewer's issues as prompt hints and re-parses them.

## Modules (building blocks — not the user entry point)

- `io.py` — the single home for shared IO: EntryFile read/write (atomic, via the model), batch-item
  load, LLM-JSON extraction, work-dir paths. **A strict leaf** (imports only stdlib + `entry_models`).
- `entry_models.py` — the `Entry` / `Rule` / `Extent` / `ParseReview` pydantic model + validators.
- `dates.py` — verbatim date strings → validated calendar windows.
- `rows.py` — load synopsis rows; `species.py` — species menu.
- `parse_context.py` — build the parse prompt (identity + bindable-boundary menu + species + regs).
- `review_exporter.py` — build the review prompt (reuses `parse_context.render_boundary_menu`, so the
  reviewer sees `id — label [kind]` + the entry's tributary state).
- `batch_exporter.py` — rows → batches (`--skip-existing` / `--only-changed` / `--flagged` row filters).
- `synth_responses.py` — rebuild `responses/` from current entries (for `review`).
- `dispatch.py` — run parse/review through the `claude` CLI (the credit step).
- `ingest.py` — validate + write EntryFiles + fill `parse_review` (`--apply-reviews`, `--review-only-locked`).
- `validate.py` — the same validation gate as a standalone self-check CLI for the parse agent.
- `prune_superseded.py` — remove bare `item_id` entries once their per-row replacements exist.

## Safety notes

- Every EntryFile write goes through `io.write_entryfile` — validated through the model, sorted by
  `entry_id`, and written atomically (temp + `os.replace`). It is byte-identical to the current files,
  so re-runs don't churn.
- A `locked` entry's content is never overwritten by a re-parse; `--review-only-locked` refreshes only
  its `parse_review`.
- `entries/region-*.json` is the durable progress. The `data/generated/regs/parse/` work dir is transient — deleting
  it never loses parsed entries.
