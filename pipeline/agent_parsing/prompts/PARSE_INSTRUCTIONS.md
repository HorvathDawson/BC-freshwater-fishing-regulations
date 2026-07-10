# Agent parsing — subagent operator guide

This is a **thin** guide for dispatching one exported batch to a subagent. The
actual parsing rules are **not** repeated here — they are embedded in each
`batches/batch_NNN.prompt.txt` file (rendered from the canonical
`pipeline/parsing/prompt.txt` + `examples.json`). Hand that file to the
subagent verbatim.

## Model

Use **Opus 4.8** for the subagents. If you have a **1M-token context** window
available, prefer larger batches (see `--batch-size` in the README) — fewer
round trips and the model keeps all examples + rows in view at once.

## The contract

Each `batch_NNN.prompt.txt` asks for a **JSON array** where every element is:

```json
{ "index": <the item's index, unchanged>, "entry": { ...ParsedEntry... } }
```

Non-negotiables the ingest step enforces (invalid entries are rejected and left
pending — they are never silently accepted):

1. **`index` is copied verbatim** from the input item. It is the only key that
   maps a result back to the correct row. Never renumber or reorder it.
2. **`entry.regs_verbatim` is a character-for-character copy of the item's
   `raw_regs`** — same whitespace, punctuation, and `**` bold markers. This is
   an exact-equality check.
3. **Every input item gets exactly one output object.** No omissions, no
   duplicate indices.
4. **No Markdown fences, no prose** around the JSON array.

## Dispatch loop (per batch)

1. Read `batches/batch_NNN.prompt.txt`.
2. Send it to an Opus 4.8 subagent.
3. Save the subagent's raw JSON reply to `responses/batch_NNN.json`.
4. **Review** (recommended): render a review prompt and dispatch an independent
   reviewer subagent, then apply any fixes it reports:

   ```sh
   PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.review_exporter \
       responses/batch_NNN.json --out-dir <agent-dir>
   ```

   The reviewer confirms the `.validate()` dry-run passes AND audits content
   correctness + cross-row consistency (the spirit of the validators), returning
   a `{"verdict", "issues"}` report. Hand blocking/consistency issues back to the
   parsing agent, which fixes them and re-runs the dry-run until `failed : 0`.
   See `CHAT_INVOCATION.md` "Step 2.5" for the reviewer template + fix loop.
5. Ingest (see README) — first with `--dry-run` to review, then for real.

Failures reported by ingest stay pending; fix the offending entries and
re-ingest the same `responses/*.json`, or re-dispatch that batch.
