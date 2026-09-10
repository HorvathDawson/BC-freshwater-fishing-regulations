# Where I left off

_Regenerate the live numbers any time — free, no credits:_

```bash
bash pipeline/regs/parsing/run_parse.sh status
```

## Resume the parse

```bash
bash pipeline/regs/parsing/run_parse.sh all      # parse, then the agent review pass
bash pipeline/regs/parsing/run_parse.sh parse    # parse only — cheaper, review later
```

As of 2026-09-10: **915 of 1393** synopsis rows parsed, **478 remaining** (~16 batches).
The last run stopped on a credit limit at batch 21 of 33.

**Nothing already parsed is re-sent.** The export drops every row already in EntryFiles, so the
count only goes down. Resuming is always safe and always cheaper than the run before it.

Knobs: `MODEL` (parse, default sonnet) · `REVIEW_MODEL` (default haiku) · `ESCALATE_MODEL`
(repass, default opus) · `CONCURRENCY` (3) · `BATCH_SIZE` (30).

After a review pass, re-parse only what the reviewer flagged:

```bash
bash pipeline/regs/parsing/run_parse.sh repass
```

## Manual curation review

```bash
bash curation-review/backend/run.sh                 # :8787
cd curation-review/frontend && npx vite             # :5173, proxies /api to :8787
```

If it says `Address already in use`, a backend is already running — reuse it, or stop it with
`pkill -f "uvicorn app:app"`.

## Still open

- `burton_creek__hwy_6_bridge` and `dutch_creek__dutch_creek_into_columbia_river` are the only two
  curated splits that do not resolve — rules naming them go to review.
- 22 curated splits bind only through an ALIAS (a gauge that landed on the same measure). That
  works everywhere, but it means the id stored is the gauge's, not the dam's.
- The DFO salmon corpus is still on the retired prose model.
