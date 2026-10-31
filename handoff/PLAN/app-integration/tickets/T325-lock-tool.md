# T325 — Lock tool: reviewed marks → one final curated file per domain

Status: OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

`python -m pipeline.regs.review.lock <domain>`: refuses until the domain's review is finished; writes the final file
(no review fields, schema in data-flow.md §4, read through `CURATED`); proves a side build is byte-identical; then CLOSES
the domain. Owner chose option (a): marks.jsonl is never edited — a closed-domains list
(`data/curated/review/closed.json`) that the exporter and review app skip (tab shows "locked"). Reopen = remove the
entry. Tests: refuse-when-open, byte-identical proof, closed domain skipped. Depends: T266. Size: M.
