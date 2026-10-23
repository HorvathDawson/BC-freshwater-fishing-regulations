# T97 — Curation review, phases 1–5: marks that last, one vintage, a review screen, queue, gate

Status: BLOCKED (owner) — the owner reads handoff/PLAN/curation-review/PLAN.md first (owner 2026-10-22: "proper before
someone puts effort in").

Source: PLAN.md §4 phases 1–5, §5 (how review fits the pipeline), §6 Q2–Q9 (a3f66b7c, T66).

## Work (split into one ticket per phase when the owner agrees)
1. Marks store `data/curated/regulations/review/marks.jsonl` + one fingerprint module (text / meaning / binding /
   answer), typed stale reasons, `carry_marks`; retire `verification.py`.
2. One vintage on screen: per-entry `entry_digests` in reach report, bundle meta and release manifest; "awaiting build".
3. Review screen separate from fixing (review mode default, `--fix` owner-only), keys, evidence crops, split cards.
4. `precheck.py`, one risk-ordered queue, ticket export, "commit marks" touching only the marks file, `cut_pick` editor.
5. Gate: AGENTS 16 parity over all 3,406 rules via heavy.sh; a 30-entry pilot by another person.
Ingest: retire `--replace-edited` and the ledger; new ids only plus `--replace-entry <id>`.

## Acceptance
Each phase's "Accept when" in PLAN.md (0 false-stale marks on a no-op normalisation; ≤ 1.5 min per simple entry; parity
100 % or every difference explained).

Depends: T96; owner answers Q5, Q34–Q41. Size: L (3–4 weeks). Owner: yes (approve the plan). Default: PLAN.md §6.
