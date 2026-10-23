# T97 — Curation review, phases 1–5: marks that last, one vintage, a review screen, queue, gate

Status: OPEN — owner approved PLAN.md 2026-10-23 ("The review app changes seem good"), all §6 defaults (Q5, Q34–Q41).
This ticket is now PHASE 1 (marks that last) plus the umbrella; phases 2–5 are T133–T136; a later salmon pass is T132.

Source: PLAN.md §4 phases 1–5, §5 (how review fits the pipeline), §6 Q2–Q9 (a3f66b7c, T66).

## Work (phase 1 here; 2 → T133, 3 → T134, 4 → T135, 5 → T136)
1. Marks store `data/curated/regulations/review/marks.jsonl` + one fingerprint module (text / meaning / binding /
   answer), typed stale reasons, `carry_marks`; retire `verification.py`.
2. One vintage on screen: per-entry `entry_digests` in reach report, bundle meta and release manifest; "awaiting build".
3. Review screen separate from fixing (review mode default, `--fix` owner-only), keys, evidence crops, split cards.
4. `precheck.py`, one risk-ordered queue, ticket export, "commit marks" touching only the marks file, `cut_pick` editor.
5. Gate: AGENTS 16 parity over all 3,406 rules via heavy.sh; a 30-entry pilot by another person.
Ingest: retire `--replace-edited` and the ledger; new ids only plus `--replace-entry <id>`.

## Owner 2026-10-23 — splits mid-review
"Make sure we could add splits and rebuild then continue review with the new splits etc." The binding fingerprint and
`carry_marks` (phase 1) must make this work: a new or moved split stales exactly the marks whose binding moved (reason
"binding moved"), keeps every other mark, and the new split gets its own `split`-scope mark to confirm. Phase 1's
acceptance adds: "add a split on a fixture water → only that water's entries go stale, with that reason". The rest of
the loop (pending-build panel, reload without restart, S0 queue, end-to-end check) is in T133–T136.

## Acceptance
Each phase's "Accept when" in PLAN.md (0 false-stale marks on a no-op normalisation; ≤ 1.5 min per simple entry; parity
100 % or every difference explained).

Depends: T96 (done). Size: M (phase 1: 3–4 days; all phases 3–4 weeks). Owner: no (plan approved).
