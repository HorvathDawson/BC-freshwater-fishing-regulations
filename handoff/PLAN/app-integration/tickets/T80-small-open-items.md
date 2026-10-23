# T80 — Small open items (audit N22, D29, HANDOFF-ui)

Status: OPEN.

Each item closes with a fix + test, or a recorded "no" with the reason:
1. S1 photo minors (implement_progress): draft photos named after the first draft id; the web picker hangs where the
   input has no "cancel" event (Safari < 16.4); unit words "m" and "m3/s" in ui/leadUp.ts instead of words.ts; a second
   web photo-store instance does not revoke an object URL on delete.
2. The page-bug mutation "count comparison drops a 2-fish release" lost its Kitimat witness
   (test_answers_rows_fixes.py: "needs another water"). Find another witness water.
3. D19: klmb_1010's pooled row is drawn on no card path (`core/regs/keep.ts`, excluded from Keep parity). Fix
   data-side: a pipeline `group` for the row, then remove the exclusion (needs a deliver).
4. "Map → Conditions flash on tab switch" (HANDOFF-ui; one frame of the wrong colouring). Reproduce; fix or close.
5. Whole "Kootenay Lake" not a search result (log 2026-10-21). Probably fixed (search.test.ts); verify and close.
6. A real CI run on a scratch branch has never been done. Do one.

Acceptance: each item is a line here marked fixed (commit + test) or declined (reason).
Depends: none (3 needs a deliver). Size: M. Owner: delegated. Default for D19: (a) a pipeline-side group.
