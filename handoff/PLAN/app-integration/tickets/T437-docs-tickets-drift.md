# T437 — Docs and tickets: drift, stale statuses, one START-HERE

Status: DONE 2026-11-06 — d9a72c206. check-tickets compares file vs ledger status (13 aligned; IN REVIEW defined);
AGENTS 16/17/18/39/59 true; one START-HERE (5 NEXT-SESSION pages + 2 plans deleted); tsbuildinfo untracked; copilot file a
pointer. Not done (recorded): tests.md §2 kept (`tools/regs-walk.mjs` names its checks); archive/ pointers (AGENTS header,
rule 21/37, `classify.py:17`, …) are T335's; findings 11 (golden.json freeze, owner), 12, 14 (test shapes) left open.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: tests 5, 6, 8–12, 14, 16, 18–22; review-deploy 9, 10, 16; `*.verified.md` has the evidence per finding).

## What happens
12 ticket file-vs-ledger status mismatches (e.g. T322, T390: PARTIAL in file, DONE in ledger); "IN REVIEW" is
accepted by check-tickets but undefined in the README. AGENTS rules name a missing side build and the deleted
`check.mjs`; AGENTS header + rule 37 point at archive/ (T335 deletes it); AGENTS 59's text vs
`test_one_data_flow.py:44-53`; `classify.py:17` takes its policy from an archive doc; a doc names
`DFO_UNMATCHED_LEADS` at the wrong path; `OWNER-TODO.md:65` links RELEASE-PLAN-0.5.0; `.github/copilot-
instructions.md` is boilerplate contradicting AGENTS.md; tracked `tsconfig*.tsbuildinfo`.

## Fix
check-tickets compares file Status with the ledger row (and defines IN REVIEW); fix the 12; AGENTS rules made true;
one `handoff/PLAN/START-HERE.md` pointing at the newest NEXT-SESSION; delete the safe list (`NEXT-SESSION.md`,
`-0.5.0`, `-BATCH2`, `-UI`, `-after-0.5.0`, `proposed-tickets-2026-10-28.md`, `RELEASE-PLAN-0.5.0.md` after its
link); keep `IMPLEMENT-HANDOFF.md`, `tests.md` (trim §2), `implement_progress.md` (repoint first); untrack
tsbuildinfo; replace the copilot file with a pointer to AGENTS.md.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
