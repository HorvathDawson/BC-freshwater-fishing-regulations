# T63 — Cleanup wave 4: docs, the big test cleanup, unused-code guard

Status: OPEN. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 4" + owner: "a big test cleanup".
- Docs: one current map of the pipeline and the app; stale docs archived (git keeps history).
- Tests: remove tautologies and duplicates, keep whole-value + mutation-pinned tests, faster suites.
- knip in `pnpm check` (N23).

- From T81 (Q17 = no): `useSearchView(..., follow)` / `subjectOf(..., follow)` in app/packages/ui/src/search.ts are only
  called with `false` outside tests (search.ts:387) — drop the parameter and the dead branch. Also `stationsFor` (no
  non-test caller) and the --noUnusedLocals leftovers T70 listed.
