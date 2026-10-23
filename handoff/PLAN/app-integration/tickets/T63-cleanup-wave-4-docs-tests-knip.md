# T63 — Cleanup wave 4: docs, the big test cleanup, unused-code guard

Status: OPEN. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 4" + owner: "a big test cleanup".
- Docs: one current map of the pipeline and the app; stale docs archived (git keeps history).
- Tests: remove tautologies and duplicates, keep whole-value + mutation-pinned tests, faster suites.
- knip in `pnpm check` (N23).

- From T81 (Q17 = no): `useSearchView(..., follow)` / `subjectOf(..., follow)` in app/packages/ui/src/search.ts are only
  called with `false` outside tests (search.ts:387) — drop the parameter and the dead branch. Also `stationsFor` (no
  non-test caller) and the --noUnusedLocals leftovers T70 listed.
- Vintage pins (2026-10-22, three rebuilds in a row broke them): live-release tests that hard-code answer keys, frame
  ids, section handles or counts (store.release, blob.test, inputs.release, edge.parity, harness 5., WaterScreen.unnamed)
  must derive the value from a stated source (taps.json / golden from the Python reference decoder, the manifest, the
  store) or name the property they test. A guard: no live-release test asserts a bare integer id literal without a
  comment naming why it is fixed. (Triage of the 305 failures on r-36fbc5326506 is lane wf_64942248-585.)
