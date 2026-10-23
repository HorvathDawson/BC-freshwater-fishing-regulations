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
  Done (lane wf_64942248-585, e768ce63 5f1eec34 84d5919d): all 305 triaged — ~35 literal pins now read taps.json /
  rule keys / the province store (blob.test's table → conformance/blob.release.test.ts, Python + zlib); the page v36
  oracle's rix is mapped to the store by key (`oracleRixOver`: 273 settle/card/describe failures were T89's dropped
  Endako 7A rule renumbering every later rix); Rainbow Alley on Babine parts 1-2 (T89) recorded as ORACLE_PREDATES;
  klmb_1010 D19 (a) → D19_GROUPS. Still owed: re-cut page_v36.html's blocks + hydrate.json (human: `page_data --html`,
  then `node app/design/regs-v11/port/hydrate.mjs`), which retires ORACLE_PREDATES.
- Page v36 oracle refresh (2026-10-22): `python -m pipeline.deliver.answers.reference.page_data --html
  pipeline/deliver/answers/reference/page_v36.html` + `node app/design/regs-v11/port/hydrate.mjs` on r-36fbc5326506
  produced a 3-line page diff but broke 275 card/settle/describe parity tests (vedder_515, chw*, wilkes, x15p*), so it
  was reverted. The parity tests map oracle rule numbers to the store by key (5f1eec34) and pass on the old page; a
  refresh needs its own lane: find why the regenerated page disagrees (subset build: 42 waters / 727 rules; the
  translation assuming the old numbering; T80 groups), then drop settle's ORACLE_PREDATES entries. Decide with the
  owner's call 2 (archive the prototype answers reference) — the refresh may not be worth doing at all.
