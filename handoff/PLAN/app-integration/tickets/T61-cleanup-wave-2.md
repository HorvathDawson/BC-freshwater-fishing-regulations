# T61 — Cleanup wave 2: merge old pathways and duplicates

Status: OPEN. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 2" (items 2.x), with the owner's corrections: keep
stocking / bathymetry / salmon code as "prepared"; prove every script unneeded before deleting it.
Includes the ONE name normaliser (owner accepts a reviewed binding diff), retiring the dev bundle + riffle fixture
(N6), finishing P20 (no regs.sqlite, N5).

## Items (audit PLAN.md §2 wave 2; checked against git 2026-10-22)
- ALREADY DONE, do not redo: 2.5 water_continue made by the bundle (a6aa4d86); N7 deliver key covers the raw gpkg and
  clim.json (b274cd3a); N8 the atlas records the matches.json it cut at (f9ff2207).
- Open: 2.1–2.3 dev-bundle pathway + riffle fixture (N6; then delete riffle.html); 2.4 no regs.sqlite (N5); 2.6–2.7
  gauge logic homes; 2.8 one name normaliser (binding diff first); 2.9 varint writer; 2.10 tolerances; 2.11 one
  pipeline entry + **climatology as a build stage (N9)**; 2.12 tools/ layout; 2.13 silent `except Exception`;
  2.14 glossary into the bundle; 2.15–2.17 app helpers and tools' CDP copy; 2.19 colour-mode split documented (until
  T74); 2.20 Engine module; 2.21 one map checker.
- With 2.3/2.17: move `pipeline/deliver/answers/reference/page_v36.*`, `convert.py`, `page_data.py` out of `pipeline/`
  (owner call 2: archive the prototype answers reference; its v35 harness already went in 36a491ac) after repointing
  `conformance/src/regs/ruleSources.ts`, `taps.py`, `tools/check.mjs` and the `test_answers_*` readers.

Acceptance: each item's acceptance from the audit plan; binding diff reviewed where bindings move; strict + smoke.
