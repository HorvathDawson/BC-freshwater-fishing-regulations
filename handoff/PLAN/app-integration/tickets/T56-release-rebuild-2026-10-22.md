# T56 — Release rebuild: Twin Lakes bindings + stream polygons + "See X" pointers

Status: IN PROGRESS (2026-10-22) — reach swapped (020f4ca8); deliver running (scratchpad t56.sh). Process ticket: one data rebuild ships the pipeline fixes merged today.

## What ships
- Twin Lakes 2-8 and 18 more multi-polygon lakes bind every polygon (matcher b9a832ca, catalogue ad77fda, Main Lake →
  Mine Lake f455228). Reach rerun into data/generated/reaches/full_twin + binding diff
  (handoff/PLAN/audit/binding-diff-twin.json).
- Stream-item polygons get their water's flow panel (669fd065..ac8a0e1c). The panel walk through polygons is HELD
  (T101, owner Q44) and does not ship in this rebuild.
- "See X" group entries as pointers (search "In the regulations" group, 45 pointers / 51 rows); member lakes under
  their own book names (77 water rows renamed — diff the "applies" lines); identical boat rule from two rows reads once
  (merged db08fe06 31619b6a edfd32f5).

## Steps (one heavy job at a time, scripts/heavy.sh)
1. Review the binding diff: only lake sections of the 19 stamped entries change; nothing on the Fraser, Stave,
   Columbia, Pitt or Nelson Slough. Then swap reaches/full_twin in (keep the old run until checked).
2. `deliver all`. Check the size gate (14,000,000 B gz) and the paint gate.
3. `pytest pipeline/tests/test_stream_polygon_panels.py` without UI_EXPORT_ALLOW_PREDATING.
4. Re-cut fixtures: taps.py, `pnpm -C app regs-fixture`, harness + goldenSlices with UPDATE_REGS_FIXTURES=1.
5. Strict `REGS_STORE_REQUIRED=1 pnpm -C app check` and smoke ONCE. Commit the dev fixture.

## Acceptance
Twin Lakes 2-8: both lakes show the closure in the app. Nicomen Slough and the Vedder Canal are coloured in Conditions.
Release committed and served; all gates green.
