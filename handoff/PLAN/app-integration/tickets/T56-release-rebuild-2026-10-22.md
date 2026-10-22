# T56 — Release rebuild: Twin Lakes bindings + stream polygons + "See X" pointers

Status: IN PROGRESS (2026-10-22). Process ticket: one data rebuild ships the pipeline fixes merged today.

## What ships
- Twin Lakes 2-8 and 18 more multi-polygon lakes bind every polygon (matcher ae5554fc, catalogue ad77fda, Main Lake →
  Mine Lake f455228). Reach rerun into data/generated/reaches/full_twin + binding diff
  (handoff/PLAN/audit/binding-diff-twin.json).
- Stream-item polygons get their water's flow panel (5f9af9a7..3462a565), and the panel walk reads the water's kind
  (lane wf_3976eddc-bba, when merged).
- "See X" group entries as pointers; identical rules read once (lane wf_e4e2b70f-f21, when merged).

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
