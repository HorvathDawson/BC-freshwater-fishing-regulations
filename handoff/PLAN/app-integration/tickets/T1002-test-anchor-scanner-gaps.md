# T1002 — Test-anchor scanners: an `absent` rule for pipeline/tests/anchors.py; the record-equality and toBeUndefined gaps; part numbers after short template slots

Status: OPEN (left by T768's lane, 2026-11-22).

Source: T768 / T993 plan item 10 (`audit-2026-11-21/t993-inventory.md`, 0.8.5 stop 9). The app scanner
(`app/tools/check-test-anchors.mjs`) gained an `absent` kind in ff946c86e; three gaps were left, each measured there.

## What is left
1. **The pipeline half.** `pipeline/tests/anchors.py` (same rule, AGENTS W46) has no `absent` kind: a pipeline test that
   follows a release may still assert a field's absence (`assert "f" not in row`, a typed key set). It was another
   lane's file during T768. Python's `not in` is also an everyday idiom, so measure the hits on the live tree first.
2. **What the app rule cannot see.** A whole release record typed (`expect(g.exit).toEqual({…})`, the exact 0.8.5
   stop 9 form) and `expect(x.f).toBeUndefined()` on a release field. Both are left alone because a syntax scan cannot
   tell them from legitimate value pins: 2026-11-22, about 35 record pins and 3 `toBeUndefined` reads in release-following
   blocks, and none of the three reads was a column. Either find a sound rule (type information? the release's own
   column list?) or record it as a decided gap in the scanner header. Meanwhile the release's follows run on the
   candidate, and `app/tools/part-end-proof.mjs` for part ends, are what catch it.
3. **Part numbers after a short-named slot.** The in-text rule needs a slot named `part` / `partIx` / `part_ix` with
   3+ letters before it. `onPart={(i, p) => calls.push(\`part ${i} ${p}\`)}` (`WaterMapView.test.tsx`) let
   "part gnis:8634 4" through. The part-end proof found it, not the scanner (fixed in 1716bcb87).

## Done when
Each of the three has either a rule with an accepted case and a near-miss test (the header's protocol), or a stated,
measured reason in the scanner header why it stays unscanned.
