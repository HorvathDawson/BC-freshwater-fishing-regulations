# T1002 — Test-anchor scanners: an `absent` rule for pipeline/tests/anchors.py; the record-equality and toBeUndefined gaps; part numbers after short template slots

Status: IN PROGRESS — items 1 and 2 decided as measured gaps in the scanner headers (lane L6, 2026-11-22); left (Claude): items 3, 4 and 5.

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
4. **More `absent` spellings** (T768 review, 2026-11-22; f89b7b093 added four): `Object.keys(x)` checked with
   `.toHaveLength(n)`, keys read through a variable, and `expect.not.objectContaining`.
5. **PROVENANCE is too wide:** it excuses all of `tools/regs-fixture.test.ts` from the part-end proof's count, not just
   its byte-rebuild test. Narrow it to that test's name.

## Done when
Each of the three has either a rule with an accepted case and a near-miss test (the header's protocol), or a stated,
measured reason in the scanner header why it stays unscanned.

## Lane L6, 2026-11-22
- **1, the pipeline half: a DECIDED GAP**, stated and measured in `pipeline/tests/anchors.py`'s header ("NO `absent`
  KIND HERE"). Measured on the live tree: in the files the scanner calls live, 387 `assert "<str>" not in …` in 114
  files and 84 typed key-set equalities (`set(x) == {…}`, `sorted(x) == […]`, `x.keys() == …`). Read through, nearly all
  are value pins (rule ids absent from a binding, species codes, words, section ids, group members): Python's `in` asks a
  dict for a key and a list or set for a member, and the AST does not say which. What catches a field assertion
  instead: the release's follows run those tests on the candidate (`RELEASE_DIR`) before the promote.
- **2, record pins and `toBeUndefined`: a DECIDED GAP**, now labelled so in the app scanner's `absent` header (the
  measurement was already there: 3 reads, about 35 record pins, none a column). No rule: no column list is read there,
  and a syntax scan cannot tell them from value pins.
- **Left (Claude): 3** (a part number after a short slot: `part ${i} ${p}` — the number's slot is `p`, one space after
  another slot, so neither the 3-letter rule nor the slot name sees it; a sound rule needs the callback's name
  (`onPart`) or the slot's position after a `part` slot), **4** (more `absent` spellings), **5** (PROVENANCE narrowed
  to the byte-rebuild test `6. deterministic: …`: needs a part-end proof run to see which `regs-fixture.test.ts` tests
  the mutant fails besides 6, or the narrowed list breaks the proof — not run here).
