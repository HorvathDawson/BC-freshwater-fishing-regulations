# T1002 — Test-anchor scanners: an `absent` rule for pipeline/tests/anchors.py; the record-equality and toBeUndefined gaps; part numbers after short template slots

Status: IN PROGRESS — items 1 and 2 decided as measured gaps in the scanner headers (lane L6, 2026-11-22); left (Claude): items 3, 4 and 5.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1002 | Test-anchor scanners: an `absent` rule for pipeline/tests/anchors.py; the record-equality and toBeUndefined gaps; part numbers after short template slots | IN PROGRESS — items 1, 2 decided as measured gaps (scanner headers); left (Claude): items 3, 4, 5 | T768 | T768's lane, 2026-11-22 |

Source: T768 / T993 plan item 10 (the deleted docs/research/t993-inventory.md, 0.8.5 stop 9; git history). The app scanner
(`app/tools/check-test-anchors.mjs`) gained an `absent` kind in ff946c86e; three gaps were left, each measured there.

Done (lane L6, 2026-11-22), both DECIDED GAPS stated and measured in the scanner headers:
1. The pipeline half: `pipeline/tests/anchors.py` header "NO `absent` KIND HERE" — on the live tree 387 `assert "<str>"
   not in …` in 114 files and 84 typed key-set equalities, nearly all value pins (the AST does not say dict key vs
   member). What catches a field assertion instead: the release's follows run those tests on the candidate
   (`RELEASE_DIR`) before the promote.
2. Record pins and `toBeUndefined`: labelled in the app scanner's `absent` header (3 reads, about 35 record pins, none a
   column); the follows run on the candidate and `app/tools/part-end-proof.mjs` catch it.

## What is left (Claude)
3. **Part numbers after a short-named slot.** The in-text rule needs a slot named `part` / `partIx` / `part_ix` with
   3+ letters before it. `onPart={(i, p) => calls.push(\`part ${i} ${p}\`)}` (`WaterMapView.test.tsx`) let
   "part gnis:8634 4" through (the part-end proof found it, fixed in 1716bcb87): the number's slot is `p`, one space
   after another slot, so neither the 3-letter rule nor the slot name sees it; a sound rule needs the callback's name
   (`onPart`) or the slot's position after a `part` slot.
4. **More `absent` spellings** (T768 review, 2026-11-22; f89b7b093 added four): `Object.keys(x)` checked with
   `.toHaveLength(n)`, keys read through a variable, and `expect.not.objectContaining`.
5. **PROVENANCE is too wide:** it excuses all of `tools/regs-fixture.test.ts` from the part-end proof's count, not just
   its byte-rebuild test (`6. deterministic: …`). Narrowing needs a part-end proof run to see which
   `regs-fixture.test.ts` tests the mutant fails besides 6, or the narrowed list breaks the proof — not run yet.

## Done when
Each of the three has either a rule with an accepted case and a near-miss test (the header's protocol), or a stated,
measured reason in the scanner header why it stays unscanned.
