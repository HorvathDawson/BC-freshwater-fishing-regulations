// T755 follow-up (2026-11-15): why an extent cannot be edited, and the "old extent still in force" flag — run by
// `node --test` (Node strips the types), from pipeline/tests/test_review_dfo.py.
import { test } from "node:test";
import assert from "node:assert/strict";
import { cannotEditOf } from "../src/dfoExtent.ts";
import { oldExtentWords } from "../src/checklist.ts";

test("an extent the editor can change has no reason; one it cannot says why and what to do", () => {
  assert.equal(cannotEditOf({ cannot_change: [], cannot_edit: null }), null);
  assert.equal(cannotEditOf(null), null);
  const why = { why: ["The extent is held only inside Region 6, …"], can: "You can still confirm it as it is" };
  assert.deepEqual(cannotEditOf({ cannot_change: ["extent 1 (whole) holds within_area …"], cannot_edit: why }), why);
  // an older backend (no plain words): its raw reason, still with what to do — never a hidden button with no reason
  const raw = cannotEditOf({ cannot_change: ["extent 1 (whole) holds within_area …"] });
  assert.deepEqual(raw?.why, ["extent 1 (whole) holds within_area …"]);
  assert.match(raw?.can ?? "", /confirm it as it is/);
});

test("the old-extent flag says what the last save said and that the older extent holds", () => {
  assert.equal(oldExtentWords({ verdict: "wrong", date: "2026-11-15", rides: null }),
    "old extent still in force: your last save (2026-11-15) said wrong with no fix, so the older extent is still what this place has");
  assert.match(oldExtentWords({ verdict: "unsure", date: null, rides: "1:river:a" }), /said not sure .* it rides \(1:river:a\)/);
});
