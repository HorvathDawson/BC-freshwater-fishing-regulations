// owner 2026-11-09: a refused save kept in one browser outlived the owner's later saves from another (Little Qualicum,
// refused 11-02 with P y + a fix; saved since 11-02 unsure and 11-03 wrong). `supersededBy` says so; it never drops it.
import { test } from "node:test";
import assert from "node:assert/strict";
import { supersededBy, type Refusal } from "../src/refusals.ts";

const LQ = "1:little-qualicum-river:from-the-falls-in-little-qualicum";
const refused: Refusal = {
  key: `dfo|${LQ}|`, at: "2026-11-02T12:00:00", scope: "dfo", target: LQ,
  reasons: ["A proposed extent fix says the extent is wrong: answer P n (or ?) to record it."],
  mark: { scope: "dfo", target: LQ, answers: { T: "y", R: "y", P: "y" }, reviewer: "Dawson" },
};

test("a refusal whose item was saved on a later day reads superseded by that mark", () => {
  assert.deepEqual(supersededBy(refused, [{ scope: "dfo", rule_id: null, date: "2026-11-03", verdict: "wrong" }]),
                   { date: "2026-11-03", verdict: "wrong", sameDay: false });
});

test("a mark of the same day is said, with the doubt (a mark holds only its date)", () => {
  assert.deepEqual(supersededBy(refused, [{ scope: "dfo", rule_id: null, date: "2026-11-02", verdict: "unsure" }]),
                   { date: "2026-11-02", verdict: "unsure", sameDay: true });
});

test("an older mark, another scope or another rule supersedes nothing", () => {
  assert.equal(supersededBy(refused, [{ scope: "dfo", rule_id: null, date: "2026-11-01", verdict: "verified" }]), null);
  assert.equal(supersededBy(refused, [{ scope: "entry", rule_id: null, date: "2026-11-03", verdict: "verified" }]), null);
  assert.equal(supersededBy(refused, [{ scope: "dfo", rule_id: "r1", date: "2026-11-03", verdict: "verified" }]), null);
  assert.equal(supersededBy(refused, []), null);
});
