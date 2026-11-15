// owner 2026-11-15 (marks 2583, 2586, 2651; Atnarko marked twice): the DFO editor's tributary call in plain controls,
// "all the other places" with no water, the earlier mark said against a save, and DFO's words against the extent —
// warnings only. Run by `node --test` (Node strips the types).
import { test } from "node:test";
import assert from "node:assert/strict";
import { earlierDiff, sameTribCall, tribChoiceOf, tribWarnings, withOp, withTribChoice } from "../src/dfoExtent.ts";
import { fixParts, partsText } from "../src/extentWords.ts";
import type { ExtentFix } from "../src/types.ts";

const bella: ExtentFix = { scope: "entry", op: "whole", points: [], items: ["gnis:11611"], tributaries: true };

test("the four tributary choices set the place's fields, and read back", () => {
  const only = withTribChoice(bella, "tributaries_only");
  assert.deepEqual([only.tributaries, only.tributaries_only], [true, true]);
  assert.equal(tribChoiceOf(only), "tributaries_only");
  const water = withTribChoice({ ...only, tributary_excludes: ["gnis:17209"] }, "water_only");
  assert.deepEqual([water.tributaries, water.tributaries_only, water.tributary_excludes], [false, false, []]);
  assert.equal(tribChoiceOf(withTribChoice(bella, "as_now")), "as_now");
  assert.equal(tribChoiceOf(withTribChoice(bella, "water_and_tributaries")), "water_and_tributaries");
});

test("one call for every piece of the place", () => {
  const two = sameTribCall([bella, { ...bella, items: ["gnis:1"], tributaries: null }],
                           { ...bella, tributaries_only: true, tributary_excludes: ["gnis:17209"] });
  assert.ok(two.every((f) => f.tributaries_only === true && f.tributary_excludes?.[0] === "gnis:17209"));
});

test("“all the other places on this water” never carries a water (Qualicum, 2651)", () => {
  assert.deepEqual(withOp({ ...bella, items: ["gnis:27804"] }, "other_places").items, []);
  assert.deepEqual(withOp(bella, "between").items, ["gnis:11611"]);
});

test("the extent in words says tributaries only and what it leaves out", () => {
  const f = { ...bella, tributaries_only: true, tributary_excludes: ["gnis:17209"] };
  assert.equal(partsText(fixParts(f, () => null, (i) => (i === "gnis:17209" ? "Atnarko River" : "Bella Coola River"))),
               "the whole water · on Bella Coola River · the tributaries only (not the water itself) · except the tributaries Atnarko River");
});

test("the earlier mark is said against this save: what differs, or nothing", () => {
  const words = (f: ExtentFix) => f.op;
  assert.deepEqual(earlierDiff([bella], [bella], words), []);
  assert.deepEqual(earlierDiff([], [bella], words), []);
  assert.deepEqual(earlierDiff([bella], [{ ...bella, tributaries: false }], words),
                   ["tributaries: earlier “the water and its tributaries”, now “the water only”"]);
  assert.deepEqual(earlierDiff([bella], [{ ...bella, tributary_excludes: ["gnis:17209"] }], words),
                   ["the excepted tributaries differ"]);
});

test("DFO's words against the extent: a warning line each, never a block", () => {
  const w = { only: true, with: true, without: false, names: ["Atnarko River"], excepted: ["gnis:17209"] };
  assert.deepEqual(tribWarnings(w, { walks: true, only: false, excepted: [] }, (i) => (i === "gnis:17209" ? "Atnarko River" : i)), [
    "DFO's words are on the tributaries only; this extent holds the water itself too.",
    "DFO's words leave out Atnarko River; this extent does not except it."]);
  assert.deepEqual(tribWarnings(w, { walks: true, only: true, excepted: ["gnis:17209"] }), []);
  const said = { only: false, with: true, without: false, names: [], excepted: [] };
  assert.deepEqual(tribWarnings(said, { walks: false, only: false, excepted: [] }),
                   ["DFO's words take the tributaries in; this extent has none."]);
  const main = { only: false, with: false, without: true, names: [], excepted: [] };
  assert.deepEqual(tribWarnings(main, { walks: true, only: false, excepted: [] }),
                   ["DFO's words leave the tributaries out (main stem / excluding tributaries); this extent walks them."]);
  assert.deepEqual(tribWarnings(null, { walks: true, only: false, excepted: [] }), []);
});

test("an end at the tidal boundary is an open end, never a cut", async () => {
  const { toTidalEnd } = await import("../src/dfoExtent.ts");
  const a = { kind: "split" as const, split_id: "a" }, b = { kind: "split" as const, split_id: "mouth" };
  assert.deepEqual(toTidalEnd({ ...bella, op: "between", points: [a, b] }, 1), { ...bella, op: "downstream_of", points: [a] });
  assert.deepEqual(toTidalEnd({ ...bella, op: "upstream_of", points: [b] }, 0), { ...bella, op: "whole", points: [] });
  assert.equal(toTidalEnd({ ...bella, op: "downstream_of", points: [a] }, 0), null);
});
