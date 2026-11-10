// T532: the checklist screen's logic — run by `node --test` from pipeline/tests/test_review_dfo.py.
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkBody, checkStatus, hintWords, openRows, pinsFc, problemOf, step, where, wrongNote } from "../src/checklist.ts";
import type { CheckPlace, CheckRow, PlaceCheck } from "../src/types.ts";

const row = (n: number, kind: CheckRow["kind"], state: CheckRow["state"], coord?: [number, number]): CheckRow =>
  ({ n, kind, state, quote: `q${n}`, ours: `o${n}`, reason: state === "ok" ? "" : "why", anchor: coord ? { coord } : {} });
const ROWS = [row(1, "water", "ok", [1, 2]), row(2, "from", "ok", [3, 4]), row(3, "also", "unsure"),
              row(4, "except", "bad", [3, 4])];
const PC: PlaceCheck = { rows: ROWS, digest: "0123456789abcdef", summary: { ok: 2, unsure: 1, bad: 1, worst: "bad" },
                         check: "unchecked" };
const who = { reviewer: "dawson", seconds: 3 };

test("All correct waits for every ? and ✗ row to be ticked", () => {
  assert.deepEqual(openRows(ROWS, new Set()).map((r) => r.n), [3, 4]);
  assert.equal(hintWords(ROWS, new Set([3])), "1 row not matching — tick each one you have checked, or report it.");
  assert.equal(hintWords(ROWS, new Set([3, 4])), "Every row matches.");
});

test("a DFO check never carries an extent fix and names the extent it was made on", () => {
  const seen = { extents: [{ op: "whole" }], tributaries: null };
  const ok = checkBody({ source: "dfo", id: "2:x", seen }, PC, "ok", new Set([3, 4]), new Set(), "", who);
  assert.equal(ok.scope, "dfo");
  assert.equal(ok.verdict, "verified");
  assert.deepEqual(ok.answers, { P: "y" });
  assert.equal(ok.extent_fix, undefined);
  assert.deepEqual(ok.extent_before, seen);
  assert.deepEqual(ok.checklist!.rows.map((r) => [r.n, !!r.ticked, !!r.wrong]),
                   [[1, false, false], [2, false, false], [3, true, false], [4, true, false]]);
  const bad = checkBody({ source: "dfo", id: "2:x", seen }, PC, "wrong", new Set(), new Set([4]), "Vedder Crossing", who);
  assert.equal(bad.verdict, "wrong");
  assert.deepEqual(bad.answers, { P: "n" });
  assert.equal(bad.problem, "undrawn part / unresolved place");
  assert.equal(bad.note, "wrong: row 4 (except “q4” — ours: o4) · Vedder Crossing");
  assert.deepEqual(bad.checklist!.rows.filter((r) => r.wrong).map((r) => r.n), [4]);
});

test("a regulation check is a rule mark", () => {
  const b = checkBody({ source: "regs", entry_id: "r1:a@1-1", rule_id: "a.r1" }, PC, "ok", new Set([3, 4]), new Set(), "", who);
  assert.equal(b.scope, "rule");
  assert.equal(b.target, "r1:a@1-1");
  assert.equal(b.rule_id, "a.r1");
  assert.equal(b.extent_before, undefined);
  assert.equal(problemOf(ROWS, new Set([2, 4])), "split misplaced");
  assert.equal(wrongNote(ROWS, new Set([1]), ""), "wrong: row 1 (water “q1” — ours: o1)");
});

test("the status line says what the check in force holds", () => {
  assert.equal(checkStatus(PC), "Not checked yet.");
  assert.equal(checkStatus({ ...PC, check: "stale" }), "Its words or its extent changed since it was checked: check it again.");
  const done = { ...PC, check: "checked" as const, checked: { date: "2026-11-09", verdict: "verified", note: "",
    rows: [{ n: 1, kind: "water", state: "ok" as const }, { n: 2, kind: "x", state: "bad" as const, ticked: true }] } };
  assert.equal(checkStatus(done), "Checked 2026-11-09: all correct · 2 rows, 1 ticked by eye");
  assert.equal(checkStatus({ ...done, check: "wrong", checked: { ...done.checked, rows: [
    { n: 3, kind: "x", state: "bad", wrong: true }, { n: 5, kind: "y", state: "ok", wrong: true }] } }),
    "Checked 2026-11-09: wrong — rows 3, 5");
});

const place = (id: string, w: string): CheckPlace => ({ source: "dfo", id, water: w.toUpperCase(), water_key: w, region: "2",
  text: "", summary: PC.summary, check: "unchecked" });
test("stepping finishes a water before the next and counts places per water", () => {
  const q = [place("a1", "a"), place("a2", "a"), place("b1", "b")];
  assert.equal(step(q, "a2", 1), "b1");
  assert.equal(step(q, "a1", -1), null);
  assert.equal(step(q, null, 1), "a1");
  assert.deepEqual(where(q, "a2"), { pos: 2, total: 3, water: "A", onWater: 2, ofWater: 2 });
});

test("one numbered pin per row with a point; rows on one point sit side by side", () => {
  const fc = pinsFc(ROWS, 4);
  assert.deepEqual(fc.features.map((f) => f.properties!.n), ["1", "2", "4"]);
  assert.equal(fc.features[2].properties!.on, true);
  assert.notDeepEqual((fc.features[1].geometry as GeoJSON.Point).coordinates, (fc.features[2].geometry as GeoJSON.Point).coordinates);
});
