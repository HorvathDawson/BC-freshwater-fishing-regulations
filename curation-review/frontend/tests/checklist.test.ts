// T532: the checklist screen's logic — run by `node --test` from pipeline/tests/test_review_dfo.py.
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkBody, checkStatus, groupLine, groupSave, hintWords, openRows, pinsFc, problemOf, step, untickable, where,
  wordDiff, wrongNote } from "../src/checklist.ts";
import type { CheckPlace, CheckRow, DfoGroupMember, PlaceCheck } from "../src/types.ts";

const row = (n: number, kind: CheckRow["kind"], state: CheckRow["state"], coord?: [number, number]): CheckRow =>
  ({ n, kind, state, quote: `q${n}`, ours: `o${n}`, reason: state === "ok" ? "" : "why", anchor: coord ? { coord } : {} });
const ROWS = [row(1, "water", "ok", [1, 2]), row(2, "from", "ok", [3, 4]), row(3, "also", "bad"),
              row(4, "except", "bad", [3, 4])];
const PC: PlaceCheck = { rows: ROWS, digest: "0123456789abcdef", summary: { ok: 2, bad: 2, worst: "bad" },
                         check: "unchecked" };
const who = { reviewer: "dawson", seconds: 3 };

test("All correct waits for every ✗ row to be ticked", () => {
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

// owner 2026-11-09: older wordings are never assumed to match — each starts unticked; a ticked one takes the check,
// and this place's extent when its own differs; the save line names exactly what it writes
const SEEN = { extents: [{ op: "whole" }], tributaries: null } as unknown as DfoGroupMember["extent"];
const mem = (id: string, o: Partial<DfoGroupMember> = {}): DfoGroupMember => ({
  id, why: null, this: false, water: "W", text: `text ${id}`, status: "historic", verify: "unmarked" as DfoGroupMember["verify"],
  same_extent: true, extent: SEEN, cannot_change: [], cannot_take: [], digest: `d${id}`.padEnd(16, "0"), held: null,
  check: "unchecked", ...o });
const GROUP = [mem("a", { this: true }), mem("b"), mem("c", { same_extent: false }), mem("d", { same_extent: false,
  cannot_change: ["extent 1 (within) holds within_area"] })];

test("older wordings start unticked: the save writes this place only", () => {
  assert.deepEqual(groupSave("a", GROUP, new Set(), false, true), { targets: ["a"], members: {} });
  assert.equal(groupLine(GROUP, new Set(), false, true), "Saves on this wording only. 3 older wordings not ticked: they stay unchecked.");
  const body = checkBody({ source: "dfo", id: "a", seen: SEEN }, PC, "ok", new Set([3, 4]), new Set(), "", who,
                         groupSave("a", GROUP, new Set(), false, true));
  assert.equal(body.targets, undefined);
  assert.equal(body.members, undefined);
});

test("ticked wordings are listed with what was seen, and the line names which take this extent", () => {
  const g = groupSave("a", GROUP, new Set(["b", "c", "d"]), false, true);
  assert.deepEqual(g.targets, ["a", "b", "c"]);                     // d holds within_area: never written
  assert.deepEqual(g.members.c, { extent_before: SEEN, digest: "dc".padEnd(16, "0") });
  assert.equal(groupLine(GROUP, new Set(["b", "c", "d"]), false, true),
    "Also saves on 2 older wordings: “text b”, “text c”; 1 takes this extent: “text c”. 1 older wording not ticked: it stays unchecked.");
  const body = checkBody({ source: "dfo", id: "a", seen: SEEN }, PC, "ok", new Set([3, 4]), new Set(), "", who, g);
  assert.deepEqual(body.targets, ["a", "b", "c"]);
  assert.deepEqual(Object.keys(body.members ?? {}), ["b", "c"]);
  assert.match(untickable(GROUP[3], false, true), /within_area/);
  assert.equal(untickable(GROUP[3], false, false).length > 0, true);
  assert.equal(untickable(mem("e", { held: "your save of 2026-11-09 has not landed yet: …" }), false, true).length > 0, true);
  assert.equal(untickable(mem("e", { held: "x" }), false, false), "");     // the editor's save replaces its own save
  // a change (an extent fix): every ticked wording takes it
  assert.match(groupLine(GROUP, new Set(["b"]), true, false), /1 takes this extent: “text b”/);
});

test("an older wording's words mark what differs from the shown place's", () => {
  const got = wordDiff("From the Vedder Crossing bridge downstream", "From Vedder Crossing to the Fraser downstream");
  assert.deepEqual(got.filter((t) => t.diff).map((t) => t.w), ["to", "the", "Fraser"]);
  assert.deepEqual(got.map((t) => t.w).join(" "), "From Vedder Crossing to the Fraser downstream");
});
