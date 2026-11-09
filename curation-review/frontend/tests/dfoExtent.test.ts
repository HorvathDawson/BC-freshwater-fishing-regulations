// T525: what a DFO mark does to the place's curated extent — run by `node --test` (Node strips the types), from
// pipeline/tests/test_review_dfo.py. Real places: Morice Lake (whole, its tributaries from the DFO title) and the
// Cowichan from 66 Mile Trestle (between two cuts).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  addPiece, dropPiece, effectOf, heldBy, incompletePieces, markBody, placeAnswer, previewOf, proposesCuts, saveWords, tribWords, withPiece,
} from "../src/dfoExtent.ts";
import { extentParts, fixParts, partsText, piecesParts } from "../src/extentWords.ts";
import type { DfoExtentNow, ExtentFix } from "../src/types.ts";

const between = (a: string, b: string, trib: boolean | null = null): ExtentFix => ({
  scope: "entry", op: "between", items: ["gnis:12227"], tributaries: trib,
  points: [{ kind: "split", split_id: a }, { kind: "split", split_id: b }] });
const COWICHAN = between("cowichan_river__cnr_trestle", "cowichan_river__white_triangle_boundary_sign_loc");
const GREENDALE = between("cowichan_river__cnr_trestle", "cowichan_river__greendale_trestle");
const SUMAS: ExtentFix = { scope: "entry", op: "downstream_of", items: ["gnis:26216"], tributaries: null,
                           points: [{ kind: "split", split_id: "sumas_river__barrow_town_pump_station" }] };
const now = (pieces: ExtentFix[]): DfoExtentNow => ({
  pieces, cannot_change: [], tributaries: null,
  seen: { extents: [{ op: "between", splits: ["cowichan_river__cnr_trestle", "cowichan_river__white_triangle_boundary_sign_loc"],
                      item_id: "gnis:12227" }], tributaries: null } });
const base = { target: "1:cowichan-river:from-66-mile-trestle-downstream-to", answers: { T: "y", R: "y" } as const,
               problem: "", note: "", reviewer: "dawson", seconds: 3, carried: [], targets: [] };

test("the effect a change has on the extent now: keep, replace, add a piece", () => {
  assert.equal(effectOf([COWICHAN], [COWICHAN]), "keep");
  assert.equal(effectOf([COWICHAN], [{ ...COWICHAN, items: ["gnis:12227"] }]), "keep");
  assert.equal(effectOf([COWICHAN], [GREENDALE]), "replace");
  assert.equal(effectOf([COWICHAN], [COWICHAN, SUMAS]), "add");
  assert.equal(effectOf([COWICHAN, SUMAS], [SUMAS, COWICHAN]), "keep", "a union has no order");
  assert.equal(effectOf([COWICHAN, SUMAS], [COWICHAN]), "replace", "dropping a piece replaces");
  assert.equal(effectOf([COWICHAN], [{ ...COWICHAN, tributaries: true }]), "replace", "a tributary call is a change");
  assert.equal(effectOf([], [GREENDALE]), "replace", "setting an extent where there is none");
  assert.equal(effectOf([COWICHAN], []), "keep", "no pieces change nothing");
});

test("pieces are added, replaced and dropped; a new piece takes the place's call (T524 review)", () => {
  assert.deepEqual(withPiece([COWICHAN], 0, GREENDALE), [GREENDALE]);
  assert.deepEqual(withPiece([COWICHAN], 1, SUMAS), [COWICHAN, SUMAS]);
  assert.deepEqual(dropPiece([COWICHAN, SUMAS], 0), [SUMAS]);
  const off = { ...COWICHAN, tributaries: false };
  assert.equal(addPiece([off], true)[1].tributaries, false, "piece 1 switched off: the new piece follows it");
  assert.equal(addPiece([], true)[0].tributaries, true);
  assert.deepEqual(addPiece([COWICHAN], null)[1], { scope: "entry", op: "downstream_of", points: [], items: [],
                                                     tributaries: null });
});

test("a piece's preview is the piece alone, or the place's union with it", () => {
  assert.deepEqual(previewOf([COWICHAN], 0, GREENDALE), GREENDALE);
  assert.deepEqual(previewOf([COWICHAN, SUMAS], 1, { ...SUMAS, op: "upstream_of" }), [COWICHAN, { ...SUMAS, op: "upstream_of" }]);
  assert.deepEqual(previewOf([COWICHAN], 1, SUMAS), [COWICHAN, SUMAS], "a new piece previews with the union");
});

test("keep, not sure and wrong never carry a fix; every mark names the extent it was made on", () => {
  for (const call of ["keep", "unsure", "wrong"] as const) {
    const body = markBody({ ...base, call, fixes: [GREENDALE], now: now([COWICHAN]) });
    assert.equal(body.extent_fix, undefined, call);
    assert.equal(body.new_splits, undefined, call);
    assert.deepEqual(body.extent_before, now([COWICHAN]).seen, call);
    assert.equal(body.answers.P, placeAnswer(call, false));
  }
  assert.deepEqual(markBody({ ...base, call: "keep", fixes: [COWICHAN], now: now([COWICHAN]) }).answers,
                   { T: "y", R: "y", P: "y" });
});

test("a change carries its pieces only when it changes the extent", () => {
  const same = markBody({ ...base, call: "change", fixes: [COWICHAN], now: now([COWICHAN]) });
  assert.equal(same.extent_fix, undefined);
  const moved = markBody({ ...base, call: "change", fixes: [GREENDALE], now: now([COWICHAN]) });
  assert.deepEqual(moved.extent_fix, [GREENDALE]);
  assert.equal(moved.answers.P, "y", "existing cuts: the extent is set (T210)");
  const cut = { ...GREENDALE, points: [GREENDALE.points[0], { kind: "new" as const, split_id: "n1" }] };
  const carried = [{ ref: "n1", anchor: "bridge" as const, name: "Greendale Trestle" }];
  const withCut = markBody({ ...base, call: "change", fixes: [cut], carried, now: now([COWICHAN]) });
  assert.equal(withCut.answers.P, "?", "a cut still to build");
  assert.deepEqual(withCut.new_splits, carried);
  const deferred = markBody({ ...base, call: "change", fixes: [GREENDALE], now: now([COWICHAN]), defer: true,
                              note: "later" });
  assert.equal(deferred.extent_fix, undefined);
  assert.equal(deferred.verdict, "deferred");
  assert.deepEqual(deferred.answers, { T: "y", R: "y" });
  const group = markBody({ ...base, call: "keep", fixes: [], now: now([COWICHAN]), targets: ["a", "b"] });
  assert.deepEqual(group.targets, ["a", "b"]);
});

test("the save is held, in words, until it can be made", () => {
  const ok = { call: "keep" as const, answers: { T: "y", R: "y" } as const, problem: "", note: "", effect: "keep" as const,
               cuts: false, cannotChange: [], extents: 1, incomplete: [] as string[] };
  assert.equal(heldBy(ok), "");
  assert.match(heldBy({ ...ok, answers: { T: "y" } }), /answer T and R/);
  assert.match(heldBy({ ...ok, call: null }), /pick your call/);
  assert.match(heldBy({ ...ok, extents: 0 }), /no extent to keep/);
  assert.match(heldBy({ ...ok, call: "change" }), /is the extent now/);
  assert.equal(heldBy({ ...ok, call: "change", cuts: true }), "", "proposing a cut alone is a change");
  assert.match(heldBy({ ...ok, call: "change", effect: "replace", cannotChange: ["holds within_area"] }), /cannot be changed/);
  assert.match(heldBy({ ...ok, call: "change", effect: "add", incomplete: ["piece 2 (downstream of): 1 of its 1 point not picked"] }),
               /finish your change first — piece 2 \(downstream of\): 1 of its 1 point not picked/);
  assert.match(heldBy({ ...ok, call: "wrong" }), /problem/);
  assert.equal(heldBy({ ...ok, call: "wrong", problem: "wrong extent", note: "ends at Greendale" }), "");
});

test("the save button says what it does", () => {
  assert.equal(saveWords("keep", "keep"), "Save: keep extent");
  assert.equal(saveWords("change", "keep"), "Save: keep extent");
  assert.equal(saveWords("change", "replace"), "Save: replace extent");
  assert.equal(saveWords("change", "add"), "Save: add a piece");
  assert.equal(saveWords("unsure", "keep"), "Save: not sure (keeps the extent)");
  assert.equal(saveWords("wrong", "keep"), "Save: wrong (keeps the extent)");
  assert.ok(proposesCuts([], [{ ref: "n1", anchor: "bridge" }]));
  assert.ok(!proposesCuts([COWICHAN], []));
});

test("a reach line never reads as excluding the tributaries it walks", () => {
  assert.equal(tribWords(true, false), " (with tributaries — not drawn; tick Show tributaries to draw)");
  assert.equal(tribWords(true, true), " (with tributaries)");
  assert.equal(tribWords(false, false), " (no tributaries)");
  assert.equal(tribWords(undefined, false), "");
});

// the server's own op table (`marks.OP_POINTS`), as GET /api/extent-fix/ops serves it
const OPS = { whole: 0, upstream_of: 1, downstream_of: 1, between: 2, within: 0, rest: 0, steelhead_waters: 0 };

test("a piece half made is named, with what it lacks (owner 11-09: the Chilliwack shot saved an empty piece)", () => {
  assert.deepEqual(incompletePieces([COWICHAN, SUMAS], OPS), []);
  const empty: ExtentFix = { scope: "entry", op: "downstream_of", points: [], items: [], tributaries: null };
  assert.deepEqual(incompletePieces([COWICHAN, empty], OPS), ["piece 2 (downstream of): 1 of its 1 point not picked"]);
  const half = { ...COWICHAN, points: [COWICHAN.points[0], { kind: "split" as const }] };
  assert.deepEqual(incompletePieces([half], OPS), ["piece 1 (between): 1 of its 2 points not picked"]);
  assert.deepEqual(incompletePieces([{ ...empty, op: "within" }], OPS), ["piece 1 (within an area): its area not picked"]);
  assert.deepEqual(incompletePieces([{ ...empty, points: [{ kind: "coord" }] }], OPS),
                   ["piece 1 (downstream of): 1 of its 1 point not picked"]);
  assert.deepEqual(incompletePieces([{ ...empty, points: [{ kind: "confluence", name: "Slesse Creek" }] }], OPS), []);
});

test("an extent reads by its cuts' labels; a cut with no label is its own part, never a bare id", () => {
  const label = (id: string) => ({ cowichan_river__cnr_trestle: "CNR Mile 66 Trestle" } as Record<string, string>)[id] ?? null;
  const item = (id: string) => (id === "gnis:12227" ? "Cowichan River" : id);
  const ex = { op: "between", splits: ["cowichan_river__cnr_trestle", "cowichan_river__nowhere"], item_id: "gnis:12227" };
  const parts = extentParts(ex, label, item);
  assert.deepEqual(parts, ["between", " ", "CNR Mile 66 Trestle", " and ", { cut: "cowichan_river__nowhere" },
                           " · on Cowichan River"]);
  assert.equal(partsText(parts), "between CNR Mile 66 Trestle and [cut cowichan_river__nowhere has no label] · on Cowichan River");
  assert.equal(partsText(piecesParts([COWICHAN, SUMAS], (f) => fixParts(f, label, item))),
               "piece 1: between CNR Mile 66 Trestle and [cut cowichan_river__white_triangle_boundary_sign_loc has no label]"
               + " · on Cowichan River · tributaries as the place has them + piece 2: downstream of"
               + " [cut sumas_river__barrow_town_pump_station has no label] · on gnis:26216 · tributaries as the place has them");
});
