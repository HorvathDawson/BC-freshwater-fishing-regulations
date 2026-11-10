// T532: the checklist screen's logic — run by `node --test` from pipeline/tests/test_review_dfo.py.
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkBody, checkStatus, chipOf, earlierWords, groupHeading, groupLine, groupSave, hintWords, needWords,
  needsExtent, openRows, pinsFc, problemOf, progressWords, sameAsOptions, step, tickAll, untickable, waterGroups, where,
  wordDiff, wrongNote } from "../src/checklist.ts";
import type { CheckPlace, CheckRow, DfoGroupMember, PlaceCheck, WaterPlace } from "../src/types.ts";

const row = (n: number, kind: CheckRow["kind"], state: CheckRow["state"], coord?: [number, number]): CheckRow =>
  ({ n, kind, state, quote: `q${n}`, ours: `o${n}`, reason: state === "ok" ? "" : "why", anchor: coord ? { coord } : {} });
const ROWS = [row(1, "water", "ok", [1, 2]), row(2, "from", "ok", [3, 4]), row(3, "also", "bad"),
              row(4, "except", "bad", [3, 4])];
const PC: PlaceCheck = { rows: ROWS, digest: "0123456789abcdef", summary: { ok: 2, bad: 2, worst: "bad" },
                         check: "unchecked" };
const who = { reviewer: "dawson", seconds: 3 };

test("All correct waits for every ✗ row to be ticked", () => {
  assert.deepEqual(openRows(ROWS, new Set()).map((r) => r.n), [3, 4]);
  assert.equal(hintWords(ROWS, new Set([3])), "1 row not matching — tick the ✗ box on each one you have checked, or use “Report a problem with it”.");
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

// ---- owner 2026-11-09 (after trying it): earlier work counts, a no-extent place leads with the fix, the water in sight

test("a verified mark made before the checklist reads as done; any other earlier mark says what it was", () => {
  const earlier = { date: "2026-11-02", status: "verified", note: "" };
  assert.equal(checkStatus({ ...PC, check: "earlier", earlier }), "Verified earlier (2026-11-02), before the checklist.");
  assert.equal(checkStatus({ ...PC, check: "unchecked", earlier: { date: "2026-11-02", status: "unsure", note: "which bridge?" } }),
    "Not checked yet. Your earlier mark (2026-11-02) was: not sure — which bridge?.");
  assert.match(earlierWords({ date: "2026-11-02", status: "stale", note: "" }), /no longer holds/);
  assert.equal(checkStatus({ ...PC, check: "unchecked" }), "Not checked yet.");
  assert.equal(progressWords({ done: 88, total: 287, left: 199 }), "88 of 287 done · 199 left");
});

test("a place with no extent says what it needs", () => {
  const need = { op: "between", points: 2, have: 0, bound: false, missing: 2 };
  assert.equal(needsExtent("needs_cut", ROWS), true);
  assert.equal(needsExtent("unbound", ROWS), true);
  assert.equal(needsExtent("bound", ROWS), false);
  assert.equal(needsExtent("bound", [{ ...ROWS[0], kind: "where", ours: "no extent" }]), true);
  assert.equal(needWords(need, "Fraser River", 0), "No extent yet: this place needs two points on Fraser River.");
  assert.equal(needWords({ ...need, op: "upstream_of", points: 1 }, "Fraser River", 0),
    "No extent yet: this place needs one point on Fraser River.");
  assert.equal(needWords({ ...need, op: null, points: null }, "", 0), "No extent yet: set this place's extent.");
});

const wp = (id: string, o: Partial<WaterPlace> = {}): WaterPlace => ({ source: "dfo", id, text: id, live: true,
  check: "unchecked", bad: 0, state: "bound", extents: [{ op: "whole" }], tributaries: null, extent_key: `k-${id}`,
  older_of: null, duplicate_of: null, proposal: false, same: id, why: [], ...o });

test("the water's places: one chip each, the same place under one heading", () => {
  assert.equal(chipOf(wp("a", { check: "checked" })), "done");
  assert.equal(chipOf(wp("a", { check: "earlier" })), "verified earlier");
  assert.equal(chipOf(wp("a", { check: "wrong" })), "reported wrong");
  assert.equal(chipOf(wp("a", { extents: [], state: "needs_cut" })), "needs extent");
  assert.equal(chipOf(wp("a", { check: "stale" })), "changed");
  assert.equal(chipOf(wp("a", { bad: 2 })), "has ✗");
  assert.equal(chipOf(wp("a")), "to check");
  const places = [wp("a"), wp("b"), wp("a2", { same: "a", why: ["an older wording of the place"], live: false }),
                  wp("c", { same: "a", why: ["the identical extent"] })];
  const gs = waterGroups(places);
  assert.deepEqual(gs.map((g) => g.places.map((p) => p.id)), [["a", "a2", "c"], ["b"]]);
  assert.deepEqual(gs[0].why, ["an older wording of the place", "the identical extent"]);
  assert.equal(groupHeading(gs[0]), "These 3 wordings are the same place");
  assert.equal(groupHeading(gs[1]), "");
});

test("same place as: only DFO places that have an extent, one per extent, DFO's own rewording link first", () => {
  const places = [wp("me", { extents: [], extent_key: "", duplicate_of: "old" }), wp("x"), wp("x2", { extent_key: "k-x" }),
                  wp("none", { extents: [], extent_key: "" }), wp("r", { source: "regs" }), wp("old", { live: false })];
  assert.deepEqual(sameAsOptions(places, "me").map((p) => p.id), ["old", "x"]);
});

test("these N wordings are the same place: ticks every tickable older wording, never one that cannot be", () => {
  assert.deepEqual([...tickAll(GROUP, false, true)].sort(), ["b", "c"]);       // d holds what a fix cannot say
  const g = groupSave("a", GROUP, tickAll(GROUP, false, true), false, true);
  assert.deepEqual(g.targets, ["a", "b", "c"]);
  assert.deepEqual([...tickAll([mem("a", { this: true })], false, true)], []);
});

test("the place's one state line is one of exactly six", async () => {
  const { placeState } = await import("../src/checklist.ts");
  const base = { editing: false, check: "unchecked" as const, hasExtent: true };
  assert.equal(placeState({ ...base, hasExtent: false }).text, "Nothing saved yet");
  assert.equal(placeState({ ...base, editing: true, check: "checked", checkedDate: "2026-11-09" }).text,
    "You are previewing a draft: not saved");
  assert.equal(placeState({ ...base, check: "saved", saved: { state: "waits_cut", date: "2026-11-09" } }).text,
    "Saved by you (2026-11-09): done — waiting only for the new cut to be built");
  assert.equal(placeState({ ...base, check: "saved", saved: { state: "built", date: "2026-11-09" } }).text,
    "Saved by you (2026-11-09): done — built as saved");
  const bad = placeState({ ...base, check: "unapplied", saved: { state: "not_applied", date: "2026-11-09" } });
  assert.equal(bad.tone, "bad");
  assert.match(bad.text, /NOT applied to the map data — nothing for you to re-check/);
  assert.equal(placeState({ ...base, check: "stale" }).text, "Changed since it was saved or checked: check it again");
  assert.equal(placeState({ ...base, saved: { state: "landed", date: "2026-11-09" } }).text, "Saved and on the map");
  assert.equal(placeState({ ...base, saved: { state: "landed", date: "2026-11-09" }, hasExtent: false }).text, "Nothing saved yet");
  assert.equal(placeState({ ...base, check: "checked", checkedDate: "2026-11-09" }).text, "Confirmed by you (2026-11-09)");
  assert.equal(placeState({ ...base, check: "earlier", earlierDate: "2026-11-02" }).text, "Verified earlier (2026-11-02)");
  assert.equal(placeState({ ...base, saved: { state: "deferred", date: "2026-11-09" } }).text, "Nothing saved yet");
});

test("after a save: the next not-done place on the same water, else the next water with work", async () => {
  const { nextAfterSave, placeState, checkStatus, chipOf, isDone } = await import("../src/checklist.ts");
  const p = (id: string, water_key: string, check: CheckPlace["check"]): CheckPlace => ({ source: "dfo", id, water: water_key.toUpperCase(),
    water_key, region: "2", text: id, summary: { ok: 1, bad: 0, worst: "ok" }, check });
  const q = [p("a1", "a", "unchecked"), p("a2", "a", "saved"), p("a3", "a", "earlier"), p("a4", "a", "stale"),
             p("b1", "b", "checked"), p("b2", "b", "unchecked")];
  assert.deepEqual(nextAfterSave(q, "a2"), { next: "a4" });                 // skips the done one, list order
  assert.deepEqual(nextAfterSave(q, "a4"), { next: "a1" });                 // wraps on its own water before leaving it
  const done = q.map((x) => (x.water_key === "a" ? { ...x, check: "checked" as const } : x));
  assert.deepEqual(nextAfterSave(done, "a2"), { next: null, water: "A", total: 4, nextWater: done[5] });
  const all = done.map((x) => ({ ...x, check: "checked" as const }));
  assert.equal((nextAfterSave(all, "a2") as { nextWater: unknown }).nextWater, null);
  // an extent save waiting only on its cut is done, and never reads "your earlier mark was: not sure"
  assert.equal(isDone({ check: "saved" }), true);
  const saved = { state: "waits_cut" as const, date: "2026-11-09", cuts: ["c"] };
  assert.equal(placeState({ editing: false, check: "saved", saved, hasExtent: false }).text,
    "Saved by you (2026-11-09): done — waiting only for the new cut to be built");
  assert.equal(checkStatus({ ...PC, check: "saved", saved, earlier: { date: "2026-11-09", status: "unsure", note: "x" } }),
    "Saved by you (2026-11-09): done — waiting only for the new cut to be built.");
  assert.equal(chipOf({ check: "saved", state: "needs_cut", extents: [], bad: 1 }), "saved, waits build");
  assert.equal(chipOf({ check: "saved", state: "bound", extents: [{ op: "whole" }], bad: 1 }), "saved, built");
  assert.equal(chipOf({ check: "unapplied", state: "bound", extents: [{ op: "whole" }], bad: 0 }), "saved, not applied");
  assert.equal(isDone({ check: "unapplied" }), false);
  assert.equal(checkStatus({ ...PC, check: "stale", differs: { saved: [{ op: "whole" }], now: [] } }),
    "The extent is no longer the one you saved: check it again.");
});

test("save for all: every wording that can take the extent is covered, the rest named; a taken-out wording is no member", async () => {
  const { saveForAll, coveredWords } = await import("../src/checklist.ts");
  const got = saveForAll("a", GROUP, true);
  assert.deepEqual(got.group.targets, ["a", "b", "c"]);
  assert.deepEqual(got.covered.map((m) => m.id), ["b", "c"]);
  assert.deepEqual(got.left.map((x) => x.m.id), ["d"]);                   // d holds what an extent fix cannot say
  assert.match(got.left[0].why, /within_area/);
  assert.equal(coveredWords(got.covered), ", and the same extent given to 2 other wordings of this place: “text b”, “text c”");
  assert.equal(coveredWords([]), "");
  // the group is the server's list of members: one taken out (not_variant_of) is not in it, so never covered
  assert.deepEqual(saveForAll("a", GROUP.filter((m) => m.id !== "b"), true).group.targets, ["a", "c"]);
  assert.deepEqual(saveForAll("a", [GROUP[0]], true).group, { targets: ["a"], members: {} });
});

test("the next water with work, from anywhere; a water's left count; a group's wordings that still need him", async () => {
  const { nextWaterWithWork, waterLeft, groupStillNeeds } = await import("../src/checklist.ts");
  const p = (id: string, water_key: string, check: CheckPlace["check"]): CheckPlace => ({ source: "dfo", id, water: water_key.toUpperCase(),
    water_key, region: "2", text: id, summary: { ok: 1, bad: 0, worst: "ok" }, check });
  const q = [p("a1", "a", "saved"), p("a2", "a", "earlier"), p("b1", "b", "checked"), p("c1", "c", "unchecked"), p("d1", "d", "stale")];
  assert.equal(nextWaterWithWork(q, "a1")!.id, "c1");            // skips a finished water
  assert.equal(nextWaterWithWork(q, "d1")!.id, "c1");            // wraps
  assert.equal(nextWaterWithWork(q, "c1")!.id, "d1");            // never its own water
  assert.equal(nextWaterWithWork(q.slice(0, 3), "a1"), null);
  assert.deepEqual(waterLeft(q, "a"), { total: 2, left: 0 });
  assert.deepEqual(waterLeft(q, "c"), { total: 1, left: 1 });
  // one rule: a wording is done by its own state — a saved / verified-earlier / checked one needs nothing from him
  const ms = [mem("a", { this: true, check: "saved" }), mem("b", { check: "saved" }), mem("c", { check: "earlier" }),
              mem("d", { check: "unchecked" }), mem("e", { check: "stale" }), mem("f", { check: null })];
  assert.deepEqual(groupStillNeeds(ms).map((m) => m.id), ["d", "e", "f"]);
  assert.deepEqual(groupStillNeeds(ms.slice(0, 3)), []);
});

test("hide done, and easiest first: least work first, a water never split, the shown place kept", async () => {
  const { arrange } = await import("../src/checklist.ts");
  const p = (id: string, water_key: string, check: CheckPlace["check"], rank?: number): CheckPlace => ({ source: "dfo", id, water: water_key,
    water_key, region: "2", text: id, summary: { ok: 1, bad: 0, worst: "ok" }, check, ease: rank == null ? null : { rank, word: `r${rank}` } });
  const q = [p("a1", "a", "unchecked", 6), p("a2", "a", "saved"), p("b1", "b", "unchecked", 0), p("b2", "b", "unchecked", 5),
             p("c1", "c", "checked"), p("d1", "d", "unchecked", 2)];
  assert.deepEqual(arrange(q, { easiest: false, hideDone: false }).list.map((x) => x.id), q.map((x) => x.id));
  const hid = arrange(q, { easiest: false, hideDone: true, keep: "c1" });
  assert.deepEqual(hid.list.map((x) => x.id), ["a1", "b1", "b2", "c1", "d1"]);      // done hidden, the shown one kept
  assert.equal(hid.hidden, 1);
  // d (2) < b (0 + 5) < a (6); the finished water c last; inside b the easiest place first; a's done place last
  assert.deepEqual(arrange(q, { easiest: true, hideDone: false }).list.map((x) => x.id), ["d1", "b1", "b2", "a1", "a2", "c1"]);
  assert.deepEqual(arrange(q, { easiest: true, hideDone: true }).list.map((x) => x.id), ["d1", "b1", "b2", "a1"]);
});

test("a confirm-as-it-is from the inline editor reads as confirmed by him, not as verified earlier", async () => {
  const { placeState, checkStatus } = await import("../src/checklist.ts");
  const earlier = { date: "2026-11-10", status: "verified", note: "confirmed as it is by the owner from the checklist's editor (the draft equalled the extent) · T and R not asked" };
  assert.equal(placeState({ editing: false, check: "earlier", earlierDate: "2026-11-10", confirmedAsIs: true, hasExtent: true }).text, "Confirmed by you (2026-11-10)");
  assert.equal(checkStatus({ ...PC, check: "earlier", earlier }), "Confirmed by you (2026-11-10): the extent as it is.");
});

test("entry B: a closed area reads in one line and draws only when both its ends are placed", async () => {
  const { closedLine, closedPairs } = await import("../src/checklist.ts");
  const dead = { name: "Deadman", metres: 3616, ends: [{ label: "upper signs", new: true, coord: [-120.88, 50.75] as [number, number] },
                                                       { label: "CNR bridge", new: false, coord: [-120.92, 50.74] as [number, number] }] };
  const none = { name: "Juniper", metres: null, ends: [] as { label: string; new: boolean; coord: [number, number] | null }[] };
  assert.equal(closedLine(dead), "Deadman · 3.6 km: NEW cut upper signs → CNR bridge");
  assert.equal(closedLine({ ...dead, metres: 420 }), "Deadman · 420 m: NEW cut upper signs → CNR bridge");
  assert.equal(closedLine(none), "Juniper: not placed yet");
  assert.deepEqual(closedPairs([dead, none, { ...dead, ends: [dead.ends[0], { ...dead.ends[1], coord: null }] }]), [[[-120.88, 50.75], [-120.92, 50.74]]]);
  // one model, two shapes; and never drawn while DFO's page does not name it
  const { closedCircles } = await import("../src/checklist.ts");
  const ring = [[-126, 55], [-126.01, 55], [-126, 55.01], [-126, 55]] as [number, number][];
  const circle = { name: "Morrison Creek", kind: "circle", state: "placed", radius_m: 400, metres: null, ends: [], ring };
  assert.equal(closedLine(circle), "Morrison Creek: a 400 m circle at its mouth");
  assert.equal(closedLine({ ...circle, ring: undefined, state: "unplaced" }), "Morrison Creek: a 400 m circle, its mouth not placed yet");
  assert.equal(closedLine({ ...circle, state: "radius" }), "RADIUS DIFFERS — Morrison Creek: a 400 m circle at its mouth");
  assert.equal(closedLine({ ...circle, state: "not live" }), "Morrison Creek: no longer on DFO's page — kept on file, not drawn");
  assert.equal(closedLine({ ...dead, state: "changed" }), "CHANGED, check again — Deadman · 3.6 km: NEW cut upper signs → CNR bridge");
  assert.equal(closedCircles([circle, { ...circle, state: "not live" }, { ...circle, ring: undefined }]).features.length, 1);
  assert.deepEqual(closedPairs([{ ...dead, state: "not live" }]), []);
});
