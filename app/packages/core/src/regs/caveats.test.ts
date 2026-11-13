/**
 * caveats.ts, store-free, over hand-built inputs (real rule rows from testing/ruleRows.ts where a rule's words matter).
 * No v38 state draws a boat-only or not-today in-part block, or the timed (b)/(c) items — they are
 * pinned here. The 134-state parity against the golden is app/conformance/src/regs/caveats.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import type { Moment } from "../answers.generated";
import type { HydRes } from "./card";
import {
  anglerOf, answerTxt, caveatsOf, daysTxt, gearInPart, inPartOf, KEEP_FAMS, momentTxt, partClosedOf, sideOf, timedOf,
} from "./caveats";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
import type { CaveatVM } from "./model";
import type { Verdict } from "./ladder";
import { plain } from "./line";
import { noRule, ruleView } from "./rules";
import type { Settled } from "./settle";
import { BOOK } from "./testing/speciesBook";
import { RULE_ROWS } from "./testing/ruleRows";

const view = (rix: number, base: number, exp: Partial<ExportRule> = {}, fields: Record<string, unknown> = {}, facts: Partial<RuleFacts> = {}): RuleView => {
  const [e, f] = RULE_ROWS[base]!;
  return ruleView(rix, { ...e, rule_id: `${e.rule_id}#${rix}`, ...exp, fields: { ...e.fields, ...fields } }, f && { ...f, ...facts }, BOOK, "reach");
};
const indexOf = (views: readonly RuleView[]): RuleIndex => {
  const m = new Map(views.map((v) => [v.rix, v]));
  return { get: (rix) => m.get(rix) ?? noRule(rix), has: (rix) => m.has(rix), size: m.size };
};
const OPEN = { status: "own", closing: [], strip: 0 } as unknown as DisplayFrame;
const settled = (o: Partial<Settled>): Settled =>
  ({ s: 0, day: { month: 10, day: 10 }, status: OPEN, inpart: [], side: [], anglerclosed: [], L: null, ...o }) as unknown as Settled;
const W = (...d: string[]) => d;
const WEEKDAY = W("Monday", "Tuesday", "Wednesday", "Thursday", "Friday"), WEEKEND = W("Saturday", "Sunday");
const ALL7 = [...WEEKDAY, ...WEEKEND];

describe("time words (page v36 daysTxt, momentTxt, answerTxt)", () => {
  it("daysTxt: runs of days, Monday first", () => {
    expect(daysTxt(["Saturday", "Sunday"])).toBe("Sat–Sun");
    expect(daysTxt(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])).toBe("Mon–Fri");
    expect(daysTxt(["Sunday", "Friday", "Saturday"])).toBe("Fri–Sun");
    expect(daysTxt(["Saturday"])).toBe("Sat");
    expect(daysTxt(["Monday", "Wednesday", "Thursday"])).toBe("Mon, Wed–Thu");
  });
  it("momentTxt: its weekdays (none for all 7), its hours or 'other hours'", () => {
    const h = { start: { solar: "sunset", offset_min: 60 }, end: { solar: "sunrise", offset_min: -60 } };
    expect(momentTxt({ weekdays: WEEKEND, hours: null } as unknown as Moment)).toBe("Sat–Sun");
    expect(momentTxt({ weekdays: ALL7, hours: { ...h, in: true } } as unknown as Moment)).toBe("60 min after sunset–60 min before sunrise");
    expect(momentTxt({ weekdays: WEEKDAY, hours: { ...h, in: false } } as unknown as Moment)).toBe("Mon–Fri, other hours");
    expect(momentTxt(null)).toBe("");
  });
  it("answerTxt: the stored answer in a word or two", () => {
    const r = (status: string, daily: number | null) => ({ status, daily }) as unknown as HydRes;
    expect([answerTxt(r("keep", 5)), answerTxt(r("nolimit", Infinity)), answerTxt(r("release", null)), answerTxt(r("closed", null)), answerTxt(null)])
      .toEqual(["5 a day", "no limit", "release", "closed", "no rule"]);
  });
});

describe("(T623 F4) partClosedOf: a closure of a part of the water the map can't draw", () => {
  // the Shuswap's p.32 closures (the real rows' words): Salmon Arm Bay Sep 1–Dec 31, the map B area Mar 15–May 14
  const bay = view(11, 522, { label: "No fishing, Sep 1-Dec 31 — in part: Salmon Arm Bay", parts: { what: "No fishing" } },
    { undrawn_part: "Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point",
      when: { dates: [{ from_month: 9, from_day: 1, to_month: 12, to_day: 31 }] } }, { book: [] });
  const albas = view(12, 522, { label: "No fishing, Mar 15-May 14 — in part: entire area north of Albas (map B)", parts: { what: "No fishing" } },
    { undrawn_part: "entire area north of Albas (map B)", when: { dates: [{ from_month: 3, from_day: 15, to_month: 5, to_day: 14 }] } }, { book: [] });
  const release = view(13, 3351, { label: "Rainbow trout and Char — release all", parts: { what: "Rainbow trout and Char — release all" } },
    { undrawn_part: "the bay (map A)" }, { book: [] });
  const lake = { item: { kind: "lake" } } as unknown as RegsInputs;
  const NOV10 = { month: 11, day: 10 };
  const lines = (c: CaveatVM | null) => c && [c.kind, c.title, c.calm, c.hot, ...c.items.map((i) => `${plain(i.what)} | ${i.where}`),
    c.later?.map((i) => `${plain(i.what)} | ${i.where}`) ?? null];
  it("in force on the day: 'Part of this lake is closed today', that closure first, the others under Other dates", () => {
    const s = settled({ day: NOV10, inpart: [{ rule: albas, now: false }, { rule: bay, now: true }, { rule: release, now: true }] });
    expect(lines(partClosedOf(s, lake, NOV10))).toEqual(["partclosed", "Part of this lake is closed today",
      "The map can’t draw that part yet, so this page shows the rest of the lake.", true,
      "No fishing · Sep 1–Dec 31 | Where: Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point",
      ["No fishing · Mar 15–May 14 | Where: Entire area north of Albas (map B in the regulations)"]]);
    // another day than today says the day
    expect(partClosedOf(s, lake, { month: 11, day: 11 })!.title).toBe("Part of this lake is closed on Nov 10");
  });
  it("not in force on the day: a standing warning, every closure listed with its dates", () => {
    const s = settled({ day: { month: 7, day: 1 }, inpart: [{ rule: albas, now: false }, { rule: bay, now: false }] });
    expect(lines(partClosedOf(s, { item: { kind: "stream" } } as unknown as RegsInputs, { month: 7, day: 1 }))).toEqual(["partclosed",
      "Part of this river closes at times", "The map can’t draw that part yet, so this page shows the rest of the river.", false,
      "No fishing · Mar 15–May 14 | Where: Entire area north of Albas (map B in the regulations)",
      "No fishing · Sep 1–Dec 31 | Where: Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point", null]);
  });
  it("no closure of a part (a release only), a closed day, tidal water: no warning", () => {
    expect(partClosedOf(settled({ inpart: [{ rule: release, now: true }] }), lake, NOV10)).toBeNull();
    for (const status of ["closed", "tidal"])
      expect(partClosedOf(settled({ inpart: [{ rule: bay, now: true }], status: { status } as unknown as DisplayFrame }), lake, NOV10)).toBeNull();
  });
  it("the in-part block no longer repeats a closure the warning says; it keeps the release", () => {
    const s = settled({ inpart: [{ rule: bay, now: true }, { rule: release, now: true }] });
    expect(inPartOf(s, lake, KEEP_FAMS)!.items.map((i) => plain(i.what))).toEqual(["Rainbow trout and Char — release all"]);
    expect(inPartOf(settled({ inpart: [{ rule: bay, now: true }] }), lake, KEEP_FAMS)).toBeNull();
  });
});

describe("inPartOf: rules of parts the map can't draw", () => {
  const spot = view(1, 3351, { label: "No fishing — in part: a bay", parts: { what: "No fishing — in part: a bay" } }, { undrawn_part: "various spots" }, { book: [[32, 34]] });
  const dated = view(2, 3351, { label: "Rainbow trout — release all" }, { undrawn_part: "the bay (map B)", when: { dates: [{ from_month: 5, from_day: 15, to_month: 3, to_day: 14 }] } }, { book: [] });
  const boat = view(3, 78, { family: "vessel" }, { undrawn_part: "east arm" }, { book: [] });
  const inp = { item: { kind: "lake" } } as unknown as RegsInputs;
  const lines = (c: ReturnType<typeof inPartOf>) => c && [c.title, c.calm, c.hot, ...c.items.map((i) => [plain(i.what), i.where, plain(i.book)]),
    c.later?.map((i) => plain(i.what)) ?? null];
  it("hot: a non-boat rule in force today; the rest fold under 'Other dates'", () => {
    expect(lines(inPartOf(settled({ inpart: [{ rule: dated, now: true }, { rule: spot, now: false }] }), inp, KEEP_FAMS))).toEqual([
      "Fishing is different in some spots today", "Everywhere else, follow this page.", true,
      ["Rainbow trout — release all · May 15–Mar 14", "Where: The bay (map B in the regulations)", ""], ["No fishing"]]);
  });
  it("boat rules only today: the lake/river calm words; none today: the later list is the items, no fold", () => {
    expect(lines(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), inp, ["vessel"]))!.slice(0, 3))
      .toEqual(["Boat rules in some spots", "Fishing rules are the same everywhere on this lake.", false]);
    expect(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), { item: { kind: "stream" } } as unknown as RegsInputs, ["vessel"])!.calm)
      .toBe("Fishing rules are the same everywhere on this river.");
    // (T116) a marsh is a wetland, not a river
    expect(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), { item: { kind: "wetland" } } as unknown as RegsInputs, ["vessel"])!.calm)
      .toBe("Fishing rules are the same everywhere on this wetland.");
    // (T116 review) an unnamed section has no item: its key says what water it is
    expect(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), { item: null, key: { kind: "lake" } } as unknown as RegsInputs, ["vessel"])!.calm)
      .toBe("Fishing rules are the same everywhere on this lake.");
    expect(lines(inPartOf(settled({ inpart: [{ rule: spot, now: false }] }), inp, KEEP_FAMS))).toEqual([
      "Some spots have their own rules", "Not in force today. The map can’t draw these places yet, so check where you are.", false,
      ["No fishing", "Where: some spots, marked by buoys and signs. Look for signs.", "Rule book p.32 ↗"], null]);
  });
  it("a place the book leaves vague reads 'some spots … look for signs' (various / do not identify / buoys and signs)", () => {
    for (const p of ["at various locations", "the regulations do not identify it", "areas marked by buoys and signs"]) {
      const r = view(8, 3351, {}, { undrawn_part: p }, { book: [] });
      expect(inPartOf(settled({ inpart: [{ rule: r, now: false }] }), inp, KEEP_FAMS)!.items[0]!.where, p)
        .toBe("Where: some spots, marked by buoys and signs. Look for signs.");
    }
  });
  it("T208: an area rule reads 'Outlet area only (see map 1): …', set apart, with the map's synopsis page linked", () => {
    const fr = view(4, 3351, { label: "Char — 2 per day" }, { undrawn_part: "the area at the outlet of François Lake described on map 1" }, { book: [[51, 57]] });
    const withMap = ruleView(4, { ...fr.export, not_yet_mapped: { part: fr.part!, identified: true, map: { name: "map 1", page: 51, cite: "map 1 on p.51" } } }, fr.facts, BOOK, "reach");
    const whole = view(5, 3351, { label: "Trout — 2 per day" }, { undrawn_part: "east arm" }, { book: [] });
    const win = { item: { kind: "lake" }, statics: { pdfPages: { 51: 57 } } } as unknown as RegsInputs;
    const c = inPartOf(settled({ inpart: [{ rule: withMap, now: true }, { rule: whole, now: true }] }), win, KEEP_FAMS)!;
    expect(plain(c.items[0]!.what)).toBe("Char — 2 per day");
    expect(c.items[0]!.area).toEqual({ tag: "Outlet area only (see map 1)", map: [{ t: "text", s: "See map 1 · " }, { t: "book", printed: 51, pdf: 57, bare: true }] });
    expect(c.items[1]!.area).toBeUndefined();   // not an outlet/inlet or a mapped part: as before
    // the block's head says the area (never only behind the fold); in force today → it opens unfolded
    expect([c.areas, c.areaNow]).toEqual([["Outlet area only (see map 1)"], true]);
    const later = inPartOf(settled({ inpart: [{ rule: withMap, now: false }] }), win, KEEP_FAMS)!;
    expect([later.title, later.areas, later.areaNow]).toEqual(["Some spots have their own rules", ["Outlet area only (see map 1)"], false]);
    const plainSpot = inPartOf(settled({ inpart: [{ rule: whole, now: true }] }), win, KEEP_FAMS)!;
    expect(["areas" in plainSpot, "areaNow" in plainSpot]).toEqual([false, false]);   // no area: the block as before
  });
  it("T208: an outlet / inlet rule the book gives no map for names its area and claims no map; an unnamed book map says 'see the map'", () => {
    const win = { item: { kind: "lake" }, statics: { pdfPages: { 28: 30 } } } as unknown as RegsInputs;
    const ruby = view(4, 3351, { label: "No fishing" }, { undrawn_part: "in the outlet bay within 100 m of the head of Ruby Creek" }, { book: [] });
    const inlet = view(5, 3351, { label: "No fishing" }, { undrawn_part: "within 100 m of the mouth of the inlet stream" }, { book: [] });
    const m = view(6, 3351, { label: "No fishing" }, { undrawn_part: "within fishing boundary signs at the western tip of the lake" }, { book: [] });
    const mahood = ruleView(6, { ...m.export, not_yet_mapped: { part: m.part!, identified: true, map: { name: null, page: 28, cite: "the map on p.28" } } } as never, m.facts, BOOK, "reach");
    const c = inPartOf(settled({ inpart: [{ rule: ruby, now: true }, { rule: inlet, now: true }, { rule: mahood, now: true }] }), win, KEEP_FAMS)!;
    expect(c.items.map((x) => [x.area?.tag, x.area?.map ? plain(x.area.map) : null])).toEqual([
      ["Outlet area only", null], ["Inlet area only", null], ["Part of this lake only (see the map)", "See the map · p.28 ↗"]]);
  });
  it("another family's rules are not this block's; none → null", () => {
    expect(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), inp, KEEP_FAMS)).toBeNull();
  });
  it("(T209, owner 2026-11-12) the day's in-part VESSEL rules are the vessel section's, never this block's; gear stays", () => {
    const okanagan = view(7, 78, { family: "vessel", type: "vessel_rule" }, { aspect: "propulsion", level: "none", undrawn_part: "various locations (as buoyed and signed)" }, { book: [] });
    const bait = view(8, 3351, { label: "Bait ban" }, { undrawn_part: "within signs at the western tip" }, { book: [] });
    const withGear = { item: { kind: "lake" }, rules: indexOf([okanagan, bait]),
      frames: { gear: { in_part: [7, 8], vessel: { active: [], timed: [] } } } } as unknown as RegsInputs;
    expect(gearInPart(withGear).map((x) => x.item.rule)).toEqual([bait.key]);
    expect(inPartOf(settled({}), withGear, KEEP_FAMS)!.items.map((i) => i.rule)).toEqual([bait.key]);
  });
});

describe("sideOf / anglerOf", () => {
  it("side: the data's words, else '<side> half only', then ' · ' and where", () => {
    const a = view(4, 3351, { label: "No fishing", parts: { what: "No fishing", where: "near the outfall" } }, { side: "West" }, { book: [[51, 53]] });
    const c = sideOf(settled({ side: [a] }), KEEP_FAMS)!;
    expect([c.title, plain(c.items[0]!.what), c.items[0]!.where, plain(c.items[0]!.book)])
      .toEqual(["⚠ On one side of the channel only", "No fishing", "West half only · near the outfall", "Rule book p.51 ↗"]);
    expect(sideOf(settled({ side: [] }), KEEP_FAMS)).toBeNull();
  });
  it("angler: the rule's label, no where", () => {
    const a = view(5, 3351, { label: "Angling closed to guided anglers" }, {}, { book: [[50, 52]] });
    const c = anglerOf(settled({ anglerclosed: [a] }))!;
    expect([c.title, plain(c.items[0]!.what), c.items[0]!.where, c.items[0]!.rule]).toEqual(["⚠ Closed to some anglers", "Angling closed to guided anglers", null, a.key]);
  });
});

describe("timedOf: the day reads differently by moment", () => {
  // One day, two moments: weekdays (segment 0) and the weekend (segment 1). Day 283 = Oct 10.
  const M = [{ weekdays: WEEKDAY, hours: null }, { weekdays: WEEKEND, hours: null }] as unknown as Moment[];
  const ko = view(10, 1366, {}, {}, { book: [[9, 11]] }), rel = view(11, 1367), angler = view(12, 3351, { label: "Angling closed (weekends)" }, {}, { kind: "anglerclosure" });
  const closer = view(13, 522, {}, {}, { book: [[23, 25]] });
  const R = indexOf([ko, rel, angler, closer]);
  const rows = (status: "keep" | "release", win: number, daily: number | null): RowsValue => ({ spp: ["KO"], steelhead_line: null, rows: [],
    fish: { KO: { hatchery: { status, win, daily, narrow: null, lines: [], roles: [], lift_notes: [] }, wild: null, lead: "hatchery" } } }) as unknown as RowsValue;
  const inputs = (o: { display?: DisplayFrame[]; rows?: RowsValue[]; moments?: Moment[] | null } = {}) => ({
    day: { month: 10, day: 10 }, calendarYear: 2026, rules: R, ruleset: null, key: { tidal: false }, item: null,
    statics: { exportSpecies: { ...BOOK, groups: { ...BOOK.groups } }, fish: ["KO", "RB"] },
    year: { segStarts: [1, 1], moments: o.moments === undefined ? M : o.moments, displayBySeg: o.display ?? [OPEN, OPEN],
      rowsBySeg: o.rows ?? [rows("release", 11, null), rows("keep", 10, 5)], ladderBySeg: [null, null], cells: [], displayByDay: [] },
  }) as unknown as RegsInputs;
  const L = (speaks: number[]) => new Map([["KO", { none: new Map(speaks.map((ix) => [ix, { state: "speaks", partly: null } as Verdict])) }]]);
  const at = (Ls: Map<string, unknown>[]) => (j: number) => settled({ s: j, L: Ls[j] as Settled["L"] });
  const lines = (c: ReturnType<typeof timedOf>) => c && [c.title, ...c.items.map((i) => [plain(i.what), i.where, i.rule, i.fish, plain(i.book)])];

  it("(b) a fish whose stored answer differs: its answers by days, and today's", () => {
    expect(lines(timedOf(inputs(), settled({ s: 0 }), at([L([]), L([])])))).toEqual(["⚠ At certain times",
      ["Kokanee: release Mon–Fri; 5 a day Sat–Sun", "today (Mon–Fri): release", null, "KO", ""]]);
  });
  it("(c) an angler closure speaking at another moment, not the card's: its describe() and the days", () => {
    const same = rows("release", 11, null);
    expect(lines(timedOf(inputs({ rows: [same, same] }), settled({ s: 0 }), at([L([]), L([12])])))).toEqual(["⚠ At certain times",
      ["Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap.", "Sat–Sun", angler.key, null, "Rule book p.9 ↗"]]);
    expect(timedOf(inputs({ rows: [same, same] }), settled({ s: 1 }), at([L([]), L([12])]))).toBeNull();   // speaks on the card's own moment
  });
  it("(a) a closed moment while the card's is open: its stored closure, unless the card's moment has it too", () => {
    const shut = { status: "closed", closing: [[13, ["KO", "RB"]]], strip: 1 } as unknown as DisplayFrame;
    const same = rows("release", 11, null);
    expect(lines(timedOf(inputs({ display: [OPEN, shut], rows: [same, same] }), settled({ s: 0 }), at([L([]), L([])])))).toEqual(["⚠ At certain times",
      ["No fishing for any game fish", "Sat–Sun", closer.key, null, "Rule book p.23 ↗"]]);
    const both = { status: "own", closing: [[13, ["KO"]]], strip: 0 } as unknown as DisplayFrame;
    expect(lines(timedOf(inputs({ display: [both, shut], rows: [same, same] }), settled({ s: 0 }), at([L([]), L([])])))).toEqual(["⚠ At certain times",
      ["Missing from the answers file: the closure that closes it Sat–Sun", null, null, null, ""]]);
  });
  it("no moments, or one segment that day → null", () => {
    expect(timedOf(inputs({ moments: null }), settled({ s: 0 }), at([L([]), L([])]))).toBeNull();
  });
});

describe("caveatsOf: renderCard's order, open keyed days only", () => {
  const a = view(5, 3351, { label: "Angling closed to guided anglers" });
  const s = view(4, 3351, { label: "No fishing" }, { side: "West" });
  const inp = { item: null, rules: indexOf([]), year: null } as unknown as RegsInputs;
  it("side before angler; none on a closed, tidal or unkeyed day", () => {
    expect(caveatsOf(inp, settled({ side: [s], anglerclosed: [a] }), () => settled({})).map((c) => c.kind)).toEqual(["side", "angler"]);
    for (const status of [{ status: "closed" }, { status: "tidal" }, null])
      expect(caveatsOf(inp, settled({ side: [s], status: status as unknown as DisplayFrame }), () => settled({}))).toEqual([]);
  });
});
