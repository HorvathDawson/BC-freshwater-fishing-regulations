/**
 * caveats.ts, store-free, over hand-built inputs (real rule rows from testing/ruleRows.ts where a rule's words matter).
 * No v38 state draws an "uncertain" block, a boat-only or not-today in-part block, or the timed (b)/(c) items — they are
 * pinned here. The 134-state parity against the golden is app/conformance/src/regs/caveats.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { Moment } from "../answers.generated";
import type { HydRes } from "./card";
import {
  anglerOf, answerTxt, caveatsOf, daysTxt, friendlyWhy, inPartOf, KEEP_FAMS, momentTxt, sideOf, timedOf, uncertainOf,
} from "./caveats";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
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

describe("time words (page v36 daysTxt, momentTxt, answerTxt, friendlyWhy)", () => {
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
  it("friendlyWhy: a placement code is not words", () => {
    expect([friendlyWhy("locator:abc"), friendlyWhy("The book names no landmark."), friendlyWhy(null)])
      .toEqual(["the regulations don’t say exactly where", "The book names no landmark.", ""]);
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
  it("another family's rules are not this block's; none → null", () => {
    expect(inPartOf(settled({ inpart: [{ rule: boat, now: true }] }), inp, KEEP_FAMS)).toBeNull();
  });
});

describe("sideOf / anglerOf / uncertainOf", () => {
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
  it("uncertain: the store's unplaced rules in the families, with the friendly why; a rix the store lacks is not drawn", () => {
    const u = view(6, 3351, { label: "**No fishing** near the dam", prov: { rank: 0, why: "locator:dam" } });
    const g = view(7, 78, { family: "gear_and_method" });
    const inp = { item: { uncertain: [6, 7, 99] }, rules: indexOf([u, g]) } as unknown as RegsInputs;
    const c = uncertainOf(inp, KEEP_FAMS)!;
    expect([c.title, ...c.items.map((i) => [plain(i.what), i.where])]).toEqual([
      "? Check: rules for parts of this water that couldn’t be placed on the map", ["No fishing near the dam", "the regulations don’t say exactly where"]]);
    expect(uncertainOf({ item: { uncertain: [] }, rules: indexOf([]) } as unknown as RegsInputs, KEEP_FAMS)).toBeNull();
    expect(uncertainOf({ item: null, rules: indexOf([]) } as unknown as RegsInputs, KEEP_FAMS)).toBeNull();
  });
});

describe("timedOf: the day reads differently by moment", () => {
  // One day, two moments: weekdays (segment 0) and the weekend (segment 1). Day 283 = Oct 10.
  const M = [{ weekdays: WEEKDAY, hours: null }, { weekdays: WEEKEND, hours: null }] as unknown as Moment[];
  const ko = view(10, 1366, {}, {}, { book: [[9, 11]] }), rel = view(11, 1367), angler = view(12, 3351, { label: "Angling closed (weekends)" }, {}, { kind: "anglerclosure" });
  const closer = view(13, 522, {}, {}, { book: [[23, 25]] });
  const R = indexOf([ko, rel, angler, closer]);
  const rows = (status: "keep" | "release", win: number, daily: number | null): RowsValue => ({ spp: ["KO"], steelhead_line: null, rows: [],
    fish: { KO: { hatchery: { status, win, daily, narrow: null, lines: [], roles: [], lift_notes: [] }, wild: null } } }) as unknown as RowsValue;
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
