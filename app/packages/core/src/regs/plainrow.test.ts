/**
 * plainrow.ts, store-free: the year's words over canned runs (the T13 `runOf` seam faked per call — what is tested is the
 * wording of the stored strip, not the run finding), the pill, names, release line, week split and VM shape. The 31-row
 * parity is app/conformance/src/regs/plainrow.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { CardModel, HydRow, YearRun } from "./card";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleView } from "./inputs";
import { plain } from "./line";
import type { Day, FishPicVM } from "./model";
import { isPlainRow, namedIn, plainRowOf, runSentence, yearWindowOf, YearWords, type PlainRowSeams, type YearRunOf } from "./plainrow";
import { runOf } from "./strip";
import { ruleView } from "./rules";
import type { Settled } from "./settle";
import { BOOK } from "./testing/speciesBook";

const DIM = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const BOOK_DAYS: Day[] = DIM.flatMap((n, m) => Array.from({ length: n }, (_, i) => ({ month: m + 1, day: i + 1 })));
const ix = (d: Day) => BOOK_DAYS.findIndex((x) => x.month === d.month && x.day === d.day);
const D = (month: number, day: number): Day => ({ month, day });
/** A seam whose runOf answers from a queue (the runs T13 would find), in call order. */
const seams = (runs: YearRunOf[] = [], o: Partial<PlainRowSeams> = {}): PlainRowSeams => ({
  rowTitle: () => "Char", runOf: () => runs.shift()!, bookIndex: ix, BOOK_DAYS, more: "See every regulation line ›",
  fishName: (c) => BOOK.fish[c]?.name ?? c, hasFish: (c) => c !== "ZZ",
  pictures: (cs) => cs.map((c): FishPicVM => (c === "EB" ? { code: c, name: c, sil: true } : { code: c, name: c, caption: `${c} ›`, open: `sp:${c}`, sil: false })),
  ...o,
});
/** Inputs whose water is closed on the given leap-calendar days (1..366). */
const inputs = (closed: (d: number) => boolean = () => false, terms: string[] = ["catch_and_release", "no_fishing_for", "steelhead"]) => ({
  year: { displayByDay: Array.from({ length: 366 }, (_, i) => ({ status: closed(i + 1) ? "closed" : "own" }) as unknown as DisplayFrame), rowsBySeg: [] },
  statics: { exportSpecies: BOOK, glossary: { terms: terms.map((id) => ({ id })) } }, rules: { get: (n: number) => ({ rix: n, key: `k${n}`, facts: { book: [[6, 8]] } }) },
  item: { name: "Elk River", kind: "stream" }, place: null,
}) as unknown as RegsInputs;
const rule = (rix: number, label: string, when: unknown = null): RuleView => ruleView(rix,
  { entry_id: "e", rule_id: `r${rix}`, type: "t", family: "retention", label, verbatim: label, fields: { when }, prov: { rank: 0 } } as ExportRule,
  { kind: "gate", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
const year = (...r: YearRun[]): YearRun[] => r;
const row = (o: Omit<Partial<HydRow>, "raw"> & { raw?: Record<string, unknown> } = {}): HydRow =>
  ({ kind: "release", pool: null, win: rule(1, "x"), members: ["EB"], allMembers: ["EB"], prot: null, year: year(["release", 365]), ...o,
     raw: { group: null, presence: null, week: null, restates: null, ...o.raw } }) as unknown as HydRow;

describe("(T21 review) the group's year window (prototype .gyear over page v36 miniStrip + stripHtml runTxt)", () => {
  const real = seams([], { runOf: (perDay, d) => runOf(perDay.map((st) => [st, 1] as const), d) });
  it("a strip that changes through the year: 'Through the year' and today's run, worded", () => {
    expect(yearWindowOf(row({ year: year(["release", 90], ["keep", 214], ["release", 61]) }), inputs(), D(10, 10), real))
      .toEqual({ label: "Through the year", says: "You can keep them until Oct 31. Then release only from Nov 1." });
    expect(yearWindowOf(row({ year: year(["release", 90], ["keep", 214], ["release", 61]) }), inputs(), D(1, 5), real)!.says)
      .toBe("Release only until Mar 31. You can keep them from Apr 1.");
  });
  it("none for a one-run strip, or one that only repeats the water's own closed days (page v36 draws no strip)", () => {
    expect(yearWindowOf(row({ year: year(["keep", 365]) }), inputs(), D(10, 10), real)).toBeNull();
    const water = (d: number) => d >= 153 && d <= 182;   // Jun 1 – 30 (leap calendar)
    expect(yearWindowOf(row({ year: year(["keep", 151], ["closed", 30], ["keep", 184]) }), inputs(water), D(10, 10), real)).toBeNull();
    expect(yearWindowOf(row({ year: year(["keep", 151], ["closed", 30], ["keep", 184]) }), inputs(), D(10, 10), real)).not.toBeNull();
  });
});

describe("the year's words over the stored strip (page v36 runTxt / closedRuns / runUntil)", () => {
  it("keep now: until when, then what, from when", () => {
    const y = new YearWords(inputs(), seams([{ now: "keep", all: false, from: D(5, 1), until: D(10, 31), next: D(11, 1), then: "release" }]));
    expect(y.runTxt([], D(10, 10), true)).toBe("You can keep them until Oct 31. Then release only from Nov 1.");
  });
  it("release now: the runs that follow until keeping, a whole-water closure named as such, 'next year' when it wraps", () => {
    const water = (d: number) => d >= 183 && d <= 244;   // Jul 1 – Aug 31 (leap calendar)
    const y = new YearWords(inputs(water), seams([
      { now: "release", all: false, from: D(1, 1), until: D(6, 30), next: D(7, 1), then: "closed" },
      { now: "closed", all: false, from: D(7, 1), until: D(8, 31), next: D(9, 1), then: "release" },
      { now: "release", all: false, from: D(9, 1), until: D(9, 30), next: D(10, 1), then: "keep" },
    ]));
    expect(y.runTxt([], D(3, 1), false)).toBe("Release only until Jun 30. Then the whole water is closed Jul 1–Aug 31. Then release only Sep 1–30. You can keep them from Oct 1.");
    const y2 = new YearWords(inputs(), seams([{ now: "closed", all: false, from: D(11, 1), until: D(1, 31), next: D(2, 1), then: "keep" }]));
    expect(y2.runTxt([], D(12, 1), false)).toBe("Closed until Jan 31 next year. You can keep them from Feb 1 next year.");
    const y3 = new YearWords(inputs(), seams([{ now: "release", all: false, from: D(1, 1), until: D(6, 30), next: D(7, 1), then: "closed" },
      { now: "closed", all: false, from: D(7, 1), until: D(12, 31), next: D(1, 1), then: "release" }]));
    expect(y3.runTxt(["release", "closed"], D(3, 1), true)).toBe("Then closed Jul 1–Dec 31. No keeping at any time of year.");
  });
  it("all year: nothing as a tail, 'All year.' for a release or closed run on its own", () => {
    const y = new YearWords(inputs(), seams([{ now: "closed", all: true }, { now: "closed", all: true }, { now: "keep", all: true }]));
    expect([y.runTxt([], D(1, 1), true), y.runTxt([], D(1, 1), false), y.runTxt([], D(1, 1), false)]).toEqual(["", "All year.", ""]);
  });
  it("closedRuns: none, all year, a New Year run as one, the whole water's own closure vs the fish's", () => {
    const y = new YearWords(inputs((d) => d >= 335 || d <= 31), seams());
    const days = (spec: [string, number][]) => spec.flatMap(([s, n]) => Array<string>(n).fill(s));
    expect(y.closedRuns(days([["release", 365]]))).toBe("");
    expect(y.closedRuns(days([["closed", 365]]))).toBe("All year.");
    expect(y.closedRuns(days([["closed", 31], ["release", 303], ["closed", 31]]))).toBe("The whole water is closed Dec 1–Jan 31.");
    expect(y.closedRuns(days([["release", 181], ["closed", 31], ["release", 153]]))).toBe("Closed Jul 1–31.");
  });
});

describe("runSentence (page v36 summaryHtml, !pool)", () => {
  it("a release row that never keeps: its closures; a closed row: no 'No keeping' sentence", () => {
    const water = (d: number) => d >= 186 && d <= 244;
    const strip = year(["release", 184], ["closed", 59], ["release", 122]);
    expect(runSentence(row({ year: strip }), inputs(water), D(10, 10), seams()))
      .toBe("You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jul 4–Aug 31.");
    expect(runSentence(row({ kind: "closed", year: year(["closed", 365]) }), inputs(), D(10, 10), seams()))
      .toBe("Don’t fish for them. If one bites, carefully release it. All year.");
  });
  it("a row that keeps later: 'until' its run's end, then the tail", () => {
    const s = seams([{ now: "release", all: false, from: D(1, 1), until: D(10, 15), next: D(10, 16), then: "keep" },
      { now: "release", all: false, from: D(1, 1), until: D(10, 15), next: D(10, 16), then: "keep" }]);
    expect(runSentence(row({ year: year(["release", 288], ["keep", 77]) }), inputs(), D(10, 10), s))
      .toBe("You can fish for them, but release every one until Oct 15. You can keep them from Oct 16.");
  });
  it("no members: the winning rule's own dates", () => {
    const w = rule(2, "x", { dates: [{ from_month: 4, from_day: 1, to_month: 6, to_day: 14 }] });
    expect(runSentence(row({ members: [], win: w }), inputs(), D(10, 10), seams())).toBe("You can fish for them, but release every one. Apr 1–Jun 14.");
  });
});

describe("plainRowOf: the VM", () => {
  const env = (r: HydRow, o: { terms?: string[]; s?: PlainRowSeams } = {}) => ({
    inp: inputs(undefined, o.terms), settled: { day: D(10, 10), lifted: new Map() } as unknown as Settled,
    card: { rows: [r, r], R: {}, ctx: { s: -1 } } as unknown as CardModel, seams: o.s ?? seams(),
  });
  it("pill and its ⓘ (only when the glossary has the term); the release line with pictures; presence + ⓘ steelhead; the sheet key", () => {
    const r = row({ members: ["EB", "LT", "ZZ"], allMembers: ["EB", "LT"], raw: { presence: { line: "known_with_rules", says: "Steelhead are here." } } });
    const v = plainRowOf(1, env(r));
    expect([v.title, v.pill, v.release?.names, v.release?.pictures.map((p) => p.sil), plain(v.presence), v.more, v.scope]).toEqual([
      "Char", { kind: "release", text: "↩ Release", term: "catch_and_release" }, "Brook trout · Lake trout", [true, false],
      "Steelhead are here. ⓘ", { text: "See every regulation line ›", open: "src:r1" }, null]);
    const c = plainRowOf(0, env(row({ kind: "closed" }), { terms: [] }));
    expect([c.pill, c.release]).toEqual([{ kind: "closed", text: "⊘ Closed", term: "" }, null]);
  });
  it("names: a protected row's species; a memberless row's rule list (namedIn); folded 'Which fish (N)' over 5", () => {
    const p = plainRowOf(0, env(row({ prot: ["CULTUS_LAKE_SCULPIN", "GREEN_STURGEON"] as never, members: [] })));
    expect([p.names, p.whichFish]).toEqual(["Cultus Lake sculpin · Green sturgeon", null]);
    const w = rule(3, "Release all fish including: a, b, c, d, e and f.");
    const n = plainRowOf(0, env(row({ members: [], win: w })));
    expect([n.names, n.whichFish]).toEqual(["a · b · c · d · e and f", null]);
    const w6 = rule(4, "Includes: a, b, c, d, e, f.");
    expect(plainRowOf(0, env(row({ members: [], win: w6 }))).whichFish).toBe("Which fish (6)");
    expect(namedIn("No limit")).toBe("");
  });
  it("a week row: the split instead of the sentence, each part's days, words, (today) and book; the ladder's results dropped", () => {
    const r = row({ raw: { week: { species: ["EB"], status: "Weekends only", says: "Keep on weekends", parts: [
      { days: "Sat–Sun", says: "keep 2", today: true, rules: [7], species: [], when: "", weekdays: [], status: "keep", n: 2 },
      { days: "Mon–Fri", says: "release", today: false, rules: [], species: [], when: "", weekdays: [], status: "release", n: null }] },
      restates: [{ r: 8, of: 1, says: "Zone B also prints this." }] } });
    const v = plainRowOf(0, env(r));
    expect([v.sum, v.week && { ...v.week, parts: v.week.parts.map((p) => [p.days, p.says, p.today, plain(p.book)]), notes: v.week.notes.map(plain) },
      v.ladder.sections.map((s) => s.result)]).toEqual([null, { status: "Weekends only", says: "Keep on weekends.",
      parts: [["Sat–Sun", "keep 2 (today)", true, "Rule book p.6 ↗"], ["Mon–Fri", "release", false, ""]], notes: ["Zone B also prints this. Rule book p.6 ↗"] }, [null]]);
  });
  it("isPlainRow: no pool and no group headline", () => {
    expect([isPlainRow(row()), isPlainRow(row({ pool: rule(1, "p") })), isPlainRow(row({ raw: { group: { headline: { n: 2 } } } }))]).toEqual([true, false, false]);
  });
});
