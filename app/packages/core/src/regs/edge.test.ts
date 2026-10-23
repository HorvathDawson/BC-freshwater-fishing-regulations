/**
 * T42 (A): the edge states over hand-built inputs (no store, no golden) — outside B.C., an unnamed section's header,
 * the hours-window ("At certain times") caveat words, and which edge each input is. The store-backed checks (tidal
 * x31p_1010, the hours window x15p2_1010, the weekday row klwa_1010/1012, every moment state, the live unnamed handle)
 * are app/conformance/src/regs/edge.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { Moment } from "../answers.generated";
import { caveatsOf } from "./caveats";
import { edgeKind, outsideKeep, tidalKeep } from "./edge";
import { headerOf } from "./header";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
import { plain } from "./line";
import type { Day } from "./model";
import { buildRegsPage } from "./page";
import { noRule, ruleView } from "./rules";
import type { Settled } from "./settle";
import { BOOK } from "./testing/speciesBook";
import { RULE_ROWS } from "./testing/ruleRows";

const OCT10: Day = { month: 10, day: 10 };
const EMPTY_RULES: RuleIndex = { get: (rix) => noRule(rix), has: () => false, size: 0 };
const OUTSIDE_LINE = "This part of the water is outside B.C.: no B.C. rules apply.";

/** A place with no key: a handle outside B.C. (T09 `loadRegsInputs` gives key/place/item/frames all null). */
const outsideInputs = (): RegsInputs => ({
  target: { kind: "section", handle: 2_000_000_000 }, day: OCT10, weekday: "Saturday", calendarYear: 2026,
  item: null, partIx: 0, place: null, key: null, s: null, cell: null, frames: null, year: null, ruleset: null,
  rules: EMPTY_RULES, lics: new Map(), mergedParts: null,
  statics: { exportSpecies: BOOK, fish: [], glossary: { terms: [] } },
}) as unknown as RegsInputs;

const key = (o: { tidal?: boolean } = {}) => ({ akey: 2191, ruleset: 1, licensingSet: null, steelheadWater: false, steelhead: null,
  steelheadRules: false, kind: "stream", tidal: o.tidal ?? false, seg: 0, mom: null, nseg: 1 });
const OPEN = { status: "own", closing: [], strip: 0 } as unknown as DisplayFrame;
const TIDAL = { status: "tidal", tidal: {}, note: "Tidal water: a different regulation system.", see: null, licence: null } as unknown as DisplayFrame;

describe("edgeKind: which shape the page takes, from the stored key and frame", () => {
  const inp = (o: Partial<RegsInputs>) => ({ target: { kind: "part", item: "gnis:1", partIx: 0 }, key: key(), frames: { display: OPEN }, ...o }) as unknown as RegsInputs;
  it("no key → outside; a tidal key or a tidal frame → tidal; a section tap → unnamed; else normal", () => {
    expect([
      edgeKind(inp({ key: null })),
      edgeKind(inp({ key: null, target: { kind: "section", handle: 1 } })),
      edgeKind(inp({ key: key({ tidal: true }) as RegsInputs["key"] })),
      edgeKind(inp({ frames: { display: TIDAL } as RegsInputs["frames"] })),
      edgeKind(inp({ target: { kind: "section", handle: 1178240 } })),
      edgeKind(inp({ target: { kind: "section", handle: 1178240 }, key: key({ tidal: true }) as RegsInputs["key"] })),
      edgeKind(inp({})),
    ]).toEqual(["outside", "outside", "tidal", "tidal", "unnamed", "tidal", "normal"]);
  });
});

describe("1. outside B.C. (key null): the one foot line; no Gear, Methods or Licence", () => {
  const page = buildRegsPage(outsideInputs(), { today: OCT10, who: { age: "adult", residency: "resident" } });
  it("page status, the Keep tab, and the empty tabs", () => {
    expect(page.status).toBe("outside");
    expect(page.keep).toEqual({ order: ["outside"], outside: OUTSIDE_LINE, strip: null, inherited: null, banner: null, groups: [],
      plainRows: [], otherFish: null, otherFishKey: [], caveats: [], possession: null, whereNot: null });
    expect([page.gear, page.methods, page.licence]).toEqual([null, null, null]);
    expect([page.header.dot, page.header.place, page.header.action, page.header.tabs]).toEqual(["faint", null, null,
      [{ tab: "keep", label: "What you can keep" }]]);   // no section to scroll to: Gear / Methods / Licence are not listed
  });
  it("outsideKeep is the Keep tab page.ts draws", () => {
    expect(outsideKeep()).toEqual(page.keep);
  });
});

describe("tidalKeep: the stored tidal frame's banner alone", () => {
  it("a tidal key whose day frame is tidal: the 'Tidal water' banner in the frame's note; nothing else", () => {
    const inp = { key: key({ tidal: true }), target: { kind: "part", item: "x", partIx: 0 }, frames: { display: TIDAL } } as unknown as RegsInputs;
    const k = tidalKeep(inp, { status: TIDAL } as unknown as Settled);
    expect(k).toEqual({ order: ["banner"], strip: null, inherited: null, groups: [], plainRows: [], otherFish: null, otherFishKey: [],
      caveats: [], possession: null, whereNot: null, outside: null,
      banner: { kind: "tidal", big: ["Tidal water"], paras: ["Tidal water: a different regulation system."], chain: [], reopen: null, merged: null } });
  });
  it("a tidal display frame on a key not flagged tidal still draws the banner (either stored answer says tidal)", () => {
    const inp = { key: key(), target: { kind: "part", item: "x", partIx: 0 }, frames: { display: TIDAL } } as unknown as RegsInputs;
    expect(tidalKeep(inp, { status: TIDAL } as unknown as Settled).order).toEqual(["banner"]);
  });
  it("a tidal key with no tidal frame draws nothing (page v36 missing(…))", () => {
    const inp = { key: key({ tidal: true }), target: { kind: "part", item: "x", partIx: 0 } } as unknown as RegsInputs;
    expect(tidalKeep(inp, { status: OPEN } as unknown as Settled).order).toEqual([]);
  });
});

describe("2. unnamed header (D11 a): the store's place name, no part label, no Map / Parts action", () => {
  const inp = { target: { kind: "section", handle: 1178240 }, day: OCT10, weekday: "Saturday", item: null, partIx: 0,
    place: { label: "Region 5", name: "Unnamed stream · Region 5" }, key: key(), frames: { display: OPEN } } as unknown as RegsInputs;
  it("edge unnamed; header name, place null, action null, four tabs", () => {
    expect(edgeKind(inp)).toBe("unnamed");
    const h = headerOf(inp, { today: OCT10, status: "open" });
    expect([h.name, h.place, h.action, h.tabs.map((t) => t.label)]).toEqual(["Unnamed stream · Region 5", null, null,
      ["What you can keep", "Gear", "Methods", "Licence"]]);
  });
});

describe("3. timed caveat words (D10 a): an hours window, the card at inside=false, the night closed", () => {
  // all week; moment 0 = outside the window (the card, open), moment 1 = inside it (closed by the stored closure 13)
  const h = { start: { solar: "sunset", offset_min: 60 }, end: { solar: "sunrise", offset_min: -60 } };
  const ALL7 = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const M = [{ weekdays: ALL7, hours: { ...h, in: false } }, { weekdays: ALL7, hours: { ...h, in: true } }] as unknown as Moment[];
  const view = (rix: number, base: number, facts: Partial<RuleFacts> = {}): RuleView => {
    const [e, f] = RULE_ROWS[base]!;
    return ruleView(rix, { ...e, rule_id: `${e.rule_id}#${rix}` } as ExportRule, f && { ...f, ...facts }, BOOK, "reach");
  };
  const closer = view(13, 522, { book: [[23, 25]] });
  const R: RuleIndex = { get: (rix) => (rix === 13 ? closer : noRule(rix)), has: (rix) => rix === 13, size: 1 };
  const shut = { status: "closed", closing: [[13, ["KO", "RB"]]], strip: 1 } as unknown as DisplayFrame;
  const rows = { spp: ["KO"], steelhead_line: null, rows: [],
    fish: { KO: { hatchery: { status: "release", win: 11, daily: null, narrow: null, lines: [], roles: [], lift_notes: [] }, wild: null } } } as unknown as RowsValue;
  const inp = { day: OCT10, calendarYear: 2026, rules: R, ruleset: null, key: key(), item: null,
    statics: { exportSpecies: { ...BOOK, groups: { ...BOOK.groups } }, fish: ["KO", "RB"] },
    year: { segStarts: [1, 1], moments: M, displayBySeg: [OPEN, shut], rowsBySeg: [rows, rows], ladderBySeg: [null, null], cells: [], displayByDay: [] },
  } as unknown as RegsInputs;
  const settled = (s: number) => ({ s, day: OCT10, status: s === 0 ? OPEN : shut, inpart: [], side: [], anglerclosed: [], L: null }) as unknown as Settled;

  it("keep.caveats[0] is the timed block: the title, the closure's words, the window's words, the Rule book page", () => {
    const c = caveatsOf(inp, settled(0), settled);
    expect(c.map((x) => x.kind)).toEqual(["timed"]);
    expect([c[0]!.title, c[0]!.items.map((i) => [plain(i.what), i.where, i.rule, plain(i.book)])]).toEqual(["⚠ At certain times",
      [["No fishing for any game fish", "60 min after sunset–60 min before sunrise", closer.key, "Rule book p.23 ↗"]]]);
  });
  it("mutation pin: the card at the closed moment draws no caveat (the closed day has none)", () => {
    expect(caveatsOf(inp, settled(1), settled)).toEqual([]);
  });
});
