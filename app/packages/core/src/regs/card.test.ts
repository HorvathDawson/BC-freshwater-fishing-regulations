/**
 * card.ts, store-free, over a hand-built rows frame and a RuleIndex stub. The 134-state parity against page v36's own
 * buildModel() is app/conformance/src/regs/card.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { Decided, DisplayFrame, Moment, RegsInputs, Row, RowsValue, RuleIndex, RuleView } from "./inputs";
import { buildModel, hydFact, hydLine, hydRes, hydRow, mainRes, rowsAt, st_ } from "./card";
import { makePlace } from "./place";
import { settle, settleAt } from "./settle";

const R: RuleIndex = {
  get: (rix) => ({ rix, key: `k${rix}`, rank: 0 }) as unknown as RuleView,
  has: () => true, size: 0,
};
const row = (o: Partial<Row>): Row => ({ kind: "keep", pool: null, win: null, members: [], all_members: [], daily: null, narrow: null,
  everyone: [], groups: [], prot: null, wins: null, lift_notes: [], scope: null, year: [["none", 365]], ...o }) as Row;
const KEEP = row({ kind: "keep", pool: 3091, daily: 2, members: ["RB", "CT"], all_members: ["RB", "CT"], narrow: 3095, wins: [7] });
const NOLIMIT = row({ kind: "no_limit", pool: 1362, members: ["LMB"], all_members: ["LMB"] });
const CLOSED = row({ kind: "closed", win: 3080 });
const dec = (status: Decided["status"], win: number, daily: number | null, narrow: number | null = null): Decided =>
  ({ status, win, daily, narrow, lines: [{ r: win, t: "cap", a: 0, b: null, take: 2 }], roles: [[win, "governs", null], [5, "agrees", win]],
     lift_notes: [[6, { q: 1 }]] }) as unknown as Decided;
const FRAME: RowsValue = { spp: ["RB", "CT", "LMB"], steelhead_line: "known_with_rules", rows: [KEEP, NOLIMIT, CLOSED], fish: {
  RB: { hatchery: dec("keep", 3091, 2), wild: dec("release", 3098, null), lead: "hatchery" },
  CT: { hatchery: dec("release", 528, null), wild: dec("release", 3098, null), lead: "hatchery" },
  LMB: { hatchery: dec("no_limit", 1362, null), wild: null, lead: "hatchery" } } };

const OPEN_F = { status: "own", closing: [], strip: 1 } as unknown as DisplayFrame;
const SHUT_F = { status: "closed", closing: [[3080, ["RB"]]], strip: 2 } as unknown as DisplayFrame;
/** Two segments: Jan 1 – May 31 the keep frame (open), Jun 1 on no rows frame and closed. */
function inputs(o: { rows?: (RowsValue | null)[]; starts?: number[]; moments?: Moment[] | null; display?: DisplayFrame[] } = {}): RegsInputs {
  const starts = o.starts ?? [1, 153], rows = o.rows ?? [FRAME, null], display = o.display ?? [OPEN_F, SHUT_F];
  return {
    day: { month: 5, day: 15 }, s: 0, key: { akey: 1, tidal: false }, rules: R, ruleset: null, item: null, partIx: 0,
    statics: { ladderReasons: [], ladderReasonState: [] },
    year: { segStarts: starts, moments: o.moments ?? null, cells: [], ladderBySeg: starts.map(() => null), rowsBySeg: rows,
      displayBySeg: display, displayByDay: [] },
  } as unknown as RegsInputs;
}

describe("st_", () => {
  it("1. st_ names no_limit nolimit", () => {
    expect([st_("no_limit"), st_("keep"), st_("release"), st_("closed")]).toEqual(["nolimit", "keep", "release", "closed"]);
  });
});

describe("hydRow / hydRes / hydLine / hydFact: the stored rows with rule refs resolved", () => {
  it("1. a keep row keeps its daily; a no_limit row's null daily is Infinity; a closed row's stays null", () => {
    const k = hydRow(R, KEEP), n = hydRow(R, NOLIMIT), c = hydRow(R, CLOSED);
    expect([k.kind, k.daily, k.pool?.rix, k.narrow?.rix, k.win, k.members, k.allMembers, k.wins.map((r) => r.rix), k.raw])
      .toEqual(["keep", 2, 3091, 3095, null, ["RB", "CT"], ["RB", "CT"], [7], KEEP]);
    expect([n.kind, n.daily, n.pool?.rix]).toEqual(["nolimit", Infinity, 1362]);
    expect([c.kind, c.daily, c.pool, c.win?.rix, c.wins, c.conds, c.items, c.real]).toEqual(["closed", null, null, 3080, [], null, null, null]);
  });

  it("conds: rule refs resolved, a null b is Infinity, an absent b stays undefined", () => {
    const x = hydRow(R, row({ conds: [{ r: 4, c: "size", a: 30, b: null }, { r: null, c: "outercap", sub: null }] as Row["conds"] }));
    expect(x.conds!.map((c) => [c.r?.rix ?? null, c.b, "b" in c])).toEqual([[4, Infinity, true], [null, undefined, true]]);
  });

  it("hydRes: status, win, daily (no_limit → Infinity), narrow, lines, roles keyed by rule KEY, lift notes", () => {
    const h = hydRes(R, dec("keep", 3091, 2, 3095), "RB", "hatchery")!;
    expect([h.S, h.o, h.status, h.win.rix, h.daily, h.narrow?.rix]).toEqual(["RB", "hatchery", "keep", 3091, 2, 3095]);
    expect([...h.roles]).toEqual([["k3091", { role: "governs", by: null }], ["k5", { role: "agrees", by: "k3091" }]]);
    expect(h.liftNotes.map((n) => [n.by.rix, n.q])).toEqual([[6, { q: 1 }]]);
    expect([h.lines[0]!.t, h.lines[0]!.r.rix, h.lines[0]!.b, h.lines[0]!.take]).toEqual(["cap", 3091, Infinity, 2]);
    expect([hydRes(R, dec("no_limit", 1, null), "LMB", "wild")!.daily, hydRes(R, dec("release", 1, null), "RB", "wild")!.daily]).toEqual([Infinity, null]);
    expect(hydRes(R, null, "RB", "wild")).toBeNull();
  });

  it("hydLine: outer resolved, daily null → Infinity, status renamed, the wire kept as raw", () => {
    const w = { r: 1, t: "outercap", outer: 2 } as const;
    const l = hydLine(R, w);
    expect([l.r.rix, l.outer?.rix, l.raw]).toEqual([1, 2, w]);
    const x = hydLine(R, { r: 1, t: "xref", members: [], rules: [], daily: null } as never);
    expect(x.daily).toBe(Infinity);
    expect(hydLine(R, { r: 1, t: "exc", members: [], rules: [], status: "no_limit" } as never).status).toBe("nolimit");
  });

  it("3. hydFact: carve_of [30, null, 0] → {a:30, b:Infinity, take:0}; a fact without rules lists its own rule", () => {
    const f = hydFact(R, { r: 3, t: "cap", members: ["RB"], rules: [3, 4], a: 0, b: 30, take: 1, carve_of: [30, null, 0] } as never);
    expect([f.carveOf, [...f.members], f.rules.map((r) => r.rix), f.b]).toEqual([{ a: 30, b: Infinity, take: 0 }, ["RB"], [3, 4], 30]);
    const g = hydFact(R, { r: 8, t: "steel", members: [] } as never);
    expect([g.rules.map((r) => r.rix), g.carveOf]).toEqual([[8], undefined]);
  });
});

describe("mainRes reads the stored leading origin (rows 33, T161)", () => {
  const one = (fish: RowsValue["fish"]) => {
    const I = inputs({ rows: [{ ...FRAME, spp: Object.keys(fish), rows: [], fish }, null] });
    return rowsAt(I, makePlace(I), 0)!;
  };
  it("2. the origin the store names leads — even where a keep-first pick would choose the other (the app picks nothing)", () => {
    const r = one({ RB: { hatchery: dec("keep", 1, 2), wild: dec("release", 2, null), lead: "wild" },
                    CT: { hatchery: dec("release", 3, null), wild: dec("keep", 4, 5), lead: "hatchery" } });
    expect([mainRes(r.R.RB)?.o, mainRes(r.R.RB)?.win.rix, mainRes(r.R.CT)?.o, mainRes(r.R.CT)?.win.rix])
      .toEqual(["wild", 2, "hatchery", 3]);
    expect(mainRes(r.R.RB)).toBe(r.R.RB!.W);
  });
  it("3. no answer, no leader; an unknown fish, no leader", () => {
    const r = one({ RB: { hatchery: null, wild: null, lead: null } });
    expect([mainRes(r.R.RB), mainRes(r.R.XX), mainRes(undefined)]).toEqual([null, null, null]);
  });
  it("4. a lead naming an absent answer, a null lead beside an answer, or no lead at all is refused, never re-picked", () => {
    expect(() => one({ RB: { hatchery: null, wild: dec("release", 2, null), lead: "hatchery" } })).toThrow(/lead hatchery/);
    expect(() => one({ RB: { hatchery: dec("keep", 1, 2), wild: null, lead: null } })).toThrow(/lead null/);
    expect(() => one({ RB: { hatchery: dec("keep", 1, 2), wild: null } as unknown as RowsValue["fish"][string] }))
      .toThrow(/not a rows 33 frame/);
  });
});

describe("rowsAt / buildModel", () => {
  it("rowsAt hydrates the segment's stored frame in its own fish order, memoised; null where there is none", () => {
    const inp = inputs(), p = makePlace(inp), r = rowsAt(inp, p, 0)!;
    expect([r.spp, Object.keys(r.R), r.rows.map((x) => x.kind), r.line]).toEqual([["RB", "CT", "LMB"], ["RB", "CT", "LMB"],
      ["keep", "nolimit", "closed"], "known_with_rules"]);
    expect([r.R.LMB!.H!.daily, r.R.LMB!.W]).toEqual([Infinity, null]);
    expect(rowsAt(inp, p, 0)).toBe(r);
    expect(rowsAt(inp, p, 1)).toBeNull();
  });

  it("buildModel: the settled day's rows; broad copied only on a closed day; no rows → missingRows", () => {
    const inp = inputs(), p = makePlace(inp);
    const open = buildModel(inp, p, settle(inp, p));
    expect([open.status, open.broad, open.missingRows, open.rows.length, open.line]).toEqual(["own", [], false, 3, "known_with_rules"]);
    const shut = buildModel(inp, p, settleAt(inp, p, 1, { month: 6, day: 15 }));
    expect([shut.status, shut.broad.map((r) => r.rix), shut.missingRows, shut.rows, shut.spp, shut.R, shut.line])
      .toEqual(["closed", [3080], true, [], [], {}, null]);
    // the settle broad of an OPEN day is not copied
    const openWithClosing = inputs({ display: [{ status: "own", closing: [[3080, ["RB"]]], strip: 1 } as unknown as DisplayFrame, SHUT_F] });
    const q = makePlace(openWithClosing), c = settle(openWithClosing, q);
    expect([c.broad.length, buildModel(openWithClosing, q, c).broad]).toEqual([1, []]);
  });
});

describe("the year strip is the store's (rows 30): hydRow hands it on, nothing derives a day", () => {
  it("each row's stored `year` is HydRow.year, the same runs, even where the frame's answers would colour a day otherwise", () => {
    // the frame's RB keeps all year, yet the stored strip says closed for 30 days: the app draws the store's answer
    const y: Row["year"] = [["keep", 150], ["closed", 30], ["keep", 185]];
    const k = hydRow(R, row({ ...KEEP, year: y }));
    expect(k.year).toBe(y);
    expect(k.year).toEqual([["keep", 150], ["closed", 30], ["keep", 185]]);
    expect(hydRow(R, CLOSED).year).toEqual([["none", 365]]);
    const I = inputs({ rows: [{ ...FRAME, rows: [row({ ...KEEP, year: y }), NOLIMIT, CLOSED] }, null] }), q = makePlace(I);
    const m = buildModel(I, q, settle(I, q));
    expect(m.rows.map((r) => r.year)).toEqual([[["keep", 150], ["closed", 30], ["keep", 185]], [["none", 365]], [["none", 365]]]);
  });
});

describe("no per-day status is derived in TS (owner rule 2026-10-21)", () => {
  it("card exports no dayStatus / strip / RANKSTAT: a row's year strip is the pipeline's answer, not a ranking of stored answers", async () => {
    const mod = (await import("./card")) as Record<string, unknown>;
    expect(["dayStatus", "strip", "RANKSTAT"].filter((k) => k in mod)).toEqual([]);
  });
});
