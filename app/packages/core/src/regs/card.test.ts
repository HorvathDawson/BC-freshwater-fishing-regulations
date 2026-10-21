/**
 * card.ts, store-free, over a hand-built rows frame and a RuleIndex stub. The 134-state parity against page v36's own
 * buildModel() / strip() is app/conformance/src/regs/card.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { Decided, DisplayFrame, Moment, RegsInputs, Row, RowsValue, RuleIndex, RuleView } from "./inputs";
import { buildModel, dayStatus, hydFact, hydLine, hydRes, hydRow, KEEPISH, mainRes, RANKSTAT, rowsAt, st_, strip, type HydRes } from "./card";
import { makePlace } from "./place";
import { settle, settleAt } from "./settle";

const R: RuleIndex = {
  get: (rix) => ({ rix, key: `k${rix}`, rank: 0 }) as unknown as RuleView,
  has: () => true, size: 0,
};
const row = (o: Partial<Row>): Row => ({ kind: "keep", pool: null, win: null, members: [], all_members: [], daily: null, narrow: null,
  everyone: [], groups: [], prot: null, wins: null, lift_notes: [], scope: null, ...o }) as Row;
const KEEP = row({ kind: "keep", pool: 3091, daily: 2, members: ["RB", "CT"], all_members: ["RB", "CT"], narrow: 3095, wins: [7] });
const NOLIMIT = row({ kind: "no_limit", pool: 1362, members: ["LMB"], all_members: ["LMB"] });
const CLOSED = row({ kind: "closed", win: 3080 });
const dec = (status: Decided["status"], win: number, daily: number | null, narrow: number | null = null): Decided =>
  ({ status, win, daily, narrow, lines: [{ r: win, t: "cap", a: 0, b: null, take: 2 }], roles: [[win, "governs", null], [5, "agrees", win]],
     lift_notes: [[6, { q: 1 }]] }) as unknown as Decided;
const FRAME: RowsValue = { spp: ["RB", "CT", "LMB"], steelhead_line: "known_with_rules", rows: [KEEP, NOLIMIT, CLOSED], fish: {
  RB: { hatchery: dec("keep", 3091, 2), wild: dec("release", 3098, null) },
  CT: { hatchery: dec("release", 528, null), wild: dec("release", 3098, null) },
  LMB: { hatchery: dec("no_limit", 1362, null), wild: null } } };
const RELEASE_ALL: RowsValue = { spp: ["RB"], steelhead_line: null, rows: [row({ kind: "release", win: 9, members: ["RB"] })],
  fish: { RB: { hatchery: dec("release", 9, null), wild: dec("release", 9, null) } } };

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

describe("st_, KEEPISH, RANKSTAT", () => {
  it("1. st_ names no_limit nolimit; KEEPISH is keep or nolimit; RANKSTAT ranks them", () => {
    expect([st_("no_limit"), st_("keep"), st_("release"), st_("closed")]).toEqual(["nolimit", "keep", "release", "closed"]);
    expect((["keep", "nolimit", "release", "closed"] as const).map(KEEPISH)).toEqual([true, true, false, false]);
    expect(RANKSTAT).toEqual({ keep: 3, nolimit: 3, release: 2, closed: 1 });
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

describe("mainRes", () => {
  const res = (status: HydRes["status"], o: "hatchery" | "wild") => ({ status, o }) as HydRes;
  it("2. the keepable answer leads, hatchery first; else hatchery; else wild", () => {
    const Hk = res("keep", "hatchery"), Wr = res("release", "wild"), Hr = res("release", "hatchery"), Wk = res("keep", "wild");
    expect(mainRes(Hk, Wr)).toBe(Hk);
    expect(mainRes(Hr, Wk)).toBe(Wk);
    expect(mainRes(Hr, null)).toBe(Hr);
    expect(mainRes(null, Wr)).toBe(Wr);
    expect(mainRes(null, null)).toBeNull();
    expect(mainRes(res("nolimit", "hatchery"), Wk)?.o).toBe("hatchery");
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

describe("dayStatus / strip", () => {
  it("the most permissive stored answer of the members (nolimit counts as keep); none where the day has no rows", () => {
    const inp = inputs(), p = makePlace(inp);
    expect([dayStatus(inp, p, 515, ["RB", "CT"], 2026), dayStatus(inp, p, 515, ["CT"], 2026), dayStatus(inp, p, 515, ["LMB"], 2026),
      dayStatus(inp, p, 615, ["RB"], 2026), dayStatus(inp, p, 515, ["XX"], 2026)]).toEqual(["keep", "release", "keep", "none", "none"]);
  });

  it("weekday moments: the best over the day's weekday segments (a weekday window is said in words, not as days)", () => {
    const wk = [{ weekdays: ["Monday"], hours: null }, { weekdays: ["Sunday"], hours: null }] as unknown as Moment[];
    const inp = inputs({ starts: [1, 1], moments: wk, rows: [RELEASE_ALL, FRAME], display: [OPEN_F, OPEN_F] }), p = makePlace(inp);
    expect(dayStatus(inp, p, 1012, ["RB"], 2026)).toBe("keep");   // a Monday, yet the Sunday segment keeps
    const hrs = [{ weekdays: ["Monday"], hours: null }, { weekdays: ["Monday"], hours: { in: true } }] as unknown as Moment[];
    const inp2 = inputs({ starts: [1, 1], moments: hrs, rows: [RELEASE_ALL, FRAME], display: [OPEN_F, OPEN_F] }), p2 = makePlace(inp2);
    expect(dayStatus(inp2, p2, 1012, ["RB"], 2026)).toBe("release");   // an hours window is not a weekday split: the day's own segment
  });

  it("strip: run-length over the 365 book days", () => {
    const inp = inputs(), p = makePlace(inp);
    const s = strip(inp, p, ["RB"], 2026);
    expect(s).toEqual([["keep", 151], ["none", 214]]);
    expect(s.reduce((a, [, n]) => a + n, 0)).toBe(365);
  });
});
