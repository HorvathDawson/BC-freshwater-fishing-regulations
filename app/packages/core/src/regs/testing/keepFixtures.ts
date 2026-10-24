/**
 * Hand-built Keep-tab inputs for the store-free T17–T19 unit tests: a Vedder-like group (Region 2's 4 a day over the
 * streams' 2, the river's own 4 hatchery rainbow), its RuleIndex and card model. Every sentence is copied from the
 * golden (app/fixtures/golden/golden.json, vedder_1010 / klwa_1010) or the store; nothing is invented except
 * the rule indices. The 134-state parity is app/conformance/src/regs/{grouphead,tree,tail,others}.parity.test.ts.
 */
import type { GroupTotal, Row, TreeKind, TreeTier } from "../../answers.generated";
import type { CardModel, HydRes, HydRow } from "../card";
import { hydRow } from "../card";
import type { RegsInputs, RuleIndex, RuleView } from "../inputs";
import { BOOK } from "./speciesBook";

type RuleSpec = { book?: [number, number][]; f?: Record<string, unknown>; entry?: string; species?: string[]; except?: string[] };
export const RULES: Record<number, RuleSpec> = {
  1: { book: [[21, 23]], f: { species: ["TROUT_CHAR"] }, entry: "Region 2", species: ["RB", "ST", "CT", "GB", "DV", "LT", "EB"] },
  2: { book: [[21, 23]], f: { water: "stream" }, species: ["RB", "ST", "CT", "GB", "DV", "LT", "EB"] },
  3: { book: [[22, 24]] },
  4: { book: [[21, 23]] },
  5: { book: [[22, 24], [23, 25]] },
  6: { f: { species: ["ST"] }, species: ["ST"] },
};
export const rv = (rix: number): RuleView => {
  const s = RULES[rix] ?? {};
  return { rix, key: `e${rix}::r${rix}`, missing: !RULES[rix], f: s.f ?? {}, species: s.species ?? [], speciesExcept: s.except ?? [],
    facts: s.book ? { book: s.book } : null, export: { prov: { rank: 3, entry_name: s.entry } } } as unknown as RuleView;
};
export const RIX: RuleIndex = { get: rv, has: (r) => !!RULES[r], size: Object.keys(RULES).length };

export const kind = (o: Partial<TreeKind>): TreeKind => ({ node: "kind", species: [], name: "", n: null, says: "", detail: "",
  sizes: { cm: [], in: [] }, also_counts: [], duties: [], sentence: "", ...o });
export const tier = (o: Partial<TreeTier>): TreeTier => ({ node: "tier", id: "t", tier: "own", n: null, says: "", by: 3, children: [], ...o });

/** vedder_1010 r0, as rows 30 holds it (abridged to the parts the head and tree read) */
export const SHARE = tier({ id: "t3", tier: "share", n: 2, by: 4, says: "Only 2 of the 4 may be from streams",
  origin: "From streams, keep hatchery fish only — wild trout and char go back.",
  children: [tier({ id: "t4", tier: "size", n: 1, by: 4, over_cm: 50, says: "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead",
    extra: { n: 2, species: ["ST"] },
    children: [kind({ species: ["EB", "GB", "CT"], name: "Brook trout, brown trout and cutthroat trout", n: 2,
      says: "Up to 2 may be hatchery brook, brown or cutthroat trout", detail: "hatchery", size_rule: "keep 30 cm (12 in) or longer; release the rest" }),
    kind({ species: ["ST"], name: "Steelhead", n: 2, says: "Up to 2 may be hatchery steelhead only — wild steelhead go back",
      detail: "rainbow trout, over 50 cm", duties: ["10 a year", "stop fishing this river after 2 a day", "record each on your licence right away"],
      presence: { line: "known_with_rules", says: "A rainbow trout over 50 cm (20 in) here counts as a steelhead." } })] })] });
export const OWN = tier({ id: "t2", tier: "own", n: 4, by: 3, says: "This river’s rule: up to 4 hatchery rainbow trout a day",
  whose: "this river’s rule", relation: "Not part of the 2 from streams; part of the 4 a day across Region 2",
  children: [kind({ species: ["RB"], name: "Rainbow trout", n: 4, says: "Up to 4 may be hatchery rainbow trout", detail: "50 cm or less", folded: true })] });
export const ROOT = tier({ id: "t1", tier: "total", n: 4, by: 1, says: "4 a day per angler across Region 2, lakes included", children: [SHARE, OWN] });

export const GROUP = (o: Partial<GroupTotal> = {}): GroupTotal => ({
  pool: 1, total: 4, kinds: [], subcaps: [], headline: { n: 2, unit: "a day", says: "2 a day per angler from streams" }, origin: null, lifts: [],
  total_line: null, buckets: [], states: [], table: {} as GroupTotal["table"], cap_kinds: [], caps: [],
  tree: [ROOT], header: { says: "", tier: "t1", tag: null },
  not_kept: [{ species: ["RB", "CT"], status: "release", says: "Rainbow trout and cutthroat trout: release every one", by: 3, rules: [3], whose: "this river’s rule" }],
  worked: [{ kept: 4, q: "If you keep 4 hatchery rainbow trout here", a: "that’s your trout and char for the day, everywhere.", species: ["RB"],
    origin: "hatchery", share: null, here: null, other: null, away: null, at: "here", over_cm: null, most: "exactly", of: null },
  { kept: 1, q: "Kept 1 at a lake?", a: "Keep 3 more here.", species: [], origin: null, share: null, here: null, other: null, away: null,
    at: "lake", over_cm: null, most: "exactly", of: null }],
  ...o,
});

export const rowOf = (o: Partial<Row>): Row => ({ kind: "keep", pool: null, win: null, members: [], all_members: [], daily: null, narrow: null,
  everyone: [], groups: [], prot: null, wins: null, lift_notes: [], scope: null, year: [["none", 365]], ...o }) as Row;

export const TROUT_ROW = rowOf({ pool: 1, daily: 4, narrow: 2, members: ["RB", "ST", "CT", "GB", "DV", "LT", "EB"],
  all_members: ["RB", "ST", "CT", "GB", "DV", "LT", "EB"], scope: { of: "region", entry: "r2", share: true, apart: false, narrow_all: true },
  group: GROUP(), elsewhere: { says: "Your own limit, counted across every Region 2 stream you fish today.", ask: [] } });

export function cardOf(rows: Row[], o: { line?: CardModel["line"]; R?: CardModel["R"]; status?: CardModel["status"] } = {}): CardModel {
  return { R: o.R ?? {}, rows: rows.map((r) => hydRow(RIX, r)), spp: [], line: o.line ?? null, missingRows: false, broad: [],
    status: o.status === undefined ? "own" : o.status, ctx: { status: { status: o.status ?? "own" } } as unknown as CardModel["ctx"] };
}
export const res = (role: [number, string][] = []): HydRes =>
  ({ status: "keep", roles: new Map(role.map(([r, x]) => [rv(r).key, { role: x, by: null }])) }) as unknown as HydRes;

export const INP = (o: { licence?: unknown; name?: string } = {}): RegsInputs => ({
  rules: RIX, statics: { exportSpecies: BOOK }, item: { name: o.name ?? "Vedder River" }, place: null,
  frames: { licence: o.licence ?? { holds: {}, profiles: [] } },
}) as unknown as RegsInputs;

export const hyd = (r: Row): HydRow => hydRow(RIX, r);
