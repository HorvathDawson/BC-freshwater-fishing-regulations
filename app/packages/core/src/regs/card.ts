/**
 * The card's rows model (T08): the PIPELINE's rows answers for a segment (answers `rows`: rows_frame → rows_decided per
 * fish × origin, rows_row per drawn row — T04 joined them into a `RowsValue`) with every rule reference resolved to the
 * part's `RuleView`, and the day's `CardModel` the Keep tab reads.
 *
 * Owner rule 2026-10-21 (decisions.md): every keep / release / closed, every daily take, every winning and narrowing rule,
 * every role and lift note here is the store's (`Decided.status/win/daily/narrow/roles/lift_notes`, `Row.kind/pool/win/
 * daily/…`). This file DECODES them (rix → RuleView, `no_limit` → "nolimit", a null cap → Infinity as page v36 reads it),
 * SELECTS (by segment, fish and origin) and GROUPS them for display; it decides nothing. One display selection is page
 * v36's and is kept exactly: `mainRes` (which of a fish's two stored origin answers a one-line view leads with — the
 * keepable one).
 *
 * THE YEAR STRIP IS THE STORE'S (rows 30): page v36's `dayStatus` / `strip` / `RANKSTAT` (the most permissive of the
 * members' answers over the day's weekday moments) is NOT ported — it is a per-day decision, and the pipeline makes it
 * (`pipeline/deliver/answers/year.py`, the page's rule). Each decoded row carries `year`, run-length [[status, days]] over
 * the 365 book days (the shape hydrate.json `dayStatus` carries); `hydRow` hands it on as it is (`HydRow.year`) and T13
 * draws it. Nothing here ranks, merges or recomputes a day. (The store holds each distinct strip once — static
 * `rows.strips`, the rows frame naming each row's — and the data layer's `reads.rows` puts it back on the row.)
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): KEEPISH, st_, hydLine, hydRes, hydFact, hydRow, rowsAt, mainRes,
 * buildModel. Hidden globals replaced: SEC.rows.{frames, decided, rows, at} (→ `inp.year.rowsBySeg`, joined by T04),
 * w._seg['R'+s] (→ WeakMap), PLACE (→ `place`).
 */
import type { Badge, Decided, RealDaily, Row, RowsValue } from "../answers.generated";
import type { RegsInputs, RuleIndex, RuleView } from "./inputs";
import type { RuleKey } from "./line";
import type { RegsPlace } from "./place";
import type { Settled } from "./settle";

export type CardStatus = "keep" | "nolimit" | "release" | "closed";
/** One run of a row's stored year strip (rows 30 `rows[].year`): a status and how many book days it holds. */
export type YearRun = Row["year"][number];
type WireStatus = "keep" | "no_limit" | "release" | "closed";

/** page KEEPISH: a fish that may be kept (with or without a limit). */
export const KEEPISH = (s: CardStatus): boolean => s === "keep" || s === "nolimit";
/** page st_: the wire's `no_limit` as the page names it. */
export function st_(s: WireStatus): CardStatus { return s === "no_limit" ? "nolimit" : s; }

type WireLine = Decided["lines"][number];
type WireFact = Row["everyone"][number];
type WireCond = NonNullable<Row["conds"]>[number];

/** A decided line / row fact with its rule refs resolved (page hydLine): the wire's fields as they are, `r`/`outer` as
 *  views, a null upper bound or daily as Infinity, the status as the page names it. `raw` is the wire line. */
export interface HydLine {
  readonly t: string; readonly r: RuleView;
  readonly a?: number; readonly b?: number;
  readonly take?: number; readonly outer?: RuleView;
  readonly daily?: number;
  readonly status?: CardStatus;
  readonly raw: Readonly<Record<string, unknown>>;
}
export interface HydFact extends HydLine {
  readonly members: ReadonlySet<string>; readonly rules: readonly RuleView[];
  readonly carveOf?: { readonly a: number; readonly b: number; readonly take: number };
}
/** One fish × origin's stored answer (page hydRes). */
export interface HydRes {
  readonly S: string; readonly o: "hatchery" | "wild"; readonly status: CardStatus; readonly win: RuleView;
  /** null for release/closed; Infinity for no limit. */
  readonly daily: number | null;
  readonly narrow: RuleView | null; readonly lines: readonly HydLine[];
  readonly roles: ReadonlyMap<RuleKey, { readonly role: string; readonly by: RuleKey | null }>;
  readonly liftNotes: readonly { readonly by: RuleView; readonly q: Readonly<Record<string, unknown>> }[];
}
/** One drawn row (page hydRow). */
export interface HydRow {
  readonly kind: CardStatus; readonly pool: RuleView | null; readonly win: RuleView | null;
  readonly members: readonly string[]; readonly allMembers: readonly string[];
  readonly daily: number | null; readonly narrow: RuleView | null;
  readonly everyone: readonly HydFact[];
  readonly groups: readonly { readonly members: readonly string[]; readonly facts: readonly HydFact[] }[];
  readonly prot: Row["prot"]; readonly wins: readonly RuleView[];
  readonly liftNotes: readonly { readonly by: RuleView; readonly q: Readonly<Record<string, unknown>> }[];
  readonly scope: Badge | null;
  readonly conds: readonly (Readonly<Record<string, unknown>> & { readonly r: RuleView | null; readonly b: number })[] | null;
  readonly items: Row["items"] | null; readonly real: RealDaily | null;
  /** The row's year strip as the store holds it (rows 30): [[keep | release | closed | none, days]] over the 365 book days. */
  readonly year: readonly YearRun[];
  /** The wire row: group, elsewhere, week, restates, presence are read from it (T15–T19). */
  readonly raw: Row;
}
export interface CardRows {
  readonly spp: readonly string[];
  readonly R: Readonly<Record<string, { readonly H: HydRes | null; readonly W: HydRes | null }>>;
  readonly rows: readonly HydRow[];
  readonly line: RowsValue["steelhead_line"];
}
export interface CardModel {
  readonly R: CardRows["R"]; readonly rows: readonly HydRow[]; readonly spp: readonly string[];
  readonly line: RowsValue["steelhead_line"]; readonly missingRows: boolean;
  /** settled.broad only when the day's status is "closed", else []. */
  readonly broad: readonly RuleView[];
  readonly status: "base" | "own" | "closed" | "tidal" | null;
  readonly ctx: Settled;
}

export function hydLine(R: RuleIndex, l: WireLine | WireFact): HydLine {
  const w = l as Readonly<Record<string, unknown>> & { r: number; t: string };
  const o: Record<string, unknown> = { ...w, r: R.get(w.r), raw: l };
  if ("b" in w) o.b = w.b == null ? Infinity : w.b;
  if ("outer" in w) o.outer = R.get(w.outer as number);
  if ("daily" in w) o.daily = w.daily == null ? Infinity : w.daily;
  if (w.status) o.status = st_(w.status as WireStatus);
  return o as unknown as HydLine;
}

export function hydRes(R: RuleIndex, d: Decided | null, S: string, o: "hatchery" | "wild"): HydRes | null {
  if (!d) return null;
  return {
    S, o, status: st_(d.status), win: R.get(d.win),
    daily: d.daily == null ? (d.status === "no_limit" ? Infinity : null) : d.daily,
    narrow: d.narrow == null ? null : R.get(d.narrow), lines: d.lines.map((l) => hydLine(R, l)),
    roles: new Map(d.roles.map(([r, role, by]) => [R.get(r).key, { role, by: by == null ? null : R.get(by).key }])),
    liftNotes: d.lift_notes.map(([by, q]) => ({ by: R.get(by), q })),
  };
}

export function hydFact(R: RuleIndex, l: WireFact): HydFact {
  const o = hydLine(R, l) as HydLine & Record<string, unknown>;
  o.members = new Set(l.members);
  o.rules = (l.rules || [l.r]).map((ix) => R.get(ix));
  const c = (l as { carve_of?: readonly [number, number | null, number] | null }).carve_of;
  if (c) o.carveOf = { a: c[0], b: c[1] == null ? Infinity : c[1], take: c[2] };
  return o as unknown as HydFact;
}

export function hydRow(R: RuleIndex, x: Row): HydRow {
  const kind = st_(x.kind);
  return {
    kind, pool: x.pool == null ? null : R.get(x.pool), win: x.win == null ? null : R.get(x.win),
    members: x.members, allMembers: x.all_members,
    daily: x.daily == null ? (kind === "nolimit" ? Infinity : null) : x.daily,
    narrow: x.narrow == null ? null : R.get(x.narrow),
    everyone: x.everyone.map((l) => hydFact(R, l)),
    groups: x.groups.map((g) => ({ members: g.members, facts: g.facts.map((l) => hydFact(R, l)) })),
    prot: x.prot, wins: (x.wins || []).map((ix) => R.get(ix)),
    liftNotes: x.lift_notes.map(([by, q]) => ({ by: R.get(by), q })), scope: x.scope,
    // page: `c.b === null ? Infinity : c.b` — an absent b stays undefined
    conds: x.conds ? x.conds.map((c: WireCond) => {
      const w = c as Readonly<Record<string, unknown>> & { r: number | null };
      return { ...w, r: w.r == null ? null : R.get(w.r), b: (w.b === null ? Infinity : w.b) as number };
    }) : null,
    items: x.items || null, real: x.real_daily ?? null, year: x.year, raw: x,
  };
}

const ROWS = new WeakMap<RegsInputs, Map<number, CardRows | null>>();

/** The card on segment `s` (page rowsAt): the stored rows frame hydrated, or null where the key has no rows frame. */
export function rowsAt(inp: RegsInputs, place: RegsPlace, s: number): CardRows | null {
  let memo = ROWS.get(inp);
  if (!memo) ROWS.set(inp, memo = new Map());
  if (memo.has(s)) return memo.get(s)!;
  const f = inp.year?.rowsBySeg[s] ?? null;
  let out: CardRows | null = null;
  if (f) {
    const R = place.R, fish: Record<string, { H: HydRes | null; W: HydRes | null }> = {};
    for (const [S, a] of Object.entries(f.fish)) fish[S] = { H: hydRes(R, a.hatchery, S, "hatchery"), W: hydRes(R, a.wild, S, "wild") };
    out = { spp: f.spp, R: fish, rows: f.rows.map((x) => hydRow(R, x)), line: f.steelhead_line };
  }
  memo.set(s, out);
  return out;
}

/** page mainRes: of a fish's two stored origin answers, the one a one-line view leads with — the keepable one, hatchery
 *  first; else hatchery, else wild. */
export const mainRes = (H: HydRes | null, W: HydRes | null): HydRes | null =>
  H && KEEPISH(H.status) ? H : W && KEEPISH(W.status) ? W : (H || W);

/** page buildModel for the settled day: the stored rows of its segment, and the day's closures only when it is closed. */
export function buildModel(inp: RegsInputs, place: RegsPlace, settled: Settled): CardModel {
  const r = settled.s >= 0 ? rowsAt(inp, place, settled.s) : null;
  return {
    R: r?.R ?? {}, rows: r?.rows ?? [], spp: r?.spp ?? [], line: r?.line ?? null, missingRows: !r,
    broad: settled.status?.status === "closed" ? settled.broad : [], status: settled.status?.status ?? null, ctx: settled,
  };
}
