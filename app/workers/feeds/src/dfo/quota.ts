// T332 port of pipeline/regs/dfo_salmon/quota.py's feed half: a species' numbers on an open day (`species_tree` — a
// node of the GAME-FISH QUOTA TREE, the answers model's TreeTier, not T320's bespoke `q`), the region's salmon bucket
// (`bucket`, its tree the TOTAL tier) and the header closure that wins over the table (`header_closure`).
// `residue` / `unreadReason` live in typed.ts (to_rules uses them).
import type { TypedRule } from "./typed";

type R = TypedRule & Record<string, unknown>;

/** the answers model's TreeKind (pipeline/deliver/answers/model/rows.py), the fields a salmon kind fills */
export interface QuotaKind {
  node: "kind"; species: string[]; name: string; n: number; says: string; detail: string;
  sizes: { cm: (number | null)[][]; in: (number | null)[][] }; also_counts: never[]; duties: never[]; sentence: string;
  folded?: true;
}
/** the answers model's TreeTier; `period` the one field T332 adds to it (monthly / annual) */
export interface QuotaTier {
  node: "tier"; id: string; tier: "total" | "count" | "size"; n: number; says: string; by: 0;
  children: (QuotaTier | QuotaKind)[]; over_cm?: number; species?: string[]; period?: string;
}
export interface Quota { tree?: QuotaTier; u?: 1 }

const NAMES: Record<string, string> = { CH: "chinook", CO: "coho", SK: "sockeye", PK: "pink", CM: "chum", SA: "salmon" };
const byPair = (a: number[], b: number[]) => a[0]! - b[0]! || a[1]! - b[1]!;
/** common.inches: whole inches, the nearest (half up) */
const inches = (cm: number) => Math.floor(cm / 2.54 + 0.5);

const saysDaily = (d: number, hatchery: boolean, all: boolean) =>
  hatchery ? `${d} a day, hatchery-marked only` : all ? `${d} a day, all salmon combined here` : `${d} a day`;
const saysWild = (w: number, d: number) => (w >= d ? "marked or unmarked" : `up to ${w} unmarked (wild), the rest hatchery-marked`);

function kind(species: string[], name: string, n: number, says: string, mx: number | null, folded: boolean): QuotaKind {
  const k: QuotaKind = { node: "kind", species, name, n, says, detail: "",
    sizes: { cm: [[0, mx]], in: [[0, mx === null ? null : inches(mx)]] }, also_counts: [], duties: [], sentence: says };
  if (folded) k.folded = true;
  return k;
}

function size(seq: number[], n: number, overCm: number, says: string, period: string | null = null, species?: string[]): QuotaTier {
  seq[0]! += 1;
  const t: QuotaTier = { node: "tier", id: `t${seq[0]}`, tier: "size", n, says, by: 0, children: [], over_cm: overCm };
  if (species) t.species = species;
  if (period) t.period = period;
  return t;
}

function one(rule: R, subs: R[]): Quota {
  const d = (rule.take as number | undefined) || 0;
  const species = [...((rule.species as string[] | undefined) ?? [])];
  const hatchery = rule.origin === "hatchery";
  const all = species.includes("SA");
  const seq = [1];
  const over: number[][] = [];
  const monthly: number[][] = [];
  let mx: number | null = null;
  let wild: number | null = null;
  let unread = !!rule.review_reason;
  for (const s of subs) {
    unread = unread || !!s.review_reason;
    if (s.origin === "wild" && s.take !== undefined && s.take !== null) {
      wild = s.take as number;
      continue;
    }
    if (s.period === "monthly") {
      for (const b of s.lengths ?? []) if (b.min_cm !== undefined && b.min_cm !== null) monthly.push([b.min_cm, s.take as number]);
      continue;
    }
    for (const band of s.lengths ?? []) {
      const lo = band.min_cm ?? null;
      const hi = band.max_cm ?? null;
      if ("take" in band && band.take === 0 && lo !== null) mx = mx === null ? lo : Math.min(mx, lo);
      else if (lo !== null && s.take !== undefined && s.take !== null) over.push([lo, s.take as number]);
      else if (hi !== null) mx = mx === null ? hi : Math.min(mx, hi);
    }
  }
  const says = saysDaily(d, hatchery, all);
  const names = species.map((c) => NAMES[c] ?? c).join(" or ") || "salmon";
  const kids: (QuotaTier | QuotaKind)[] = [];
  if (hatchery || wild !== null || mx !== null) kids.push(kind(species, hatchery ? `hatchery-marked ${names}` : names, d, says, mx, true));
  if (wild !== null) kids.push(kind(species, `unmarked (wild) ${names}`, wild, saysWild(wild, d), mx, false));
  for (const [cm, n] of over.sort(byPair)) kids.push(size(seq, n!, cm!, `only ${n} over ${cm} cm`));
  if (mx !== null) kids.push(size(seq, 0, mx, `none over ${mx} cm`));
  for (const [cm, n] of monthly.sort(byPair)) kids.push(size(seq, n!, cm!, `${n} over ${cm} cm a month`, "monthly"));
  const tree: QuotaTier = { node: "tier", id: "t1", tier: "count", n: d, says, by: 0, species, children: kids };
  return unread ? { tree, u: 1 } : { tree };
}

/** quota.species_tree: the rules that opened the species that day → `{tree}`, or `{u: 1}` (not one reading) */
export function speciesTree(opens: R[], subs: Map<string, R[]>): Quota | null {
  const qs = opens.map((r) => one(r, subs.get(r.rule_id || "") ?? []));
  if (!qs.length) return null;
  const first = JSON.stringify(qs[0]);
  return qs.some((q) => q.u) || qs.slice(1).some((q) => JSON.stringify(q) !== first) ? { u: 1 } : qs[0]!;
}

/** quota.walk: every node, the tier first (pre-order) */
export function* walk(t: QuotaTier): Generator<QuotaTier | QuotaKind> {
  yield t;
  for (const c of t.children) if (c.node === "tier") yield* walk(c); else yield c;
}

/** quota.words: "4 a day · only 2 over 50 cm" — the tiers and the unfolded kinds, in order */
export const words = (t: QuotaTier | null | undefined): string | null =>
  t ? [...walk(t)].filter((x) => x.node === "tier" || !x.folded).map((x) => x.says).join(" · ") : null;

type Band = { min_cm?: number; max_cm?: number };
type Water = { n: string; ws?: boolean; key: string };
type Note = { id?: string; k: string; t: string; n?: unknown; lengths?: Band[]; sp?: string[]; w?: Water[]; x?: unknown[]; xp?: unknown[];
              on?: number[][]; hatchery?: boolean };
/** quota._over: a note's length floor (`lengths[0].min_cm`) */
const overOf = (n: Note): number | undefined => n.lengths?.[0]?.min_cm;
const isInt = (x: unknown): x is number => typeof x === "number" && Number.isInteger(x);

/** quota.bucket: the header's combined limits — `tree` the TOTAL tier (its annual / monthly limits as tiers with a
 *  `period`), DFO's words, possession, min sizes — or null */
export function bucket(notes: Note[]): Record<string, unknown> | null {
  const daily = notes.filter((n) => n.k === "daily" && !(n.w && n.w.length) && isInt(n.n));
  if (!daily.length) return null;
  const d = daily[0]!;
  const seq = [0];
  const tree: QuotaTier = { node: "tier", id: "t0", tier: "total", n: d.n as number, says: `Up to ${d.n} salmon a day in total`, by: 0, children: [] };
  for (const n of notes) {
    if (!((n.k === "annual" || n.k === "monthly") && !(n.w && n.w.length) && isInt(n.n))) continue;
    const sp = [...(n.sp ?? [])];
    const cm = overOf(n);
    if (isInt(cm)) tree.children.push(size(seq, n.n, cm, n.t, n.k, sp));
    else {
      seq[0]! += 1;
      tree.children.push({ node: "tier", id: `t${seq[0]}`, tier: "count", n: n.n, says: n.t, by: 0, children: [], species: sp, period: n.k });
    }
  }
  const out: Record<string, unknown> = { tree, t: d.t, ids: d.id ? [d.id] : [] };
  const poss = notes.filter((n) => n.k === "possession" && !(n.w && n.w.length));
  if (poss.length) {
    const p = poss[0]!;
    if (isInt(p.n)) out.possession = p.n;
    out.tp = p.t;
    if (p.id) (out.ids as string[]).push(p.id);
  }
  return out;
}

/** quota.shared_buckets (T349, T356): a scoped `aggregate` note -> the region's `shared` tiers, with or without a bucket */
export function sharedBuckets(notes: Note[]): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  for (const n of notes) {
    if (n.k !== "aggregate" || !(n.w && n.w.length) || !isInt(n.n)) continue;
    const sp = [...(n.sp ?? [])];
    const hatchery = !!n.hatchery;
    const names = sp.map((c) => NAMES[c] ?? c).join(" or ") || "salmon";
    const whose = n.w.map((w) => (w.ws ? `the ${w.n} watershed` : w.n)).join(" and ");
    const tree: Record<string, unknown> = { node: "tier", id: "t0", tier: "total", n: n.n, by: 0, children: [], species: sp, whose,
      says: `Up to ${n.n} ${hatchery ? "hatchery-marked " : ""}${names} a day in total across ${whose}` };
    if (hatchery) tree.origin = "hatchery";
    out.push({ tree, t: n.t, id: n.id ?? null, w: n.w.map((w) => w.key) });
  }
  return out;
}

/** two plain values equal whatever their key order (Python's dict ==) */
const sameJson = (a: unknown, b: unknown): boolean => {
  const norm = (x: unknown): unknown => (Array.isArray(x) ? x.map(norm) : x && typeof x === "object"
    ? Object.fromEntries(Object.entries(x).sort(([p], [q]) => (p < q ? -1 : 1)).map(([k, v]) => [k, norm(v)])) : x);
  return JSON.stringify(norm(a)) === JSON.stringify(norm(b));
};

const SIZE_KEY: Record<string, string> = { min_size: "min", adult: "adult", jack: "jack" };
const SPECIES_ORDER = ["CH", "CO", "SK", "PK", "CM"];

/** quota.sizes (T356): the header sizes per species, ordered — a water's own entries first, then the region's */
export function sizes(notes: Note[]): Record<string, unknown>[] {
  for (const n of notes)
    if (n.k in SIZE_KEY && ((n.x && n.x.length) || (n.xp && n.xp.length)))
      throw new Error(`sizes: ${n.id} excepts waters — a size exception is not modelled`);
  const region: Record<string, Record<string, unknown>> = {};
  for (const n of notes)
    if (n.k in SIZE_KEY && !(n.w && n.w.length) && n.lengths?.length)
      for (const sp of n.sp ?? []) {
        const e = (region[sp] ??= {});
        if (!(SIZE_KEY[n.k]! in e)) e[SIZE_KEY[n.k]!] = { ...n.lengths[0] };
      }
  const out: Record<string, unknown>[] = [];
  const put = (sp: string, e: Record<string, unknown>) => {
    const last = out[out.length - 1];
    if (last) {
      const { sp: lsp, ...rest } = last;
      if (sameJson(rest, e)) { (lsp as string[]).push(sp); return; }
    }
    out.push({ sp: [sp], ...e });
  };
  for (const n of notes)
    if (n.k in SIZE_KEY && n.w && n.w.length && n.lengths?.length)
      for (const sp of n.sp ?? [])
        put(sp, { ...(region[sp] ?? {}), [SIZE_KEY[n.k]!]: { ...n.lengths[0] }, w: n.w.map((w) => w.key), ...(n.on && n.on.length ? { on: n.on } : {}) });
  for (const sp of SPECIES_ORDER) if (region[sp]) put(sp, region[sp]!);
  return out;
}

/** quota.header_closure: a closed page's places answer with the header's closure (owner 2026-10-31), or null */
export function headerClosure(notes: Note[], slug: string): R[] | null {
  const closed = notes.filter((n) => n.k === "closed" && !(n.w && n.w.length));
  if (!closed.length) return null;
  // T356: the note's id rides on the rule, so its verdict carries `c` (salmon.ts)
  return [{ rule_id: `${slug}:header-closed`, type: "retention_limit", verbatim: closed[0]!.t, species: ["SA"], take: 0, may_target: false,
            header_note: closed[0]!.id } as R];
}
