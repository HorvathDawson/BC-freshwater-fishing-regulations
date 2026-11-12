// T332 port of pipeline/regs/dfo_salmon/quota.py's feed half: a species' numbers on an open day (`species_tree` — a
// node of the GAME-FISH QUOTA TREE, the answers model's TreeTier, not T320's bespoke `q`), the region's salmon bucket
// (`bucket`, its tree the TOTAL tier) and the header closure that wins over the table (`header_closure`).
// `residue` / `unreadReason` live in typed.ts (to_rules uses them).
import { clockWords, type ClockW } from "./salmon";
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
  /** (T349) a `total` tier's scope: the set of areas the count is pooled across ("Management Units 5-6 to 5-11") */
  whose?: string;
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
  let tree: QuotaTier = { node: "tier", id: "t1", tier: "count", n: d, says, by: 0, species, children: kids };
  const pooled = rule.pooled_across as string[] | undefined;
  if (pooled && pooled.length) {
    // T349 / T358 (quota._one): the aggregate tier — one count over a set of areas
    const whose = poolWords(pooled);
    tree = { ...tree, tier: "total", whose, says: saysShared(d, names, hatchery, whose) };
  }
  return unread ? { tree, u: 1 } : { tree };
}

// ── T349 / T358 SHARED QUOTAS (quota.pool_of / pool_words / mark_shared; owner 2026-11-02, re-read 2026-11-12) ──────────
// Region 5B's "All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 …
// unless otherwise stated below": the region row's limits are ONE count over every water of MUs 5-6 to 5-11 not listed as
// its own row; a water with its own row has its own (unpooled) quota; the region's 4-a-day all-species bucket stands
// beside both. The live join marks the location's rules (`join` in live.ts), as the Python's `live.join` does.
const RE_AGGREGATE = /\bin the aggregate\b|\bcombined total\b/i;
const RE_MU_GROUP = /^area:mu_group:management_units_(\d+)_(\d+)_to_(\d+)_(\d+)$/;

const saysShared = (d: number, names: string, hatchery: boolean, scope: string) =>
  `Up to ${d} ${hatchery ? "hatchery-marked " : ""}${names} a day in total, all waters of ${scope} combined`;

/** quota.pool_of: the AREAS a location's quota is pooled across — its own `within` extents, where its words say "in the
 *  aggregate" / "combined total"; [] where they do not. A pooled location with no `within` area is an error. */
export function poolOf(loc: { location_id: string; source_text?: { specific_area?: string } | null;
                              binding?: { extents?: { op?: string | null; area_id?: string | null }[] } | null }): string[] {
  const t = String(loc.source_text?.specific_area ?? "");
  if (!RE_AGGREGATE.test(t)) return [];
  const areas = [...new Set((loc.binding?.extents ?? []).filter((e) => e.op === "within" && e.area_id).map((e) => e.area_id!))]
    .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  if (!areas.length) throw new Error(`${loc.location_id}: its quota is pooled (${JSON.stringify(t.slice(0, 60))}…) but it is bound to no \`within\` area`);
  return areas;
}

/** quota.pool_words: "Management Units 5-6 to 5-11"; an area with no words here is an error */
export function poolWords(areas: string[]): string {
  return areas.map((a) => {
    const m = RE_MU_GROUP.exec(a);
    if (!m) throw new Error(`pool_words: no words for the pooled area ${JSON.stringify(a)} — add its kind here`);
    return `Management Units ${m[1]}-${m[2]} to ${m[3]}-${m[4]}`;
  }).join(" and ");
}

/** quota.mark_shared: each top-level count (a retention limit above zero, no `within`) `pooled_across` the areas */
export function markShared<T extends object>(rules: readonly T[], areas: string[]): T[] {
  if (!areas.length) return [...rules];
  return rules.map((r) => {
    const x = r as Record<string, unknown>;
    return x.type === "retention_limit" && !x.within && ((x.take as number | undefined) || 0) > 0 ? { ...r, pooled_across: [...areas] } : r;
  });
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
              on?: number[][]; hatchery?: boolean; hours?: unknown };
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
  // (T343) the daily total says where it counts: DFO's "from tidal and fresh waters combined"
  const where = /tidal and fresh|fresh and tidal/i.test(d.t) ? ", fresh and tidal waters combined" : "";
  const tree: QuotaTier = { node: "tier", id: "t0", tier: "total", n: d.n as number, says: `Up to ${d.n} salmon a day in total${where}`, by: 0, children: [] };
  const out: Record<string, unknown> = { tree, t: d.t, ids: d.id ? [d.id] : [] };
  for (const n of notes) {
    if (!((n.k === "annual" || n.k === "monthly") && !(n.w && n.w.length) && isInt(n.n))) continue;
    if (n.id) (out.ids as string[]).push(n.id); // (T343) said by the bucket's tier
    const sp = [...(n.sp ?? [])];
    const cm = overOf(n);
    if (isInt(cm)) tree.children.push(size(seq, n.n, cm, n.t, n.k, sp));
    else {
      seq[0]! += 1;
      tree.children.push({ node: "tier", id: `t${seq[0]}`, tier: "count", n: n.n, says: n.t, by: 0, children: [], species: sp, period: n.k });
    }
  }
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

/** (owner T343) quota.ADULT_CHINOOK: what an "adult chinook" is where the page names no length for it */
const ADULT_CHINOOK = { min_cm: 50 };

/** quota.sizes (T356, T343): the header sizes per species, ordered — a water's own entries first, then the waters a
 *  region-wide note excepts (adult chinook: the default), then the region's; a page without an adult chinook length gets
 *  the default. `used` collects the ids of the notes said. */
export function sizes(notes: Note[], used: Set<string> = new Set()): Record<string, unknown>[] {
  for (const n of notes)
    if (n.k in SIZE_KEY && n.xp && n.xp.length)
      throw new Error(`sizes: ${n.id} excepts parts of waters — a size exception is not modelled`);
  const region: Record<string, Record<string, unknown>> = {};
  const excepted: [Note, string][] = [];
  for (const n of notes)
    if (n.k in SIZE_KEY && !(n.w && n.w.length) && n.lengths?.length) {
      for (const sp of n.sp ?? []) {
        const e = (region[sp] ??= {});
        if (!(SIZE_KEY[n.k]! in e)) {
          e[SIZE_KEY[n.k]!] = { ...n.lengths[0] };
          if (n.x && n.x.length) excepted.push([n, sp]);
        }
      }
      if (n.id) used.add(n.id);
    }
  if (Object.keys(region).length && !("adult" in (region.CH ?? {}))) (region.CH ??= {}).adult = { ...ADULT_CHINOOK };
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
  for (const n of notes)
    if (n.k in SIZE_KEY && n.w && n.w.length && n.lengths?.length && n.id) used.add(n.id);
  for (const [n, sp] of excepted) {
    const e = Object.fromEntries(Object.entries(region[sp]!).filter(([k]) => k !== SIZE_KEY[n.k]));
    if (n.k === "adult" && sp === "CH") e.adult = { ...ADULT_CHINOOK };
    if (Object.keys(e).length) put(sp, { ...e, w: (n.x as { key: string }[]).map((w) => w.key) });
  }
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

/** (T343, owner 2026-10-31) deliver.salmon.DROPPED_KINDS: header notes never shown */
const DROPPED_KINDS = new Set(["facility", "measure", "bait_ban"]);

/** deliver.salmon.header_answers (T356, T343): the region's `bucket` / `shared` / `sizes`, and its `notes` again with
 *  each said elsewhere marked `fold` (bucket / sizes / shared / closure — where that structure read it — or drop) */
export function headerAnswers(notes: Note[]): Record<string, unknown> {
  const b = bucket(notes);
  const shared = sharedBuckets(notes);
  const sized = new Set<string>();
  const sz = sizes(notes, sized);
  const closure = headerClosure(notes, "");
  const day = notes.find((n) => n.k === "daylight" && n.hours && !(n.w && n.w.length));
  const h = day?.hours as { start: ClockW; end: ClockW } | undefined;
  const hours = day && h ? { hours: h, id: day.id ?? null,
    says: `Salmon fishing in daylight only: ${clockWords(h.start)} to ${clockWords(h.end)}` } : null;
  const fold: Record<string, string> = {};
  if (day?.id && hours) fold[day.id] = "hours";
  for (const i of sized) fold[i] = "sizes";
  for (const i of ((b?.ids as string[] | undefined) ?? [])) fold[i] = "bucket";
  for (const x of shared) if (x.id) fold[x.id as string] = "shared";
  if (closure) fold[(closure[0] as unknown as { header_note: string }).header_note] = "closure";
  const marked = notes.map((n) => {
    const { fold: _was, ...rest } = n as Note & { fold?: string };
    const how = DROPPED_KINDS.has(rest.k) ? "drop" : rest.id !== undefined ? fold[rest.id] : undefined;
    return (how ? { ...rest, fold: how } : rest) as Note;
  });
  return { notes: marked, ...(b ? { bucket: b } : {}), ...(hours ? { hours } : {}), shared, sizes: sz };
}
