// T320 port of pipeline/regs/dfo_salmon/quota.py's feed half: a species' numbers on an open day (`species_quota`, the
// verdict's `q`), the region's salmon bucket (`bucket`) and the header closure that wins over the table (`header_closure`).
// `residue` / `unreadReason` live in typed.ts (to_rules uses them).
import type { TypedRule } from "./typed";

type R = TypedRule & Record<string, unknown>;
export interface Quota { d?: number; h?: 1; a?: 1; w?: number; o?: number[][]; mx?: number; mo?: number[][]; u?: 1 }

const byPair = (a: number[], b: number[]) => a[0]! - b[0]! || a[1]! - b[1]!;

function one(rule: R, subs: R[]): Quota {
  const q: Quota = { d: (rule.take as number | undefined) || 0 };
  if (rule.origin === "hatchery") q.h = 1;
  if ((rule.species ?? []).includes("SA")) q.a = 1;
  const over: number[][] = [];
  const monthly: number[][] = [];
  let mx: number | null = null;
  for (const s of subs) {
    if (s.origin === "wild" && s.take !== undefined && s.take !== null) {
      q.w = s.take as number;
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
  if (over.length) q.o = over.sort(byPair);
  if (mx !== null) q.mx = mx;
  if (monthly.length) q.mo = monthly.sort(byPair);
  if (rule.review_reason || subs.some((s) => s.review_reason)) q.u = 1;
  return q;
}

const same = (a: Quota, b: Quota) => JSON.stringify(Object.entries(a).sort()) === JSON.stringify(Object.entries(b).sort());

/** quota.species_quota: the rules that opened the species that day → the verdict's `q` (`u` where they disagree) */
export function speciesQuota(opens: R[], subs: Map<string, R[]>): Quota | null {
  const qs = opens.map((r) => one(r, subs.get(r.rule_id || "") ?? []));
  if (!qs.length) return null;
  return qs.slice(1).some((q) => !same(q, qs[0]!)) ? { u: 1 } : qs[0]!;
}

type Note = { id?: string; k: string; t: string; n?: unknown; cm?: unknown; sp?: string[]; w?: unknown[] };
const isInt = (x: unknown): x is number => typeof x === "number" && Number.isInteger(x);

/** quota.bucket: the header's combined limits as the salmon bucket's total tier, or null */
export function bucket(notes: Note[]): Record<string, unknown> | null {
  const daily = notes.filter((n) => n.k === "daily" && !(n.w && n.w.length) && isInt(n.n));
  if (!daily.length) return null;
  const d = daily[0]!;
  const out: Record<string, unknown> = { node: "tier", tier: "total", n: d.n, says: `Up to ${d.n} salmon a day in total`, t: d.t, ids: d.id ? [d.id] : [] };
  const poss = notes.filter((n) => n.k === "possession" && !(n.w && n.w.length));
  if (poss.length) {
    const p = poss[0]!;
    if (isInt(p.n)) out.possession = p.n;
    out.tp = p.t;
    if (p.id) (out.ids as string[]).push(p.id);
  }
  const mins: Record<string, number> = {};
  for (const n of notes)
    if (n.k === "min_size" && !(n.w && n.w.length) && isInt(n.cm)) for (const sp of n.sp ?? []) if (!(sp in mins)) mins[sp] = n.cm;
  if (Object.keys(mins).length) out.min_cm = mins;
  const per = notes
    .filter((n) => (n.k === "annual" || n.k === "monthly") && !(n.w && n.w.length) && isInt(n.n))
    .map((n) => ({ k: n.k, sp: [...(n.sp ?? [])], over_cm: n.cm ?? null, n: n.n ?? null, t: n.t }));
  if (per.length) out.per = per;
  return out;
}

/** quota.header_closure: a closed page's places answer with the header's closure (owner 2026-10-31), or null */
export function headerClosure(notes: Note[], slug: string): R[] | null {
  const closed = notes.filter((n) => n.k === "closed" && !(n.w && n.w.length));
  if (!closed.length) return null;
  return [{ rule_id: `${slug}:header-closed`, type: "retention_limit", verbatim: closed[0]!.t, species: ["SA"], take: 0, may_target: false } as R];
}
