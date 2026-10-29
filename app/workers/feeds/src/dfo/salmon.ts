// T225 port of the pieces of pipeline/deliver/salmon.py the live feed uses: reading a rule on a day (verdict_on,
// gear_on), the map status, runs, and the interner — plus live.location_calendar / compose on top.
import { dumps } from "./pyjson";
import type { TypedRule } from "./typed";
import { DATES_UNPARSED, MONTHS, monthDay, OTHER_FISH, YEAR } from "./typed";

export const SPECIES = ["CH", "CO", "SK", "PK", "CM"] as const;
const ALL_SALMON = "SA";
export const MAP_CODES = ["none", "open", "release", "closed", "check"] as const;
export const NOT_LISTED = { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 };
const GEAR_TYPES = new Set(["tackle_restriction", "bait_restriction", "gear_restriction", "method_restriction"]);

/** every calendar day as MMDD, Jan 1 → Dec 31 of a leap year */
export const DAYS: number[] = Array.from({ length: YEAR }, (_, i) => {
  const [m, d] = monthDay(i + 1);
  return m * 100 + d;
});

type R = TypedRule & Record<string, unknown>;

export function holdsOn(rule: R, md: number): boolean | null {
  const w = (rule.when ?? {}) as { dates?: { from_month: number; from_day: number; to_month: number; to_day: number }[]; unparsed?: string[] };
  if (w.unparsed && w.unparsed.length && !(w.dates && w.dates.length)) return null;
  const dates = w.dates ?? [];
  if (!dates.length) return true;
  for (const d of dates) {
    const a = d.from_month * 100 + d.from_day;
    const b = d.to_month * 100 + d.to_day;
    if (a <= b ? a <= md && md <= b : md >= a || md <= b) return true;
  }
  return false;
}

/** robust mode: how a rule holds on a day — "dated" (a printed range, or no dates: all year), "open" (an "until further
 * notice" rule on a day before its start: it runs on, but any dated rule outranks it), false, or null (unreadable). */
export function holdsKind(rule: R, md: number): "dated" | "open" | false | null {
  const w = (rule.when ?? {}) as { dates?: { from_month: number; from_day: number; to_month: number; to_day: number }[]; unparsed?: string[]; open_ended?: boolean };
  const h = holdsOn(rule, md);
  if (h === null) return null;
  if (h) return "dated";
  return w.open_ended && w.dates && w.dates.length ? "open" : false;
}

const speciesOf = (r: R): string[] => (r.species && r.species.length ? r.species : r.when_targeting && r.when_targeting.length ? r.when_targeting : []);
const covers = (r: R, sp: string): boolean => {
  const s = speciesOf(r);
  return s.includes(sp) || s.includes(ALL_SALMON);
};
const isGear = (r: R): boolean => !!(r.gear && r.gear.length) || GEAR_TYPES.has(r.type);
/** T229: a row about another fish (eulachon) — never a salmon verdict, one of the gear lines (salmon.py `_other_fish`) */
const otherFish = (r: R): boolean => r.type === "advisory" && String(r.review_reason ?? "").startsWith(OTHER_FISH);

const fmtMd = (md: number) => `${MONTHS[Math.floor(md / 100) - 1]} ${md % 100}`;
function rangeTxt(a: number, b: number): string {
  if (a === b) return fmtMd(a);
  if (Math.floor(a / 100) === Math.floor(b / 100)) return `${fmtMd(a)}–${b % 100}`;
  return `${fmtMd(a)}–${fmtMd(b)}`;
}
type ClockW = { at?: string; solar?: string; offset_min?: number };
function clockWords(c: ClockW): string {
  if (c.at) return c.at;
  const off = c.offset_min || 0;
  const n = Math.abs(off);
  if (!n) return c.solar!;
  const unit = n % 60 ? `${n} minutes` : n === 60 ? "one hour" : `${Math.floor(n / 60)} hours`;
  return `${unit} ${off < 0 ? "before" : "after"} ${c.solar}`;
}

/** answers.common.when_words(when, "plain") — a DFO `when` carries dates, hours and unparsed, never weekdays */
function whenWords(when: R["when"]): string {
  const w = (when ?? {}) as { dates?: { from_month: number; from_day: number; to_month: number; to_day: number }[]; hours?: { start: ClockW; end: ClockW }; unparsed?: string[]; weekdays?: string[] };
  if (w.weekdays && w.weekdays.length) throw new Error("salmon: a DFO rule with weekdays is not ported");
  const bits: string[] = [];
  const ow = w as { open_ended?: boolean };
  if (w.dates && w.dates.length)
    bits.push(
      w.dates
        .map((d, i) => {
          const a = d.from_month * 100 + d.from_day;
          if (ow.open_ended && i === w.dates!.length - 1) return a === 101 ? "until further notice" : `from ${fmtMd(a)} until further notice`;
          return rangeTxt(a, d.to_month * 100 + d.to_day);
        })
        .join(", "),
    );
  if (w.hours) bits.push(`${clockWords(w.hours.start)} to ${clockWords(w.hours.end)}`);
  if (w.unparsed && w.unparsed.length) bits.push("as printed: " + w.unparsed.join("; "));
  return bits.join(", ");
}

function text(rules: R[], subs: Map<string, R[]>): string {
  const bits: string[] = [];
  for (const r of rules) {
    let t = r.verbatim || "";
    const kids = (subs.get(r.rule_id || "") ?? []).map((k) => k.verbatim || "");
    if (kids.length) t = `${t}, ${kids.filter((k) => k).join(", ")}`;
    const w = whenWords(r.when);
    bits.push(w ? `${t} (${w})` : t);
  }
  return [...new Set(bits.filter((b) => b))].join("; ");
}

function notice(rules: R[]): string | null {
  const ns = [...new Set(rules.filter((r) => r.notice).map((r) => r.notice!))].sort();
  return ns.join(", ") || null;
}

export interface Verdict {
  s: string;
  t: string;
  n?: string;
  d?: number;
}

/** One location's rules, read once: what verdict_on / gear_on look at for every day. */
class Loc {
  subs = new Map<string, R[]>();
  limits: R[][]; // per species
  unreadable: R[];
  gear: R[];
  checks: R[][]; // robust: per species, the rules whose dates did not read (`check`)
  gearChecks: R[];
  memo = new Map<string, string>();
  verdicts = new Map<string, Verdict>();
  gears = new Map<string, string[]>();
  constructor(public rules: R[]) {
    for (const r of rules) if (r.within) {
      const l = this.subs.get(r.within);
      if (l) l.push(r);
      else this.subs.set(r.within, [r]);
    }
    this.limits = SPECIES.map((sp) => rules.filter((r) => !r.within && !isGear(r) && covers(r, sp) && ("take" in r || "may_target" in r)));
    this.unreadable = rules.filter((r) => !r.within);
    this.gear = rules.filter((r) => (isGear(r) && (!speciesOf(r).length || SPECIES.some((sp) => covers(r, sp)))) || otherFish(r));
    this.checks = SPECIES.map((sp) => rules.filter((r) => r.check && !r.within && !isGear(r) && (!speciesOf(r).length || covers(r, sp))));
    this.gearChecks = this.gear.filter((r) => r.check);
  }
  text(rs: R[]): string {
    const k = rs.map((r) => this.rules.indexOf(r)).join(",");
    let v = this.memo.get(k);
    if (v === undefined) {
      v = text(rs, this.subs);
      this.memo.set(k, v);
    }
    return v;
  }
}

/** salmon.verdict_on over a chain of read locations. `robust`: a rule whose dates did not read makes the species'
 * verdict `check` on EVERY day (it may close or open any of them — never a silently missing limit), and an "until
 * further notice" rule fills only the days no dated rule speaks for. */
function verdictOn(chain: Loc[], k: number, md: number, robust = false): Verdict | null {
  const sp = SPECIES[k]!;
  for (const loc of chain) {
    if (robust && loc.checks[k]!.length) {
      const why = loc.checks[k]!;
      const key = "unparsed:" + why.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.verdicts.get(key);
      if (!out) {
        out = { s: "check", t: `${DATES_UNPARSED[0]!.toUpperCase()}${DATES_UNPARSED.slice(1)}: ${loc.text(why)}` };
        const n = notice(why);
        if (n) out.n = n;
        loc.verdicts.set(key, out);
      }
      return out;
    }
    let held: R[];
    if (robust) {
      const kinds = loc.limits[k]!.map((r) => [r, holdsKind(r, md)] as const);
      held = kinds.filter(([, h]) => h === "dated").map(([r]) => r);
      if (!held.length) held = kinds.filter(([, h]) => h === "open").map(([r]) => r);
    } else held = loc.limits[k]!.filter((r) => holdsOn(r, md));
    if (held.length) {
      const ownSp = held.filter((r) => speciesOf(r).includes(sp));
      const own = ownSp.length ? ownSp : held;
      const opens = own.filter((r) => ((r.take as number | undefined) || 0) > 0);
      let s: string;
      let why: R[];
      if (opens.length) [s, why] = ["open", opens];
      else if (own.some((r) => r.may_target === true)) [s, why] = ["release", own.filter((r) => r.may_target === true)];
      else [s, why] = ["closed", own];
      const key = s + ":" + why.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.verdicts.get(key);
      if (!out) {
        out = { s, t: loc.text(why) };
        const n = notice(why);
        if (n) out.n = n;
        loc.verdicts.set(key, out);
      }
      return out;
    }
    const unread = loc.unreadable.filter((r) => holdsOn(r, md) === null && (!speciesOf(r).length || covers(r, sp)));
    if (unread.length) {
      const key = "check:" + unread.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.verdicts.get(key);
      if (!out) loc.verdicts.set(key, (out = { s: "check", t: loc.text(unread) }));
      return out;
    }
  }
  return null;
}

function gearOn(chain: Loc[], md: number, robust = false): string[] {
  for (const loc of chain) {
    let held: R[];
    if (robust) {
      const kinds = loc.gear.map((r) => [r, holdsKind(r, md)] as const);
      held = kinds.filter(([, h]) => h === "dated").map(([r]) => r);
      if (!held.length) held = kinds.filter(([, h]) => h === "open").map(([r]) => r);
      held.push(...loc.gearChecks); // its dates did not read: shown on every day, flagged by its text
    } else held = loc.gear.filter((r) => holdsOn(r, md));
    if (held.length) {
      const key = held.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.gears.get(key);
      if (!out) loc.gears.set(key, (out = [...new Set(held.map((r) => (r.check ? `${r.check}: ` : "") + loc.text([r])))]));
      return out;
    }
  }
  return [];
}

export function mapStatus(verdicts: (Verdict | null)[]): string {
  const s = verdicts.map((v) => (v ? v.s : "closed"));
  if (!s.length) return "none";
  for (const st of ["open", "release", "check"]) if (s.includes(st)) return st;
  return "closed";
}

export function runsOf(values: number[]): number[] {
  const out: number[] = [];
  let prev: number | undefined;
  values.forEach((v, i) => {
    if (i === 0 || v !== prev) {
      out.push(i, v);
      prev = v;
    }
  });
  return out;
}

/** salmon._Interner: index by json.dumps(v, sort_keys=True). A value object seen before (the port reuses its verdict
 * and gear objects) is found by identity first — the text key costs a dump per species per day otherwise. */
export class Interner<T> {
  items: T[] = [];
  private ix = new Map<string, number>();
  private seen = new WeakMap<object, number>();
  add(v: T): number {
    if (typeof v === "object" && v !== null) {
      const hit = this.seen.get(v as object);
      if (hit !== undefined) return hit;
      const i = this.addByKey(v);
      this.seen.set(v as object, i);
      return i;
    }
    return this.addByKey(v);
  }
  private addByKey(v: T): number {
    const k = dumps(v, { sortKeys: true });
    let i = this.ix.get(k);
    if (i === undefined) {
      i = this.items.length;
      this.ix.set(k, i);
      this.items.push(v);
    }
    return i;
  }
}

/** live.location_calendar: one location's year */
export function locationCalendar(rules: R[], verdicts: Interner<unknown>, gears: Interner<unknown>, robust = false): { sp: number[][]; g: number[] } {
  const chain = [new Loc(rules)];
  const perSp: number[][] = SPECIES.map(() => []);
  const gear: number[] = [];
  for (const md of DAYS) {
    for (let k = 0; k < SPECIES.length; k++) {
      const v = verdictOn(chain, k, md, robust);
      perSp[k]!.push(v ? verdicts.add(v) : -1);
    }
    const g = gearOn(chain, md, robust);
    gear.push(g.length ? gears.add(g) : -1);
  }
  return { sp: perSp.map(runsOf), g: runsOf(gear) };
}
