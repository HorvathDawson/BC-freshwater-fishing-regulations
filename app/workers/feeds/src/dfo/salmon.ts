// T225 port of the pieces of pipeline/deliver/salmon.py the live feed uses: reading a rule on a day (verdict_on,
// gear_on), the map status, runs, and the interner — plus live.location_calendar / compose on top.
import { dumps } from "./pyjson";
import { speciesTree, type QuotaTier } from "./quota";
import type { TypedRule } from "./typed";
import { DATES_UNPARSED, GEAR_PERMISSION, MONTHS, monthDay, OTHER_FISH, YEAR } from "./typed";

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

/** (T676, salmon.py `holds_kind`) how a rule holds on a day — "dated" (a printed range, or no dates: all year), "open" (an
 * "until further notice" rule on a day before its start: it runs on, but any dated rule outranks it), false, or null
 * (unreadable). */
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
/** (T676, salmon.py `_held`) the rules that hold on a day: those dated that day; only where none is, the open-ended ones */
function heldOn(rules: R[], md: number): R[] {
  const kinds = rules.map((r) => [r, holdsKind(r, md)] as const);
  const dated = kinds.filter(([, h]) => h === "dated").map(([r]) => r);
  return dated.length ? dated : kinds.filter(([, h]) => h === "open").map(([r]) => r);
}
const isGear = (r: R): boolean => !!(r.gear && r.gear.length) || GEAR_TYPES.has(r.type);
/** T229: a row about another fish (eulachon) — never a salmon verdict, one of the gear lines (salmon.py `_other_fish`) */
const otherFish = (r: R): boolean => r.type === "advisory" && String(r.review_reason ?? "").startsWith(OTHER_FISH);

/** (T671 D2, salmon.py `unread_row`) a row nobody could type: an advisory that is not about another fish and not a gear
 * permission. Never dropped: on the days it holds it reads "check DFO" for the species its cell names. */
const unreadRow = (r: R): boolean =>
  r.type === "advisory" && !r.within && !otherFish(r) && !String(r.review_reason ?? "").startsWith(GEAR_PERMISSION);

const fmtMd = (md: number) => `${MONTHS[Math.floor(md / 100) - 1]} ${md % 100}`;
function rangeTxt(a: number, b: number): string {
  if (a === b) return fmtMd(a);
  if (Math.floor(a / 100) === Math.floor(b / 100)) return `${fmtMd(a)}–${b % 100}`;
  return `${fmtMd(a)}–${fmtMd(b)}`;
}
export type ClockW = { at?: string; solar?: string; offset_min?: number };
export function clockWords(c: ClockW): string {
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

/** T349 (salmon.py `says_catch_and_release`): SALMON NON-RETENTION IS CLOSED TO SALMON. A row answers `release` only
 * where DFO's own words say catch and release; plain non-retention or a limit of 0 is `closed`. */
const RE_CATCH_AND_RELEASE = /\bcatch[- ]and[- ]release\b/i;
const releases = (r: R): boolean => r.may_target === true && RE_CATCH_AND_RELEASE.test(r.verbatim || "");

/** the days of the year a rule's dates hold (MMDD) — (T671 D1) "dated inside" compares these */
const daysOf = (r: R): Set<number> => new Set(DAYS.filter((md) => holdsOn(r, md)));
const within = (a: Set<number>, b: Set<number>): boolean => [...a].every((d) => b.has(d));

/** (T671 D1, salmon.py `_all_closure_wins`, owner 2026-11-12) AN "ALL" CLOSURE AGAINST A SPECIES' OWN ROW, ON ONE PLACE:
 * the All closure wins UNLESS the species row is dated inside the All row's dates. The rows that speak: the species rows
 * dated inside every All closure held today, else those closures. */
function allClosureWins(own: R[], alls: R[]): R[] {
  const closures = alls.filter((a) => !(((a.take as number | undefined) || 0) > 0) && !releases(a));
  if (!own.length || !closures.length) return own;
  const inside = own.filter((r) => closures.every((a) => within(daysOf(r), daysOf(a))));
  return inside.length ? inside : closures;
}

export interface Verdict {
  s: string;
  t: string;
  n?: string;
  d?: number;
  /** (T332) the open day's quota, a node of the game-fish quota tree; `u` not one reading */
  tree?: QuotaTier;
  u?: 1;
  /** (T356) the region header note whose closure this is */
  c?: string;
}

/** One location's rules, read once: what verdict_on / gear_on look at for every day. */
class Loc {
  subs = new Map<string, R[]>();
  limits: R[][]; // per species
  unreadable: R[];
  gear: R[];
  checks: R[][]; // (T676) per species, the rules whose dates did not read (`check`)
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

/** salmon.verdict_on over a chain of read locations. (T676) A rule whose dates did not read makes the species' verdict
 * `check` on EVERY day (it may close or open any of them — never a silently missing limit), and an "until further
 * notice" rule fills only the days no dated rule speaks for. */
function verdictOn(chain: Loc[], k: number, md: number): Verdict | null {
  const sp = SPECIES[k]!;
  for (const loc of chain) {
    if (loc.checks[k]!.length) {
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
    const held = heldOn(loc.limits[k]!, md);
    if (held.length) {
      // the species' own row beats an all-salmon row — (T671 D1) unless an All closure that day is not one it is dated
      // inside
      const ownSp = held.filter((r) => speciesOf(r).includes(sp));
      const own = ownSp.length ? allClosureWins(ownSp, held.filter((r) => !speciesOf(r).includes(sp))) : held;
      const opens = own.filter((r) => ((r.take as number | undefined) || 0) > 0);
      let s: string;
      let why: R[];
      if (opens.length) [s, why] = ["open", opens];
      else if (own.some(releases)) [s, why] = ["release", own.filter(releases)];
      else [s, why] = ["closed", own];
      const key = s + ":" + why.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.verdicts.get(key);
      if (!out) {
        out = { s, t: loc.text(why) };
        if (s === "open") {
          // T320 / T332: the species' numbers under the region's bucket (quota.speciesTree)
          Object.assign(out, speciesTree(why, loc.subs) ?? {});
        }
        const n = notice(why);
        if (n) out.n = n;
        // T356 (salmon.verdict_on): the region header's closure carries its note id
        const hc = [...new Set(why.map((r) => r.header_note).filter((x): x is string => typeof x === "string"))].sort();
        if (hc.length) out.c = hc[0];
        loc.verdicts.set(key, out);
      }
      return out;
    }
    // (T671 D2) a season that does not parse (any row), or a row nobody could type on a day it holds
    const unread = loc.unreadable.filter((r) => (!speciesOf(r).length || covers(r, sp)) && (holdsOn(r, md) === null || (unreadRow(r) && !!holdsKind(r, md))));
    if (unread.length) {
      const key = "check:" + unread.map((r) => loc.rules.indexOf(r)).join(",");
      let out = loc.verdicts.get(key);
      if (!out) loc.verdicts.set(key, (out = { s: "check", t: loc.text(unread) }));
      return out;
    }
  }
  return null;
}

function gearOn(chain: Loc[], md: number): string[] {
  for (const loc of chain) {
    // dated lines, else open-ended ones; a line whose dates did not read shows on every day, flagged by its text
    const held = [...heldOn(loc.gear, md), ...loc.gearChecks];
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
export function locationCalendar(rules: R[], verdicts: Interner<unknown>, gears: Interner<unknown>): { sp: number[][]; g: number[] } {
  const chain = [new Loc(rules)];
  const perSp: number[][] = SPECIES.map(() => []);
  const gear: number[] = [];
  for (const md of DAYS) {
    for (let k = 0; k < SPECIES.length; k++) {
      const v = verdictOn(chain, k, md);
      perSp[k]!.push(v ? verdicts.add(v) : -1);
    }
    const g = gearOn(chain, md);
    gear.push(g.length ? gears.add(g) : -1);
  }
  return { sp: perSp.map(runsOf), g: runsOf(gear) };
}
