/**
 * THE LIVE DFO SALMON FEED (T211 part 2): `feeds/dfo/index.json` (`dfo-feed/3`, `pipeline/regs/dfo_salmon/live.py`)
 * laid over the release's salmon file, so a DFO rule change reaches the app without a rebuild.
 *
 * THE RELEASE OWNS WHERE, THE FEED OWNS WHAT. The release's chains (`chains[].l`: the DFO locations on a section,
 * narrowest first) and its section runs never change here. The feed carries, per DFO location and day, each species'
 * verdict index (-1: the location says nothing that day) and the gear lines. A chain's year is re-composed from them
 * exactly as the pipeline composes it (`live.compose` == `deliver.salmon.chain_calendar`, pinned by
 * `salmonFeed.golden.ts`): per species and day the FIRST location of the chain with an answer, else verdict 0 (not
 * opened by DFO: closed); gear the first with any; the map status open > release > check > closed. Nothing is decided
 * here that the pipeline did not decide.
 *
 * WHICH ONE (owner 2026-10-27): the last feed this device CAPTURED, whatever its age, with that capture's time on the
 * card — the release's own file only when no feed was ever captured, or when the release was built from pages newer
 * than the capture (`pickSalmon`).
 */
import { MONTHS } from "./calendar.generated";
import { BC_TIME_LABEL } from "./clock";
import { bcFields } from "./day";
import { FEED_FORMATS } from "./feedFormats.generated";
import { dfoRow, salmonCard, type SalmonUnplacedPlace, type SalmonCardVM, type SalmonFacts, type SalmonGearInput, type SalmonMapStatus, type SalmonNote, type SalmonBucket, type SalmonHours, type SalmonShared, type SalmonSizes, type SalmonWords } from "./salmon";

type Payload = SalmonFacts["payload"];
type Chain = Payload["chains"][number];
type Verdict = Payload["verdicts"][number];

/** (T332) a verdict's quota is `tree`, a node of the game-fish quota tree; (T356) dfo-feed/3: a header closure's verdict
 *  carries `c`, a region with notes its `shared` and `sizes`. An older capture is never read — the release's salmon/4
 *  answers until a dfo-feed/3 is fetched */
export const SALMON_FEED_FORMAT = FEED_FORMATS.dfoFeed;   // (T556) generated: pipeline/common/feed_formats.py

export interface SalmonFeedRegion {
  readonly name: string; readonly url: string; readonly asof: string | null;
  /** when this region's page was last read; `stale`: the last read failed and these are the answers before it */
  readonly checked: string | null; readonly stale: boolean; readonly held: string | null;
  /** (owner 2026-10-28) the region page's header as typed notes — the feed's win over the release's */
  readonly notes?: readonly SalmonNote[];
  /** (T320) the salmon bucket read from those notes (`quota.bucket`) */
  readonly bucket?: SalmonBucket;
  /** (T356) beside the notes: the scoped aggregate limits and the header sizes (`deliver.salmon.header_answers`) */
  readonly shared?: readonly SalmonShared[];
  readonly sizes?: readonly SalmonSizes[];
  /** (T343) the page's salmon fishing hours, read from those notes (`header_answers`): absent where none is printed */
  readonly hours?: SalmonHours;
  /** (T549 item 5) places held at their last answers this run (a row the reader refused): `carried` — the last answers
   *  stand (the card says so); `check` — there were none, the place reads "check DFO" */
  readonly holds?: readonly { readonly location_id: string; readonly kept: "carried" | "check" }[];
}
export interface SalmonFeedLocation {
  readonly r: string; readonly sp: readonly (readonly number[])[]; readonly g: readonly number[];
  /** (T321) the location's rows in DFO's words (new wording, an unread number, a note) */
  readonly x?: readonly SalmonWords[];
}
export interface SalmonFeed {
  readonly format: typeof SALMON_FEED_FORMAT;
  /** when the feed was published (UTC ISO) — the capture's as-of */
  readonly at: string;
  readonly regions: Readonly<Record<string, SalmonFeedRegion>>;
  readonly verdicts: readonly Verdict[];
  readonly gears: readonly (readonly string[])[];
  readonly locs: Readonly<Record<string, SalmonFeedLocation>>;
  /** (T521) how many places the feed lists that no curated location knows (index.json's count) */
  readonly unplaced?: number;
  /** (T521) those places, read from `unplaced.json` beside the index (`parseDfoUnplaced`); absent: not read */
  readonly unplacedRows?: readonly SalmonUnplacedPlace[];
  /** (T668 option 2, owner 2026-11-12) THE STRADDLER PAIR TABLE (`pipeline/deliver/salmon_pairs.py`, carried by the
   *  worker): for two regions' differing openings on a two-region chain, [a, b, the Python's answer] in fold order.
   *  Absent: no table (a pair not held keeps both regions' words). */
  readonly pairs?: { readonly format: typeof SALMON_PAIRS_FORMAT; readonly release: string;
                     readonly pairs: readonly (readonly [Verdict, Verdict, Verdict])[] };
}
export const SALMON_PAIRS_FORMAT = FEED_FORMATS.dfoPairs;   // (T556) generated: pipeline/common/feed_formats.py

/**
 * (T521) The feed's `unplaced.json` as the app reads it: every row with its region, its water's app items (`items`, the
 * feeds worker's T521 field; [] for a water no binding knows), the place and its rows in DFO's words. Throws, naming
 * the row, on anything else — a half-read list is never shown as the whole one.
 */
export function parseDfoUnplaced(x: unknown): readonly SalmonUnplacedPlace[] {
  if (!Array.isArray(x)) throw new Error("dfo unplaced.json: not a list");
  const str = (v: unknown) => typeof v === "string";
  return x.map((u, i) => {
    const r = u as Record<string, unknown> | null;
    if (!r || typeof r !== "object" || !str(r.fingerprint) || !str(r.region) || !str(r.water) || !(r.scope == null || str(r.scope))
        || !Array.isArray(r.rows) || !r.rows.every(str) || !Array.isArray(r.items) || !r.items.every(str))
      throw new Error(`dfo unplaced.json: row ${i} is not a place with fingerprint, region, water, rows and items`);
    return { fingerprint: r.fingerprint as string, region: r.region as string, items: r.items as string[], water: r.water as string,
             scope: (r.scope as string | null) ?? "", rows: r.rows as string[] };
  });
}

/** The feed as the app reads it, or null when it is not a `dfo-feed/3` it can trust (never a half-read feed). */
export function parseSalmonFeed(x: unknown): SalmonFeed | null {
  const f = x as Partial<SalmonFeed> | null;
  if (!f || typeof f !== "object" || f.format !== SALMON_FEED_FORMAT || typeof f.at !== "string") return null;
  if (!Array.isArray(f.verdicts) || !f.verdicts.length || !Array.isArray(f.gears)) return null;
  if (!f.locs || typeof f.locs !== "object" || !f.regions || typeof f.regions !== "object") return null;
  // (T356) a region's notes come with what they resolve to: never notes without their sizes and shared limits
  for (const r of Object.values(f.regions)) if (r.notes && (!Array.isArray(r.shared) || !Array.isArray(r.sizes))) return null;
  // (T549 item 5) a region's holds, when it carries them, are whole: every one a place id and how it was kept
  for (const r of Object.values(f.regions))
    if (r.holds !== undefined && (!Array.isArray(r.holds)
        || !r.holds.every((h) => h && typeof h.location_id === "string" && (h.kept === "carried" || h.kept === "check")))) return null;
  // (T668 option 2) the pair table, when it carries one, is whole: a dfo-pairs table of [opening, opening, answer] verdicts
  if (f.pairs !== undefined) {
    const p = f.pairs as Partial<NonNullable<SalmonFeed["pairs"]>> | null;
    const verdict = (v: unknown) => !!v && typeof v === "object" && typeof (v as Verdict).s === "string" && typeof (v as Verdict).t === "string";
    if (!p || p.format !== SALMON_PAIRS_FORMAT || typeof p.release !== "string" || !Array.isArray(p.pairs)
        || !p.pairs.every((x) => Array.isArray(x) && x.length === 3 && x.every(verdict) && x[0]!.s === "open" && x[1]!.s === "open")) return null;
  }
  for (const l of Object.values(f.locs)) {
    if (!l || !Array.isArray(l.sp) || l.sp.length !== 5 || !Array.isArray(l.g)) return null;
    for (const runs of [...l.sp, l.g]) for (let k = 1; k < runs.length; k += 2) {
      const v = runs[k]!;
      if (!Number.isInteger(v) || v >= (runs === l.g ? f.gears.length : f.verdicts.length)) return null;
    }
  }
  return f as SalmonFeed;
}

const DAYS = 366;
const MAP_CODES: readonly (SalmonMapStatus | "none")[] = ["none", "open", "release", "closed", "check"];

function at(runs: readonly number[], day: number): number {
  let v = runs.length > 1 ? runs[1]! : -1;
  for (let k = 0; k < runs.length; k += 2) {
    if (runs[k]! > day) break;
    v = runs[k + 1]!;
  }
  return v;
}

/**
 * (T716) THE DAYS A YEAR CAN CHANGE ON: day 0 and every run start of `runs` — between two of them every `at(runs, day)`
 * is the same, so whatever a day's answer is made of only changes there. The composition asks those days and fills the
 * rest (the same arrays a day-by-day pass makes: 366 days x 5 fish x each place, scanned from the start each time, was
 * ~0.7 ms a chain on a desktop — 290 ms for the province's 435 chains, ~1 s on the emulator's JS thread).
 */
function changeDays(runs: Iterable<readonly number[]>): number[] {
  const s = new Set<number>([0]);
  for (const r of runs) for (let k = 0; k < r.length; k += 2) if (r[k]! > 0 && r[k]! < DAYS) s.add(r[k]!);
  return [...s].sort((a, b) => a - b);
}

/** Each change day with the day after its span (`changeDays`), last one to `DAYS`. */
function spans(days: readonly number[]): [number, number][] {
  return days.map((d, i) => [d, days[i + 1] ?? DAYS]);
}

function runsOf(values: readonly number[]): number[] {
  const out: number[] = [];
  let prev: number | undefined;
  values.forEach((v, i) => { if (i === 0 || v !== prev) { out.push(i, v); prev = v; } });
  return out;
}

function mapStatus(ss: readonly string[]): SalmonMapStatus {
  for (const s of ["open", "release", "check"] as const) if (ss.includes(s)) return s;
  return "closed";
}

/** (T668) the verdict and gear tables a two-region chain's composition may add to (a merged verdict, both regions'
 *  gear lines): the feed's, copied per `withSalmonFeed` call. `live.compose`'s `intern` / `gear_intern`. */
export interface SalmonFeedTables {
  readonly verdicts: Verdict[]; readonly gears: string[][];
  /** (T668 option 2) the feed's pair table by `pairKey(a, b)`: the Python's answer for two differing openings */
  readonly pairs?: ReadonlyMap<string, Verdict>;
}

/** (T668) The tables a composition over `feed` adds to: copies of the feed's, never the feed's. */
export function feedTables(feed: SalmonFeed): SalmonFeedTables {
  return { verdicts: [...feed.verdicts], gears: feed.gears.map((g) => [...g]),
           ...(feed.pairs ? { pairs: new Map(feed.pairs.pairs.map(([a, b, r]) => [pairKey(a, b), r] as const)) } : {}) };
}

/** (T668 option 2) a pair of openings as the table keys it: both verdicts' whole content, in fold order */
const pairKey = (a: Verdict, b: Verdict): string => `${stable(a)}\n${stable(b)}`;

/** (T668 D2/D3, RULINGS Z4a) how strict a verdict is, most strict first — the pipeline's `deliver.salmon.STRICT` */
const STRICT = ["closed", "release", "check", "open"] as const;

const stable = (x: unknown): string => JSON.stringify(x, (_k, v: unknown) =>
  (v && typeof v === "object" && !Array.isArray(v) ? Object.fromEntries(Object.entries(v as object).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : v));

/** an equal entry's index, else appended (the pipeline's interner) */
function intern<T>(items: T[], x: T): number {
  const k = stable(x);
  const i = items.findIndex((y) => stable(y) === k);
  if (i >= 0) return i;
  items.push(x);
  return items.length - 1;
}

/**
 * (T668) The answers of a two-region chain's groups for one species and day, as one: the most strict status speaks
 * (closed > release > check > open, the first region on a tie). TWO DIFFERENT OPENINGS: the Python's answer from the
 * feed's pair table (T668 option 2, owner 2026-11-12: `deliver.salmon.stricter_opening` — one region's opening by the
 * freshwater Z4 ladder, or both regions' words), looked up by the two verdicts' whole content; a pair the table does not
 * hold (a limit changed since the last sync, or no table) shows both regions' words, no number (`u`) — never looser.
 * The app holds no copy of the ladder (the cascade is resolved in Python).
 */
function strictestIx(ixs: readonly number[], t: SalmonFeedTables | undefined, V: readonly Verdict[]): number {
  let best = ixs[0]!;
  for (const j of ixs.slice(1)) {
    const a = V[best]!, b = V[j]!;
    if (STRICT.indexOf(b.s) < STRICT.indexOf(a.s)) best = j;
    else if (a.s === "open" && b.s === "open" && stable(a) !== stable(b)) {
      if (!t) throw new Error("salmon feed: two regions' openings need a merged verdict and no tables were given");
      const said = t.pairs?.get(pairKey(a, b));
      if (said) { best = intern(t.verdicts, said); continue; }
      const n = [...new Set([a, b].flatMap((v) => (v.n ? v.n.split(", ") : [])).filter(Boolean))].sort();
      best = intern(t.verdicts, { s: "open", t: [...new Set([a.t, b.t].filter(Boolean))].join("; "), u: 1, ...(n.length ? { n: n.join(", ") } : {}) } as Verdict);
    }
  }
  return best;
}

/** (T668) the gear lines of a two-region chain's groups that day: every region's, in region order, a line said once
 *  (`deliver.salmon.both_gear`) — DFO's words, not ranked */
function gearOf(gs: readonly number[], t: SalmonFeedTables | undefined, G: readonly (readonly string[])[]): number {
  const have = gs.filter((x) => x >= 0);
  if (have.length <= 1) return have[0] ?? -1;
  if (!t) throw new Error("salmon feed: two regions' gear lines need tables and none were given");
  const merged = [...new Set(have.flatMap((x) => G[x]!))];
  const same = have.find((x) => stable(G[x]) === stable(merged));
  return same ?? intern(t.gears, merged);
}

/**
 * (T668 fix 4) `a`: per species, the place of `lids` whose answer shows (-1 none, -2 both regions together) — the
 * pipeline's `salmon_who.answering_places`, read off the composed answer, never re-decided.
 */
function answeringPlaces(sp: readonly (readonly number[])[], places: readonly (SalmonFeedLocation | undefined)[],
                         groups: readonly (readonly number[])[] | undefined,
                         daysOf: (runs: Iterable<readonly number[]>) => number[]): number[][] {
  const gs = groups ?? [places.map((_, i) => i)];
  return sp.map((runs, k) => runsOf(byChangeDay(daysOf([runs, ...places.flatMap((p) => (p ? [p.sp[k]!] : []))]), (day) => {
    const want = at(runs, day);
    for (const g of gs) {
      let own: [number, number] | null = null;
      for (const i of g) { const p = places[i]; const v = p ? at(p.sp[k]!, day) : -1; if (v >= 0) { own = [i, v]; break; } }
      if (!own) { if (groups && g.length && want === 0) return g[0]!; }
      else if (want === own[1]) return own[0];
    }
    return groups ? -2 : -1;
  })));
}

/** A value per day of the year, asked only on `days` (the days it can change on, `changeDays`) and held to the next. */
function byChangeDay(days: readonly number[], f: (day: number) => number): number[] {
  const out: number[] = new Array<number>(DAYS);
  for (const [from, to] of spans(days)) out.fill(f(from), from, to);
  return out;
}

/** One release chain's year, from the feed (the pipeline's `live.compose`). `undrawn`: the chain's `u` — a DFO place
 *  on its water not drawn yet; only whether the header closure `c` holds for it too. `speaks`: an undrawn place's own
 *  chain, which carries `n` (the days DFO speaks for it). (T668) `groups`: the chain's `s` — each region's places
 *  answer apart and the most strict per fish speaks (`strictestIx`; `tables` take a merged verdict or gear list);
 *  `who`: also `a`, the answering place per species and day (a release with fix 4). */
export function composeChain(lids: readonly string[], feed: SalmonFeed, undrawn: readonly string[] = [], speaks = false,
                             groups?: readonly (readonly number[])[], tables?: SalmonFeedTables,
                             who = false): Pick<Chain, "sp" | "g" | "m" | "c" | "n" | "a"> {
  return composeChainOn(changeDays, lids, feed, undrawn, speaks, groups, tables, who);
}

/** (T716) `composeChain` asked on EVERY day of the year, as it was before the change-day pass: the reference that pass
 *  is pinned against (`salmonFeed.test.ts`, every chain of the live release). */
export function composeChainEveryDay(...a: Parameters<typeof composeChain>): ReturnType<typeof composeChain> {
  return composeChainOn(() => EVERY_DAY, ...a);
}
const EVERY_DAY = Array.from({ length: DAYS }, (_, d) => d);

function composeChainOn(daysOf: (runs: Iterable<readonly number[]>) => number[],
                        lids: readonly string[], feed: SalmonFeed, undrawn: readonly string[] = [], speaks = false,
                        groups?: readonly (readonly number[])[], tables?: SalmonFeedTables,
                        who = false): Pick<Chain, "sp" | "g" | "m" | "c" | "n" | "a"> {
  const V = tables?.verdicts ?? feed.verdicts, G = tables?.gears ?? feed.gears;
  const subs = (groups ?? [lids.map((_, i) => i)]).map((g) => g.map((i) => feed.locs[lids[i]!]).filter((c): c is SalmonFeedLocation => !!c));
  const first = (xs: Iterable<number>, none: number) => { for (const v of xs) if (v >= 0) return v; return none; };
  const sp: number[][] = [[], [], [], [], []].map(() => new Array<number>(DAYS)), g = new Array<number>(DAYS),
        m = new Array<number>(DAYS);
  // (T716) asked on the days the places' calendars can change on, held to the next: the same arrays as day by day
  const days = daysOf(subs.flatMap((cals) => cals.flatMap((c) => [...c.sp, c.g])));
  for (const [day, to] of spans(days)) {
    const ss: string[] = [];
    for (let k = 0; k < 5; k++) {
      const ixs = subs.map((cals) => first(cals.map((c) => at(c.sp[k]!, day)), 0));
      const ix = ixs.length === 1 ? ixs[0]! : strictestIx(ixs, tables, V);
      sp[k]!.fill(ix, day, to);
      ss.push(V[ix]!.s);          // every feed index checked by parseSalmonFeed; a merged one is ours
    }
    const gs = subs.map((cals) => first(cals.map((c) => at(c.g, day)), -1));
    g.fill(gs.length === 1 ? gs[0]! : gearOf(gs, tables, G), day, to);
    m.fill(MAP_CODES.indexOf(mapStatus(ss)), day, to);
  }
  const spr = sp.map(runsOf);
  // (T356) closed by the region header every day, and so is every undrawn place the feed lists: `c`, never partial
  const shut = undrawn.every((l) => !feed.locs[l] || closedByHeader(feed.locs[l]!.sp, V).length);
  const c = shut ? closedByHeader(spr, V) : [];
  // (T372, owner 2026-11-04: the trout way) an undrawn place never paints the map — it is the card's caveat; `n`: the
  // pipeline's `speaks_runs`, a day some species has DFO's own verdict
  const n = speaks ? runsOf(sp[0]!.map((_, day) => (sp.some((x) => !V[x[day]!]!.d) ? 1 : 0))) : null;
  const a = who ? answeringPlaces(spr, lids.map((l) => feed.locs[l]), groups, daysOf) : null;
  return { sp: spr, g: runsOf(g), m: runsOf(m), ...(c.length ? { c } : {}), ...(n ? { n } : {}), ...(a ? { a } : {}) };
}

/** (T356) the pipeline's `deliver.salmon.closed_by_header`: the header notes answering every species every day, else [] */
function closedByHeader(sp: readonly (readonly number[])[], V: readonly Verdict[]): string[] {
  const ids = new Set<string>();
  for (const runs of sp) for (let k = 1; k < runs.length; k += 2) {
    const c = runs[k]! >= 0 ? V[runs[k]!]!.c : undefined;
    if (!c) return [];
    ids.add(c);
  }
  return [...ids].sort();
}

function wordsWithFeed(release: Readonly<Record<string, readonly SalmonWords[]>>, feed: SalmonFeed): Record<string, readonly SalmonWords[]> {
  const out: Record<string, readonly SalmonWords[]> = {};
  for (const [l, w] of Object.entries(release)) if (!feed.locs[l]) out[l] = w;
  for (const [l, c] of Object.entries(feed.locs)) if (c.x?.length) out[l] = c.x;
  return out;
}

/** What a salmon file read through a feed says about the feed (absent: the release's own file). */
export interface SalmonFeedFacts {
  readonly at: string; readonly stale: readonly string[];
  /** (T521) places the feed lists that no curated location knows, whose list (`unplaced.json`) could not be read */
  readonly unplacedUnread?: number;
}
const FEED = Symbol("salmonFeed");
type WithFeed = SalmonFacts & { readonly [FEED]?: SalmonFeedFacts };

/** The release's salmon with the feed's answers: same sections, same chains, the feed's verdicts, gears and page
 *  dates. A region the feed does not carry keeps the release's page date. */
export function withSalmonFeed(f: SalmonFacts, feed: SalmonFeed): SalmonFacts {
  const { payload, stale, unread } = feedPayload(f.payload, feed);
  const out: WithFeed = { handles: f.handles, chains: f.chains, chainOf: (sid) => f.chainOf(sid), payload, loaded: f.loaded,
                          [FEED]: { at: feed.at, stale, ...(unread ? { unplacedUnread: unread } : {}) } };
  return out;
}

/**
 * (T716) A LIST WHOSE ITEMS ARE MADE WHEN FIRST READ, then kept: an ordinary array to its readers (index, length, map,
 * spread, equality), each slot a getter until it is read, then its value.
 */
function lazyList<T>(n: number, make: (i: number) => T): T[] {
  const out = new Array<T>(n);
  for (let i = 0; i < n; i++) {
    Object.defineProperty(out, i, {
      configurable: true, enumerable: true,
      get() {
        const v = make(i);
        Object.defineProperty(out, i, { value: v, enumerable: true, writable: false, configurable: false });
        return v;
      },
    });
  }
  return out;
}

/**
 * (T716) THE FEED LAID OVER THE RELEASE'S HEAD, ONCE PER HEAD AND FEED. It reads only the head (`payload`: every chain
 * of the province) and the feed, never the slices — yet each set of slices asked (a page's regions; every one for the
 * map) is its own `SalmonFacts`, and each was composed again: 1.0 s on the emulator's JS thread the first time a Fraser
 * page asked its four regions (testbed `regs-open`, `[work] dfo pick 1010.5 ms`), the map held over the pane all that
 * while. Keyed by the two objects themselves, so a new release head or a new feed is composed afresh — never an older
 * answer.
 */
const COMPOSED = new WeakMap<Payload, WeakMap<SalmonFeed, { payload: Payload; stale: string[]; unread: number }>>();
function feedPayload(P: Payload, feed: SalmonFeed): { payload: Payload; stale: string[]; unread: number } {
  let byFeed = COMPOSED.get(P);
  if (!byFeed) { byFeed = new WeakMap(); COMPOSED.set(P, byFeed); }
  let done = byFeed.get(feed);
  if (!done) { done = composeFeedPayload(P, feed); byFeed.set(feed, done); }
  return done;
}

function composeFeedPayload(P: Payload, feed: SalmonFeed): { payload: Payload; stale: string[]; unread: number } {
  const regions = Object.fromEntries(Object.entries(P.regions).map(([slug, r]) => {
    const fr = feed.regions[slug];
    if (!fr) return [slug, r];
    // (T350) the bucket is read from the notes, so it goes with them: a feed region that carries notes and no bucket
    // has none today (the page dropped its combined limit) — never the release's bucket under the feed's notes
    // (T356) and so are the shared limits and the sizes
    // (T482) the hours too: a feed's daylight note folds into ITS hours, never the release's (or none)
    const { bucket: _releaseBucket, hours: _releaseHours, ...rest } = r;
    const head = fr.notes ? { notes: fr.notes, ...(fr.bucket ? { bucket: fr.bucket } : {}), ...(fr.hours ? { hours: fr.hours } : {}),
                              shared: fr.shared!, sizes: fr.sizes! }
      : { notes: r.notes, ...(r.bucket ? { bucket: r.bucket } : {}), ...(r.hours ? { hours: r.hours } : {}), shared: r.shared, sizes: r.sizes };
    return [slug, { ...rest, asof: fr.asof ?? r.asof, ...head }];
  }));
  // (T668) a two-region chain may add a merged verdict or gear list: to copies of the feed's tables, never the feed's
  const tables = feedTables(feed);
  // the release's own closure (`c`) never outlives its verdicts: the feed's composition says whether one holds;
  // (T668 fix 4) `a` re-read from the feed where the release names places (absent before fix 4: none, as it was)
  // (T716, owner 2026-11-13: "Maybe a card builds its item when it needs it?") EACH CHAIN WHEN FIRST READ: a page reads
  // its own water's chains, the Salmon map the chains of the sections it draws; none is built for nobody
  const chains = lazyList(P.chains.length, (i) => {
    const { c: _c, n, a, ...c } = P.chains[i]!;
    return { ...c, ...composeChain(c.l, feed, c.u ?? [], n !== undefined, c.s, tables, a !== undefined) };
  });
  // (T549) the release's location set: what it shows (chains) and what it states it does not (`off`)
  const shown = new Set(P.chains.flatMap((c) => [...c.l, ...(c.u ?? [])]));
  const offFeed = Object.fromEntries(Object.keys(P.off ?? {}).filter((l) => feed.locs[l]).map((l) => {
    const { sp, g, m } = composeChain([l], feed);
    return [l, { sp, g, m }];
  }));
  const feedOnly: Record<string, number> = {};
  for (const [l, c] of Object.entries(feed.locs)) if (!shown.has(l) && !P.off?.[l]) feedOnly[c.r] = (feedOnly[c.r] ?? 0) + 1;
  // (T549 item 5) a place whose row the feed could not read keeps its last answers: said on the card
  const held = Object.values(feed.regions).flatMap((r) => (r.holds ?? []).filter((h) => h.kept === "carried").map((h) => h.location_id)).sort();
  const payload: Payload = {
    ...P, regions, verdicts: tables.verdicts, gears: tables.gears,
    // (T321) a location the feed carries answers with the feed's rows in DFO's words; the rest keep the release's
    x: wordsWithFeed(P.x ?? {}, feed),
    chains,
    ...(Object.keys(offFeed).length ? { offFeed } : {}),
    ...(Object.keys(feedOnly).length ? { feedOnly } : {}),
    ...(held.length ? { held } : {}),
    // a place not drawn yet shows while DFO lists it: the feed's say, not the build's (so never dormant here)
    unmapped: P.unmapped.filter((u) => P.chains[u.chain]?.l.some((l) => !!feed.locs[l]))
      .map(({ d: _d, ...u }) => u),
    // (T521) the feed's places no curated location knows, shown on their water's page
    ...(feed.unplacedRows?.length ? { feedUnplaced: feed.unplacedRows } : {}),
  };
  const stale = Object.entries(feed.regions).filter(([, r]) => r.stale || r.held).map(([s]) => s).sort();
  // (T521) the index counts places its unplaced.json was not read for: said on the card's feed line, never hidden
  const unread = feed.unplacedRows ? 0 : feed.unplaced ?? 0;
  return { payload, stale, unread };
}

/**
 * (T276, owner 2026-10-29) THE ONE LINE BY THE MODE SWITCH: when we last fetched DFO's pages — the feed's own time (`at`),
 * not the pages' dates. Null when the salmon file is the release's own (no feed read: the bundle holds no fetch time).
 */
export function salmonCheckedLine(f: SalmonFacts | null): string | null {
  const at = salmonFeedOf(f)?.at;
  const d = at ? new Date(at) : null;
  if (!d || Number.isNaN(d.getTime())) return null;
  const b = bcFields(d.getTime());      // (T551) the B.C. day, the same one the card's line below says
  return `DFO rules checked ${MONTHS[b.month - 1]} ${b.day}`;   // the calendar spec's month names (gate 4)
}

/** The feed a salmon file was read through, or null (the release's own). */
export const salmonFeedOf = (f: SalmonFacts | null): SalmonFeedFacts | null => (f as WithFeed | null)?.[FEED] ?? null;

/**
 * THE ONE CHOICE: the captured feed, unless there is none or the release was cut from pages newer than the capture
 * (its newest page date after the capture's day) — then the release's own file.
 */
export function pickSalmon(release: SalmonFacts, feed: SalmonFeed | null): SalmonFacts {
  const newest = Object.values(release.payload.regions).map((r) => r.asof).filter((x): x is string => !!x).sort().pop();
  if (feed && !(newest && newest > feed.at.slice(0, 10))) return withSalmonFeed(release, feed);
  return releaseOnly(release);
}

/** The release's own file, its places that were dormant at build (`d: 1`) left off the card. */
function releaseOnly(f: SalmonFacts): SalmonFacts {
  const P = f.payload;
  const unmapped = P.unmapped.filter((u) => !(u as { d?: number }).d);
  if (unmapped.length === P.unmapped.length) return f;
  return { handles: f.handles, chains: f.chains, chainOf: (sid) => f.chainOf(sid), payload: { ...P, unmapped }, loaded: f.loaded };
}

/** The card's line for the feed: "Checked with DFO Oct 27, 2026, 11:04 am" (B.C. clock, labelled), the stale regions said. */
export function salmonFeedLine(f: SalmonFacts | null): string | null {
  const live = salmonFeedOf(f);
  if (!live) return null;
  const t = new Date(live.at);
  if (Number.isNaN(t.getTime())) return null;
  // (T551, T580 F15) B.C.'s clock, labelled, not the device's `getHours()`: Hermes (OS tz data) and Chrome (ICU) disagreed
  const b = bcFields(t.getTime()), h = b.hour, mm = String(b.minute).padStart(2, "0");
  const when = `${MONTHS[b.month - 1]} ${b.day}, ${b.year}, ${h % 12 || 12}:${mm} ${h < 12 ? "am" : "pm"} ${BC_TIME_LABEL}`;
  const stale = live.stale.length
    ? ` (Region ${live.stale.map((s) => s.toUpperCase()).join(", ")} could not be read; its earlier rules shown)` : "";
  const n = live.unplacedUnread ?? 0;
  const unread = n ? ` (${n} DFO place${n === 1 ? "" : "s"} not yet on the map could not be read)` : "";
  return `Checked with DFO ${when}${stale}${unread}`;
}

/** The Salmon card with the feed's line (`checked`: null when the release's own file answers). */
export interface SalmonCardLiveVM extends SalmonCardVM {
  readonly checked: string | null;
  /** (T236) the feed's publish time (ISO) the card was read through; null: the release's own file */
  readonly feedAt?: string | null;
}

export function salmonCardLive(f: SalmonFacts, page: { sids: readonly number[]; item: string | null; gear?: SalmonGearInput },
                               on: Date): SalmonCardLiveVM {
  return { ...salmonCard(f, page, on), checked: salmonFeedLine(f), feedAt: salmonFeedOf(f)?.at ?? null };
}

/** (T521) The bell's DFO section words. */
export const DFO_NOTICE_WORDS = {
  title: "New DFO waters",
  intro: "DFO has posted salmon rules for waters we have not placed on the map yet. Check each one before you fish.",
  badge: (n: number) => `${n} DFO ${n === 1 ? "water" : "waters"} not on the map yet`,
  where: (region: string, name: string, water: string) => [`DFO Region ${region.toUpperCase()} ${name}`.trim(), water].filter(Boolean).join(" · "),
  unread: (n: number) => `${n} DFO place${n === 1 ? "" : "s"} not yet on the map could not be read. They will show when the next check reads them.`,
} as const;

/** (T521) One bell row: a DFO rule on a water no curated binding knows (the in-season rows' shape, no link). */
export interface DfoNoticeRow { readonly id: string; readonly where: string; readonly text: string }
export interface DfoNoticeVM {
  readonly rows: readonly DfoNoticeRow[];
  /** places the index counts whose list could not be read: said in the bell, and counted */
  readonly unread: string | null;
  readonly count: number;
  readonly label: string | null;
}

/**
 * (T521, owner 2026-11-09) THE BELL'S DFO SECTION, from the same feed read as the Salmon card: a place on a water no
 * curated binding knows (`items` empty — a completely new water) is a global notice naming DFO's region and its words,
 * one row per DFO row. A place on a known water is that water's Salmon card's (`salmonCard` waterRules), never here.
 * No feed captured: nothing to say (the card's own line says the feed's state).
 */
export function dfoNoticeRows(feed: SalmonFeed | null): DfoNoticeVM {
  const rows = (feed?.unplacedRows ?? []).filter((u) => !u.items.length).flatMap((u) => u.rows.map((row, k) => ({
    id: `${u.region}:${u.fingerprint}:${k}`,
    where: DFO_NOTICE_WORDS.where(u.region, feed!.regions[u.region]?.name ?? "", u.water),
    text: [u.scope, dfoRow(row)].filter(Boolean).join(" — "),
  })));
  const n = feed && !feed.unplacedRows ? feed.unplaced ?? 0 : 0;
  const count = rows.length + (n ? 1 : 0);
  const label = [rows.length ? DFO_NOTICE_WORDS.badge(new Set(rows.map((r) => r.id.split(":").slice(0, 2).join(":"))).size) : null,
                 n ? DFO_NOTICE_WORDS.unread(n) : null].filter(Boolean).join("; ") || null;
  return { rows, unread: n ? DFO_NOTICE_WORDS.unread(n) : null, count, label };
}
