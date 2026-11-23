/**
 * THE LIVE DFO SALMON FEED (T211 part 2): `feeds/dfo/index.json` (`dfo-feed/3`, `pipeline/regs/dfo_salmon/live.py`)
 * laid over the release's salmon file, so a DFO rule change reaches the app without a rebuild.
 *
 * THE RELEASE OWNS WHERE, THE FEED OWNS WHAT. The release's chains (`chains[].l`: the DFO locations on a section,
 * narrowest first) and its section runs never change here. The feed carries, per DFO location and day, each species'
 * verdict index (-1: the location says nothing that day) and the gear lines. A chain's year is re-composed from them
 * exactly as the pipeline composes it (`live.compose` == `deliver.salmon.chain_calendar`, pinned by
 * `salmonFeed.golden.ts`): per species and day the FIRST location of the chain with an answer, else verdict 0 (not
 * opened by DFO: closed); (T770) gear every location's lines that day, narrowest first, a line once; the map status
 * open > release > check > closed. Nothing is decided here that the pipeline did not decide.
 *
 * (T796 A5) THE RELEASE'S STATIC PLACES STAND UNDER A FEED: the rule book's full closure of a stretch is a place of its
 * chains (first) that DFO's feed never carries; its year is the release's own (`payload.static`), its verdicts copied
 * into the composed tables (`staticLocs`) — never dropped for being missing from the feed.
 *
 * WHICH ONE (owner 2026-10-27): the last feed this device CAPTURED, whatever its age, with that capture's time on the
 * card — the release's own file only when no feed was ever captured, or when the release was built from pages newer
 * than the capture (`pickSalmon`).
 */
import { MONTHS } from "./calendar.generated";
import { BC_TIME_LABEL } from "./clock";
import { bcFields } from "./day";
import { FEED_FORMATS } from "./feedFormats.generated";
import { reportFailure, reportOnce } from "./failure";
import { isSalmonIssues, unknownIssueKinds, type SalmonIssues } from "./salmonIssues";
import { closedByHeader, dfoRow, salmonCard, salmonDayIndex, type SalmonUnplacedPlace, type SalmonCardVM, type SalmonFacts, type SalmonGearInput, type SalmonMapStatus, type SalmonNote, type SalmonBucket, type SalmonHours, type SalmonShared, type SalmonSizes, type SalmonWords } from "./salmon";

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
  /** (T770) the place's closure lines of another fish (`live.location_calendar` `o`), runs into `gears`; absent: none */
  readonly o?: readonly number[];
  /** (T321) the location's rows in DFO's words (new wording, an unread number, a note) */
  readonly x?: readonly SalmonWords[];
  /** (T870) the location's DFO rows as printed, the rows in force (`live.rows_by_location`); absent: an older feed, or none */
  readonly pr?: readonly string[];
}
/** (T757 item 6) WHAT A FEED STATES IT CARRIES (`index.json` `dfo`, the worker's `/status.json` value): `pieces` >= 1 —
 *  it emits T573's closed-area pieces as locations. Absent on an older feed. The next shape field is one more key here. */
export interface SalmonFeedShape { readonly pieces?: number }

export interface SalmonFeed {
  readonly format: typeof SALMON_FEED_FORMAT;
  /** (T757) the feed's shape; absent: an older feed, which states nothing */
  readonly dfo?: SalmonFeedShape;
  /** (T759) the rows DFO prints that the app has not placed or confirmed, by water (`salmonIssues.ts`); absent: an older
   *  feed, no issues */
  readonly issues?: SalmonIssues;
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
   *  Absent: no table (a pair not held keeps both regions' words). (T669 item 3, LF) `named`: for two openings whose
   *  answer names other regions in another region pair, [a, b, region of a, region of b, the answer] per region pair
   *  (`salmon_pairs.pairs`); `pairs` then holds the numbers with no names, for an app before 0.8.4. */
  readonly pairs?: { readonly format: typeof SALMON_PAIRS_FORMAT; readonly release: string;
                     readonly pairs: readonly (readonly [Verdict, Verdict, Verdict])[];
                     readonly named?: readonly (readonly [Verdict, Verdict, string, string, Verdict])[] };
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
  // (T757) the shape, when it carries one, is whole: an object of numbers
  if (f.dfo !== undefined && (!f.dfo || typeof f.dfo !== "object" || Array.isArray(f.dfo)
      || Object.values(f.dfo).some((v) => typeof v !== "number" || !Number.isFinite(v)))) return null;
  // (T759) the issues, when it carries them, are whole
  if (f.issues !== undefined && !isSalmonIssues(f.issues)) return null;
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
    // (T669 item 3, LF) the named answers per region pair, when it carries them, are whole too
    if (p.named !== undefined && (!Array.isArray(p.named) || !p.named.every((x) => Array.isArray(x) && x.length === 5
        && verdict(x[0]) && verdict(x[1]) && x[0]!.s === "open" && x[1]!.s === "open" && typeof x[2] === "string"
        && typeof x[3] === "string" && verdict(x[4])))) return null;
  }
  for (const l of Object.values(f.locs)) {
    if (!l || !Array.isArray(l.sp) || l.sp.length !== 5 || !Array.isArray(l.g) || (l.o !== undefined && !Array.isArray(l.o))) return null;
    // (T870) the printed rows, when it carries them, are whole: strings
    if (l.pr !== undefined && (!Array.isArray(l.pr) || !l.pr.length || l.pr.some((r) => typeof r !== "string" || !r))) return null;
    const lines = l.o ? [l.g, l.o] : [l.g];
    for (const runs of [...l.sp, ...lines]) for (let k = 1; k < runs.length; k += 2) {
      const v = runs[k]!;
      if (!Number.isInteger(v) || v >= (lines.includes(runs) ? f.gears.length : f.verdicts.length)) return null;
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
  /** (T669 item 3, LF) its answers per region pair, by `namedKey(a, b, region of a, region of b)`: asked first */
  readonly named?: ReadonlyMap<string, Verdict>;
}

/** (T668) The tables a composition over `feed` adds to: copies of the feed's, never the feed's. */
export function feedTables(feed: SalmonFeed): SalmonFeedTables {
  return { verdicts: [...feed.verdicts], gears: feed.gears.map((g) => [...g]),
           ...(feed.pairs ? { pairs: new Map(feed.pairs.pairs.map(([a, b, r]) => [pairKey(a, b), r] as const)) } : {}),
           ...(feed.pairs?.named ? { named: new Map(feed.pairs.named.map(([a, b, ra, rb, r]) => [namedKey(a, b, ra, rb), r] as const)) } : {}) };
}

/** (T668 option 2) a pair of openings as the table keys it: both verdicts' whole content, in fold order */
const pairKey = (a: Verdict, b: Verdict): string => `${stable(a)}\n${stable(b)}`;
/** (T669 item 3, LF) and with the regions of the two groups (`regionLabel`), as `named` keys it */
const namedKey = (a: Verdict, b: Verdict, ra: string, rb: string): string => `${pairKey(a, b)}\n${ra}\n${rb}`;

/** (T669 item 3, LF) a group's region as a merged line names it, from its places' ids: the pipeline's
 *  `deliver.salmon.region_label` ("5b:region:section" -> "Region 5B"; a rule-book place, `book:…`, names none; "" none).
 *  Only the key of the pair table's `named` answers: the words themselves are the Python's. */
const regionLabel = (lids: readonly string[]): string => {
  const slugs = [...new Set(lids.filter((l) => !l.startsWith("book:")).map((l) => l.split(":")[0]!))].sort();
  return slugs.length ? `Region ${slugs.map((x) => x.toUpperCase()).join(", ")}` : "";
};

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
 * freshwater Z4 ladder, T669 item 3 one quota tree of both regions' caps that bind, or both regions' words), looked up
 * by the two verdicts' whole content — a merged tree is drawn like any other (`quotaLine`); a pair the table does not
 * hold (a limit changed since the last sync, or no table) shows both regions' words, no number (`u`) — never looser.
 * The app holds no copy of the ladder (the cascade is resolved in Python).
 */
function strictestIx(ixs: readonly number[], t: SalmonFeedTables | undefined, V: readonly Verdict[],
                     labels: readonly string[] = []): number {
  let best = ixs[0]!, rb = labels[0] ?? "";
  for (let k = 1; k < ixs.length; k++) {
    const j = ixs[k]!, r = labels[k] ?? "";
    const a = V[best]!, b = V[j]!;
    if (STRICT.indexOf(b.s) < STRICT.indexOf(a.s)) { best = j; rb = r; }
    else if (a.s === "open" && b.s === "open" && stable(a) !== stable(b)) {
      if (!t) throw new Error("salmon feed: two regions' openings need a merged verdict and no tables were given");
      // (T669 item 3, LF) the answer for THIS region pair first (it names its regions), then the pair's own
      const said = t.named?.get(namedKey(a, b, rb, r)) ?? t.pairs?.get(pairKey(a, b));
      // the fold's region after a merge, as `salmon_pairs.pairs` folds it: the region whose opening it is, else none
      if (said) { best = intern(t.verdicts, said); rb = stable(said) === stable(a) ? rb : stable(said) === stable(b) ? r : ""; continue; }
      const n = [...new Set([a, b].flatMap((v) => (v.n ? v.n.split(", ") : [])).filter(Boolean))].sort();
      best = intern(t.verdicts, { s: "open", t: [...new Set([a.t, b.t].filter(Boolean))].join("; "), u: 1, ...(n.length ? { n: n.join(", ") } : {}) } as Verdict);
      rb = "";
    }
  }
  return best;
}

/** (T668) the gear lines of a two-region chain's groups that day: every region's, in region order, a line said once
 *  (`deliver.salmon.both_gear`) — DFO's words, not ranked. (T770, owner 2026-11-16: "Show both") and the same within
 *  a group: every place's lines, narrowest first (`deliver.salmon.gear_on`, `live.compose`) */
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
                             who = false): Pick<Chain, "sp" | "g" | "o" | "m" | "c" | "n" | "a"> {
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
                        who = false): Pick<Chain, "sp" | "g" | "o" | "m" | "c" | "n" | "a"> {
  const V = tables?.verdicts ?? feed.verdicts, G = tables?.gears ?? feed.gears;
  const subs = (groups ?? [lids.map((_, i) => i)]).map((g) => g.map((i) => feed.locs[lids[i]!]).filter((c): c is SalmonFeedLocation => !!c));
  // (T669 item 3, LF) each group's region, the key of the pair table's `named` answers (`live.compose` `labels`)
  const labels = groups ? groups.map((g) => regionLabel(g.map((i) => lids[i]!))) : [];
  const first = (xs: Iterable<number>, none: number) => { for (const v of xs) if (v >= 0) return v; return none; };
  const sp: number[][] = [[], [], [], [], []].map(() => new Array<number>(DAYS)), g = new Array<number>(DAYS),
        o = new Array<number>(DAYS), m = new Array<number>(DAYS);
  // (T716) asked on the days the places' calendars can change on, held to the next: the same arrays as day by day
  const days = daysOf(subs.flatMap((cals) => cals.flatMap((c) => [...c.sp, c.g, ...(c.o ? [c.o] : [])])));
  for (const [day, to] of spans(days)) {
    const ss: string[] = [];
    for (let k = 0; k < 5; k++) {
      const ixs = subs.map((cals) => first(cals.map((c) => at(c.sp[k]!, day)), 0));
      const ix = ixs.length === 1 ? ixs[0]! : strictestIx(ixs, tables, V, labels);
      sp[k]!.fill(ix, day, to);
      ss.push(V[ix]!.s);          // every feed index checked by parseSalmonFeed; a merged one is ours
    }
    // (T770) every place's gear lines that day, narrowest first, a line once — then two regions' groups the same way
    const gs = subs.map((cals) => gearOf(cals.map((c) => at(c.g, day)), tables, G));
    g.fill(gs.length === 1 ? gs[0]! : gearOf(gs, tables, G), day, to);
    // (T770, `live.compose` `o`) another fish's closure lines: every place's, then every group's, exactly as the gear
    const os = subs.map((cals) => gearOf(cals.map((c) => (c.o ? at(c.o, day) : -1)), tables, G));
    o.fill(os.length === 1 ? os[0]! : gearOf(os, tables, G), day, to);
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
  return { sp: spr, g: runsOf(g), ...(o.some((x) => x >= 0) ? { o: runsOf(o) } : {}), m: runsOf(m),
           ...(c.length ? { c } : {}), ...(n ? { n } : {}), ...(a ? { a } : {}) };
}

/** (T870) each place's printed rows: a place the feed carries has the feed's (none when it carries none: an older feed —
 *  never the release's rows under the feed's answers); the rest keep the release's */
function rowsWithFeed(release: Readonly<Record<string, readonly string[]>>, feed: SalmonFeed): Record<string, readonly string[]> {
  const out: Record<string, readonly string[]> = {};
  for (const [l, r] of Object.entries(release)) if (!feed.locs[l]) out[l] = r;
  for (const [l, c] of Object.entries(feed.locs)) if (c.pr?.length) out[l] = c.pr;
  return out;
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
  // (T796 A5) the release's static places (the rule book's closures) beside the feed's: their years from the release
  const statics = staticLocs(P, feed, tables);
  const withStatics: SalmonFeed = Object.keys(statics).length ? { ...feed, locs: { ...feed.locs, ...statics } } : feed;
  // the release's own closure (`c`) never outlives its verdicts: the feed's composition says whether one holds;
  // (T668 fix 4) `a` re-read from the feed where the release names places (absent before fix 4: none, as it was)
  // (T716, owner 2026-11-13: "Maybe a card builds its item when it needs it?") EACH CHAIN WHEN FIRST READ: a page reads
  // its own water's chains, the Salmon map the chains of the sections it draws; none is built for nobody
  const chains = lazyList(P.chains.length, (i) => {
    // (T770) the release's `o` too: the feed's composition says whether another fish's closure stands (absent: none)
    const { c: _c, o: _o, n, a, ...c } = P.chains[i]!;
    return { ...c, ...composeChain(c.l, withStatics, c.u ?? [], n !== undefined, c.s, tables, a !== undefined) };
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
    pr: rowsWithFeed(P.pr ?? {}, feed),
    chains,
    ...(Object.keys(offFeed).length ? { offFeed } : {}),
    ...(Object.keys(feedOnly).length ? { feedOnly } : {}),
    ...(held.length ? { held } : {}),
    // a place not drawn yet shows while DFO lists it: the feed's say, not the build's (so never dormant here)
    unmapped: P.unmapped.filter((u) => P.chains[u.chain]?.l.some((l) => !!feed.locs[l]))
      .map(({ d: _d, ...u }) => u),
    // (T812, owner 2026-11-10: "if they remove it circles should go away") a circle shows while today's page names it
    ...(P.ca ? { ca: Object.fromEntries(Object.entries(P.ca).filter(([l]) => !!feed.locs[l])) } : {}),
    // (T521) the feed's places no curated location knows, shown on their water's page
    ...(feed.unplacedRows?.length ? { feedUnplaced: feed.unplacedRows } : {}),
    // (T759) the feed's issues, shown as banners on their waters' stretches (`salmonIssueBanners`)
    ...(feed.issues ? { feedIssues: feed.issues } : {}),
  };
  // (T759, W46) a kind this app has no words for is shown with the generic words, and reported (once a run, per kind)
  for (const k of feed.issues ? unknownIssueKinds(feed.issues) : [])
    reportOnce(`salmon-issue-kind:${k}`, new Error(`salmon feed: issue kind "${k}" has no words in this app; shown with the generic words`));
  const stale = Object.entries(feed.regions).filter(([, r]) => r.stale || r.held).map(([s]) => s).sort();
  // (T521) the index counts places its unplaced.json was not read for: said on the card's feed line, never hidden
  const unread = feed.unplacedRows ? 0 : feed.unplaced ?? 0;
  return { payload, stale, unread };
}

/**
 * (T796 A5) The release's static places as feed locations: each one's year (`payload.static`, release verdict and gear
 * indices) re-indexed into the composed tables — a verdict the feed already holds keeps its index, another is added.
 * A static place the feed itself names is an error (an id the release reserved: `book:`), never one silently winning.
 */
function staticLocs(P: Payload, feed: SalmonFeed, tables: SalmonFeedTables): Record<string, SalmonFeedLocation> {
  const ids = Object.keys(P.static ?? {});
  if (!ids.length) return {};
  const named = ids.filter((l) => feed.locs[l]);
  if (named.length) throw new Error(`salmon feed: names the release's static place(s) ${named.join(", ")}`);
  const vIx = new Map(tables.verdicts.map((v, i) => [stable(v), i] as const));
  const gIx = new Map(tables.gears.map((g, i) => [stable(g), i] as const));
  const add = <T>(items: T[], ix: Map<string, number>, x: T): number => {
    const k = stable(x);
    let i = ix.get(k);
    if (i === undefined) { i = items.push(x) - 1; ix.set(k, i); }
    return i;
  };
  const out: Record<string, SalmonFeedLocation> = {};
  for (const lid of ids) {
    const y = P.static![lid]!;
    out[lid] = {
      r: P.p?.[lid]?.r ?? "",
      sp: y.sp.map((runs) => runs.map((x, k) => (k % 2 && x >= 0 ? add(tables.verdicts, vIx, P.verdicts[x]!) : x))),
      g: y.g.map((x, k) => (k % 2 && x >= 0 ? add(tables.gears, gIx, [...P.gears[x]!]) : x)),
    };
  }
  return out;
}


/** The feed a salmon file was read through, or null (the release's own). */
export const salmonFeedOf = (f: SalmonFacts | null): SalmonFeedFacts | null => (f as WithFeed | null)?.[FEED] ?? null;

/**
 * (T757 item 6) THE MINIMUM FEED SHAPE A RELEASE REQUIRES: `pieces: 1` when some chain names a closed-area piece that
 * covers a section (`p[l].piece` and `bound`, T573) — a feed that does not emit pieces would let each one fall through
 * to the place around it, open. Null: the release requires nothing of the feed.
 */
export function requiredFeedShape(P: Payload): Required<SalmonFeedShape> | null {
  // (T796 A5) a rule-book place (`static`) is never a piece
  const pieces = P.chains.some((c) => c.l.some((l) => { const p = P.p?.[l]; return !!p && "piece" in p && p.piece === 1 && p.bound === 1; }));
  return pieces ? { pieces: 1 } : null;
}

/** (T757) Does the feed state at least the shape required (`requiredFeedShape`)? Nothing required: yes. */
export function feedMeets(feed: SalmonFeed, need: SalmonFeedShape | null): boolean {
  return !need || (Object.entries(need) as [keyof SalmonFeedShape, number][]).every(([k, v]) => (feed.dfo?.[k] ?? 0) >= v);
}

/** (T757) a feed too old for its release: reported once per head and feed read, never per render */
const SHORT_REPORTED = new WeakMap<Payload, WeakSet<SalmonFeed>>();
const SHORT = Symbol("salmonFeedShort");

/**
 * THE ONE CHOICE: the captured feed, unless there is none or the release was cut from pages newer than the capture
 * (its newest page date after the capture's day) — then the release's own file. (T757 item 6) So too when the feed
 * does not carry the shape the release requires (`requiredFeedShape`): the release's own file for EVERY chain, said on
 * the card's feed line (`salmonFeedLine`) and reported.
 */
export function pickSalmon(release: SalmonFacts, feed: SalmonFeed | null): SalmonFacts {
  const newest = Object.values(release.payload.regions).map((r) => r.asof).filter((x): x is string => !!x).sort().pop();
  if (!feed || (newest && newest > feed.at.slice(0, 10))) return releaseOnly(release);
  const need = requiredFeedShape(release.payload);
  if (feedMeets(feed, need)) return withSalmonFeed(release, feed);
  let seen = SHORT_REPORTED.get(release.payload);
  if (!seen) { seen = new WeakSet(); SHORT_REPORTED.set(release.payload, seen); }
  if (!seen.has(feed)) {
    seen.add(feed);
    reportFailure(new Error(`salmon feed: the release requires feed shape ${JSON.stringify(need)}, the feed of ${feed.at} `
                            + `states ${JSON.stringify(feed.dfo ?? {})}; the release's own salmon file is shown`));
  }
  const own = releaseOnly(release);
  const out: SalmonFacts & { readonly [SHORT]?: true } = { handles: own.handles, chains: own.chains, chainOf: (sid) => own.chainOf(sid),
                                                           payload: own.payload, loaded: own.loaded, [SHORT]: true };
  return out;
}

/**
 * (T796 A6) DOES THE FEED CLOSE ANY PLACE TO ALL FISHING ON `on` — any place's species verdict that day carrying `f` (DFO's
 * "Finfish closure")? The web's cue to read the release's salmon files for the Standard map (`useSalmonColour`'s
 * `finfish`): asked of the feed index the app already holds, so a day with none costs no request. Province-wide, not the
 * view: a place is tied to sections only by the release's chains, which are in the files this decides whether to read.
 */
export function feedFinfishOn(feed: SalmonFeed, on: Date): boolean {
  const day = salmonDayIndex(on);
  return Object.values(feed.locs).some((l) => l.sp.some((runs) => {
    const v = at(runs, day);
    return v >= 0 && !!feed.verdicts[v]?.f;
  }));
}

/** The release's own file, its places that were dormant at build (`d: 1`) left off the card. */
function releaseOnly(f: SalmonFacts): SalmonFacts {
  const P = f.payload;
  const unmapped = P.unmapped.filter((u) => !(u as { d?: number }).d);
  if (unmapped.length === P.unmapped.length) return f;
  return { handles: f.handles, chains: f.chains, chainOf: (sid) => f.chainOf(sid), payload: { ...P, unmapped }, loaded: f.loaded };
}

/** (T757) the card's feed line when the latest feed could not be used with this release (`pickSalmon`) */
const SHORT_FEED_LINE = "The latest DFO check could not be used with this app’s data, so the DFO rules this data was built with are shown. Check DFO’s page before you fish.";

/** The card's line for the feed: "Checked with DFO Oct 27, 2026, 11:04 am" (B.C. clock, labelled), the stale regions said. */
export function salmonFeedLine(f: SalmonFacts | null): string | null {
  // (T757) a feed the release could not use (`pickSalmon`): said where the feed's line goes, as its stale regions are
  if ((f as { readonly [SHORT]?: true } | null)?.[SHORT]) return SHORT_FEED_LINE;
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
  intro: "DFO has posted salmon rules for waters I have not placed on the map yet. Check each one before you fish.",
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
