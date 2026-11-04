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
import { salmonCard, type SalmonCardVM, type SalmonFacts, type SalmonGearInput, type SalmonMapStatus, type SalmonNote, type SalmonBucket, type SalmonShared, type SalmonSizes, type SalmonWords } from "./salmon";

type Payload = SalmonFacts["payload"];
type Chain = Payload["chains"][number];
type Verdict = Payload["verdicts"][number];

/** (T332) a verdict's quota is `tree`, a node of the game-fish quota tree; (T356) dfo-feed/3: a header closure's verdict
 *  carries `c`, a region with notes its `shared` and `sizes`. An older capture is never read — the release's salmon/4
 *  answers until a dfo-feed/3 is fetched */
export const SALMON_FEED_FORMAT = "dfo-feed/3";

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
}

/** The feed as the app reads it, or null when it is not a `dfo-feed/3` it can trust (never a half-read feed). */
export function parseSalmonFeed(x: unknown): SalmonFeed | null {
  const f = x as Partial<SalmonFeed> | null;
  if (!f || typeof f !== "object" || f.format !== SALMON_FEED_FORMAT || typeof f.at !== "string") return null;
  if (!Array.isArray(f.verdicts) || !f.verdicts.length || !Array.isArray(f.gears)) return null;
  if (!f.locs || typeof f.locs !== "object" || !f.regions || typeof f.regions !== "object") return null;
  // (T356) a region's notes come with what they resolve to: never notes without their sizes and shared limits
  for (const r of Object.values(f.regions)) if (r.notes && (!Array.isArray(r.shared) || !Array.isArray(r.sizes))) return null;
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

/** One release chain's year, from the feed (the pipeline's `live.compose`). `undrawn`: the chain's `u` — a DFO place
 *  on its water not drawn yet; only whether the header closure `c` holds for it too. `speaks`: an undrawn place's own
 *  chain, which carries `n` (the days DFO speaks for it). */
export function composeChain(lids: readonly string[], feed: SalmonFeed,
                             undrawn: readonly string[] = [], speaks = false): Pick<Chain, "sp" | "g" | "m" | "c" | "n"> {
  const cals = lids.map((l) => feed.locs[l]).filter((c): c is SalmonFeedLocation => !!c);
  const sp: number[][] = [[], [], [], [], []], g: number[] = [], m: number[] = [];
  for (let day = 0; day < DAYS; day++) {
    const ss: string[] = [];
    for (let k = 0; k < 5; k++) {
      let ix = 0;
      for (const c of cals) { const v = at(c.sp[k]!, day); if (v >= 0) { ix = v; break; } }
      sp[k]!.push(ix);
      ss.push(feed.verdicts[ix]!.s);          // every index checked by parseSalmonFeed
    }
    let gx = -1;
    for (const c of cals) { const v = at(c.g, day); if (v >= 0) { gx = v; break; } }
    g.push(gx);
    m.push(MAP_CODES.indexOf(mapStatus(ss)));
  }
  const spr = sp.map(runsOf);
  // (T356) closed by the region header every day, and so is every undrawn place the feed lists: `c`, never partial
  const shut = undrawn.every((l) => !feed.locs[l] || closedByHeader(feed.locs[l]!.sp, feed).length);
  const c = shut ? closedByHeader(spr, feed) : [];
  // (T372, owner 2026-11-04: the trout way) an undrawn place never paints the map — it is the card's caveat; `n`: the
  // pipeline's `speaks_runs`, a day some species has DFO's own verdict
  const n = speaks ? runsOf(sp[0]!.map((_, day) => (sp.some((x) => !feed.verdicts[x[day]!]!.d) ? 1 : 0))) : null;
  return { sp: spr, g: runsOf(g), m: runsOf(m), ...(c.length ? { c } : {}), ...(n ? { n } : {}) };
}

/** (T356) the pipeline's `deliver.salmon.closed_by_header`: the header notes answering every species every day, else [] */
function closedByHeader(sp: readonly (readonly number[])[], feed: SalmonFeed): string[] {
  const ids = new Set<string>();
  for (const runs of sp) for (let k = 1; k < runs.length; k += 2) {
    const c = runs[k]! >= 0 ? feed.verdicts[runs[k]!]!.c : undefined;
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
export interface SalmonFeedFacts { readonly at: string; readonly stale: readonly string[] }
const FEED = Symbol("salmonFeed");
type WithFeed = SalmonFacts & { readonly [FEED]?: SalmonFeedFacts };

/** The release's salmon with the feed's answers: same sections, same chains, the feed's verdicts, gears and page
 *  dates. A region the feed does not carry keeps the release's page date. */
export function withSalmonFeed(f: SalmonFacts, feed: SalmonFeed): SalmonFacts {
  const P = f.payload;
  const regions = Object.fromEntries(Object.entries(P.regions).map(([slug, r]) => {
    const fr = feed.regions[slug];
    if (!fr) return [slug, r];
    // (T350) the bucket is read from the notes, so it goes with them: a feed region that carries notes and no bucket
    // has none today (the page dropped its combined limit) — never the release's bucket under the feed's notes
    // (T356) and so are the shared limits and the sizes
    const { bucket: _releaseBucket, ...rest } = r;
    const head = fr.notes ? { notes: fr.notes, ...(fr.bucket ? { bucket: fr.bucket } : {}), shared: fr.shared!, sizes: fr.sizes! }
      : { notes: r.notes, ...(r.bucket ? { bucket: r.bucket } : {}), shared: r.shared, sizes: r.sizes };
    return [slug, { ...rest, asof: fr.asof ?? r.asof, ...head }];
  }));
  const payload: Payload = {
    ...P, regions, verdicts: feed.verdicts, gears: feed.gears,
    // (T321) a location the feed carries answers with the feed's rows in DFO's words; the rest keep the release's
    x: wordsWithFeed(P.x ?? {}, feed),
    // the release's own closure (`c`) never outlives its verdicts: the feed's composition says whether one holds
    chains: P.chains.map(({ c: _c, n, ...c }) => ({ ...c, ...composeChain(c.l, feed, c.u ?? [], n !== undefined) })),
    // a place not drawn yet shows while DFO lists it: the feed's say, not the build's (so never dormant here)
    unmapped: P.unmapped.filter((u) => P.chains[u.chain]?.l.some((l) => !!feed.locs[l]))
      .map(({ d: _d, ...u }) => u),
  };
  const stale = Object.entries(feed.regions).filter(([, r]) => r.stale || r.held).map(([s]) => s).sort();
  const out: WithFeed = { handles: f.handles, chains: f.chains, chainOf: (sid) => f.chainOf(sid), payload, loaded: f.loaded,
                          [FEED]: { at: feed.at, stale } };
  return out;
}

/**
 * (T276, owner 2026-10-29) THE ONE LINE BY THE MODE SWITCH: when we last fetched DFO's pages — the feed's own time (`at`),
 * not the pages' dates. Null when the salmon file is the release's own (no feed read: the bundle holds no fetch time).
 */
export function salmonCheckedLine(f: SalmonFacts | null): string | null {
  const at = salmonFeedOf(f)?.at;
  const d = at ? new Date(at) : null;
  if (!d || Number.isNaN(d.getTime())) return null;
  const m = MONTHS[d.getUTCMonth()];   // the calendar spec's month names (gate 4: one calendar spec)
  return `DFO rules checked ${m} ${d.getUTCDate()}`;
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

/** The card's line for the feed: "Checked with DFO Oct 27, 2026, 11:04 am" (local), the stale regions said. */
export function salmonFeedLine(f: SalmonFacts | null): string | null {
  const live = salmonFeedOf(f);
  if (!live) return null;
  const t = new Date(live.at);
  if (Number.isNaN(t.getTime())) return null;
  const h = t.getHours(), mm = String(t.getMinutes()).padStart(2, "0");
  const when = `${MONTHS[t.getMonth()]} ${t.getDate()}, ${t.getFullYear()}, ${h % 12 || 12}:${mm} ${h < 12 ? "am" : "pm"}`;
  const stale = live.stale.length
    ? ` (Region ${live.stale.map((s) => s.toUpperCase()).join(", ")} could not be read; its earlier rules shown)` : "";
  return `Checked with DFO ${when}${stale}`;
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
