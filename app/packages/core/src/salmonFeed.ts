/**
 * THE LIVE DFO SALMON FEED (T211 part 2): `feeds/dfo/index.json` (`dfo-feed/1`, `pipeline/regs/dfo_salmon/live.py`)
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
import { salmonCard, type SalmonCardVM, type SalmonFacts, type SalmonGearInput, type SalmonMapStatus, type SalmonNote } from "./salmon";

type Payload = SalmonFacts["payload"];
type Chain = Payload["chains"][number];
type Verdict = Payload["verdicts"][number];

export const SALMON_FEED_FORMAT = "dfo-feed/1";

export interface SalmonFeedRegion {
  readonly name: string; readonly url: string; readonly asof: string | null;
  /** when this region's page was last read; `stale`: the last read failed and these are the answers before it */
  readonly checked: string | null; readonly stale: boolean; readonly held: string | null;
  /** (owner 2026-10-28) the region page's header as typed notes — the feed's win over the release's */
  readonly notes?: readonly SalmonNote[];
}
export interface SalmonFeedLocation { readonly r: string; readonly sp: readonly (readonly number[])[]; readonly g: readonly number[] }
export interface SalmonFeed {
  readonly format: typeof SALMON_FEED_FORMAT;
  /** when the feed was published (UTC ISO) — the capture's as-of */
  readonly at: string;
  readonly regions: Readonly<Record<string, SalmonFeedRegion>>;
  readonly verdicts: readonly Verdict[];
  readonly gears: readonly (readonly string[])[];
  readonly locs: Readonly<Record<string, SalmonFeedLocation>>;
}

/** The feed as the app reads it, or null when it is not a `dfo-feed/1` it can trust (never a half-read feed). */
export function parseSalmonFeed(x: unknown): SalmonFeed | null {
  const f = x as Partial<SalmonFeed> | null;
  if (!f || typeof f !== "object" || f.format !== SALMON_FEED_FORMAT || typeof f.at !== "string") return null;
  if (!Array.isArray(f.verdicts) || !f.verdicts.length || !Array.isArray(f.gears)) return null;
  if (!f.locs || typeof f.locs !== "object" || !f.regions || typeof f.regions !== "object") return null;
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
 *  on its water not drawn yet; one the feed lists paints the map `check` every day (partial coverage). */
export function composeChain(lids: readonly string[], feed: SalmonFeed,
                             undrawn: readonly string[] = []): Pick<Chain, "sp" | "g" | "m"> {
  const cals = lids.map((l) => feed.locs[l]).filter((c): c is SalmonFeedLocation => !!c);
  const sp: number[][] = [[], [], [], [], []], g: number[] = [], m: number[] = [];
  for (let day = 0; day < DAYS; day++) {
    const ss: string[] = [];
    for (let k = 0; k < 5; k++) {
      let ix = 0;
      for (const c of cals) { const v = at(c.sp[k]!, day); if (v >= 0) { ix = v; break; } }
      sp[k]!.push(ix);
      ss.push(feed.verdicts[ix]?.s ?? "closed");
    }
    let gx = -1;
    for (const c of cals) { const v = at(c.g, day); if (v >= 0) { gx = v; break; } }
    g.push(gx);
    m.push(MAP_CODES.indexOf(mapStatus(ss)));
  }
  const partial = undrawn.some((l) => !!feed.locs[l]);
  return { sp: sp.map(runsOf), g: runsOf(g), m: partial ? [0, MAP_CODES.indexOf("check")] : runsOf(m) };
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
    return [slug, fr ? { ...r, asof: fr.asof ?? r.asof, notes: fr.notes ?? r.notes } : r];
  }));
  const payload: Payload = {
    ...P, regions, verdicts: feed.verdicts, gears: feed.gears,
    chains: P.chains.map((c) => ({ ...c, ...composeChain(c.l, feed, c.u ?? []) })),
    // a place not drawn yet shows while DFO lists it: the feed's say, not the build's (so never dormant here)
    unmapped: P.unmapped.filter((u) => P.chains[u.chain]?.l.some((l) => !!feed.locs[l]))
      .map(({ d: _d, ...u }) => u),
  };
  const stale = Object.entries(feed.regions).filter(([, r]) => r.stale || r.held).map(([s]) => s).sort();
  const out: WithFeed = { handles: f.handles, chains: f.chains, chainOf: (sid) => f.chainOf(sid), payload, loaded: f.loaded,
                          [FEED]: { at: feed.at, stale } };
  return out;
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
