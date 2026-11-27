/**
 * (T530, on T487) A GAUGE'S PUBLIC PAGE: `/gauge/<station>/<slug>` on the site. The ONE place the slug and the path are
 * made: the SEO build (`tools/seo/gauges.ts`, which writes the page and `/gauge/stations.json`) and the app's Share on the
 * Conditions gauge card both call these, so the link the app sends is the address the site built.
 *
 * WHICH STATIONS HAVE A PAGE: `gaugePageStations` below: every station that serves at least one water with a page (its own
 * water, a lake it gauges, a section it is the gauge of, at any trust).
 */
import { STEADY_RATIO } from "./change";
import { SITE_ORIGIN } from "./share";

/** "Chilliwack/Vedder River" -> "chilliwack-vedder-river"; "Écho Lake" -> "echo-lake". (T192: waters and gauges alike) */
export function slugOf(name: string): string {
  return name.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase().replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

/** `/gauge/08MH001/chilliwack-vedder-river`: the station, then its name's slug ("gauge" when the name has none). */
export const gaugePath = (station: string, name: string): string => `/gauge/${station}/${slugOf(name) || "gauge"}`;

/** The page's address on the public site. */
export const gaugeUrl = (station: string, name: string, origin: string = SITE_ORIGIN): string => `${origin}${gaugePath(station, name)}`;

export type ServesTrust = "own" | "lake" | "good" | "fair" | "weak";
/** a station's flags in the release (`gauge.tidal`, `.backwater`, `.diversion`); absent on a release cut before them (read as not flagged) */
export interface GaugeFlags { readonly tidal: boolean; readonly backwater: boolean; readonly diversion: boolean }
export interface GaugeRow { readonly station: string; readonly name: string; readonly item: string | null; readonly lat: number | null; readonly lon: number | null; readonly flags?: GaugeFlags }
export interface ServeRow { readonly station: string; readonly item: string; readonly trust: ServesTrust }
/** a release's gauges and the waters each serves (`@app/data/release` `readGaugeRows`, from its conditions + core files) */
export interface GaugeRows { readonly gauges: readonly GaugeRow[]; readonly serves: readonly ServeRow[] }

/** The quantities a gauge's chart is drawn in (`gauge_clim`'s parameter) */
export type GaugeParameter = "discharge" | "level";

/**
 * (T1045) WHAT A GAUGE MEASURES, in the words a reader searches: from the parameters the release holds for the station
 * ("flow" for discharge, "level"); none known: "conditions" (never a quantity the release does not say it has). The one place the
 * gauge page's title, heading and the water page's link say it. The forecast is never in it: whether the River Forecast Centre
 * runs one for a gauge is live data, not the release's.
 */
export function gaugeSubject(parameters: readonly GaugeParameter[]): "level and flow" | "flow" | "level" | "conditions" {
  const flow = parameters.includes("discharge"), level = parameters.includes("level");
  return flow && level ? "level and flow" : flow ? "flow" : level ? "level" : "conditions";
}

export const TRUST_RANK: Record<ServesTrust, number> = { own: 0, lake: 1, good: 2, fair: 3, weak: 4 };

/** Each station's waters, each at its best trust (own, lake, good, fair, then weak). */
export function bestServes(rows: GaugeRows): Map<string, Map<string, ServesTrust>> {
  const best = new Map<string, Map<string, ServesTrust>>();
  for (const s of rows.serves) {
    const m = best.get(s.station) ?? new Map<string, ServesTrust>();
    const was = m.get(s.item);
    if (was === undefined || TRUST_RANK[s.trust] < TRUST_RANK[was]) m.set(s.item, s.trust);
    best.set(s.station, m);
  }
  return best;
}

/** The waters a station serves (any trust), best first: what the app asks "has a page?" of, one at a time. */
export const servedItems = (rows: GaugeRows, station: string): string[] =>
  [...(bestServes(rows).get(station) ?? [])].sort((a, b) => TRUST_RANK[a[1]] - TRUST_RANK[b[1]]).map(([item]) => item);

/**
 * WHICH STATIONS HAVE A PAGE: every gauge of the release that serves at least one water with a page (`hasPage`: the water's
 * page exists, `@app/data/regs` `waterHasPage`). The SEO build lists exactly these; the app offers Share for exactly these.
 */
export function gaugePageStations(rows: GaugeRows, hasPage: (item: string) => boolean): Set<string> {
  const out = new Set<string>();
  const best = bestServes(rows);
  for (const g of rows.gauges) for (const item of best.get(g.station)?.keys() ?? []) if (hasPage(item)) { out.add(g.station); break; }
  return out;
}

/**
 * (T1194) THE GAUGE PAGE'S WORDS, the app's screen and (the shared piece) the site's page: one place (AGENTS 33), pinned as
 * finished strings. The page says what the release and the feed say and decides nothing new: it is not a second lead (a
 * water's card is unchanged), and a gauge that serves no water says so.
 */
export const GAUGE_PAGE_WORDS = {
  /** what a water opened off the page names its ‹ ("Gauge") */
  backName: "Gauge",
  /** the header's ‹ on the page itself, and its name for a screen reader */
  back: "Back to Conditions",
  stationLine: (station: string): string => `Station ${station}`,
  servesHeading: "Waters this gauge serves",
  /** the app's foot: this gauge's page on the website (the website's own foot says "Open … in the web app") */
  openOnSite: "Open on the website",
  /** a gauge serving no water: the page opens and says so (owner 2026-11-26: "yes as long as they have up to date data") */
  notMatched: "This gauge is not matched to a water we cover.",
  /** a station the release does not hold (a stale link): shown, never the map and never a neighbour */
  unknown: (station: string): string => `This version of the data has no gauge ${station}.`,
  /** the release could not be read for this gauge */
  releaseFailed: "This gauge's details could not be read.",
  /** the live reading could not be loaded: said where the reading goes; no figure, standing or colour is guessed */
  feedFailed: "The live reading could not be loaded.",
  /** no current reading: when it last reported, said plainly (a deep link to a quiet gauge) */
  noReading: (lastReported: string): string => `No current reading. ${lastReported}`,
  /** each served water's trust, in a word (`weak` is a rough guide) */
  trust: { own: "On this water", lake: "On this lake", good: "Good match", fair: "Fair match", weak: "Rough guide" } as Readonly<Record<ServesTrust, string>>,
  open: (water: string): string => `Open ${water}, conditions`,
  /** the colour bar's mark, aloud: where THIS GAUGE stands (the bar marks a gauge here, not reaches); `near` is core's `FLOW_LEGEND.near` */
  barMark: (near: string): string => `This gauge: ${near}`,
  /** what the measured line has done over about a day, beside the big reading (`gaugeTrend`) */
  trend: { rise: "rising", steady: "steady", fall: "falling" } as const,
  /** the waters list folds past `GAUGE_SERVES_SHOWN` */
  more: (n: number): string => `Show ${n} more`,
  fewer: "Show fewer",
  tidal: "This gauge is tidal: its level rises and falls with the tide and with water backed up from downstream, not only with what this river carries.",
  backwater: "This gauge sits where a lake backs up into the river: its level rises and falls with the lake as well as with what this river carries.",
  diversion: "This gauge measures a diversion: it stands on a canal that takes water out of the river at an intake, so it reads what the canal takes, not everything the river carries.",
} as const;

/** How many served waters show before the list folds (the best trust first). */
export const GAUGE_SERVES_SHOWN = 8;

/**
 * THE NOTES a station's flags put on its page, as a water card says them about a gauge (`tidalNote`, `backwaterNote`,
 * `diversionNote`): the tide and a lake's backwater on LEVEL only (Flow never mixes in a stage), a diversion on both.
 */
export function gaugeNotes(flags: { readonly tidal: boolean; readonly backwater: boolean; readonly diversion: boolean },
                           quantity: GaugeParameter | null): string[] {
  const out: string[] = [];
  if (flags.tidal && quantity === "level") out.push(GAUGE_PAGE_WORDS.tidal);
  if (flags.backwater && quantity === "level") out.push(GAUGE_PAGE_WORDS.backwater);
  if (flags.diversion) out.push(GAUGE_PAGE_WORDS.diversion);
  return out;
}

/** The trend's window: the reading against the one about a day before it. */
export const TREND_WINDOW_MS = 24 * 3_600_000;

/**
 * (T908, restored T1194) WHAT THE MEASURED LINE HAS DONE over about a day, by the same "little change" factor (`STEADY_RATIO`) the
 * forecast uses: a ratio of two FLOWS. Null when there is nothing to compare (no earlier flow above zero, no reading).
 */
export function observedTrend(before: number, now: number): keyof typeof GAUGE_PAGE_WORDS.trend | null {
  if (!(before > 0) || !(now >= 0)) return null;
  const r = now / before;
  return r >= STEADY_RATIO ? "rise" : r <= 1 / STEADY_RATIO ? "fall" : "steady";
}

/** How far from the 24 h mark the look-back value may stand, and how far from the line's end the latest value may: past it the line has a gap there. */
export const TREND_SLACK_MS = 3 * 3_600_000;

/**
 * THE TWO ENDS OF A MEASURED LINE (`at` ISO stamps, `values` aligned, a null a gap): its latest value, and the latest value at least a
 * day before it. Null where the line does not reach back a day, or either end sits in a gap (the latest value more than
 * `TREND_SLACK_MS` before the line's own end; the look-back value more than that before the 24 h mark): never a guess across a gap.
 */
function lookBack(at: readonly string[], values: readonly (number | null)[]): { before: number; now: number } | null {
  const has = (i: number) => values[i] !== null && values[i] !== undefined;
  let last = -1;
  for (let i = values.length - 1; i >= 0; i--) if (has(i)) { last = i; break; }
  if (last < 0) return null;
  const t = Date.parse(at[last] ?? ""), end = Date.parse(at[at.length - 1] ?? "");
  if (!Number.isFinite(t) || !Number.isFinite(end) || end - t > TREND_SLACK_MS) return null;
  for (let j = last - 1; j >= 0; j--) {
    const tj = Date.parse(at[j] ?? "");
    if (Number.isFinite(tj) && t - tj >= TREND_WINDOW_MS && has(j)) return t - tj - TREND_WINDOW_MS > TREND_SLACK_MS ? null : { before: values[j]!, now: values[last]! };
  }
  return null;
}

/** THE TREND OF A MEASURED FLOW LINE: its last value against the one a day before it, by the "little change" ratio (`observedTrend`). Never across a gap. */
export function gaugeTrend(at: readonly string[], values: readonly (number | null)[]): keyof typeof GAUGE_PAGE_WORDS.trend | null {
  const e = lookBack(at, values);
  return e ? observedTrend(e.before, e.now) : null;
}

/**
 * (owner 2026-11-27: "Yes, on Level too") THE TREND OF A MEASURED LEVEL LINE: the CHANGE in the stage over the same day, not a ratio (a
 * ratio of stages is not a ratio of water). Within `LEVEL_DEADBAND_M` either way is "steady": the deadband is 2 cm, chosen from the
 * feed's own levels (2026-10-28, 424 stations): a station's 30-minute reading moves a median 1 mm (p90 4 mm), so sensor noise stays far
 * inside it; its 24 h change is a median 1 cm and 2.2 cm at the 75th percentile, so a lake reads "steady" and a river that moved more than
 * 2 cm in the day says which way. Never across a gap. Tidal water has no trend word (`gaugeReadingVM`): its level swings metres with the tide.
 */
export const LEVEL_DEADBAND_M = 0.02;
export function gaugeLevelTrend(at: readonly string[], values: readonly (number | null)[]): keyof typeof GAUGE_PAGE_WORDS.trend | null {
  const e = lookBack(at, values);
  if (!e) return null;
  const d = e.now - e.before;
  return d > LEVEL_DEADBAND_M ? "rise" : d < -LEVEL_DEADBAND_M ? "fall" : "steady";
}
