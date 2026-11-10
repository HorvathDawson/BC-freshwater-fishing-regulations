/**
 * (T530, on T487) A GAUGE'S PUBLIC PAGE: `/gauge/<station>/<slug>` on the site. The ONE place the slug and the path are
 * made: the SEO build (`tools/seo/gauges.ts`, which writes the page and `/gauge/stations.json`) and the app's Share on the
 * Conditions gauge card both call these, so the link the app sends is the address the site built.
 *
 * WHICH STATIONS HAVE A PAGE: `gaugePageStations` below: every station that serves at least one water with a page (its own
 * water, a lake it gauges, a section it is the gauge of, at any trust).
 */
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
export interface GaugeRow { readonly station: string; readonly name: string; readonly item: string | null; readonly lat: number | null; readonly lon: number | null }
export interface ServeRow { readonly station: string; readonly item: string; readonly trust: ServesTrust }
/** a release's gauges and the waters each serves (`@app/data/release` `readGaugeRows`, from its conditions + core files) */
export interface GaugeRows { readonly gauges: readonly GaugeRow[]; readonly serves: readonly ServeRow[] }

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
