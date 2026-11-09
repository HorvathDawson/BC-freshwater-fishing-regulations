/**
 * (T530, on T487) A GAUGE'S PUBLIC PAGE: `/gauge/<station>/<slug>` on the site. The ONE place the slug and the path are
 * made: the SEO build (`tools/seo/gauges.ts`, which writes the page and `/gauge/stations.json`) and the app's Share on the
 * Conditions gauge card both call these, so the link the app sends is the address the site built.
 *
 * WHICH STATIONS HAVE A PAGE: every station that serves at least one water with a page (its own water, a lake it gauges,
 * a section it is the gauge of). `gaugeHasPage` is that rule on the facts both sides hold; the app asks it of the
 * stations behind the water in front of it.
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

/**
 * Whether a station has a page: it serves some water, and that water has a page (`servedWaters` are the waters it serves
 * that have one). No page, no link: nothing is shared for a station the site does not list.
 */
export const gaugeHasPage = (servedWaters: number): boolean => servedWaters > 0;
