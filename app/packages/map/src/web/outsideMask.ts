/**
 * THE BASEMAP'S LABELS, CONFINED TO BRITISH COLUMBIA: the outside-BC mask fetched once per session (`useOutsideMask`),
 * and laid on every basemap label layer as a `within` filter once it lands (`useLabelsInBC`).
 */
import { useEffect, useState, type MutableRefObject } from "react";
import type * as maplibregl from "maplibre-gl";
import { toExpression } from "../legacy-filter";

/**
 * The outside-BC mask, fetched once for the whole session.
 *
 * The `outside` FILL layer fetches this itself, from the same URL, as a normal GeoJSON
 * source. This second copy exists because the basemap's labels are confined to the province
 * with a `within` filter, and a MapLibre expression cannot reference a source — it needs the
 * polygon as a literal. Module-level and keyed by URL, so a theme toggle or a remount does
 * not refetch 61 KB, and a failure just leaves the labels as they were.
 */
const outsideCache: Record<string, unknown> = {};
export function useOutsideMask(url: string | undefined): unknown {
  const [got, setGot] = useState<unknown>(() => (url ? outsideCache[url] : undefined));
  useEffect(() => {
    if (!url || url in outsideCache) return;
    let live = true;
    fetch(url).then((r) => (r.ok ? r.json() : null)).then((j) => {
      if (!j || !live) return;
      outsideCache[url] = j;
      setGot(j);
    }).catch(() => {});
    return () => { live = false; };
  }, [url]);
  return got;
}

/**
 * Confine the BASEMAP's labels to British Columbia, once the mask has arrived.
 *
 * WHY NOT A PROPERTY FILTER. Overpass can select by province because it queries the OSM
 * database, where the admin relations live. This basemap is a pre-built Protomaps archive,
 * and a MapLibre filter can only read properties the TILE carries: `places` ships
 * `kind`, `name`, `population`, `wikidata` and forty translations, and nothing that says
 * which country a label is in, let alone which province. So the test has to be geometric.
 *
 * The mask is a rectangle with the province punched out, so "inside the mask" IS "outside
 * BC" and the filter is that, negated. Applied here rather than in the constructor because
 * the 61 KB arrives a moment after the first frame; the layers are found by source rather
 * than by a hardcoded list, so a Protomaps upgrade that adds a label layer gets it too.
 *
 * Called AFTER the map is made (`useMapInstance`): absent on the first frame and present a moment later, so this
 * effect has to run once the map exists.
 */
export function useLabelsInBC(map: MutableRefObject<maplibregl.Map | null>, outsideData: unknown) {
  useEffect(() => {
    const m = map.current;
    if (!m || !outsideData) return;
    const apply = () => {
      for (const l of m.getStyle()?.layers ?? []) {
        if (l.type !== "symbol") continue;
        if ((l as { source?: string }).source !== "basemap") continue;
        const had = (l as { filter?: unknown[] }).filter;
        const inBC = ["!", ["within", outsideData]] as unknown[];
        /*
         * CONVERT BEFORE COMBINING. Protomaps writes most of these in the LEGACY filter
         * syntax (`["in", "kind", "river", "stream"]`), and `all` is a valid operator in
         * both syntaxes — so `["all", <expression>, <legacy>]` is ambiguous and MapLibre
         * resolves it as legacy, where `!` is not an operator:
         *
         *   layers.roads_labels_major.filter[1][0]: expected one of
         *   [==, !=, >, >=, <, <=, in, !in, all, any, none, has, !has], "!" found
         *
         * `toExpression` converts a legacy filter and returns an expression untouched, so
         * it runs on all of them rather than sniffing which syntax each layer uses.
         */
        const kept = had ? toExpression(had) : null;
        // Keep the layer's own kind filter; this narrows, it does not replace.
        m.setFilter(l.id, (kept ? ["all", inBC, kept] : inBC) as never);
      }
    };
    if (m.isStyleLoaded()) apply(); else m.once("load", apply);
  }, [outsideData]);
}
