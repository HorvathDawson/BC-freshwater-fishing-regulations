/**
 * (T1107, owner 2026-11-25: "have a search option that allows search to nearest place etc like the main search bar … search by
 * place or river etc to get gauge only") THE SEARCH ON /gauges: the MAIN SEARCH'S OWN INDEX (core `buildSearchIndex` /
 * `queryIndex`, the code the app's search bar asks on every keystroke), over a file cut for gauges. One search, not a second
 * one: names, aliases, the typo tolerance and the towns are core's. What this file adds is only what a gauge page wants of an
 * answer: a water -> the gauges on it, a town -> the gauges nearest it, a station -> that gauge.
 *
 * THE FILE (`gaugeSearchFile`, written by the site build, read by the page on first use): the index data core's `searchIndex`
 * reads from the release, cut to the waters a gauge serves (a water with none is nothing to answer with) + every town; and each
 * gauge itself as a water of kind "gauge" (item `g:<station>`) whose alias is its station number, so a station's name or its
 * number is found by the SAME query. `near` is nulled: core's "waters near X's town" fallback has no meaning here (the town's
 * own answer is the distance list).
 *
 * Pure and browser-safe (the page's script bundles it): no node imports.
 */
import { buildSearchIndex, queryIndex, type IndexAlias, type IndexPlace, type IndexWater, type SearchIndex, type SearchIndexData } from "../../packages/core/src/searchIndex";
import { metresApart } from "../../packages/core/src/trace";
import { donorState } from "../../packages/core/src/conditions";

/** a gauge as the page's answers need it */
export interface SearchGauge {
  readonly station: string; readonly name: string; readonly path: string;
  readonly lat: number | null; readonly lon: number | null;
  /** its main water's name, for the result line (null: it has none to name) */
  readonly water: string | null;
  /** the items of the waters it is a gauge ON (its own water, a lake it gauges, a section it is a good gauge of) */
  readonly on: readonly string[];
}
export interface GaugeSearchFile {
  readonly format: "gauge-search/1"; readonly release: string;
  readonly index: SearchIndexData<IndexWater, IndexPlace>;
  readonly gauges: readonly SearchGauge[];
}

export const GAUGE_ITEM = "g:";
export const GAUGE_KIND = "gauge";

/** the search file from the release's whole index data (`WaterSource.searchIndex`) and the gauges the site lists */
export function gaugeSearchFile(full: SearchIndexData<IndexWater, IndexPlace>, gauges: readonly SearchGauge[], release: string): GaugeSearchFile {
  const served = new Set(gauges.flatMap((g) => g.on));
  const waters: IndexWater[] = full.waters.filter((w) => served.has(w.item)).map((w) => ({ ...w, near: null }));
  const known = new Set(waters.map((w) => w.item));
  const missing = [...served].filter((i) => !known.has(i));
  if (missing.length) throw new Error(`gauge search: the release's index has no water ${missing.slice(0, 3).join(", ")}${missing.length > 3 ? ` (+${missing.length - 3})` : ""} that a gauge serves`);
  const stations: IndexWater[] = gauges.map((g) => ({ item: GAUGE_ITEM + g.station, name: g.name, kind: GAUGE_KIND, size: null, areaHa: null, pieces: 1, regions: [], mus: [], near: null }));
  const aliases: IndexAlias[] = [...full.aliases.filter((a) => known.has(a.item)),
    ...gauges.map((g): IndexAlias => ({ item: GAUGE_ITEM + g.station, alias: g.station, source: null }))];
  return { format: "gauge-search/1", release,
           index: { waters: [...waters, ...stations], aliases, places: full.places.map((p) => ({ ...p })), cuts: [] }, gauges: [...gauges] };
}

export interface GaugeHit { readonly station: string; readonly name: string; readonly path: string; readonly water: string | null; readonly km: number | null }
/** one block of an answer: why these gauges, and which */
export interface GaugeGroup { readonly kind: "station" | "water" | "place"; readonly title: string; readonly hits: readonly GaugeHit[] }
export interface GaugeAnswer { readonly query: string; readonly groups: readonly GaugeGroup[] }

/** how many gauges a place's block lists, and how many places / waters are answered at most */
export const NEAREST = 6, PLACES = 2, WATERS = 3, OTHERS = 3, STATIONS = 6;

/** the distance in kilometres as it is said: one decimal under 10, whole above */
export const kmWords = (km: number): string => (km < 10 ? `${(Math.round(km * 10) / 10).toFixed(1)} km` : `${Math.round(km)} km`);

export interface GaugeFinder { readonly ix: SearchIndex<IndexWater, IndexPlace>; readonly file: GaugeSearchFile; readonly byStation: ReadonlyMap<string, SearchGauge> }
export function gaugeFinder(file: GaugeSearchFile): GaugeFinder {
  return { ix: buildSearchIndex(file.index), file, byStation: new Map(file.gauges.map((g) => [g.station, g])) };
}

const hitOf = (g: SearchGauge, km: number | null): GaugeHit => ({ station: g.station, name: g.name, path: g.path, water: g.water, km });
const placed = (g: SearchGauge): g is SearchGauge & { lat: number; lon: number } => g.lat !== null && g.lon !== null;

/** the gauges nearest a point, nearest first (a gauge with no position has no distance and is never in the list) */
export function nearestGauges(gauges: readonly SearchGauge[], at: { lat: number; lon: number }, limit: number, skip: ReadonlySet<string> = new Set()): GaugeHit[] {
  return gauges.filter((g) => !skip.has(g.station)).filter(placed)
    .map((g) => ({ g, m: metresApart(at, g) })).sort((a, b) => a.m - b.m || a.g.station.localeCompare(b.g.station))
    .slice(0, limit).map(({ g, m }) => hitOf(g, m / 1000));
}

/**
 * THE ANSWER to what was typed. Core's `queryIndex` finds the waters, gauges (as waters of kind "gauge") and towns; each is
 * answered as a gauge block:
 *  - a station (its name or number): that gauge;
 *  - a river / lake / water: the gauges ON it first (name order), then the nearest others to them, with the distance;
 *  - a place: the gauges nearest it, with the distance.
 * No block at all is `groups: []`: the page says so (never a blank).
 */
export function answerGauges(f: GaugeFinder, query: string): GaugeAnswer {
  const q = query.trim(), a = queryIndex(f.ix, q, 40, PLACES);
  const groups: GaugeGroup[] = [];
  // a station typed by its number is that gauge alone (core's typo tolerance would add its neighbours)
  const key = q.toLowerCase().replace(/\s+/g, "");
  const numbered = a.waters.filter((w) => w.kind === GAUGE_KIND && w.item.slice(GAUGE_ITEM.length).toLowerCase() === key);
  const stationRows = numbered.length ? numbered : a.waters.filter((w) => w.kind === GAUGE_KIND);
  const stations = stationRows.slice(0, STATIONS)
    .map((w) => f.byStation.get(w.item.slice(GAUGE_ITEM.length))).filter((g): g is SearchGauge => !!g).map((g) => hitOf(g, null));
  for (const w of a.waters.filter((x) => x.kind !== GAUGE_KIND).slice(0, WATERS)) {
    const on = f.file.gauges.filter((g) => g.on.includes(w.item)).sort((x, y) => x.name.localeCompare(y.name) || x.station.localeCompare(y.station));
    if (!on.length) continue;
    const hits: GaugeHit[] = on.map((g) => hitOf(g, null));
    // then the nearest others, each as far as from the nearest gauge on the water
    const near = new Map<string, GaugeHit>();
    for (const g of on.filter(placed)) for (const h of nearestGauges(f.file.gauges, g, OTHERS + on.length, new Set(on.map((x) => x.station))))
      if (!near.has(h.station) || h.km! < near.get(h.station)!.km!) near.set(h.station, h);
    hits.push(...[...near.values()].sort((x, y) => x.km! - y.km!).slice(0, OTHERS));
    groups.push({ kind: "water", title: w.matchedAs ? `${w.name} (${w.matchedAs})` : w.name, hits });
  }
  for (const p of a.places) groups.push({ kind: "place", title: p.name, hits: nearestGauges(f.file.gauges, p, NEAREST) });
  // the stations named by what was typed come first when nothing else was asked of; a gauge already shown under a water is not shown twice
  const shown = new Set(groups.flatMap((g) => g.hits.map((h) => h.station)));
  const lone = stations.filter((h) => !shown.has(h.station) && !(a.fuzzy && !numbered.length && groups.length));
  if (lone.length) groups.unshift({ kind: "station", title: lone.length === 1 ? "Gauge" : "Gauges", hits: lone });
  return { query: q, groups };
}

// ── current data (owner 2026-11-25: "it should also indicate gauges without current data and ones with") ─────────────────
/**
 * WHAT THE FEED SAYS OF A STATION, by the APP'S OWN RULE: core's `donorState` (the Conditions tab's gauge rows), asked of
 * the live index's entry as `@app/data` reads it (`percentile`, `observedAt`, `realtime` = listed) at the index's own time
 * (`ref` = `fetchedAt`, as `readingCurrent` is asked). reads / unranked / other-quantity are a gauge that speaks today
 * ("reporting"); `quiet` is "Not reporting today."; `not-realtime` is "Not a real-time gauge." No second rule, no threshold here.
 */
export type DataState = "reporting" | "quiet" | "not-realtime";
export interface LiveIndex {
  readonly fetchedAt: string; readonly listed?: readonly string[];
  readonly stations: Readonly<Record<string, { readonly percentile?: number | null; readonly observedAt?: string | null; readonly reports?: readonly string[] }>>;
}
export function dataStateOf(station: string, live: LiveIndex): DataState {
  const e = live.stations[station];
  const s = donorState({ percentile: e?.percentile ?? null, observedAt: e?.observedAt ?? null,
    ...(live.listed ? { realtime: live.listed.includes(station) } : {}) }, { ref: Date.parse(live.fetchedAt) });
  return s.kind === "not-realtime" ? "not-realtime" : s.kind === "quiet" ? "quiet" : "reporting";
}
/** the words, the app's own (conditions.ts `donorReads`): "Not reporting today." / "Not a real-time gauge." */
export const DATA_WORDS: Readonly<Record<DataState, string>> = { reporting: "Reporting now", quiet: "Not reporting today", "not-realtime": "Not a real-time gauge" };
