/**
 * THE RELEASE READER (P11): a RegsSource over the release's files, through the loader (lazy, verified, per file).
 *
 * THE APP'S ONE DATA READER (the dev bundle reader was retired, T61). It asks each file for its own tables: a query
 * cannot JOIN across two files, and the reads that matter most here are exactly those — search's nearest towns
 * (near → core), the gauge for a water (core → conditions), the lake road (core → conditions), the panels (paint's
 * arrays, which are not SQL at all). Everything that turns rows into an ANSWER lives in `../sql/shared` (the SQL in
 * `../sql/queries`, written in the pipeline bundle's table names) and is held to one conformance suite
 * (`app/conformance`), run against a hand-written fixture and a release cut by the builder.
 *
 *   single-file reads    `RELEASE_QUERIES` (`./queries.ts`), each on the file the contract says holds its table
 *   cross-file reads     two KEYED reads joined in code (`CROSS`), never a join across files
 *   panels, basins       paint's decoded arrays (`Paint.panelOf`, `members`, `basins`) + flow-area's target areas
 *   lake modes           `lakes` (`lakeFacts`): per lake section, surveyed and last stocked — the map's Depth / Stocked
 *   charts, stocking     no per-lake sheet or release list is shipped: `lakeInfo` null, `stockingHistory` empty,
 *                        search's `stocked` false
 *   regulations          `regulationKeys`: core → water → the key's region file (`loader.regionDb`)
 *
 * What it opens, and when, is the loader's to say: paint at start, core on the first tap or search, near on the first
 * near-search or result line, conditions on the first gauge question, flow-area on the first panel, water and
 * regs-<region> on the first water opened in that region.
 */
import { shownParameter, type WaterFix } from "@app/core";
import { num, str, type Row } from "../sql/db";
import * as BQ from "../sql/queries";
import { boxOfGeoRows, continuedOrds, gaugeWatersOf, link, nearHit, parametersOf, pentadsOf, placesOf, pointerHits, rankNameHits, routesOf, seriesOf, sid,
         traceDown, withNear, type DownOf } from "../sql/shared";
import type { Observations } from "../feed/http";
import type { Aged, BasinMember, BundleCounts, BundleInfo, GaugeLink, ItemId, NameHit, NearHit, Panel, PanelMember,
              PlaceHit, PlaceId, PointerHit, Reading, RegsSource, SectionId, StationId, Water } from "../index";
import type { DbName, ReleaseLoader } from "./loader";
import { buildStandingClasses, type StandingClasses } from "../standing";
import { CROSS, META, RELEASE_QUERIES as R } from "./queries";

/** The lowest SQLite parameter ceiling any driver may have been compiled with. */
const CHUNK = 500;

async function chunked<T>(keys: readonly T[], each: (part: T[]) => Promise<Row[]>): Promise<Row[]> {
  const out: Row[] = [];
  for (let i = 0; i < keys.length; i += CHUNK) out.push(...await each(keys.slice(i, i + CHUNK)));
  return out;
}

/** SQLite's BINARY collation on ASCII-and-beyond names, as the bundle's `ORDER BY ... name` sorted them. */
const binary = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** What a source is handed beside its data. */
export interface SourceOptions {
  /** The live feed. Absent means conditions render as "we could not check", never as a number. */
  feed?: {
    now(station: StationId): Promise<Aged<Reading> | null>;
    /** Raw observations, with no envelope — this source supplies the other half. */
    observations?(station: StationId): Promise<Observations | null>;
    /**
     * The stations transmitting right now — membership IS the answer.
     *
     * The release deliberately holds no liveness column: any boolean it baked in would be right the week it was cut
     * and wrong months later. This is the resident index the map already loads to colour its dots, so asking it costs
     * a set lookup.
     *
     * Absent means we could not check. Then nothing is preferred over anything else, which is correct — an unknown
     * must not read as "not transmitting".
     */
    live?(): Promise<ReadonlySet<string> | null>;
  };
}

export function makeReleaseSource(loader: ReleaseLoader, opts: SourceOptions = {}): RegsSource {
  const db = (n: DbName) => loader.db(n);
  const all = async (q: { file: string; sql: string }, ...p: (string | number | null)[]) =>
    (await db(q.file as DbName)).all(q.sql, ...p);
  const get = async (q: { file: string; sql: string }, ...p: (string | number | null)[]) =>
    (await db(q.file as DbName)).get(q.sql, ...p);
  const down: DownOf = async (at) => {
    const r = await get(R.DOWN_FROM!, at);
    return r ? sid(r.down_sid) : null;
  };
  const live = async () => (await opts.feed?.live?.()) ?? null;

  /** The sections of a water (core), in the water's own order. */
  const sectionsOf = async (item: ItemId): Promise<SectionId[]> =>
    (await all(R.SECTIONS_FOR_ITEM!, item)).map((r) => sid(r.sid));

  /** The towns these waters come near (near → core), nearest first; rows shaped like the bundle's NEAR_FOR_ITEMS. */
  async function ringsFor(items: readonly string[], perItem = Infinity): Promise<Row[]> {
    const ords = await chunked(items, (c) => all(CROSS.ORDS_FOR_ITEMS(c.length), ...c));
    if (!ords.length) return [];
    const itemOf = new Map(ords.map((r) => [Number(r.ord), str(r.item_id)]));
    const rings = await chunked([...itemOf.keys()], (c) => all(CROSS.RINGS_FOR_ORDS(c.length), ...c));
    const placeIds = [...new Set(rings.map((r) => Number(r.place_id)))];
    const places = new Map((await chunked(placeIds, (c) => all(CROSS.PLACES(c.length), ...c)))
      .map((p) => [Number(p.place_id), p]));
    const taken = new Map<number, number>();
    const out: Row[] = [];
    for (const r of rings) {
      const p = places.get(Number(r.place_id));
      if (!p) continue;                                   // a town with no position is not a ring
      const o = Number(r.ord), n = taken.get(o) ?? 0;
      if (n >= perItem) continue;
      taken.set(o, n + 1);
      out.push({ item_id: itemOf.get(o)!, name: p.name, kind: p.kind, lat: p.lat, lon: p.lon,
                 km: Number(r.ckm) / 100 });
    }
    return out;
  }

  /** sid → the lowest station in a lake holding it (core: section → items; conditions: items → lake_gauge). */
  async function lakeRoad(sections: readonly SectionId[]): Promise<Map<SectionId, StationId>> {
    const out = new Map<SectionId, StationId>();
    if (!sections.length) return out;
    const owners = await chunked(sections, (c) => all(CROSS.ITEMS_FOR_SECTIONS(c.length), ...c));
    const items = [...new Set(owners.map((r) => str(r.item_id)))];
    const lakes = new Map((await chunked(items, (c) => all(CROSS.LAKE_STATIONS(c.length), ...c)))
      .map((r) => [str(r.item_id), str(r.station)]));
    for (const r of owners) {
      const st = lakes.get(str(r.item_id));
      if (st === undefined) continue;
      const s = sid(r.sid), had = out.get(s);
      if (had === undefined || st < had) out.set(s, st as StationId);   // min over every lake the section is in
    }
    return out;
  }

  /** paint's section order (flow-area's), as prefix sums over its runs. */
  let index: Promise<(s: number) => number | null> | null = null;
  const indexOf = () => (index ??= loader.paint().then((p) => {
    const { starts, counts } = p.runs;
    const before = new Uint32Array(starts.length);
    for (let i = 1; i < starts.length; i++) before[i] = before[i - 1]! + counts[i - 1]!;
    return (s: number) => {
      let lo = 0, hi = starts.length - 1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (s < starts[mid]!) hi = mid - 1;
        else if (s >= starts[mid]! + counts[mid]!) lo = mid + 1;
        else return before[mid]! + (s - starts[mid]!);
      }
      return null;
    };
  }));
  /** paint's members by panel, in member order. */
  let byPanel: Promise<Map<number, PanelMember[]>> | null = null;
  const membersOf = () => (byPanel ??= loader.paint().then((p) => {
    const out = new Map<number, PanelMember[]>();
    for (const m of [...p.members].sort((a, b) => a.panel - b.panel || a.ord - b.ord)) {
      const list = out.get(m.panel) ?? [];
      list.push({ station: m.station as StationId, role: m.role, areaKm2: m.areaKm2, years: m.years,
                  regulated: m.regulated, sameRiver: m.sameRiver, ...(m.lead ? { lead: m.lead } : {}) });
      out.set(m.panel, list);
    }
    return out;
  }));
  /** paint's basins, by basin. */
  let byBasin: Promise<Map<string, BasinMember[]>> | null = null;
  const basinsOf = () => (byBasin ??= loader.paint().then((p) => {
    const out = new Map<string, BasinMember[]>();
    for (const b of p.basins) {
      const list = out.get(b.basin) ?? [];
      list.push({ station: b.station as StationId, areaKm2: b.areaKm2, years: b.years });
      out.set(b.basin, list);
    }
    return out;
  }));
  /** The Conditions map's classes, built once per source (`standingClasses`). */
  let classes: Promise<StandingClasses> | null = null;

  return {
    async info(): Promise<BundleInfo> {
      const meta = new Map((await all(META)).map((r) => [str(r.k), str(r.v)]));
      return { version: loader.release, validUntil: loader.manifest.regulations.valid_until ?? null,
               sectionHandles: loader.handles || null, reachDigest: meta.get("reach_digest") || null };
    },

    async counts(): Promise<BundleCounts> {
      const [r, paint] = await Promise.all([get(CROSS.COUNTS), loader.paint()]);
      // stations: paint lists every gauge station (the index its columns use), so conditions is not opened for a count
      return { waters: num(r?.waters) ?? 0, reaches: num(r?.reaches) ?? 0, surveyed: null,
               stations: paint.stations.length };
    },

    async itemForSection(id) {
      const r = await get(R.ITEM_FOR_SECTION!, id);
      return r ? (str(r.item_id) as ItemId) : null;
    },

    async waterFor(id) {
      const r = await get(R.ITEM_FOR_SECTION!, id);
      if (!r) return null;
      const item = await get(R.ITEM!, str(r.item_id));
      return item ? { item: str(item.item_id) as ItemId, name: str(item.name), kind: str(item.kind) } : null;
    },

    async water(id): Promise<Water | null> {
      const item = await get(R.ITEM!, id);
      if (!item) return null;
      return { item: str(item.item_id) as ItemId, name: str(item.name), kind: str(item.kind),
               sections: await sectionsOf(id) };
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      const query = q.trim(), needle = BQ.nameNeedle(query);
      if (needle === null || limit < 1) return [];
      const rows = await all(R.SEARCH!, needle, Math.max(limit * 3, 60));
      const hits = rankNameHits(query, rows, limit);
      return hits.length ? withNear(hits, await ringsFor(hits.map((h) => h.item))) : hits;
    },

    async searchPointers(q, limit): Promise<readonly PointerHit[]> {
      const query = q.trim();
      if (query.length < 2 || limit < 1) return [];
      return pointerHits(query, await all(R.SEARCH_POINTERS!, BQ.likeEscape(query), Math.max(limit * 12, 60)), limit);
    },

    async searchPlaces(q, limit): Promise<readonly PlaceHit[]> {
      if (q.trim().length < 2 || limit < 1) return [];
      return (await all(R.SEARCH_PLACES!, BQ.likeEscape(q.trim()), limit)).map((r) => ({
        place: str(r.place_id) as PlaceId, name: str(r.name), kind: str(r.kind),
        lat: Number(r.lat), lon: Number(r.lon), pop: num(r.pop),
      }));
    },

    async watersNear(place): Promise<readonly NearHit[]> {
      const id = /^\d+$/.test(place) ? Number(place) : place;
      // every water within 25 km (the cap only guards a bad build; see the bundle reader)
      const near = await all(CROSS.NEAR_PLACE, id, 2000);
      if (!near.length) return [];
      const ords = [...new Set(near.map((r) => Number(r.ord)))];
      const [items, towns] = await Promise.all([
        chunked(ords, (c) => all(CROSS.ITEMS_FOR_ORDS(c.length), ...c)),
        chunked(ords, (c) => all(CROSS.TOWNS_FOR_ORDS(c.length), ...c)),
      ]);
      const itemAt = new Map(items.map((r) => [Number(r.ord), r]));
      const townsAt = new Map(towns.map((r) => [Number(r.ord), Number(r.n)]));
      return near
        .filter((r) => itemAt.has(Number(r.ord)))
        .map((r) => {
          const i = itemAt.get(Number(r.ord))!;
          return { row: r, name: str(i.name), hit: nearHit({
            item_id: i.item_id, name: i.name, kind: i.kind, km: Number(r.ckm) / 100, mag: i.size,
            area_ha: i.area_ha, pieces: i.pieces, towns: townsAt.get(Number(r.ord)) ?? 1, gauged: i.gauged,
            stocked: 0, listed: i.has_rows }) };
        })
        .sort((a, b) => Number(a.row.ckm) - Number(b.row.ckm) || binary(a.name, b.name))
        .map((x) => x.hit);
    },

    async locate(item): Promise<WaterFix> {
      const pt = (r: Row) => ({ lat: Number(r.lat), lon: Number(r.lon) });
      const sections = await sectionsOf(item);
      const [gauges, rings] = await Promise.all([
        chunked(sections, (c) => all(CROSS.GAUGES_AT(c.length), ...c)),
        ringsFor([item], 12),
      ]);
      // stocking sites: no producer ships them (`STOCKED_ON_ITEM` has nothing to read)
      return { on: gauges.map(pt), near: rings.map((r) => ({ ...pt(r), km: Number(r.km) })) };
    },

    async waterBox(item) {
      // no water file (a release cut without regulations, like the fixture's): no geometry, and `locate` places it
      if (!loader.manifest.files.water) return null;
      const ords = (await all(CROSS.GEO_ORDS, item)).map((r) => Number(r.ord));
      if (!ords.length) return null;
      return boxOfGeoRows(await chunked(ords, (c) => all(CROSS.PART_GEO_FOR_ORDS(c.length), ...c)), 1_000_000);
    },

    async gaugeForSection(id): Promise<GaugeLink | null> {
      return link(await get(R.GAUGE_FOR_SECTION!, id), id, await live());
    },

    async lakeStationsFor(sections) {
      return lakeRoad(sections);
    },

    async panelsFor(sections) {
      const out = new Map<SectionId, Panel>();
      if (!sections.length) return out;
      const [paint, members, at, area] = await Promise.all([loader.paint(), membersOf(), indexOf(), loader.flowArea()]);
      for (const s of sections) {
        const panel = paint.panelOf(s);
        if (panel === null) continue;                     // no panel: ABSENT, never an empty one (see RegsSource)
        const i = at(s);
        out.set(s, { areaKm2: i === null ? null : area[i]!, members: members.get(panel) ?? [] });
      }
      return out;
    },

    async basinMembers() {
      return basinsOf();
    },

    lakeFacts() {
      return loader.lakes();
    },

    standingClasses() {
      return (classes ??= (async () => {
        const [paint, members, area, basins, lakeRows] = await Promise.all([
          loader.paint(), membersOf(), loader.flowArea(), basinsOf(), all(CROSS.ALL_LAKE_STATIONS)]);
        // THE LAKE ROAD, whole: every section of a gauged lake, the lowest station over every lake holding it
        const stationOf = new Map(lakeRows.map((r) => [str(r.item_id), str(r.station)]));
        const lakes = new Map<number, StationId>();
        for (const r of await chunked([...stationOf.keys()], (c) => all(CROSS.SECTIONS_FOR_ITEMS(c.length), ...c))) {
          const st = stationOf.get(str(r.item_id))!, s = Number(r.sid), had = lakes.get(s);
          if (had === undefined || st < had) lakes.set(s, st as StationId);
        }
        // paint's runs, in its section order — the order flow-area's target areas follow (`indexOf` above)
        const { starts, counts, panels } = paint.runs;
        function* sections() {
          let i = 0;
          for (let r = 0; r < starts.length; r++)
            for (let k = 0; k < counts[r]!; k++, i++)
              yield { sid: starts[r]! + k, panel: panels[r]!, areaKm2: area[i]! };
        }
        return buildStandingClasses(loader.release, sections(), members, lakes, basins);
      })());
    },

    async gaugePoints() {
      return (await all(R.GAUGE_POINTS!)).map((r) => ({
        station: str(r.station) as StationId, name: str(r.name), lon: Number(r.lon), lat: Number(r.lat),
        mag: r.mag === null || r.mag === undefined ? null : Number(r.mag),
      }));
    },

    async gaugeNow(station) { return (await opts.feed?.now(station)) ?? null; },

    /** The feed's observations over the release's envelope (`seriesOf`; clim/2: float16 bands). */
    async gaugeSeries(station, span, parameter, model) {
      const obs = (await opts.feed?.observations?.(station)) ?? null;
      const param = parameter ?? shownParameter(obs ?? {});
      const clim = await all(R.CLIMATOLOGY!, station, param);
      return seriesOf(pentadsOf(clim[0]?.bands as Uint8Array | null | undefined, "clim/2"), obs, span, param, model);
    },

    async gaugeParameters(station) {
      return parametersOf(await all(R.CLIM_PARAMETERS!, station));
    },

    async traceToGauge(from) {
      return traceDown(down, from);
    },

    async panelRoutes(section) {
      const [paint, members] = await Promise.all([loader.paint(), membersOf()]);
      const panel = paint.panelOf(section);
      const list = panel === null ? [] : members.get(panel) ?? [];
      if (!list.length) return [];
      const stations = list.map((m) => m.station);
      return routesOf(section, list, placesOf(await all(CROSS.GAUGE_PLACES(stations.length), ...stations)), down);
    },

    async gaugeWaters(section, stations) {
      // core: the section's waters; water: the continuation links (absent from a release with no water file, or one
      // cut before `water_continue`: each water is its own system); core: their item ids; conditions: the stations'
      const named = (await all(CROSS.ITEMS_FOR_SECTIONS(1), section)).map((r) => str(r.item_id));
      const start = named.length
        ? (await all(CROSS.ORDS_FOR_ITEMS(named.length), ...named)).map((r) => Number(r.ord)) : [];
      const ords = await continuedOrds(start, async (o) => {
        if (!loader.manifest.files.water) return [];
        try { return await all(CROSS.CONTINUE_LINKS(o.length), ...o, ...o); } catch { return []; }
      });
      const items = ords.length ? await chunked(ords, (c) => all(CROSS.ITEMS_FOR_ORDS(c.length), ...c)) : [];
      const gauges = stations.length
        ? await chunked([...stations], (c) => all(CROSS.GAUGE_WATERS(c.length), ...c)) : [];
      return gaugeWatersOf(items.map((r) => str(r.item_id)), gauges);
    },

    // no producer ships bathymetry charts or stocking records yet: the honest answers are none
    async lakeInfo() { return null; },
    async stockingHistory() { return []; },

    /**
     * The files a water's REGULATIONS page reads (T01/T03 decode the frames): core finds its ord, water its parts'
     * answer keys, and each key's region file is opened (`loader.regionDb`) — the Vedder (Region 2) is core + water +
     * regs-2, and a water in another region fetches that region's file. A release that ships no water file holds no
     * regulations: nothing is opened.
     */
    async regulationKeys(item) {
      if (!loader.manifest.files.water) return [];
      const o = await get(CROSS.ITEM_ORD, item);
      if (!o) return [];
      const akeys = (await all(CROSS.AKEYS_FOR_ORD, Number(o.ord))).map((r) => Number(r.akey));
      for (const akey of akeys) await loader.regionDb(akey);
      return akeys;
    },
  };
}
