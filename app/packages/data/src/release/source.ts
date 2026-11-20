/**
 * THE RELEASE READER (P11): a WaterSource over the release's files, through the loader (lazy, verified, per file).
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
 * What it opens, and when, is the loader's to say: paint at start, core on the first tap or search (a result's "near X"
 * is core's `item_near`), near on the first near-search, conditions on the first gauge question (the lake road is its
 * `lake_section`), down on the first downstream walk, climate on the first chart, flow-area on the first panel, geo on
 * the first water map or box, water and regs-<region> on the first water opened in that region. A release cut before
 * T182 holds down / climate / geo's tables in their old files and has no `item_near` / `lake_section`: each read then
 * goes the old way (`fileFor`; the two has-table checks below).
 */
import { describeBy, shownParameter, SMALL_WATER_TEXT_KEY, type IndexCut, type IndexWater, type NearPlace, type SearchIndexData, type WaterFix } from "@app/core";
import { num, str, type Db, type Row } from "../sql/db";
import * as BQ from "../sql/queries";
import { boxOfGeoRows, pointOfGeoRows, continuedOrds, gaugeWatersOf, link, nearHit, parametersOf, pentadsOf, placesOf, pointerHits, rankNameHits, routesOf, withNameSources, seriesOf, sid,
         traceDown, withNear, decodeLen, type DownOf, type RouteFacts } from "../sql/shared";
import type { Observations } from "../feed/http";
import type { Aged, BasinMember, BundleCounts, BundleInfo, GaugeLink, ItemId, NameHit, NearHit, Panel, PanelMember,
              PlaceHit, PlaceId, PointerHit, Reading, WaterSource, SectionId, StationId, Water } from "../index";
import type { DbName, ReleaseLoader } from "./loader";
import { buildStandingClasses, type StandingClasses } from "../standing";
import { decodeBasinGrid, type BasinGrid } from "../basinGrid";
import { CONDITIONS_META, CROSS, fileFor, META, RELEASE_QUERIES as R, type PackName } from "./queries";
import { CLIM_CODEC, CLIMATE_FORMAT, decodeDownBlock, decodeNear, DOWN_BLOCK_BITS, DOWN_FORMAT, downSlot, invertNear,
         NEAR_FORMAT, type DownBlock, type NearByOrd } from "./packed";

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

export function makeReleaseSource(loader: ReleaseLoader, opts: SourceOptions = {}): WaterSource {
  // (T256) core and water are read routed: each read from the whole file or the focus region's answer slice covering it
  const db = (n: DbName): Promise<Db> =>
    (n === "core" || n === "water") && loader.routed ? Promise.resolve(loader.routed(n)) : loader.db(n);
  /** The file a query's table is in, in this release (T182 `MOVED_FROM`). */
  const at = (q: { file: string }) => fileFor(loader.manifest.files, q.file as PackName) as DbName;
  const all = async (q: { file: string; sql: string }, ...p: (string | number | null)[]) =>
    (await db(at(q))).all(q.sql, ...p);
  const get = async (q: { file: string; sql: string }, ...p: (string | number | null)[]) =>
    (await db(at(q))).get(q.sql, ...p);
  /** Whether a file holds a table added after the committed fixture releases were cut (T182), asked once. */
  const has = (q: { file: string; sql: string }): (() => Promise<boolean>) => {
    let p: Promise<boolean> | null = null;
    return () => (p ??= get(q).then((r) => !!r, (e) => { p = null; throw e; }));
  };
  const hasItemNear = has(CROSS.HAS_ITEM_NEAR), hasLakeSection = has(CROSS.HAS_LAKE_SECTION);
  const hasItemAlias = has(CROSS.HAS_ITEM_ALIAS);
  /** (T555) What `item_alias` says an id became (null: no alias, or a core cut before the table). Asked only for an id
   *  the release does not hold, so a current id costs no extra read. */
  const aliasOf = async (id: string): Promise<ItemId | null> => {
    if (!(await hasItemAlias())) return null;
    const a = await get(CROSS.ITEM_CURRENT, id);
    return a ? (str(a.item_id) as ItemId) : null;
  };
  /**
   * (T555) THE ONE RESOLVER of a saved item id: the id itself when core holds it, else what `item_alias` says it became
   * (an id a later build absorbed), else null. Every by-id entry point goes through here, so a saved spot, alert or recent
   * search follows an absorbed water. A core cut before `item_alias` has no aliases.
   */
  const currentItem = async (id: string, held: (id: string) => Promise<Row | undefined | null>): Promise<[string, Row] | null> => {
    const own = await held(id);
    if (own) return [id, own];
    const now = await aliasOf(id);
    const row = now === null ? null : await held(now);
    return now !== null && row ? [now, row] : null;
  };
  /** (T555) The id `id` names in this release (itself, or what absorbed it), or null: `currentItem` over the item's ord. */
  const nowId = async (id: ItemId): Promise<ItemId | null> =>
    ((await currentItem(id, (i) => get(CROSS.ITEM_ORD, i)))?.[0] as ItemId | undefined) ?? null;
  /** core: the ords a water's geometry is read by (`GEO_ORDS`) — (T555) an id the release no longer holds follows item_alias. */
  const geoOrds = async (item: ItemId): Promise<number[]> => {
    const of = async (id: string) => (await all(CROSS.GEO_ORDS, id)).map((r) => Number(r.ord));
    const own = await of(item);
    if (own.length) return own;
    const now = await aliasOf(item);
    return now ? of(now) : [];
  };
  const hasNameSource = has(CROSS.HAS_NAME_SOURCE), hasItemArea = has(CROSS.HAS_ITEM_AREA);
  /** (T927) the boundary places search finds; none from a core cut before `cut_place`. */
  const hasCutPlace = has(CROSS.HAS_CUT_PLACE);
  /** (T224) the `name_source` rows of these waters; none from a core cut before the table. */
  const nameSourcesOf = async (items: readonly string[]): Promise<Row[]> =>
    (await hasNameSource()) ? chunked(items, (c) => all(CROSS.NAME_SOURCES_FOR_ITEMS(c.length), ...c)) : [];
  /**
   * (T686) down/2: the pointers in blocks of 4096 sids, each block read and decoded once (a walk stays in a few blocks).
   * A down/1 release (or one cut before T182: `section_down` in conditions) is read by row, as before.
   */
  const downPacked = loader.manifest.files.down?.format === DOWN_FORMAT;
  const downBlocks = new Map<number, Promise<DownBlock | null>>();
  const downBlock = (block: number): Promise<DownBlock | null> => {
    let p = downBlocks.get(block);
    if (!p) {
      p = get(CROSS.DOWN_BLOCK, block).then((r) => {
        if (!r) return null;
        if (!(r.b instanceof Uint8Array)) throw new Error(`down/2: block ${block} is not bytes`);
        return decodeDownBlock(block, r.b);
      });
      downBlocks.set(block, p);
      p.catch(() => downBlocks.delete(block));          // a failed read is not kept: the next ask reads again
    }
    return p;
  };
  /** down/2: the block and row of `s`'s pointer; null: `s` has none. */
  const downRow = async (s: SectionId | number): Promise<[DownBlock, number] | null> => {
    const n = Number(s), b = await downBlock(Math.floor(n / 2 ** DOWN_BLOCK_BITS));
    const i = b ? downSlot(b, n) : -1;
    return b && i >= 0 ? [b, i] : null;
  };
  const down: DownOf = async (at) => {
    if (downPacked) {
      const r = await downRow(at);
      return r ? sid(r[0].down[r[1]]!) : null;
    }
    const r = await get(R.DOWN_FROM!, at);
    return r ? sid(r.down_sid) : null;
  };
  /** (T252) a route's facts beyond its path: reach lengths (down, when the release has them) and water names (core). */
  const hasDownLen = has(CROSS.HAS_DOWN_LEN), hasDownLake = has(CROSS.HAS_DOWN_LAKE);
  const routeFacts = async (): Promise<RouteFacts> => ({
    // (T686) down/2 always carries both (a NULL len is packed as such)
    ...(downPacked ? {
      lenOf: async (s: SectionId) => { const r = await downRow(s); return r ? decodeLen(r[0].len[r[1]]) : null; },
      lakeOf: async (s: SectionId) => { const r = await downRow(s); return r ? r[0].lake[r[1]] === 1 : false; },
    } : {}),
    ...(!downPacked && (await hasDownLen()) ? { lenOf: async (s: SectionId) => decodeLen((await get(CROSS.LEN_OF, s))?.len) } : {}),
    // (T296) which reaches are lakes; a release cut before `section_down.lake` leaves it to the water's kind
    ...(!downPacked && (await hasDownLake()) ? { lakeOf: async (s: SectionId) => Number((await get(CROSS.LAKE_OF, s))?.lake ?? 0) === 1 } : {}),
    nameOf: async (s: SectionId) => {
      const r = await get(R.ITEM_FOR_SECTION!, s);
      const item = r ? await get(R.ITEM!, str(r.item_id)) : null;
      return item ? { name: str(item.name), kind: str(item.kind) } : null;
    },
  });
  const live = async () => (await opts.feed?.live?.()) ?? null;

  /** The sections of a water (core), in the water's own order. */
  const sectionsOf = async (item: ItemId): Promise<SectionId[]> =>
    (await all(R.SECTIONS_FOR_ITEM!, item)).map((r) => sid(r.sid));

  /**
   * (T686) near/2: one packed row per town. A town's waters are its own blob; the by-water reads (`RINGS_FOR_ORDS`,
   * `TOWNS_FOR_ORDS`, which near/1 answered by a scan) read every blob once and invert them (`invertNear`). Each answers
   * in near/1's rows and order, so everything after reads them as before.
   */
  const nearPacked = loader.manifest.files.near?.format === NEAR_FORMAT;
  let nearInverted: Promise<NearByOrd> | null = null;
  const nearByOrd = (): Promise<NearByOrd> => (nearInverted ??= all(CROSS.NEAR_ALL_BLOBS).then((rows) =>
    invertNear(rows.map((r) => {
      if (!(r.b instanceof Uint8Array)) throw new Error(`near/2: town ${String(r.place_id)} is not bytes`);
      return { place_id: Number(r.place_id), b: r.b };
    })), (e) => { nearInverted = null; throw e; }));
  /** RINGS_FOR_ORDS / TOWNS_FOR_ORDS over one chunk of ords, in their SQL's rows and order. */
  const ringsOfOrds = async (ords: readonly number[]): Promise<Row[]> => {
    if (!nearPacked) return all(CROSS.RINGS_FOR_ORDS(ords.length), ...ords);
    const ix = await nearByOrd(), out: { ord: number; place_id: number; ckm: number }[] = [];
    for (const o of new Set(ords)) {
      const [a, z] = ix.range(o);
      for (let k = a; k < z; k++) out.push({ ord: o, place_id: ix.place[k]!, ckm: ix.ckm[k]! });
    }
    // ORDER BY ckm, place_id (rows equal on both keep no order in SQL either: one town names a water once)
    return out.sort((x, y) => x.ckm - y.ckm || x.place_id - y.place_id || x.ord - y.ord);
  };
  const townCounts = async (ords: readonly number[]): Promise<Row[]> => {
    if (!nearPacked) return all(CROSS.TOWNS_FOR_ORDS(ords.length), ...ords);
    const ix = await nearByOrd();
    return [...new Set(ords)].flatMap((o) => { const [a, z] = ix.range(o); return z > a ? [{ ord: o, n: z - a }] : []; });
  };
  /** NEAR_PLACE: a town's waters, nearest first, at most `limit`. */
  const nearPlace = async (id: string | number, limit: number): Promise<Row[]> => {
    if (!nearPacked) return all(CROSS.NEAR_PLACE, id, limit);
    const r = await get(CROSS.NEAR_TOWN_BLOB, id);
    if (!r) return [];
    if (!(r.b instanceof Uint8Array)) throw new Error(`near/2: town ${String(id)} is not bytes`);
    const t = decodeNear(r.b), n = Math.min(limit, t.ord.length);
    return Array.from({ length: n }, (_, i) => ({ ord: t.ord[i]!, ckm: t.ckm[i]! }));
  };

  /** The towns these waters come near (near → core), nearest first; rows shaped like the bundle's NEAR_FOR_ITEMS. */
  async function ringsFor(items: readonly string[], perItem = Infinity): Promise<Row[]> {
    const ords = await chunked(items, (c) => all(CROSS.ORDS_FOR_ITEMS(c.length), ...c));
    if (!ords.length) return [];
    const itemOf = new Map(ords.map((r) => [Number(r.ord), str(r.item_id)]));
    const rings = await chunked([...itemOf.keys()], (c) => ringsOfOrds(c));
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

  /**
   * A search row's town(s) for these waters (core: `item_near`, then `place`), rows shaped like `ringsFor`'s: what
   * `describeBy` names from every ring is among them (pipeline `build.core_near`). A core without `item_near` reads
   * every ring from near, as before.
   */
  async function townsFor(items: readonly string[]): Promise<Row[]> {
    if (!(await hasItemNear())) return ringsFor(items);
    const ords = await chunked(items, (c) => all(CROSS.ORDS_FOR_ITEMS(c.length), ...c));
    if (!ords.length) return [];
    const itemOf = new Map(ords.map((r) => [Number(r.ord), str(r.item_id)]));
    const near = await chunked([...itemOf.keys()], (c) => all(CROSS.NEAR_TOWNS_FOR_ORDS(c.length), ...c));
    const placeIds = [...new Set(near.map((r) => Number(r.place_id)))];
    const places = new Map((await chunked(placeIds, (c) => all(CROSS.PLACES(c.length), ...c)))
      .map((p) => [Number(p.place_id), p]));
    return near.flatMap((r) => {
      const p = places.get(Number(r.place_id));
      return p ? [{ item_id: itemOf.get(Number(r.ord))!, name: p.name, kind: p.kind, lat: p.lat, lon: p.lon,
                    km: Number(r.ckm) / 100 }] : [];
    });
  }

  /**
   * sid → the lowest station in a lake holding it: conditions' `lake_section` (T182); a conditions cut before it, the
   * two keyed halves (core: section → items; conditions: items → lake_gauge).
   */
  async function lakeRoad(sections: readonly SectionId[]): Promise<Map<SectionId, StationId>> {
    const out = new Map<SectionId, StationId>();
    if (!sections.length) return out;
    if (await hasLakeSection()) {
      for (const r of await chunked(sections, (c) => all(CROSS.LAKE_SECTIONS(c.length), ...c)))
        out.set(sid(r.sid), str(r.station) as StationId);
      return out;
    }
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
                  regulated: m.regulated, sameRiver: m.sameRiver, ...(m.lead ? { lead: m.lead } : {}),
                  ...(m.lake ? { throughLake: true as const } : {}) });
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
  /**
   * (T376) THE WATERSHED GROUPS BY NAME AND BY PLACE — conditions' `basin`, `basin_frame`, `basin_grid`, decoded once. A
   * release cut before them is REFUSED (thrown, and asked again next time), never answered as "in no group".
   */
  const hasBasinGrid = has(CROSS.HAS_BASIN_GRID);
  let gridOf: Promise<BasinGrid> | null = null;
  const basinGrid = (): Promise<BasinGrid> => (gridOf ??= (async () => {
    if (!(await hasBasinGrid()))
      throw new Error("basins: this release carries no watershed groups (its conditions file has no basin_grid, cut before T376)");
    const [f, rows, names] = await Promise.all([get(CROSS.BASIN_FRAME), all(CROSS.BASIN_GRID_ROWS), all(CROSS.BASIN_NAMES)]);
    if (!f) throw new Error("basins: the release's basin_frame is empty");
    const n = (v: Row[string], what: string): number => {
      const x = num(v);
      if (x === null || !Number.isFinite(x)) throw new Error(`basins: ${what} is not a number`);
      return x;
    };
    return decodeBasinGrid(
      { w: n(f.w, "w"), s: n(f.s, "s"), dlon: n(f.dlon, "dlon"), dlat: n(f.dlat, "dlat"), cols: n(f.cols, "cols"),
        rows: n(f.rows, "rows") },
      rows.map((r) => {
        if (!(r.b instanceof Uint8Array)) throw new Error(`basins: basin_grid row ${String(r.row)} is not bytes`);
        return { row: n(r.row, "a grid row"), b: r.b };
      }),
      names.map((r) => {
        if (r.basin_id == null || r.name == null) throw new Error(`basins: basin ${String(r.ix)} has no id or name`);
        return { ix: n(r.ix, "a basin ix"), id: str(r.basin_id), name: str(r.name) };
      }));
  })().catch((e: unknown) => { gridOf = null; throw e; }));
  /**
   * THE LAKE ROAD, whole: every section of a gauged lake, the lowest station over every lake holding it —
   * conditions' `lake_section` as it is (T182), or, cut before it, built from lake_gauge + core's sections.
   */
  async function wholeLakeRoad(): Promise<Map<number, StationId>> {
    const lakes = new Map<number, StationId>();
    if (await hasLakeSection()) {
      for (const r of await all(CROSS.ALL_LAKE_SECTIONS)) lakes.set(Number(r.sid), str(r.station) as StationId);
      return lakes;
    }
    const stationOf = new Map((await all(CROSS.ALL_LAKE_STATIONS)).map((r) => [str(r.item_id), str(r.station)]));
    for (const r of await chunked([...stationOf.keys()], (c) => all(CROSS.SECTIONS_FOR_ITEMS(c.length), ...c))) {
      const st = stationOf.get(str(r.item_id))!, s = Number(r.sid), had = lakes.get(s);
      if (had === undefined || st < had) lakes.set(s, st as StationId);
    }
    return lakes;
  }

  /** The Conditions map's classes, built once per source (`standingClasses`). */
  let classes: Promise<StandingClasses> | null = null;

  /**
   * (T823) THE SMALL-WATER SENTENCE, the conditions meta's `SMALL_WATER_TEXT_KEY`, read once and only where a section
   * asked about has a flagged member. A release with `flow-small` and no sentence is broken: refused, never shown bare.
   */
  let smallText: Promise<string> | null = null;
  const smallSentence = () => (smallText ??= get(CONDITIONS_META, SMALL_WATER_TEXT_KEY).then((r) => {
    const v = r?.v;
    if (typeof v !== "string" || !v)
      throw new Error(`release ${loader.release}: flow-small flags members but conditions meta has no ${SMALL_WATER_TEXT_KEY}`);
    return v;
  }, (e) => { smallText = null; throw e; }));
  /** One `small` per (panel, mask): every section of a pair shares it, as they share their members. */
  const smallOf = new Map<number, NonNullable<Panel["small"]>>();
  /** (T823) A section's `small` from its flow-small `mask` over its panel's members in paint's order (bit k = member ord
   *  k); undefined for no bits. A bit past the panel's last member is a file that is not this paint's: refused. */
  const smallFor = (panel: number, list: readonly PanelMember[], mask: number, text: string): Panel["small"] => {
    if (!mask) return undefined;
    if (mask >>> list.length)
      throw new Error(`release ${loader.release}: flow-small flags member ${31 - Math.clz32(mask)} of panel ${panel}, which has ${list.length}`);
    const key = panel * 2 ** 31 + mask;
    let got = smallOf.get(key);
    if (!got) smallOf.set(key, (got = { stations: list.filter((_, k) => (mask >>> k) & 1).map((m) => m.station), text }));
    return got;
  };

  return {
    ...(loader.handles ? { handles: loader.handles } : {}),
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
      const found = await currentItem(id, (i) => get(R.ITEM!, i));
      if (!found) return null;
      const item = found[1];
      id = found[0] as ItemId;
      const src = (await nameSourcesOf([str(item.item_id)])).find((r) => str(r.name) === str(item.name));
      return { item: str(item.item_id) as ItemId, name: str(item.name), kind: str(item.kind),
               sections: await sectionsOf(id), nameSource: src ? str(src.label) : null };
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      const query = q.trim(), needle = BQ.nameNeedle(query);
      if (needle === null || limit < 1) return [];
      const rows = await all(R.SEARCH!, needle, Math.max(limit * 3, 60));
      const hits = rankNameHits(query, rows, limit);
      if (!hits.length) return hits;
      const items = hits.map((h) => h.item);
      return withNameSources(withNear(hits, await townsFor(items)), await nameSourcesOf(items));
    },

    /**
     * (T247) ONE READ OF CORE, whole, for the in-memory index: waters, aliases, towns, each water's "near X" town
     * (`item_near`, then `describeBy` as every search row does), its regions and units (`item_area`) and the name sources.
     * A core cut before `item_near` / `item_area` / `name_source` gives those parts empty — search still works.
     */
    async searchIndex(): Promise<SearchIndexData<IndexWater, PlaceHit>> {
      const [items, aliases, places, near, areas, sources, cuts] = await Promise.all([
        all(CROSS.INDEX_ITEMS), all(CROSS.INDEX_ALIASES), all(CROSS.INDEX_PLACES),
        hasItemNear().then((h) => (h ? all(CROSS.INDEX_NEAR) : [] as Row[])),
        hasItemArea().then((h) => (h ? all(CROSS.INDEX_AREA) : [] as Row[])),
        hasNameSource().then((h) => (h ? all(CROSS.INDEX_NAME_SOURCES) : [] as Row[])),
        hasCutPlace().then((h) => (h ? all(CROSS.INDEX_CUTS) : [] as Row[])),
      ]);
      const nearOf = new Map<number, NearPlace[]>();
      for (const r of near) {
        const list = nearOf.get(Number(r.ord)) ?? [];
        list.push({ name: str(r.name), kind: str(r.kind), lat: Number(r.lat), lon: Number(r.lon), km: Number(r.ckm) / 100 });
        nearOf.set(Number(r.ord), list);
      }
      const areaOf = new Map(areas.map((r) => [Number(r.ord), r]));
      const label = new Map(sources.map((r) => [`${str(r.item_id)}\u0000${str(r.name)}`, str(r.label)]));
      const csv = (v: unknown): string[] => (v == null || v === "" ? [] : String(v).split(","));
      return {
        waters: items.map((r): IndexWater => {
          const a = areaOf.get(Number(r.ord));
          return { item: str(r.item_id), name: str(r.name), kind: str(r.kind), size: num(r.size), areaHa: num(r.area_ha),
                   pieces: Number(r.pieces ?? 0), regions: a ? csv(a.regions) : [], mus: a ? csv(a.mus) : [],
                   near: describeBy(nearOf.get(Number(r.ord)) ?? []),
                   nameSource: label.get(`${str(r.item_id)}\u0000${str(r.name)}`) ?? null };
        }),
        aliases: aliases.map((r) => ({ item: str(r.item_id), alias: str(r.alias),
                                       source: label.get(`${str(r.item_id)}\u0000${str(r.alias)}`) ?? null })),
        places: places.map((r) => ({ place: str(r.place_id) as PlaceId, name: str(r.name), kind: str(r.kind),
                                     lat: Number(r.lat), lon: Number(r.lon), pop: num(r.pop) })),
        // (T927) the book's boundary places; a mark's position in microdegrees (NULL: no mark there)
        cuts: cuts.map((r): IndexCut => ({
          item: str(r.item_id), words: str(r.words), alt: r.alt == null ? [] : (JSON.parse(str(r.alt)) as string[]),
          generic: Number(r.generic) === 1, blk: Number(r.blk), m: Number(r.m),
          lat: r.lat == null ? null : Number(r.lat) / 1e6, lon: r.lon == null ? null : Number(r.lon) / 1e6 })),
      };
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
      const near = await nearPlace(id, 2000);
      if (!near.length) return [];
      const ords = [...new Set(near.map((r) => Number(r.ord)))];
      const [items, towns] = await Promise.all([
        chunked(ords, (c) => all(CROSS.ITEMS_FOR_ORDS(c.length), ...c)),
        chunked(ords, (c) => townCounts(c)),
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

    currentItem: nowId,

    async locate(item): Promise<WaterFix> {
      const pt = (r: Row) => ({ lat: Number(r.lat), lon: Number(r.lon) });
      let sections = await sectionsOf(item);
      // (T555) a saved id the release no longer holds follows item_alias, as `water` does
      if (!sections.length) {
        const now = await aliasOf(item);
        if (now) { item = now; sections = await sectionsOf(item); }
      }
      const [gauges, rings] = await Promise.all([
        chunked(sections, (c) => all(CROSS.GAUGES_AT(c.length), ...c)),
        ringsFor([item], 12),
      ]);
      // stocking sites: no producer ships them (`STOCKED_ON_ITEM` has nothing to read)
      return { on: gauges.map(pt), near: rings.map((r) => ({ ...pt(r), km: Number(r.km) })) };
    },

    async waterBox(item) {
      // no geometry file (a release cut without regulations, like the fixture's): no box, and `locate` places it
      if (!loader.manifest.files[at(CROSS.PART_GEO_FOR_ORDS(1))]) return null;
      const ords = await geoOrds(item);
      if (!ords.length) return null;
      return boxOfGeoRows(await chunked(ords, (c) => all(CROSS.PART_GEO_FOR_ORDS(c.length), ...c)), 1_000_000);
    },

    async waterPoint(item) {
      if (!loader.manifest.files[at(CROSS.PART_GEO_FOR_ORDS(1))]) return null;
      const ords = await geoOrds(item);
      if (!ords.length) return null;
      return pointOfGeoRows(await chunked(ords, (c) => all(CROSS.PART_GEO_FOR_ORDS(c.length), ...c)), 1_000_000) as { lat: number; lon: number; sid?: SectionId } | null;
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
      const [paint, members, at, area, small] = await Promise.all([loader.paint(), membersOf(), indexOf(), loader.flowArea(),
                                                                   loader.flowSmall()]);
      const flagged: [SectionId, number, readonly PanelMember[], number][] = [];
      for (const s of sections) {
        const panel = paint.panelOf(s);
        if (panel === null) continue;                     // no panel: ABSENT, never an empty one (see WaterSource)
        const i = at(s), list = members.get(panel) ?? [];
        out.set(s, { areaKm2: i === null ? null : area[i]!, members: list,
                     ...(paint.lakes ? { lakeKnown: true as const } : {}) });
        // (T823) flow-small's bits for this section, over its members in paint's order (bit i = member ord i)
        const mask = small && i !== null ? small[i]! : 0;
        if (mask) flagged.push([s, panel, list, mask]);
      }
      if (flagged.length) {
        const text = await smallSentence();
        for (const [s, panel, list, mask] of flagged) out.set(s, { ...out.get(s)!, small: smallFor(panel, list, mask, text) });
      }
      return out;
    },

    async basinMembers() {
      return basinsOf();
    },

    async basinAt(p) {
      return (await basinGrid()).at(p.lon, p.lat);
    },

    async basinIn(box) {
      return (await basinGrid()).mostIn(box);
    },

    lakeFacts() {
      return loader.lakes();
    },

    salmonFacts() {
      return loader.salmon();
    },

    methodFacts() {
      return loader.method();
    },

    standingClasses() {
      return (classes ??= (async () => {
        const [paint, members, area, small, basins, lakes] = await Promise.all([
          loader.paint(), membersOf(), loader.flowArea(), loader.flowSmall(), basinsOf(), wholeLakeRoad()]);
        // (T823) the class's speaker carries the sheet's small-water warning too (`panelsFor`): the same speaker, both ways
        const text = small && small.some((m) => m !== 0) ? await smallSentence() : "";
        // paint's runs, in its section order — the order flow-area's target areas and flow-small's masks follow
        const { starts, counts, panels } = paint.runs;
        function* sections() {
          let i = 0;
          for (let r = 0; r < starts.length; r++) {
            const list = members.get(panels[r]!) ?? [];
            for (let k = 0; k < counts[r]!; k++, i++)
              yield { sid: starts[r]! + k, panel: panels[r]!, areaKm2: area[i]!,
                      small: small ? smallFor(panels[r]!, list, small[i]!, text) : undefined };
          }
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
      // (T686) a climate/2 file carries clim/3; climate/1 (and a release cut before T182: conditions) clim/2
      const codec = loader.manifest.files.climate?.format === CLIMATE_FORMAT ? CLIM_CODEC : "clim/2";
      return seriesOf(pentadsOf(clim[0]?.bands as Uint8Array | null | undefined, codec), obs, span, param, model);
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
      return routesOf(section, list, placesOf(await all(CROSS.GAUGE_PLACES(stations.length), ...stations)), down,
                      await routeFacts());
    },

    async gaugeWaters(section, stations) {
      // core: the section's waters; water: the continuation links (absent from a release with no water file, or one
      // cut before `water_continue`: each water is its own system); core: their item ids; conditions: the stations'
      const named = (await all(CROSS.ITEMS_FOR_SECTIONS(1), section)).map((r) => str(r.item_id));
      const start = named.length
        ? (await all(CROSS.ORDS_FOR_ITEMS(named.length), ...named)).map((r) => Number(r.ord)) : [];
      const ords = await continuedOrds(start, async (o) => {
        if (!loader.manifest.files.water) return [];
        return await all(CROSS.CONTINUE_LINKS(o.length), ...o, ...o);    // (T368) a failed read fails, never "no continuation"
      });
      const items = ords.length ? await chunked(ords, (c) => all(CROSS.ITEMS_FOR_ORDS(c.length), ...c)) : [];
      const gauges = stations.length
        ? await chunked([...stations], (c) => all(CROSS.GAUGE_WATERS(c.length), ...c)) : [];
      const out = gaugeWatersOf(items.map((r) => str(r.item_id)), gauges);
      // (T679 #8) a release with `flow-own` says which members stand on this water (Python's one rule); none: as before
      if (!loader.manifest.files["flow-own"]) return out;
      const [paint, members, at, bits] = await Promise.all([loader.paint(), membersOf(), indexOf(), loader.flowOwn()]);
      const i = at(Number(section)), panel = paint.panelOf(Number(section));
      const list = panel === null ? [] : members.get(panel) ?? [];
      const mask = bits && i !== null ? bits[i]! : 0;
      if (mask >>> list.length)
        throw new Error(`release ${loader.release}: flow-own flags member ${31 - Math.clz32(mask)} of panel ${panel}, which has ${list.length}`);
      return { ...out, own: new Set(list.filter((_, k) => (mask >>> k) & 1).map((m) => m.station)) };
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
      const found = await currentItem(item, (i) => get(CROSS.ITEM_ORD, i));
      const o = found?.[1];
      if (!o) return [];
      const akeys = (await all(CROSS.AKEYS_FOR_ORD, Number(o.ord))).map((r) => Number(r.akey));
      for (const akey of akeys) await loader.regionDb(akey);
      return akeys;
    },
  };
}
