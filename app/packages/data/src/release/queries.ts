/**
 * THE QUERIES, IN THE RELEASE'S FORM (P08): what each bundle query in `../sql/queries.ts` becomes when the bundle is
 * the release's files (`pack.generated.ts`). Each runs on ONE file. `tools/release-parity.test.ts` holds every one to the
 * bundle form: the same rows, a SEARCH plan (never a SCAN), and the perf budget.
 *
 * NOT HERE, AND WHY: the reads that cross two files become two keyed reads in the release reader (P11 / T04), never a
 * join across files (`CROSS` below):
 *   a water's nearest towns, a town's waters      near (place_water) → core (item, place); a search row's one town
 *                                                 is core's own `item_near` + `place` (T182)
 *   the gauges on a water                         core (item_section) → conditions (gauge)
 *   the lake road                                 conditions' `lake_section` (T182); before it, conditions + core
 *                                                 (section → item → lake_gauge)
 *   panels, basins                                paint (decoded arrays, not SQL) + flow-area
 *   STOCKED_ON_ITEM, CHARTS_FOR_ITEM, RELEASES_FOR_NAME   PREPARED, NOT FED: not in the release (the reader answers empty)
 */
import * as B from "../sql/queries";

export type PackName = "core" | "near" | "conditions" | "water" | "regs-sid" | "down" | "climate" | "geo";

/**
 * (T182) THE TABLES THAT MOVED INTO FILES OF THEIR OWN, and the file each was read from before: `section_down` (down)
 * and `gauge_clim` (climate) out of conditions, `part_geo` (geo) out of water. A query names its table's file now; a
 * release cut before the move (its manifest names no such file — the committed fixture releases, an older live one)
 * holds the table in the old file, and `fileFor` reads it there. Nothing else falls back: a file the manifest does not
 * name and that is not here is refused by the loader.
 */
export const MOVED_FROM: Readonly<Partial<Record<PackName, PackName>>> = { down: "conditions", climate: "conditions",
                                                                            geo: "water" };

/** The file that holds a query's table in THIS release (`MOVED_FROM`). */
export const fileFor = (files: Readonly<Record<string, unknown>>, file: PackName): PackName =>
  files[file] ? file : MOVED_FROM[file] ?? file;

export interface ReleaseQuery {
  file: PackName;
  sql: string;
  /** The bundle form it must answer like. */
  bundle: string;
}

/**
 * Search, from `core` alone: `size` is `item.size` (the biggest reach's stream magnitude, which the bundle computed
 * through `section_gauge`), so no conditions file is needed to rank a search; `pieces` is `item.pieces` (its section
 * count, > 0 = it owns a section), precomputed so a match costs no subquery.
 */
export const SEARCH =
  "SELECT m.item_id, m.name, m.kind, m.matched_as, min(m.tier) AS tier, " +
  "       m.pieces AS pieces, m.size AS size, m.area_ha " +
  "FROM ( " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, i.area_ha, i.size, i.pieces, NULL AS matched_as, " +
  "         (CASE WHEN " + B.NAME_KEY + " LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN " + B.NAME_KEY + " LIKE ?1 || '%' ESCAPE '\\' THEN 2 " +
  "               WHEN ' ' || " + B.NAME_KEY + " LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 4 " +
  "               ELSE 6 END) AS tier " +
  "  FROM item i WHERE i.pieces > 0 AND " + B.NAME_KEY + " LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  "  UNION ALL " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, i.area_ha, i.size, i.pieces, a.alias AS matched_as, " +
  "         (CASE WHEN " + B.ALIAS_KEY + " LIKE ?1 ESCAPE '\\' THEN 2 " +
  "               WHEN " + B.ALIAS_KEY + " LIKE ?1 || '%' ESCAPE '\\' THEN 3 " +
  "               WHEN ' ' || " + B.ALIAS_KEY + " LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 5 " +
  "               ELSE 7 END) AS tier " +
  "  FROM alias a JOIN item i USING(item_id) WHERE i.pieces > 0 AND " + B.ALIAS_KEY + " LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  ") m GROUP BY m.item_id, m.matched_as " +
  "ORDER BY tier, COALESCE(size, m.area_ha) IS NULL, COALESCE(size, m.area_ha) DESC, " +
  "         length(m.name), m.name LIMIT ?2";

/**
 * THE KEYED HALVES OF THE CROSS-FILE READS (P11). Each runs on ONE file; `release/source.ts` joins their rows in code,
 * so no read ever spans two files. `IN (...)` lists are bound, never interpolated, and chunked by the caller.
 */
const ids = (n: number) => B.placeholders(n);
export const CROSS = {
  /** core: an item's ord (the key near and water are written in). */
  ITEM_ORD: { file: "core", sql: "SELECT ord FROM item WHERE item_id = ?" },
  /**
   * core: a water's ord and the ords of the waters that are a PART OF it (a lake split into closure areas owns no
   * section of its own — Kootenay, Williston, Shannon) — the keys its geometry is read by (`PART_GEO_FOR_ORDS`).
   */
  GEO_ORDS: { file: "core", sql:
    "SELECT ord FROM item WHERE item_id = ?1 UNION ALL SELECT ord FROM item WHERE part_of = ?1" },
  /** geo: the parts' boxes, integer microdegrees (`part_geo`'s primary key is (ord, part_ix)). */
  PART_GEO_FOR_ORDS: (n: number) => ({ file: "geo", sql:
    `SELECT w, s, e, n, mid_lon, mid_lat FROM part_geo WHERE ord IN (${ids(n)})` }),
  ORDS_FOR_ITEMS: (n: number) => ({ file: "core", sql: `SELECT ord, item_id FROM item WHERE item_id IN (${ids(n)})` }),
  ITEMS_FOR_ORDS: (n: number) => ({ file: "core", sql:
    "SELECT ord, item_id, name, kind, area_ha, size, pieces, gauged, has_rows FROM item " +
    `WHERE ord IN (${ids(n)})` }),
  PLACES: (n: number) => ({ file: "core", sql:
    `SELECT place_id, name, kind, lat, lon FROM place WHERE place_id IN (${ids(n)}) AND lat IS NOT NULL` }),
  /** core: which items hold these sections (the lake road's first half). */
  ITEMS_FOR_SECTIONS: (n: number) => ({ file: "core", sql:
    `SELECT s.sid, i.item_id FROM item_section s JOIN item i ON i.ord = s.ord WHERE s.sid IN (${ids(n)})` }),
  COUNTS: { file: "core", sql:
    "SELECT (SELECT COUNT(*) FROM item WHERE name <> '') AS waters, (SELECT COUNT(*) FROM item_section) AS reaches" },
  /** near: the waters by a town, nearest first (place_water's primary key). */
  NEAR_PLACE: { file: "near", sql: "SELECT ord, ckm FROM place_water WHERE place_id = ? ORDER BY ckm, ord LIMIT ?" },
  /** near: the towns each of these waters comes near. Keyed by place, so this scans — once per search, as the
   *  bundle's NEAR_FOR_ITEMS did. */
  RINGS_FOR_ORDS: (n: number) => ({ file: "near", sql:
    `SELECT ord, place_id, ckm FROM place_water WHERE ord IN (${ids(n)}) ORDER BY ckm, place_id` }),
  TOWNS_FOR_ORDS: (n: number) => ({ file: "near", sql:
    `SELECT ord, count(*) AS n FROM place_water WHERE ord IN (${ids(n)}) GROUP BY ord` }),
  /**
   * (T182) core: each water's "near X" town(s) — the ring towns at the smallest distance + kind handicap, the ones
   * `describeBy` can name (pipeline `build.core_near`) — so a search's result lines read core, not near. Its primary key
   * is (ord, place_id). Absent from a core cut before it (`HAS_ITEM_NEAR`): rings are read from near as before.
   */
  NEAR_TOWNS_FOR_ORDS: (n: number) => ({ file: "core", sql:
    `SELECT ord, place_id, ckm FROM item_near WHERE ord IN (${ids(n)}) ORDER BY ckm, place_id` }),
  HAS_ITEM_NEAR: { file: "core", sql: "SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'item_near'" },
  /** (T182) conditions: the lake road per section (`lake_section`: the lowest station over every lake holding it). */
  LAKE_SECTIONS: (n: number) => ({ file: "conditions", sql:
    `SELECT sid, station FROM lake_section WHERE sid IN (${ids(n)})` }),
  ALL_LAKE_SECTIONS: { file: "conditions", sql: "SELECT sid, station FROM lake_section" },
  /** (T247) core: whether this release carries `item_area` (a core cut before it has no regions or units to search by). */
  HAS_ITEM_AREA: { file: "core", sql: "SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'item_area'" },
  /** (T247) core, whole: what the in-memory search index is built from (`WaterSource.searchIndex`), each read once. */
  INDEX_ITEMS: { file: "core", sql:
    "SELECT ord, item_id, name, kind, area_ha, size, pieces FROM item WHERE pieces > 0 ORDER BY ord" },
  INDEX_ALIASES: { file: "core", sql:
    "SELECT a.item_id, a.alias FROM alias a JOIN item i ON i.item_id = a.item_id WHERE i.pieces > 0 ORDER BY a.rowid" },
  INDEX_PLACES: { file: "core", sql:
    "SELECT place_id, name, kind, pop, lat, lon FROM place WHERE lat IS NOT NULL AND name IS NOT NULL ORDER BY place_id" },
  INDEX_NEAR: { file: "core", sql:
    "SELECT n.ord, p.name, p.kind, p.lat, p.lon, n.ckm FROM item_near n JOIN place p ON p.place_id = n.place_id " +
    "WHERE p.lat IS NOT NULL ORDER BY n.ord, n.ckm, p.place_id" },
  INDEX_AREA: { file: "core", sql: "SELECT ord, regions, mus FROM item_area" },
  INDEX_NAME_SOURCES: { file: "core", sql: "SELECT item_id, name, label FROM name_source" },
  /** (T224) core: whether this release carries `name_source` (a core cut before it shows no name's source). */
  HAS_NAME_SOURCE: { file: "core", sql: "SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'name_source'" },
  /** (T224) core: where these waters' non-official names came from (`name_source`'s primary key is (item_id, name)). */
  NAME_SOURCES_FOR_ITEMS: (n: number) => ({ file: "core", sql:
    `SELECT item_id, name, label FROM name_source WHERE item_id IN (${ids(n)})` }),
  /**
   * (T252) down: whether `section_down` carries `len` (each reach's length along the flow path, ADDITIVE, still
   * down/1); a release cut before it has none, and a route then says no along-river distance.
   */
  HAS_DOWN_LEN: { file: "down", sql: "SELECT 1 AS x FROM pragma_table_info('section_down') WHERE name = 'len'" },
  /** (T252) down: one reach's length along the flow path, in `DOWN_LEN_UNIT_M` units (`sql/shared` decodes it). */
  LEN_OF: { file: "down", sql: "SELECT len FROM section_down WHERE sid = ?" },
  HAS_LAKE_SECTION: { file: "conditions", sql:
    "SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'lake_section'" },
  /** conditions: stations standing on these sections. */
  GAUGES_AT: (n: number) => ({ file: "conditions", sql:
    `SELECT lat, lon FROM gauge WHERE sid IN (${ids(n)}) AND lat IS NOT NULL AND lon IS NOT NULL` }),
  /** conditions: the lake road's second half — the lowest station in each lake, stably. */
  LAKE_STATIONS: (n: number) => ({ file: "conditions", sql:
    `SELECT item_id, min(station) AS station FROM lake_gauge WHERE item_id IN (${ids(n)}) GROUP BY item_id` }),
  /** conditions: EVERY gauged lake and its lowest station — the Conditions map's lake classes, once per release. */
  ALL_LAKE_STATIONS: { file: "conditions", sql:
    "SELECT item_id, min(station) AS station FROM lake_gauge GROUP BY item_id" },
  /** core: the sections of these items (the lake classes' second half). */
  SECTIONS_FOR_ITEMS: (n: number) => ({ file: "core", sql:
    `SELECT s.sid, i.item_id FROM item_section s JOIN item i ON i.ord = s.ord WHERE i.item_id IN (${ids(n)})` }),
  GAUGE_PLACES: (n: number) => ({ file: "conditions", sql: B.gaugePlaces(n) }),
  /** conditions: each station's own water and reach (T168, `gaugeWaters`). */
  GAUGE_WATERS: (n: number) => ({ file: "conditions", sql: B.gaugeWatersFor(n) }),
  /** water: the `water_continue` links touching these waters, both ways (T168, `gaugeWaters`). */
  CONTINUE_LINKS: (n: number) => ({ file: "water", sql: B.continueLinks(n) }),
  /** water: the answer keys of a water's parts (part's primary key is (item_ord, part_ix)). */
  AKEYS_FOR_ORD: { file: "water", sql:
    "SELECT DISTINCT akey FROM part WHERE item_ord = ? AND akey IS NOT NULL ORDER BY akey" },
} as const;

/**
 * A file's own meta (format, section_handles, reach_digest, …), read by `info()` on core. Not in `RELEASE_QUERIES`, which
 * are held to the bundle's rows: each release file's meta is its own by design (the loader checks format and handles on
 * every open).
 */
export const META = { file: "core", sql: B.META } as const;

/** Every single-file read, by its bundle name. Same SQL where the tables moved whole. */
export const RELEASE_QUERIES: Record<string, ReleaseQuery> = {
  ITEM: { file: "core", sql: B.ITEM, bundle: B.ITEM },
  SECTIONS_FOR_ITEM: { file: "core", sql: B.SECTIONS_FOR_ITEM, bundle: B.SECTIONS_FOR_ITEM },
  ITEM_FOR_SECTION: { file: "core", sql: B.ITEM_FOR_SECTION, bundle: B.ITEM_FOR_SECTION },
  SEARCH: { file: "core", sql: SEARCH, bundle: B.withArea(B.SEARCH, true) },
  SEARCH_PLACES: { file: "core", sql: B.SEARCH_PLACES, bundle: B.SEARCH_PLACES },
  /** The group names that only point: `see_pointer` is copied whole, so the SQL is the bundle's. */
  SEARCH_POINTERS: { file: "core", sql: B.SEARCH_POINTERS, bundle: B.SEARCH_POINTERS },
  GAUGE_FOR_SECTION: { file: "conditions", sql: B.GAUGE_FOR_SECTION, bundle: B.GAUGE_FOR_SECTION },
  GAUGE_POINTS: { file: "conditions", sql: B.GAUGE_POINTS, bundle: B.GAUGE_POINTS },
  LAKE_GAUGES: { file: "conditions", sql: B.LAKE_GAUGES, bundle: B.LAKE_GAUGES },
  CLIM_PARAMETERS: { file: "climate", sql: B.CLIM_PARAMETERS, bundle: B.CLIM_PARAMETERS },
  DOWN_FROM: { file: "down", sql: B.DOWN_FROM, bundle: B.DOWN_FROM },
  /** The bands are float16 in the release (clim/2); the parity test compares the decoded values. */
  CLIMATOLOGY: { file: "climate", sql: B.CLIMATOLOGY, bundle: B.CLIMATOLOGY },
};
