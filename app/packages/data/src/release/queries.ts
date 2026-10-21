/**
 * THE QUERIES, IN THE RELEASE'S FORM (P08): what each bundle query in `../bundle/queries.ts` becomes when the bundle is
 * the release's files (`pack.generated.ts`). Each runs on ONE file. `tools/release-parity.test.ts` holds every one to the
 * bundle form: the same rows, a SEARCH plan (never a SCAN), and the perf budget.
 *
 * NOT HERE, AND WHY: the reads that cross two files become two keyed reads in the release reader (P11 / T04), never a
 * join across files:
 *   NEAR_FOR_ITEMS, WATERS_NEAR, RINGS_FOR_ITEM    near (place_water) → core (item, place)
 *   GAUGES_ON_ITEM, GAUGE_FOR_ITEM                  core (item_section) → conditions (gauge, section_gauge)
 *   gaugesForSections, lakeStationsFor              conditions + core (the lake road: section → item → lake_gauge)
 *   panelsForSections, BASIN_MEMBERS                paint (decoded arrays, not SQL) + flow-area
 *   STOCKED_ON_ITEM, CHARTS_FOR_ITEM, RELEASES_FOR_NAME   no producer yet: not in the release (the reader answers empty)
 */
import * as B from "../bundle/queries";

export type PackName = "core" | "near" | "conditions" | "water" | "regs-sid";

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
  "         (CASE WHEN i.name LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN i.name LIKE ?1 || '%' ESCAPE '\\' THEN 2 " +
  "               WHEN ' ' || i.name LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 4 " +
  "               ELSE 6 END) AS tier " +
  "  FROM item i WHERE i.pieces > 0 AND i.name LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  "  UNION ALL " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, i.area_ha, i.size, i.pieces, a.alias AS matched_as, " +
  "         (CASE WHEN a.alias LIKE ?1 ESCAPE '\\' THEN 2 " +
  "               WHEN a.alias LIKE ?1 || '%' ESCAPE '\\' THEN 3 " +
  "               WHEN ' ' || a.alias LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 5 " +
  "               ELSE 7 END) AS tier " +
  "  FROM alias a JOIN item i USING(item_id) WHERE i.pieces > 0 AND a.alias LIKE '%' || ?1 || '%' ESCAPE '\\' " +
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
  /** water: the parts' boxes, integer microdegrees (`part_geo`'s primary key is (ord, part_ix)). */
  PART_GEO_FOR_ORDS: (n: number) => ({ file: "water", sql:
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
  /** conditions: stations standing on these sections. */
  GAUGES_AT: (n: number) => ({ file: "conditions", sql:
    `SELECT lat, lon FROM gauge WHERE sid IN (${ids(n)}) AND lat IS NOT NULL AND lon IS NOT NULL` }),
  /** conditions: GAUGE_FOR_SECTION for many sections (GAUGE_FOR_ITEM's second half). */
  SECTION_GAUGES: (n: number) => ({ file: "conditions", sql:
    "SELECT sg.sid, sg.station, sg.trust, sg.mag AS reach_mag, g.name, g.lon, g.lat, g.area_km2, g.mag " +
    `FROM section_gauge sg LEFT JOIN gauge g USING(station) WHERE sg.sid IN (${ids(n)})` }),
  SECTION_STATIONS: (n: number) => ({ file: "conditions", sql:
    `SELECT sid, station FROM section_gauge WHERE sid IN (${ids(n)})` }),
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
  GAUGE_FOR_SECTION: { file: "conditions", sql: B.GAUGE_FOR_SECTION, bundle: B.GAUGE_FOR_SECTION },
  GAUGE_POINTS: { file: "conditions", sql: B.GAUGE_POINTS, bundle: B.GAUGE_POINTS },
  LAKE_GAUGES: { file: "conditions", sql: B.LAKE_GAUGES, bundle: B.LAKE_GAUGES },
  CLIM_PARAMETERS: { file: "conditions", sql: B.CLIM_PARAMETERS, bundle: B.CLIM_PARAMETERS },
  DOWN_FROM: { file: "conditions", sql: B.DOWN_FROM, bundle: B.DOWN_FROM },
  /** The bands are float16 in the release (clim/2); the parity test compares the decoded values. */
  CLIMATOLOGY: { file: "conditions", sql: B.CLIMATOLOGY, bundle: B.CLIMATOLOGY },
};
