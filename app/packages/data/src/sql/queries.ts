/**
 * The client's SQL, in the pipeline bundle's table names (`pipeline/deliver/bundle/schema.sql`), in one file.
 *
 * The release copies the bundle's tables into its files, so each query here is either asked of the release file that
 * holds its table (`../release/queries.ts` `RELEASE_QUERIES`, held to this form by `tools/release-parity.test.ts`) or
 * is the keyed half of a cross-file read there (`CROSS`). The SQL lives here rather than in the source so that the
 * shape of a read — how many statements, in what order, over which indexes — is reviewable in one place.
 *
 * No regulation table is read here: the regulations are read through `../regs/`.
 */
import { searchKey } from "@app/core";

/** A water by its durable id. */
export const ITEM = "SELECT item_id, name, kind FROM item WHERE item_id = ?";

/**
 * A water's sections, in the water's own order.
 *
 * `ORDER BY section_id` is what this used to say, under a comment claiming section ids sort
 * mouth-to-source "because the measure is distance up the blue line". Half true and wrong
 * where it matters: the id is `{blue_line_key}:{measure}` and the column is TEXT, so
 * `...:122095` sorts before `...:9942`. A river whose sections carry four- and five-digit
 * measures came back interleaved, and the sheet drew its stretches out of order while
 * saying they were in order.
 *
 * The split-and-cast is gone because the ORDER IS NOW THE HANDLE. `sid` is an index into
 * the atlas's section_handles.txt, and that table is built in the water's own order — blue
 * line, then measure as a number — so `ORDER BY sid` is the same sequence this used to
 * compute, and it is free, because it is the primary key. Blue line first still matters: an
 * item is not one line ("Fraser River" spans 151 of them, braids and side channels included)
 * and a run of sections must never be assembled across two different waters.
 * `pipeline/tests/test_section_handles.py` is what keeps that order true.
 */
export const SECTIONS_FOR_ITEM =
  "SELECT s.sid FROM item_section s JOIN item i ON i.ord = s.ord " +
  "WHERE i.item_id = ? ORDER BY s.sid";

export const ITEM_FOR_SECTION =
  "SELECT i.item_id FROM item_section s JOIN item i ON i.ord = s.ord " +
  "WHERE s.sid = ? LIMIT 1";

/**
 * (T114) THE STORED SEARCH KEYS AS SEARCH READS THEM. The pipeline stores a key only where `lower()` of the text is not
 * already it (`pipeline.common.search_key.stored_key`: NULL for an ASCII text whose key is its lower case, ~99% of BC
 * names — the core size gate), so the key of a row is this COALESCE. Not a second rule: on every row of a built core it
 * equals `searchKey(name)` / `searchKey(alias)` (`pipeline/tests/test_release.py`), and a full key (the dev fixture's)
 * reads the same.
 */
export const NAME_KEY = "COALESCE(i.name_key, lower(i.name))";
export const ALIAS_KEY = "COALESCE(a.alias_key, lower(a.alias))";

/**
 * Name search — the CANDIDATES. The final order is decided in @app/core (`rankWaters`).
 *
 * `?1` is `nameNeedle(query)`: the query's SEARCH KEY (`@app/core` `searchKey`, T114) with LIKE's own wildcards
 * escaped, so a name with an underscore in it is not a wildcard. It is compared with the STORED keys
 * (`item.name_key`, `alias.alias_key`, written by the pipeline with the same rule), never the raw strings: LIKE
 * folds ASCII case only, so a LIKE of '%barriere%' on the raw name found none of the six Barrière waters and "childrens" never
 * found Children's Fishing Pond — rows the ranking, which folds accents and apostrophes, never saw. The tier here is the coarse half of core's ladder —
 * exact, prefix, word-prefix, anywhere, with an alias one step behind the name — and it is
 * only here so that the LIMIT keeps the right rows: "creek" matches thousands of waters and
 * the ones worth offering are the exact and prefix matches on the biggest of them, which a
 * LIMIT over rowid order would have thrown away.
 *
 * `size` is the FWA stream MAGNITUDE of the water's biggest reach (`section_gauge.mag`) —
 * the count of headwaters above it, the same measure the zoom ladder draws rivers by. It is
 * the one measure of how much water this is that the bundle carries for nearly every
 * stream. `section_panel.area_km2` looked like the answer and is not: it is the PANEL's
 * catchment, so a creek entering the Fraser reported the Fraser's 64,000 km² and "creek"
 * ranked Nathan Creek first. Lakes carry no magnitude: their size is `area_ha`, the lake's own
 * polygon in hectares. The pre-order below sorts the two by the raw number, COALESCE(size,
 * area_ha), which is the same order core's `sizeSignal` gives because its two references are
 * equal (1 ha weighs as one headwater, `NEAR_RANK.REF`); core then decides the final order.
 *
 * Aliases come back with the name they belong to so a row can say "also VEDDER RIVER"
 * instead of looking like the wrong water.
 */
export const SEARCH =
  // The union is wrapped because SQLite will not ORDER a compound SELECT by an expression
  // — only by a result column. Ordering outside also keeps the ranking in one place.
  //
  // ONLY A WATER THAT OWNS A SECTION IS FOUND. A lake the atlas cut into parts (Kootenay,
  // Williston, Shannon) owns none — its parts are the water (`item.part_of`, user ruling
  // 2026-10-03: "a lake fully covered by its parts is not searchable as a whole; search finds
  // the parts") — so "Kootenay" finds the Main Body and the West Arms, never a whole with no
  // rules of its own to show. The same EXISTS in both halves: a name match and an alias match
  // are one question.
  "SELECT m.item_id, m.name, m.kind, m.matched_as, min(m.tier) AS tier, " +
  "       (SELECT count(*) FROM item_section s WHERE s.ord = m.ord) AS pieces, " +
  "       (SELECT max(sg.mag) FROM item_section s JOIN section_gauge sg ON sg.sid = s.sid " +
  "         WHERE s.ord = m.ord) AS size, m.area_ha " +
  "FROM ( " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, %AREA% AS area_ha, NULL AS matched_as, " +
  "         (CASE WHEN " + NAME_KEY + " LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN " + NAME_KEY + " LIKE ?1 || '%' ESCAPE '\\' THEN 2 " +
  "               WHEN ' ' || " + NAME_KEY + " LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 4 " +
  "               ELSE 6 END) AS tier " +
  "  FROM item i WHERE " + NAME_KEY + " LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  "    AND EXISTS (SELECT 1 FROM item_section s WHERE s.ord = i.ord) " +
  "  UNION ALL " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, %AREA% AS area_ha, a.alias AS matched_as, " +
  "         (CASE WHEN " + ALIAS_KEY + " LIKE ?1 ESCAPE '\\' THEN 2 " +
  "               WHEN " + ALIAS_KEY + " LIKE ?1 || '%' ESCAPE '\\' THEN 3 " +
  "               WHEN ' ' || " + ALIAS_KEY + " LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 5 " +
  "               ELSE 7 END) AS tier " +
  "  FROM alias a JOIN item i USING(item_id) WHERE " + ALIAS_KEY + " LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  "    AND EXISTS (SELECT 1 FROM item_section s WHERE s.ord = i.ord) " +
  ") m GROUP BY m.item_id, m.matched_as " +
  "ORDER BY tier, COALESCE(size, m.area_ha) IS NULL, COALESCE(size, m.area_ha) DESC, " +
  "         length(m.name), m.name LIMIT ?2";

/**
 * `item.area_ha` arrived with the 2026-09-30 bundle. `%AREA%` in SEARCH is the column where the
 * bundle has it and NULL where it does not. The release's own SEARCH (`../release/queries.ts`)
 * reads `item.area_ha` from core; this fills the bundle form it is held to.
 */
export const withArea = (sql: string, has: boolean): string =>
  sql.replaceAll("%AREA%", has ? "i.area_ha" : "NULL");

/** `%` and `_` are LIKE's wildcards; a query containing either means the character. */
export const likeEscape = (q: string): string => q.replace(/[\\%_]/g, (c) => "\\" + c);

/**
 * (T114) THE NAME SEARCH'S `?1`, for both readers: the query's search key, LIKE-escaped — the one place a query becomes
 * what `SEARCH` compares with the stored `name_key` / `alias_key`. `null` when the key is under two characters (the
 * readers then answer nothing): "''" keys to "", and `LIKE '%%'` would be every water.
 */
export function nameNeedle(q: string): string | null {
  const key = searchKey(q);
  return key.length < 2 ? null : likeEscape(key);
}

/**
 * THE GROUP NAMES THAT ONLY POINT (`see_pointer`, owner 2026-10-22), whose name is the query or starts with it: every
 * named row of each, in the book's order. Candidates only — `rankPointers` (@app/core) orders and cuts them. The same
 * SQL on the bundle and on the release's `core` (the table is copied whole); the NOCASE index serves the prefix.
 */
export const SEARCH_POINTERS =
  "SELECT entry_id, name, ix, target_name, item_id FROM see_pointer " +
  "WHERE name LIKE ?1 || '%' ESCAPE '\\' ORDER BY entry_id, ix LIMIT ?2";

export const SEARCH_PLACES =
  // Biggest first among equally good matches: someone typing "vic" means Victoria, not a
  // hamlet of forty people that happens to sort earlier. An exact name beats both.
  "SELECT place_id, name, kind, pop, lat, lon FROM place " +
  "WHERE name LIKE '%' || ?1 || '%' ESCAPE '\\' AND lat IS NOT NULL " +
  "ORDER BY (CASE WHEN name LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN name LIKE ?1 || '%' ESCAPE '\\' THEN 1 ELSE 2 END), " +
  "         -COALESCE(pop, 0), length(name) LIMIT ?2";

// ---- conditions ------------------------------------------------------------------
/**
 * The gauge for one reach, with the station itself.
 *
 * Joined rather than fetched in two steps because this runs on tap: on the web every
 * statement is a range request, and the station's name is not optional context — a sheet
 * that says "08MH016" and nothing else has told the reader nothing.
 */
export const GAUGE_FOR_SECTION =
  // `sg.*`, NOT A COLUMN LIST (T93): `rel` / `dist_m` are new, and a release cut before them has neither — naming them
  // would fail the query and take the gauge with it (the `gaugePlaces` reason). `link` reads them when present.
  "SELECT sg.*, sg.mag AS reach_mag, g.name, g.lon, g.lat, g.area_km2, " +
  "       g.mag AS gauge_mag, g.sid AS gauge_section " +
  "FROM section_gauge sg LEFT JOIN gauge g USING(station) " +
  "WHERE sg.sid = ?";

/** Every station with a position — a few hundred rows, for drawing them on the map. */
// `mag` rides along because the dot on the map obeys the SAME zoom ladder as the water it
// measures — see `@app/core/ladder`. Without it every station drew at every zoom.
export const GAUGE_POINTS =
  "SELECT station, name, lon, lat, mag FROM gauge WHERE lon IS NOT NULL AND lat IS NOT NULL";

export const LAKE_GAUGES =
  "SELECT lg.station, g.name, g.lon, g.lat, g.area_km2, g.mag " +
  "FROM lake_gauge lg LEFT JOIN gauge g USING(station) " +
  "WHERE lg.item_id = ? ORDER BY lg.station";

/**
 * WHICH WATER EACH STATION STANDS ON, AND THE WATERS JOINED TO A WATER (T168, `WaterSource.gaugeWaters`): the
 * `water_continue` links touching a set of waters (both directions, each half keyed: `ord` is the primary key,
 * `next_ord` UNIQUE), and the stations' own water and reach.
 */
export const continueLinks = (n: number) =>
  `SELECT ord, next_ord FROM water_continue WHERE ord IN (${placeholders(n)}) ` +
  `UNION ALL SELECT ord, next_ord FROM water_continue WHERE next_ord IN (${placeholders(n)})`;
export const gaugeWatersFor = (n: number) =>
  `SELECT station, item_id, sid FROM gauge WHERE station IN (${placeholders(n)})`;

// ONE ENVELOPE PER (STATION, PARAMETER). A station that measures both stage and discharge
// has two, in two different units, and asking for "the" envelope of such a station is how a
// level ends up compared against a discharge — a percentile that looks fine and means
// nothing. The parameter is always part of the key.
export const CLIMATOLOGY =
  "SELECT bands FROM gauge_clim WHERE station = ? AND parameter = ?";

/** Which quantities this station has an envelope for — the toggle offers only these. */
export const CLIM_PARAMETERS =
  "SELECT DISTINCT parameter FROM gauge_clim WHERE station = ? ORDER BY parameter";

export const DOWN_FROM = "SELECT down_sid FROM section_down WHERE sid = ?";

/**
 * Where a set of stations sit — their own reach and their coordinate.
 *
 * The donor panel names stations and says nothing about where they are, because the panel
 * dictionary interns the DONOR SET and a coordinate would be the same for every panel that
 * contains it. This is the join back to the map: it is what lets a route be walked from a
 * spot to each of its donors, and each donor to be pinned where it actually stands.
 *
 * `SELECT *`, NOT A COLUMN LIST, for one column: `tidal` (T78) is new, and a release or bundle built before it has no
 * such column — naming it would fail the whole query and take every route and pin with it. `placesOf` reads the
 * columns it knows by name; an absent `tidal` reads as not flagged. A handful of rows, so the extra columns cost
 * nothing.
 */
export const gaugePlaces = (n: number) =>
  `SELECT * FROM gauge WHERE station IN (${placeholders(n)})`;

// ---- lakes: PREPARED, NOT FED ------------------------------------------------------
/**
 * A stocked water's sites, a lake's bathymetric charts and the stocking history, as the pipeline bundle's
 * `stock_water`, `chart` and `release` tables hold them. No release file carries these tables yet, so the reader
 * answers `lakeInfo` null and `stockingHistory` empty (`../release/source.ts`); the SQL is kept for when one does.
 */
export const STOCKED_ON_ITEM =
  "SELECT lat, lon FROM stock_water WHERE item_id = ? AND lat IS NOT NULL AND lon IS NOT NULL";

export const CHARTS_FOR_ITEM =
  "SELECT chart_id, title, kind, drafted, scale, area_km2, pdf FROM chart WHERE item_id = ?";

export const RELEASES_FOR_NAME =
  "SELECT date, species, count, stage FROM release WHERE name = ? ORDER BY date DESC";

export const META = "SELECT k, v FROM meta";

/** `?,?,?` — SQLite has no array binding, and string interpolation of ids is injection. */
export function placeholders(n: number): string {
  if (n < 1) throw new Error("placeholders(0): the caller should skip the query entirely");
  return new Array(n).fill("?").join(",");
}
