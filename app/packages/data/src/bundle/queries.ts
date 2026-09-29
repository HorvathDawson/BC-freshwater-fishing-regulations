/**
 * Every query the client makes, in one file.
 *
 * Written against `pipeline/deliver/bundle/schema.sql` and nothing else. The SQL lives here rather
 * than in the source implementation so that the shape of a read — how many statements, in
 * what order, over which indexes — is reviewable in one place. That matters more than it
 * looks: on the web each of these is a range request (data contract §10). A query added in
 * a component is a round trip nobody counted.
 *
 * No regulation table is read — regulations are not integrated (see `regulations.ts` in
 * @app/core).
 */

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
 * Name search — the CANDIDATES. The final order is decided in @app/core (`rankWaters`).
 *
 * `?1` is the query with LIKE's own wildcards escaped (see `likeEscape`), so a name with an
 * underscore in it is not a wildcard. The tier here is the coarse half of core's ladder —
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
 * ranked Nathan Creek first. Lakes carry no magnitude and come back null, sorted as small.
 *
 * Aliases come back with the name they belong to so a row can say "also VEDDER RIVER"
 * instead of looking like the wrong water.
 */
export const SEARCH =
  // The union is wrapped because SQLite will not ORDER a compound SELECT by an expression
  // — only by a result column. Ordering outside also keeps the ranking in one place.
  "SELECT m.item_id, m.name, m.kind, m.matched_as, min(m.tier) AS tier, " +
  "       (SELECT count(*) FROM item_section s WHERE s.ord = m.ord) AS pieces, " +
  "       (SELECT max(sg.mag) FROM item_section s JOIN section_gauge sg ON sg.sid = s.sid " +
  "         WHERE s.ord = m.ord) AS size " +
  "FROM ( " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, NULL AS matched_as, " +
  "         (CASE WHEN i.name LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN i.name LIKE ?1 || '%' ESCAPE '\\' THEN 2 " +
  "               WHEN ' ' || i.name LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 4 " +
  "               ELSE 6 END) AS tier " +
  "  FROM item i WHERE i.name LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  "  UNION ALL " +
  "  SELECT i.ord, i.item_id, i.name, i.kind, a.alias AS matched_as, " +
  "         (CASE WHEN a.alias LIKE ?1 ESCAPE '\\' THEN 2 " +
  "               WHEN a.alias LIKE ?1 || '%' ESCAPE '\\' THEN 3 " +
  "               WHEN ' ' || a.alias LIKE '% ' || ?1 || '%' ESCAPE '\\' THEN 5 " +
  "               ELSE 7 END) AS tier " +
  "  FROM alias a JOIN item i USING(item_id) WHERE a.alias LIKE '%' || ?1 || '%' ESCAPE '\\' " +
  ") m GROUP BY m.item_id, m.matched_as " +
  "ORDER BY tier, size IS NULL, size DESC, length(m.name), m.name LIMIT ?2";

/** `%` and `_` are LIKE's wildcards; a query containing either means the character. */
export const likeEscape = (q: string): string => q.replace(/[\\%_]/g, (c) => "\\" + c);

/**
 * The towns each of these waters comes within 25 km of, nearest first.
 *
 * `place_water` is keyed by place, so this scans it — about 380,000 rows held in memory,
 * a few tens of milliseconds, once per search rather than once per row.
 */
export const NEAR_FOR_ITEMS =
  "SELECT i.item_id, p.name, p.kind, p.lat, p.lon, pw.ckm / 100.0 AS km " +
  "FROM place_water pw JOIN item i ON i.ord = pw.ord JOIN place p USING(place_id) " +
  "WHERE i.item_id IN (%IDS%) AND p.lat IS NOT NULL ORDER BY pw.ckm";

export const SEARCH_PLACES =
  // Biggest first among equally good matches: someone typing "vic" means Victoria, not a
  // hamlet of forty people that happens to sort earlier. An exact name beats both.
  "SELECT place_id, name, kind, pop, lat, lon FROM place " +
  "WHERE name LIKE '%' || ?1 || '%' ESCAPE '\\' AND lat IS NOT NULL " +
  "ORDER BY (CASE WHEN name LIKE ?1 ESCAPE '\\' THEN 0 " +
  "               WHEN name LIKE ?1 || '%' ESCAPE '\\' THEN 1 ELSE 2 END), " +
  "         -COALESCE(pop, 0), length(name) LIMIT ?2";

/**
 * Joined on item_id, not on the display name.
 *
 * The precompute used to carry the name and be joined back by it — the classic silent
 * mismatch, and when it missed the source MINTED an ItemId out of the name, producing an
 * id that fails `itemExists` and routes a tap nowhere.
 */
export const WATERS_NEAR =
  "SELECT i.item_id, i.name, i.kind, pw.ckm / 100.0 AS km FROM place_water pw " +
  "JOIN item i ON i.ord = pw.ord " +
  "WHERE pw.place_id = ? ORDER BY pw.ckm LIMIT ?";

/**
 * WHERE A WATER IS — the evidence the bundle holds, since it holds no geometry.
 *
 * Points ON the water: a hydrometric station whose own reach is one of this water's
 * (`gauge.sid`), and a stocking site matched to it. Then the rings: every town within
 * 25 km and how close the water comes to it. See `fixOf` in @app/core.
 */
export const GAUGES_ON_ITEM =
  "SELECT g.lat, g.lon FROM item_section s JOIN item i ON i.ord = s.ord " +
  "JOIN gauge g ON g.sid = s.sid " +
  "WHERE i.item_id = ? AND g.lat IS NOT NULL AND g.lon IS NOT NULL";

export const STOCKED_ON_ITEM =
  "SELECT lat, lon FROM stock_water WHERE item_id = ? AND lat IS NOT NULL AND lon IS NOT NULL";

export const RINGS_FOR_ITEM =
  "SELECT p.lat, p.lon, pw.ckm / 100.0 AS km FROM place_water pw " +
  "JOIN item i ON i.ord = pw.ord JOIN place p USING(place_id) " +
  "WHERE i.item_id = ? AND p.lat IS NOT NULL ORDER BY pw.ckm LIMIT 12";

// ---- conditions ------------------------------------------------------------------
/**
 * The gauge for one reach, with the station itself.
 *
 * Joined rather than fetched in two steps because this runs on tap: on the web every
 * statement is a range request, and the station's name is not optional context — a sheet
 * that says "08MH016" and nothing else has told the reader nothing.
 */
export const GAUGE_FOR_SECTION =
  "SELECT sg.station, sg.trust, sg.mag AS reach_mag, g.name, g.lon, g.lat, g.area_km2, " +
  "       g.mag, g.sid AS gauge_section " +
  "FROM section_gauge sg LEFT JOIN gauge g USING(station) " +
  "WHERE sg.sid = ?";

/**
 * A station ON this lake — a LEVEL, not a discharge.
 *
 * Separate accessor because it is a separate measurement. Folding lake stations into
 * `section_gauge` had 11,049 stream sections being told a reservoir's level.
 */
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
 * Does this WATER have a gauge — the question a person actually asks.
 *
 * A water is many reaches and they do not agree: the lower Chilliwack is gauged well and
 * its headwaters barely at all. So this returns the BEST reach, not an average and not the
 * first one found, and hands back which reach it was — the answer "yes, 8 km downstream of
 * where you are looking" is true, and "yes" alone is not.
 *
 * Ordering is explicit rather than by a trust string, which would sort fair < good < weak.
 *
 * THE ORDER IS band, then TRANSMITTING, then the biggest reach. Each clause fixes a real
 * wrong answer on the province bundle:
 *
 *   band first     — never trade representativeness for anything else.
 *   NOT liveness   — the bundle holds no such column. The Thompson picking a closed
 *                    station over a working one is a real problem, but it is the FEED's
 *                    to solve: this returns several candidates and the caller prefers one
 *                    the feed index contains. A boolean baked in here would be right in
 *                    March and wrong by November.
 *   biggest reach  — ordering on the band alone gave the Cowichan River a station on a
 *                    13 km2 creek that clipped one of its sections. `good` spans a tenth
 *                    of a watershed to all of it, so the band cannot break its own ties;
 *                    the water's largest reach is what a person means by "this river".
 * A reach now holds exactly one station — the build already picked the one that most
 * nearly IS that water — so the ordering only has to choose between REACHES. The biggest
 * one is what a person means by "this river". This returns several candidates rather than
 * one so the caller can prefer a station the feed says is transmitting.
 *
 * A water whose only stations have closed still answers — with a record-only gauge, which
 * `GaugeLink.live` marks and the UI says out loud.
 */
export const GAUGE_FOR_ITEM =
  "SELECT sg.sid, sg.station, sg.trust, sg.mag AS reach_mag, " +
  "       g.name, g.lon, g.lat, g.area_km2, g.mag " +
  "FROM item_section it JOIN item i ON i.ord = it.ord " +
  "JOIN section_gauge sg ON sg.sid = it.sid " +
  "LEFT JOIN gauge g USING(station) " +
  "WHERE i.item_id = ? " +
  "ORDER BY (CASE sg.trust WHEN 'good' THEN 0 WHEN 'fair' THEN 1 ELSE 2 END), " +
  "         COALESCE(sg.mag, 0) DESC, sg.station LIMIT 8";

/**
 * The station for each of these reaches, in one statement.
 *
 * Called with whatever the map currently has on screen, so it is bounded by the viewport
 * rather than by the province. The alternative — holding all 558,746 rows client-side to
 * colour a map — is most of the bundle in memory to answer a question about 300 features.
 */
export const gaugesForSections = (n: number) =>
  /*
   * LAKES TOO, AND THEY COME BY A DIFFERENT ROAD.
   *
   * `section_gauge` is stream sections only, and deliberately: a lake station reports a
   * level in metres, not a discharge, and a trust ratio between the two is arithmetic on
   * different quantities — `section_gauge` once had 11,049 stream sections being told a
   * reservoir's level. So lake stations live in `lake_gauge`, keyed by ITEM rather than by
   * section, because a lake is one water however many sections it is cut into.
   *
   * The consequence was that the Conditions view coloured every river and left every lake
   * grey, including lakes with a gauge sitting in them. This union walks the lake's own
   * road — section -> item -> lake_gauge — so a gauged lake is coloured by its own reading.
   *
   * `trust` is 'good' for a lake and that is not a fudge: there is no fraction of a level.
   * The station is either in this water or it is not, which is exactly what `lake_gauge`
   * records, and it is a stronger claim than any ratio a stream section can make.
   *
   * A lake with several stations takes the lowest station id, arbitrarily but STABLY — a
   * colour that changes when the query planner changes its mind is worse than either
   * choice. 196 lake_gauge rows over fewer lakes, so this is rare.
   */
  `SELECT sid, station, trust FROM section_gauge WHERE sid IN (${placeholders(n)})
   UNION ALL
   SELECT it.sid, min(lg.station) AS station, 'good' AS trust
     FROM item_section it JOIN item i ON i.ord = it.ord
                          JOIN lake_gauge lg ON lg.item_id = i.item_id
    WHERE it.sid IN (${placeholders(n)})
      AND it.sid NOT IN (SELECT sid FROM section_gauge)
    GROUP BY it.sid`;

/**
 * Lakes with a station IN them, and nothing else.
 *
 * A LAKE IS NOT A PANEL AND MUST NOT BE ONE. The donor panel carries a reading from one
 * catchment to another on the argument that they share weather and drainage; a lake's stage
 * is set by its outlet and its own storage, so nothing about a river upstream — or another
 * lake over the ridge — transfers to it. `build_panels` therefore refuses lake nodes
 * outright, which is right, and left every lake grey once the map started colouring from
 * panels alone.
 *
 * This is the lake's own road: section -> item -> lake_gauge. Either a station is in this
 * water or it is not; there is no fraction of a level. A lake with no station stays grey,
 * which is the honest answer and not a gap.
 *
 * A lake with several stations takes the lowest station id, arbitrarily but STABLY — a
 * colour that changes when the query planner changes its mind is worse than either choice.
 */
export const lakeStationsFor = (n: number) =>
  `SELECT it.sid, min(lg.station) AS station
     FROM item_section it JOIN item i ON i.ord = it.ord
                          JOIN lake_gauge lg ON lg.item_id = i.item_id
    WHERE it.sid IN (${placeholders(n)})
    GROUP BY it.sid`;

/**
 * The panel for each of these sections, members in weight order.
 *
 * TWO STATEMENTS' WORTH OF WORK IN ONE, via the dictionary: `section_panel` is the pointer
 * and `panel_member` is the shared body, so a viewport full of one river's reaches fetches
 * that river's donors once rather than once per reach.
 */
export const panelsForSections = (n: number) =>
  `SELECT sp.sid, sp.area_km2 AS target_area,
          pm.ord, pm.station, pm.role, pm.area_km2 AS donor_area, pm.years,
          pm.regulated, pm.same_river
     FROM section_panel sp JOIN panel_member pm ON pm.panel_id = sp.panel_id
    WHERE sp.sid IN (${placeholders(n)})
    ORDER BY sp.sid, pm.ord`;

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
 * Every catchment that has a station, in one read.
 *
 * NO VIEWPORT SCOPING, unlike everything else here, and the reason is the zoom: this is
 * only ever asked at z4–8, where a viewport is a third of British Columbia. Scoping it
 * would mean re-reading most of the table on every pan to save nothing. 9,642 rows.
 */
export const BASIN_MEMBERS =
  "SELECT basin_id, station, area_km2, years FROM basin_member";

/**
 * Where a set of stations sit — their own reach and their coordinate.
 *
 * The donor panel names stations and says nothing about where they are, because the panel
 * dictionary interns the DONOR SET and a coordinate would be the same for every panel that
 * contains it. This is the join back to the map: it is what lets a route be walked from a
 * spot to each of its donors, and each donor to be pinned where it actually stands.
 */
export const gaugePlaces = (n: number) =>
  `SELECT station, name, sid, lon, lat, area_km2
     FROM gauge WHERE station IN (${placeholders(n)})`;

// ---- lakes -----------------------------------------------------------------------
export const CHARTS_FOR_ITEM =
  "SELECT chart_id, title, kind, drafted, scale, area_km2, pdf FROM chart WHERE item_id = ?";

export const RELEASES_FOR_NAME =
  "SELECT date, species, count, stage FROM release WHERE name = ? ORDER BY date DESC";

export const META = "SELECT k, v FROM meta";

/**
 * What the bundle holds, in one statement.
 *
 * ONE ROUND TRIP, because on the web every statement is a range request and this runs to
 * fill in a settings panel — four separate counts would be four. Each is a covered count
 * over a small table or an index, so the whole thing is cheap.
 */
export const COUNTS =
  "SELECT (SELECT COUNT(*) FROM item WHERE name <> '')     AS waters, " +
  "       (SELECT COUNT(*) FROM item_section)              AS reaches, " +
  "       (SELECT COUNT(DISTINCT item_id) FROM chart)      AS surveyed, " +
  "       (SELECT COUNT(*) FROM gauge)                     AS stations";

/** `?,?,?` — SQLite has no array binding, and string interpolation of ids is injection. */
export function placeholders(n: number): string {
  if (n < 1) throw new Error("placeholders(0): the caller should skip the query entirely");
  return new Array(n).fill("?").join(",");
}
