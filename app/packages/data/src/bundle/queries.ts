/**
 * Every query the client makes, in one file.
 *
 * Written against `pipeline/deliver/bundle/schema.sql` and nothing else. The SQL lives here rather
 * than in the source implementation so that the shape of a read — how many statements, in
 * what order, over which indexes — is reviewable in one place. That matters more than it
 * looks: on the web each of these is a range request, and `regsForItem` is budgeted at
 * two to four (data contract §10). A query added in a component is a round trip nobody
 * counted.
 */

/** A water's sheet, in as few statements as the schema allows. */
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
 * Split and cast, so the measure sorts as the number it is. Blue line first because an item
 * is not one line — "Fraser River" spans 151 of them, braids and side channels included —
 * and a run of sections must never be assembled across two different waters.
 */
export const SECTIONS_FOR_ITEM =
  "SELECT section_id FROM item_section WHERE item_id = ? " +
  "ORDER BY substr(section_id, 1, instr(section_id, ':') - 1), " +
  "         CAST(substr(section_id, instr(section_id, ':') + 1) AS INTEGER)";

export const ITEM_FOR_SECTION =
  "SELECT item_id FROM item_section WHERE section_id = ? LIMIT 1";

export const ENTRY_FOR_ITEM =
  "SELECT entry_id, name, full_name, verbatim, symbols, mus FROM entry WHERE item_id = ?";

export const RULES_FOR_ENTRY =
  "SELECT entry_id, rule_id, kind, scope, windows, species, subject, details, " +
  "       uncertain, text, location " +
  "FROM rule WHERE entry_id = ? ORDER BY rule_id";

/**
 * Which rules bind to these sections.
 *
 * Scope is a property of the geometry, so this asks by scope and lets the caller supply
 * section ids, MU ids or area ids — one query serves all three tiers.
 */
/**
 * The rules covering a set of sections, and the SET each section belongs to.
 *
 * TWO JOINS AND NO EXPANSION. The bundle does not store one row per (section, rule) — that
 * is 1,720,243 rows and 69.6 MB — it stores the 1,905 distinct answers and lets each section
 * name one. So this reads `section_ruleset` to find the answer and `ruleset` to read it,
 * and a 300-section viewport comes back in 0.25 ms against 1.0 ms for the flat table. Less
 * to read is faster, which is the happy direction for a compression to fail in.
 *
 * `set_id` comes back too, and is not a debugging aid: it is what lets a screen show a
 * river as the few stretches that differ rather than as 201 identical rows. See
 * `runsOfSameRules` in @app/core.
 */
export const rulesForSections = (n: number) =>
  "SELECT sr.section_id, sr.set_id, rs.via, " +
  "       r.entry_id, r.rule_id, r.kind, r.scope, r.windows, r.species, r.subject, " +
  "       r.details, " +
  "       r.uncertain, r.text, r.location " +
  "FROM section_ruleset sr " +
  "JOIN ruleset rs ON rs.set_id = sr.set_id " +
  "JOIN rule r ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id " +
  `WHERE sr.section_id IN (${placeholders(n)})`;

/** Just the set each section belongs to — for grouping, with no rule bodies fetched. */
export const setsForSections = (n: number) =>
  "SELECT section_id, set_id FROM section_ruleset " +
  `WHERE section_id IN (${placeholders(n)})`;

/**
 * Name search.
 *
 * Prefix-anchored, then contains, because "chil" should offer Chilliwack River before
 * Upper Chilliwack, and a plain LIKE '%q%' orders by rowid — which is to say, at random.
 * Aliases come back with the name they belong to so a row can say "also VEDDER RIVER"
 * instead of looking like the wrong water.
 */
export const SEARCH =
  // The union is wrapped because SQLite will not ORDER a compound SELECT by an expression
  // — only by a result column. Ordering outside also keeps the ranking in one place.
  "SELECT * FROM ( " +
  "  SELECT i.item_id, i.name, i.kind, NULL AS matched_as, " +
  "         (CASE WHEN i.name LIKE ?1 || '%' THEN 0 ELSE 1 END) AS rank " +
  "  FROM item i WHERE i.name LIKE '%' || ?1 || '%' " +
  "  UNION ALL " +
  "  SELECT i.item_id, i.name, i.kind, a.alias AS matched_as, " +
  "         (CASE WHEN a.alias LIKE ?1 || '%' THEN 2 ELSE 3 END) AS rank " +
  "  FROM alias a JOIN item i USING(item_id) WHERE a.alias LIKE '%' || ?1 || '%' " +
  ") ORDER BY rank, length(name), name LIMIT ?2";

export const PIECES =
  "SELECT item_id, count(*) AS n FROM item_section " +
  `WHERE item_id IN (%IDS%) GROUP BY item_id`;

export const SEARCH_PLACES =
  // Biggest first among equally good matches: someone typing "vic" means Victoria, not a
  // hamlet of forty people that happens to sort earlier.
  "SELECT place_id, name, kind FROM place WHERE name LIKE '%' || ?1 || '%' " +
  "ORDER BY (CASE WHEN name LIKE ?1 || '%' THEN 0 ELSE 1 END), " +
  "         -COALESCE(pop, 0), length(name) LIMIT ?2";

/**
 * Joined on item_id, not on the display name.
 *
 * The precompute used to carry the name and be joined back by it — the classic silent
 * mismatch, and when it missed the source MINTED an ItemId out of the name, producing an
 * id that fails `itemExists` and routes a tap nowhere.
 */
export const WATERS_NEAR =
  "SELECT i.item_id, i.name, i.kind, pw.km FROM place_water pw " +
  "JOIN item i USING(item_id) " +
  "WHERE pw.place_id = ? ORDER BY pw.km LIMIT ?";

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
  "       g.mag, g.section_id AS gauge_section " +
  "FROM section_gauge sg LEFT JOIN gauge g USING(station) " +
  "WHERE sg.section_id = ?";

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
  "SELECT sg.section_id, sg.station, sg.trust, sg.mag AS reach_mag, " +
  "       g.name, g.lon, g.lat, g.area_km2, g.mag " +
  "FROM item_section it " +
  "JOIN section_gauge sg ON sg.section_id = it.section_id " +
  "LEFT JOIN gauge g USING(station) " +
  "WHERE it.item_id = ? " +
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
  `SELECT section_id, station, trust FROM section_gauge WHERE section_id IN (${placeholders(n)})
   UNION ALL
   SELECT it.section_id, min(lg.station) AS station, 'good' AS trust
     FROM item_section it JOIN lake_gauge lg ON lg.item_id = it.item_id
    WHERE it.section_id IN (${placeholders(n)})
      AND it.section_id NOT IN (SELECT section_id FROM section_gauge)
    GROUP BY it.section_id`;

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
  `SELECT it.section_id, min(lg.station) AS station
     FROM item_section it JOIN lake_gauge lg ON lg.item_id = it.item_id
    WHERE it.section_id IN (${placeholders(n)})
    GROUP BY it.section_id`;

/**
 * The panel for each of these sections, members in weight order.
 *
 * TWO STATEMENTS' WORTH OF WORK IN ONE, via the dictionary: `section_panel` is the pointer
 * and `panel_member` is the shared body, so a viewport full of one river's reaches fetches
 * that river's donors once rather than once per reach.
 */
export const panelsForSections = (n: number) =>
  `SELECT sp.section_id, sp.area_km2 AS target_area,
          pm.ord, pm.station, pm.role, pm.area_km2 AS donor_area, pm.years,
          pm.regulated, pm.same_river
     FROM section_panel sp JOIN panel_member pm ON pm.panel_id = sp.panel_id
    WHERE sp.section_id IN (${placeholders(n)})
    ORDER BY sp.section_id, pm.ord`;

// ONE ENVELOPE PER (STATION, PARAMETER). A station that measures both stage and discharge
// has two, in two different units, and asking for "the" envelope of such a station is how a
// level ends up compared against a discharge — a percentile that looks fine and means
// nothing. The parameter is always part of the key.
export const CLIMATOLOGY =
  "SELECT pentad, p10, p25, p50, p75, p90 FROM gauge_clim " +
  "WHERE station = ? AND parameter = ? ORDER BY pentad";

/** Which quantities this station has an envelope for — the toggle offers only these. */
export const CLIM_PARAMETERS =
  "SELECT DISTINCT parameter FROM gauge_clim WHERE station = ? ORDER BY parameter";

export const DOWN_FROM = "SELECT down_id FROM section_down WHERE section_id = ?";

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
  `SELECT station, name, section_id, lon, lat, area_km2
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
