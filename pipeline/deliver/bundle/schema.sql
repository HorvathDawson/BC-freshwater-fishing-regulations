-- The bundle. ONE definition of the format, used by every packager.
--
-- Settled by `app/design/data-contract.html` ("One Interface, Two Storages", §4 and §10):
-- SQLite, so one file serves a resident copy on the phone and a range-read copy on the web.
-- Tables are written in the order the queries read them, so one water's rules land in one
-- or two 64 KB reads — the format is not the thing to get right, the page layout is.
--
-- NOT IN HERE, deliberately:
--   geometry, name, alias, magnitude, order, mus, areas
--       -> the TILES (pipeline/deliver/tiles/tile-contract.json). A second copy is a second
--          source of truth. The one exception is `item.name` and `alias.alias`: a PMTiles
--          archive cannot be searched by text, so the strings must also live somewhere
--          queryable. That is a search index, not a duplicate of identity.
--   the current gauge reading
--       -> a FEED. It changes every 30 minutes; the bundle changes per edition. Different
--          clocks belong in different files or the whole bundle is invalidated hourly.

CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;

-- identity ------------------------------------------------------------------------
-- item_id is durable across a rebuild (99.88%, measured). section_id is not (94%), and
-- must never leave the bundle — not in a URL, a saved pin, or a feed (AGENTS rule 5).
CREATE TABLE item (item_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT);
CREATE TABLE alias (item_id TEXT NOT NULL, alias TEXT NOT NULL);
CREATE TABLE item_section (item_id TEXT NOT NULL, section_id TEXT NOT NULL);

-- regulations ---------------------------------------------------------------------
CREATE TABLE entry (entry_id TEXT PRIMARY KEY, item_id TEXT, name TEXT,
                    verbatim TEXT, symbols TEXT, mus TEXT);

-- rule_id is unique only WITHIN an entry — 49 collide corpus-wide (AGENTS rule 8), so
-- every table keys on (entry_id, rule_id) and never on rule_id alone.
CREATE TABLE rule (entry_id TEXT NOT NULL, rule_id TEXT NOT NULL, kind TEXT,
                   windows TEXT, species TEXT, subject TEXT,
                   -- a rule nobody could place must never vote on an outcome; it can only
                   -- ever raise "unknown" (core/status.ts)
                   uncertain INTEGER NOT NULL DEFAULT 0,
                   text TEXT, location TEXT,
                   PRIMARY KEY (entry_id, rule_id)) WITHOUT ROWID;

-- reg_index. Scope is a property of the GEOMETRY, not of the rule, so one table carries
-- section-, MU- and area-scoped bindings and the client resolves precedence.
CREATE TABLE rule_section (entry_id TEXT NOT NULL, rule_id TEXT NOT NULL,
                           scope_kind TEXT NOT NULL,   -- section | mu | area
                           scope_id TEXT NOT NULL);

-- conditions ----------------------------------------------------------------------
-- trust is the representativeness class, not a hint: `none` means the station drains far
-- too much to describe this water and the app must say nothing rather than give a number.
-- The gauges themselves. Without this a client holds a station id and can say nothing:
-- not its name, not where it is, not whether it still reports.
--
-- `realtime` and `active` are SEPARATE and both are kept. `active` is ECCC's own status;
-- `realtime` is whether the station appeared in today's transmitting roster. A station can
-- be flagged active in the metadata and have stopped talking years ago, and only the second
-- flag may be used to offer a live reading. A discontinued gauge is still worth a row — it
-- anchors a climatology, and a river that has been gauged since 1913 should not be
-- described as ungauged.
--
-- `mag` is the gauge node's stream magnitude: the denominator behind every `trust` band in
-- section_gauge, stored so the client can explain a band rather than just assert it.
-- `matched_by` and `match_m` are the MATCH's own provenance: how this station found its
-- node and how far off it landed. Kept because the match is the fragile part -- v1's own
-- notes record that the last four points of match rate came from hand-written aliases, and
-- without a record of which rows lean on that mechanism there is no telling a link that is
-- holding from one about to break. `matched_by` is null when nothing resolved.
CREATE TABLE gauge (station TEXT PRIMARY KEY, name TEXT NOT NULL,
                    item_id TEXT, section_id TEXT,
                    lon REAL, lat REAL, area_km2 REAL, mag INTEGER,
                    matched_by TEXT, match_m REAL) WITHOUT ROWID;

-- ONE station per reach: the one that most nearly IS that water.
--
-- A second gauge on the same reach drains more country than the first, or less, so it can
-- only be a worse answer to the same question -- not a second opinion, a diluted one. The
-- variety that matters lives at RIVER level, where different reaches legitimately have
-- different best gauges: measured, the Stellako sees 3 distinct stations along its length,
-- the Chilliwack 6, the Thompson 7.
--
-- STREAM STATIONS ONLY. A lake station reports a level in metres, not a discharge in m3/s,
-- and a trust ratio between the two is arithmetic on different quantities. They live in
-- `lake_gauge`. See pipeline/hydro/shed.py::is_lake_station for what this cost before it
-- was separated: 11,049 stream sections were being told a reservoir's level.
--
-- `mag` is THIS section's stream magnitude, against `gauge.mag` for the station's. The
-- band alone cannot rank two gauges: 'good' spans a tenth of a watershed to all of it, so
-- ordering on it picked a 13 km2 creek station over the Cowichan's own river gauge.
CREATE TABLE section_gauge (section_id TEXT PRIMARY KEY, station TEXT NOT NULL,
                            trust TEXT NOT NULL, mag INTEGER) WITHOUT ROWID;

-- THE DONOR PANEL ------------------------------------------------------------------
--
-- `section_gauge` above answers "which ONE station speaks for this reach". These two answer
-- "which stations speak for it, and how loudly" -- the same question asked of a province
-- whose gauge network is far sparser than its stream network.
--
-- THE DICTIONARY STORES THE DONOR SET AND NOTHING DERIVED FROM IT, which is the decision
-- that makes it small. A weight depends on the TARGET's catchment as well as the donor's,
-- so baking weights into the panel gives adjacent reaches on one river slightly different
-- panels and the dictionary stops collapsing. Measured over the province:
--
--     weights baked in    16,127 panels    34,984 member rows    11.8x
--     donor set only       1,898 panels     4,337 member rows   100.6x
--
-- Eight times smaller, and the weights come out EXACT rather than rounded to whatever the
-- interning key kept. It also means the weighting formula and the error calibration can be
-- changed in a release rather than a rebuild.
--
-- `area_km2` ON THE SECTION is what makes that work: the target's own drainage, so a client
-- has both halves of every ratio. It is the value at the section's OUTLET; a tap partway up
-- is refined by `section_profile` (the staircase), which is why this is stored per section
-- rather than folded into the panel.
CREATE TABLE section_panel (section_id TEXT PRIMARY KEY,
                            panel_id INTEGER NOT NULL,
                            area_km2 REAL) WITHOUT ROWID;

-- One row per donor in a panel.
--
-- `ord` IS A STABLE MEMBER INDEX AND NOT A RANKING. It cannot be a ranking here: the order
-- depends on weights, and weights depend on the target, so two sections sharing a panel can
-- legitimately rank the same donors differently. The client sorts by the weight it computes
-- -- which is also the order it must display, so the number and the table beneath it still
-- come from one calculation.
--
-- `role` is TEXT because the table is small and 'up' read in a query beats 0 looked up in a
-- comment. Exactly two values today; a NEIGHBOUR donor -- one that shares weather rather
-- than water -- would be a third and is deliberately not built (see panel.py).
--
-- `area_km2` and `years` ARE THE DONOR'S OWN FACTS, and everything a screen shows is
-- derived from them at read time: the share is min/max against the section's area, the
-- weight follows from the share and the role and the record, and the trust class and its
-- error bars come from the ratio through one ladder mirrored into the app and pinned by a
-- test. Storing any of those instead would freeze a calibration into the bundle -- and the
-- calibration is a measurement over 9,495 gauge pairs that will be redone.
CREATE TABLE panel_member (panel_id INTEGER NOT NULL,
                           ord INTEGER NOT NULL,
                           station TEXT NOT NULL,
                           role TEXT NOT NULL,        -- up | down
                           area_km2 REAL NOT NULL,    -- the DONOR's catchment
                           years INTEGER NOT NULL,    -- its record length
                           PRIMARY KEY (panel_id, ord)) WITHOUT ROWID;

-- A station on a lake, linked to the lake. No trust band: there is no fraction of a level,
-- so the gauge is either on this water or it is not.
CREATE TABLE lake_gauge (item_id TEXT NOT NULL, station TEXT NOT NULL,
                         PRIMARY KEY (item_id, station)) WITHOUT ROWID;

-- Only for sections inside some gauge's shed. The contract cuts the rest: the sole use is
-- tracing a tap down to its gauge, and 2.02M pointers become ~119k.
--
-- `down_id` MAY NAME A SECTION WITH NO ROW OF ITS OWN — measured, 33,454 of 119,271 do.
-- This comment used to claim otherwise ("every reach on that path is in the same shed by
-- definition"), which is true walking UPSTREAM-to-gauge and false in general: a shed also
-- runs downstream of its gauge, and the last member's neighbour is outside it. A trace that
-- runs out of pointers has reached the edge of what any gauge speaks for; that is the
-- answer, not missing data, and the client must render it as the end of the chain.
CREATE TABLE section_down (section_id TEXT PRIMARY KEY, down_id TEXT NOT NULL) WITHOUT ROWID;

-- Pentad-sampled percentiles. p0 and p100 are absent on purpose: they do not interpolate
-- (82% error, measured), and a wrong extreme is worse than no extreme.
-- The envelope: what this station's flow USUALLY does, by time of year.
--
-- WHY IT IS HERE WHEN THE FEED INDEX ALREADY SHIPS A PERCENTILE. They answer different
-- questions. The index says "this dot is at the 4th percentile today", which is a NUMBER,
-- computed once by the publisher so every client agrees and the map can paint from a 35 KB
-- file without opening SQLite. This is a SHAPE: it draws the seasonal band behind a
-- hydrograph, and it lets a spot saved last October still say where that day sat -- offline,
-- with no feed, forever.
--
-- Both copies come from ONE producer. The hydro job owns the HYDAT extract; it publishes
-- the envelope as a feed artifact, uses it to compute the percentiles it puts in the index,
-- and the build snapshots it into here. The cron never reads the bundle -- the arrows only
-- point one way, which is what keeps the two from drifting.
-- Keyed by PARAMETER as well as station: 440 BC stations publish both a discharge and a
-- level envelope, and they answer different questions. "Is the river low" and "is the river
-- deep" are not the same query, and their units are not comparable, so they are never
-- folded into one distribution.
CREATE TABLE gauge_clim (station TEXT NOT NULL, parameter TEXT NOT NULL,
                         pentad INTEGER NOT NULL,
                         p10 REAL, p25 REAL, p50 REAL, p75 REAL, p90 REAL,
                         PRIMARY KEY (station, parameter, pentad)) WITHOUT ROWID;

-- What the envelope above is BUILT FROM, so a reader can weigh it.
--
-- "Below normal for the date" means something different backed by 97 years than by 11, and
-- a percentile with no stated record is a number pretending to more authority than it has.
-- These are the facts worth putting on screen next to it: how long the record runs, how
-- many years actually contributed, and how complete they were.
--
-- The floor for publishing an envelope is THREE years, so `years` here is genuinely small
-- for some stations and the reader must be able to see it. Three years supports "low for
-- the date"; it does not support "fourth percentile", and only this row tells them apart.
CREATE TABLE gauge_stats (station TEXT NOT NULL,
                          parameter TEXT NOT NULL,   -- discharge | level
                          from_year INTEGER, to_year INTEGER,
                          years INTEGER,          -- years with any observation
                          days INTEGER,           -- daily observations pooled
                          PRIMARY KEY (station, parameter)) WITHOUT ROWID;

-- stocking ------------------------------------------------------------------------
--
-- THE LINK, AND ONLY THE LINK. `waterbody_id` is FIDQ's own stable TEXT key, so the feed is
-- addressed by it and nothing here has to change when a release is added. What was stocked,
-- when, how many and how big is feed data on a weekly clock:
--
--     stocking/index.json           {waterbody_id: last release date}  -- map recency
--     stocking/{waterbody_id}.json  every release ever for that water  -- one fetch, on tap
--
-- `matched_by` carries the same provenance discipline as `gauge`: FIDQ names a water the
-- way an angler does and the FWA names it the way a surveyor does, so some of these will
-- always lean on a curated alias, and a link leaning on one should be visible as such.
CREATE TABLE stock_water (waterbody_id TEXT PRIMARY KEY, item_id TEXT,
                          name TEXT NOT NULL, lon REAL, lat REAL,
                          matched_by TEXT, match_m REAL) WITHOUT ROWID;

-- Species and life stage as integers against a dictionary, because "Rainbow Trout" written
-- out 200,000 times is 3.4 MB of one string. The feed carries the codes; this decodes them.
CREATE TABLE stock_code (kind TEXT NOT NULL, code INTEGER NOT NULL, label TEXT NOT NULL,
                         PRIMARY KEY (kind, code)) WITHOUT ROWID;   -- kind: species | stage

-- lakes ---------------------------------------------------------------------------
CREATE TABLE chart (item_id TEXT, name TEXT, chart_id TEXT, title TEXT, kind TEXT,
                    drafted TEXT, scale INTEGER, area_km2 REAL, pdf TEXT);
-- RETIRED, kept until the FIDQ fetch lands so nothing silently loses a table. It is keyed
-- on a NAME, which is exactly the join that cannot be trusted: two lakes share a name and
-- neither gets the right fish. `stock_water` + the stocking feed replace it.
CREATE TABLE release (name TEXT NOT NULL, date TEXT, species TEXT, count INTEGER, stage TEXT);

-- precomputes ---------------------------------------------------------------------
-- place_id is NOT the name. 373 names repeat in the gazetteer, so keying on it made
-- INSERT OR REPLACE last-write-wins and destroyed 532 places — and because the fetch sorts
-- least-important-last, the survivor was the wrong one: "Hope" resolved to a hamlet in
-- Idaho instead of the BC town, and "Richmond" to a Calgary suburb.
-- An INTEGER key, because place_water carries it 350,000 times. `osm` is the stable
-- natural key (name@lat,lon) so a rebuild maps back to the same place.
CREATE TABLE place (place_id INTEGER PRIMARY KEY, osm TEXT NOT NULL, name TEXT, kind TEXT,
                    pop INTEGER, lat REAL, lon REAL);

-- Measured to every named WATERBODY, not to every section. That is what makes the full
-- matrix cheap enough to just compute rather than approximate.
--
-- Keyed on item_id, not on the display name: joining a precompute back to a water by its
-- NAME is the classic silent mismatch, and item_id is the id that survives a rebuild
-- (99.88%, AGENTS rule 5). The name rides along so a list can be rendered without a join.
-- No name column: it is `item.name`, and duplicating it across 350,000 rows cost more
-- than the whole rest of the bundle. One join, or none at all if the caller already has
-- the item.
CREATE TABLE place_water (place_id INTEGER NOT NULL, item_id TEXT NOT NULL,
                          km REAL NOT NULL);
