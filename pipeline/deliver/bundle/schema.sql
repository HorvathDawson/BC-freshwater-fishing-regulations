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
-- item_id is durable across a rebuild (99.88%, measured). A SECTION is not named by a string
-- here at all: it is named by `sid`, its index in the atlas's `section_handles.txt`. That is
-- 3 bytes instead of ~12 and takes 12.5 MB off the bundle, and it makes AGENTS rule 5 —
-- section identity must never leave the bundle, not in a URL, a saved pin, or a feed —
-- structural rather than a rule to remember: a handle is meaningless without the table that
-- made it, and it changes whenever the atlas does.
--
-- THE TILE CARRIES THE SAME HANDLE, so the two artifacts must be built from one atlas. The
-- table's digest is in `meta.section_handles` and in the tile sidecar, and the app compares
-- them before it colours anything: a mismatch is not a missing lookup, it is a hit on the
-- WRONG section, which would show real regulations for the wrong piece of river.
-- `ord` IS THE ROWID, and it exists so place_water can name an item in two bytes instead
-- of eleven. It is a STORAGE HANDLE, not an identity: it is assigned by insertion order and
-- is NOT durable across a rebuild, so it must never leave the bundle. `item_id` is still the
-- identity and still unique — the UNIQUE index below is the same b-tree the old TEXT PRIMARY
-- KEY built, so naming the rowid costs nothing and buys 15.9 MB (measured) in place_water.
CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT NOT NULL UNIQUE,
                   name TEXT NOT NULL, kind TEXT);
CREATE TABLE alias (item_id TEXT NOT NULL, alias TEXT NOT NULL);
-- `ord` and `sid` are HANDLES, not ids — item.ord and the section handle table. See
-- pipeline/common/section_handles for who owns the section one and why it may never leave
-- the bundle.
CREATE TABLE item_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL);

-- regulations ---------------------------------------------------------------------
-- `name` is the display name — "Chilliwack River". `full_name` is what the curator wrote:
-- "CHILLIWACK / VEDDER RIVERS (does not include Sumas River) (see map on page 24)".
--
-- ONLY THE SHORT ONE SHIPPED, and it made a real sentence absurd. Six of the Chilliwack's
-- seven rules arrive from this entry by the tributary walk, so the screen wants to say
-- "these are not written for the Chilliwack — it joins X, whose entry includes tributaries".
-- With only the display name, X IS "Chilliwack River" and the river is told it joins itself.
-- The full name says which waters the entry actually covers, and 224 of 1,393 entries carry
-- an extent or an exclusion in that parenthetical that exists nowhere else.
-- `pages` is where the row is PRINTED in the synopsis, so a reader who wants to check us can
-- be told which page to open. A JSON array: seven MU 6-1 lakes are printed on two pages each.
CREATE TABLE entry (entry_id TEXT PRIMARY KEY, item_id TEXT, name TEXT, full_name TEXT,
                    verbatim TEXT, symbols TEXT, mus TEXT, pages TEXT);

-- rule_id is unique only WITHIN an entry — 49 collide corpus-wide (AGENTS rule 8), so
-- every table keys on (entry_id, rule_id) and never on rule_id alone.
-- `scope` is WHERE THE RULE WAS WRITTEN, and it drives precedence: a rule written for this
-- water displaces a zone default that contradicts it (see `evaluate` in core/status.ts).
-- Every rule in the corpus today is `section` — all 3,050 name a river or a lake — and `mu`
-- awaits zone regulations ("in MU 4-5, no bait") being parsed. It is stored rather than
-- assumed so that when they arrive the precedence rule does not have to be rediscovered.
--
-- DO NOT CONFUSE IT WITH `ruleset.via`, which is how a rule REACHES one section. They are
-- orthogonal: specificity is a property of the rule, provenance of the (section, rule) pair,
-- and an earlier draft of this schema had one column trying to be both.
-- `limits` is the quota AS NUMBERS: a JSON list of {take, over_cm, under_cm, water, kind,
-- combined, origin, within}. Empty is NOT "no limit" — it means nobody has structured that
-- rule yet and `details` is still the only place its count exists. It ships so a client can
-- lay quotas out as a table and show which of two rules overrides the other, neither of
-- which is possible against a sentence. See pipeline/regs/parsing/entry_models::Limit, and
-- limit_words for the ONE place those numbers are turned back into English.
-- THE RULE, AS A TYPE AND ITS CONDITIONS. `kind`/`details` are gone with the prose model.
--
-- `type` is one of fifteen and `family` one of six; both ship because the reader is shown
-- families as sections and a client should not carry its own copy of the mapping.
--
-- `dimension` is the OTHER HALF OF THE PRECEDENCE KEY, and it is why this replaced `subject`.
-- Two rules compete when they share (type, dimension): a water's daily quota displaces its
-- region's daily quota, and a fly-only rule does NOT displace a barbless rule because a fly
-- must also be barbless — so tackle's dimension is the facet each constrains, not the type.
-- `subject` came from `exempts_from` and was populated on 2 rules out of 3,273, so the
-- displacement machinery existed for two years and never had data to fire on.
--
-- `label` is GENERATED from type + conditions. It is not the curator's prose: that field was
-- removed because a label typed beside a number drifts from it, and this corpus proved it
-- twice. Every surface reads this one string, so the map, the sheet and the curation app
-- cannot word the same rule differently.
--
-- `take` and `may_target` ARE COLUMNS, not just conditions, because they are what separates a
-- closure from catch-and-release and the client decides an outcome from them. "No fishing for
-- bull trout" is take=0 with may_target=0; "bull trout catch and release" is take=0 with
-- may_target=1. Reading take=0 alone as "closed" turned 605 closures into permissions once,
-- in the label generator, and the same misreading is available to anything that only sees a
-- quota of zero.
CREATE TABLE rule (entry_id TEXT NOT NULL, rule_id TEXT NOT NULL,
                   type TEXT NOT NULL,        -- one of 15 (catalogue.RuleType)
                   family TEXT NOT NULL,      -- one of 6  (retention, gear_and_method, ...)
                   dimension TEXT NOT NULL,   -- precedence key, second half
                   label TEXT NOT NULL,       -- generated; never authored
                   scope TEXT NOT NULL DEFAULT 'section',   -- section | mu | area
                   windows TEXT, species TEXT, species_except TEXT,
                   take INTEGER, may_target INTEGER,        -- see above; both may be NULL
                   conditions TEXT,           -- the rest of the rule's set fields, as JSON
                   -- a rule nobody could place must never vote on an outcome; it can only
                   -- ever raise "unknown" (core/status.ts)
                   uncertain INTEGER NOT NULL DEFAULT 0,
                   verbatim TEXT,             -- the sentence, quoted from regs_verbatim
                   extent_text TEXT,          -- the reach in the page's words, when unbound
                   PRIMARY KEY (entry_id, rule_id)) WITHOUT ROWID;

-- SAY IT ONCE AND POINT AT IT. The rules covering a section, as a SET the section names.
--
-- The obvious table is one row per (section, rule). Built from the real corpus that is
-- 1,720,243 rows and 69.6 MB — on a bundle of 51.8 MB, and against a data contract that
-- budgets about 10 MB for all regulation data. A 7x blow-out of the whole artifact.
--
-- It is also 1,720,243 rows carrying nowhere near that many FACTS. A regulation applies to
-- a stretch of river and a stretch of river is many sections, so a river with one closure
-- repeats that closure down every section of its length. Counted: 583,654 sections carry a
-- rule and between them they have 1,905 distinct sets. One set is shared by 104,185
-- sections. Interning them costs 12.1 MB for both tables — 83% smaller — and READS FASTER,
-- because there is less of it: 0.25 ms for a 300-section viewport against 1.0 ms.
--
-- WHY NOT STORE THE SCOPE AND WALK ON THE CLIENT. Because the walk is not a prefix match.
-- Tributary scope is relative to the RULE'S EXTENT, not to the named river: "no fishing
-- between A and B, including tributaries" means the streams joining THAT STRETCH. The
-- watershed-code shortcut gets it wrong in the expensive direction — on the Kootenay it
-- adds 3,205 km of water joining BELOW the regulated reach, which is the app announcing a
-- closure that does not exist. The membership is a graph-walk result and has to be stored.
--
-- NOTHING ABOUT THIS TOUCHES THE TILE. `mus` and `areas` ride on tile features because
-- administrative geography exists whether or not anything is regulated; a rule-derived set
-- does not, and the map is not where regulation knowledge lives.
CREATE TABLE section_ruleset (sid INTEGER PRIMARY KEY,
                              set_id INTEGER NOT NULL);

-- The sets themselves: 1,905 of them across 6,622 rows.
--
-- `via` is WHY this rule reaches this section — `reach` if the rule names this water,
-- `trib` if it arrived by the tributary walk. Not decoration: 98.6% of all bindings are
-- tributary (1,696,351 against 23,892), so the sweep IS the corpus, and it is the part that
-- can be wrong over thousands of kilometres at once. A row that cannot say how it got here
-- cannot be audited. Carrying it costs 249 extra sets and 0.3 MB.
CREATE TABLE ruleset (set_id INTEGER NOT NULL, entry_id TEXT NOT NULL, rule_id TEXT NOT NULL,
                      via TEXT NOT NULL,          -- reach | trib
                      PRIMARY KEY (set_id, entry_id, rule_id)) WITHOUT ROWID;

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
                    item_id TEXT, sid INTEGER,
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
CREATE TABLE section_gauge (sid INTEGER PRIMARY KEY, station TEXT NOT NULL,
                            trust TEXT NOT NULL, mag INTEGER);

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
CREATE TABLE section_panel (sid INTEGER PRIMARY KEY,
                            panel_id INTEGER NOT NULL,
                            area_km2 REAL);

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
--
-- `regulated` IS NOT DERIVED AND CANNOT BE. It comes from HYDAT's STN_REGULATION, which the
-- client does not ship, and it changes what the number MEANS rather than how much it is
-- worth: a percentile at a dammed station is a percentile of somebody's dispatch decision.
-- Such a donor is admitted only for water that is all but its own (REGULATED_MAX_RATIO in
-- panel.py), where that schedule is what this water is actually doing — and the screen still
-- has to say so, or the app presents a release schedule as a description of the weather.
CREATE TABLE panel_member (panel_id INTEGER NOT NULL,
                           ord INTEGER NOT NULL,
                           station TEXT NOT NULL,
                           role TEXT NOT NULL,        -- up | down
                           area_km2 REAL NOT NULL,    -- the DONOR's catchment
                           years INTEGER NOT NULL,    -- its record length
                           regulated INTEGER NOT NULL DEFAULT 0,   -- a dam governs it
                           -- ON THE SAME BLUE LINE as the section it speaks for.
                           --
                           -- Two donors of identical catchment size can be two entirely
                           -- different relationships: one where the water flows past both
                           -- points, one where they share a rain shadow and nothing else.
                           -- The area ratio cannot tell them apart, and the Skeena showed
                           -- what that costs — two gauges on the Skeena at the 77th and
                           -- 78th percentile, weighted equally with the Babine at the 24th,
                           -- disagreeing past what the interval can express, so the app
                           -- refused and drew "no baseline" over a river with two of its
                           -- own gauges reporting.
                           same_river INTEGER NOT NULL DEFAULT 1,
                           PRIMARY KEY (panel_id, ord)) WITHOUT ROWID;

-- WHICH STATION SPEAKS FOR EACH CATCHMENT, for the zoomed-out field.
--
-- The tile carries a `basin_id` and no reading — a reading changes every half hour and a
-- tile changes once a build — so this is the join, and it lives here because it changes
-- with the GAUGE NETWORK: a station opens, a station is retired, and the same geometry
-- answers to somebody else. Two clocks, two artifacts.
--
-- `levels_up` IS NOT DECORATION. Only 4.5% of the province has a gauge in its own
-- catchment; 48% inherits from the catchment it drains into and 27% from a grandparent.
-- That is a real hydrological claim — the water here drains into the water measured there —
-- and a weaker one the further it travels. A field drawn without it would be the most
-- confident-looking thing in the app and the least directly measured, so the number rides
-- along and the screen is obliged to be able to say it.
--
-- A basin nothing upstream measures gets NO ROW, and is drawn as unmeasured. 13% of the
-- province, and silence is the honest answer there.
-- EVERY gauge standing in a watershed group, not the biggest one.
--
-- This was one row per group, holding whichever station had the largest catchment. Two ways
-- that went blank on a group that is plainly gauged:
--
--   · the chosen station had no climatology, so it could never produce a percentile -- not
--     today, ever. 13 groups elected one, the Skeena among them (KISP chose SKEENA RIVER AT
--     HAZELTON), while other gauges in the same group had a full record sitting unused.
--   · the chosen station was simply not transmitting this hour. Chilliwack went grey with
--     six gauges in the group, five of them reporting, because the one row named 08MH047.
--
-- Both are the same mistake: electing a single representative makes the group's answer
-- hostage to that one station. So the group carries its whole roster and the client
-- combines them -- see `useBasinStandings`, which weights by `area_km2` (how much of the
-- group a station actually observes) and `years` (how well its own baseline is known), and
-- averages in probit space using the SAME transform the reach panels use.
--
-- Only stations with a baseline are written. Liveness is the feed's business and changes
-- every half hour; having a record to rank against is static, known here, and disqualifying.
CREATE TABLE basin_member (basin_id TEXT NOT NULL,
                           station TEXT NOT NULL,
                           area_km2 REAL,          -- the station's catchment
                           years INTEGER NOT NULL, -- length of its record
                           PRIMARY KEY (basin_id, station)) WITHOUT ROWID;

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
CREATE TABLE section_down (sid INTEGER PRIMARY KEY, down_sid INTEGER NOT NULL);

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
-- ONE ROW PER ENVELOPE, NOT PER PENTAD, because the envelope is what is always read: the
-- only query is "every pentad for this station and quantity", and `bandAt` wants the whole
-- array. 152,276 rows become 2,398 blobs. Measured:
--
--     73 rows of 5 REAL, WITHOUT ROWID      10.08 MB    26.5 us
--     this                                   4.06 MB    10.1 us
--
-- NOTE THE ROWID. The same blob in a WITHOUT ROWID table came to 9.01 MB, not 4.06: an
-- index b-tree keeps only ~1000 bytes of payload local, so every 1.3 KB envelope spilled
-- and burned a whole 4096-byte overflow page — 3.25 MB of data in 8.98 MB of pages. A rowid
-- table keeps ~4061 local, so the blob stays inline. The UNIQUE index in indexes.sql is what
-- (station, parameter) is looked up by.
--
-- THIS IS THE OPPOSITE CHOICE TO place_water, ON PURPOSE. There the query is "the nearest N",
-- so a blob would have to be fully read to answer it and SQL does the job better. Here the
-- query IS the whole group, so the blob is the answer already decoded. Access pattern
-- decides, not size.
--
-- `bands` is, per pentad, ordered by pentad, little-endian:
--     uint8 pentad, then float32 p10, p25, p50, p75, p90     (21 bytes)
--
-- FLOAT32 IS NOT A ROUNDING. Worst relative error over all 761,380 values is 5.9e-08 —
-- 0.0000009 m3/s on a 15.80 m3/s reading, against a transfer error of +/-11.7 PERCENTILE
-- POINTS. The precision that matters was never in the eighth digit.
CREATE TABLE gauge_clim (station TEXT NOT NULL, parameter TEXT NOT NULL,
                         bands BLOB NOT NULL);

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
-- NEVER KEYED ON THE DISPLAY NAME: joining a precompute back to a water by its NAME is the
-- classic silent mismatch. It is keyed on `item.ord`, which resolves to item_id in the same
-- join that fetches the name — and item_id is the id that survives a rebuild (99.88%, AGENTS
-- rule 5), which ord deliberately is not. No name column either: it is `item.name`, and
-- duplicating it across 379,673 rows cost more than the whole rest of the bundle.
--
-- THREE INTEGERS, AND THE TABLE IS ITS OWN INDEX. This was (place_id, item_id TEXT, km REAL)
-- with a separate `water_by_place(place_id, km)` index, and the pair cost 20.9 MB — a third
-- of the entire bundle. Measured, on the real 379,673 rows:
--
--     today (TEXT id + REAL km + index)        20.88 MB     21 us
--     WITHOUT ROWID, same columns              12.17 MB     18 us
--     this                                      4.94 MB     14 us
--     one packed blob per place                 2.33 MB     25 us
--
-- Smaller AND faster, which is not a trade: the win is fewer pages, and on the web every
-- statement is an HTTP range request. The blob was smaller still and was rejected — it buys
-- 2.6 MB by making the rows unreadable to the sqlite CLI, to a test, and to any question we
-- have not thought of yet, such as "which places are near this water".
--
-- `ckm` IS CENTIKM AND IS LOSSLESS. Every distance was already rounded to two decimals and
-- capped at 25.00 km, so 0..2500 is the same number in 2 bytes rather than 8 — not a coarser
-- one. `ord` is item.ord, a storage handle; the caller joins for the id and the name.
CREATE TABLE place_water (place_id INTEGER NOT NULL,
                          ckm INTEGER NOT NULL,     -- hundredths of a km, 0..2500
                          ord INTEGER NOT NULL,     -- item.ord, NOT an item_id
                          PRIMARY KEY (place_id, ckm, ord)) WITHOUT ROWID;
