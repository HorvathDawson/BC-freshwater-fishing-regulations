"""THE REACH POLICY CHANGELOG: `POLICY_VERSION` and, above it, one line per outcome change of the reach classifier.

This module held a per-entry result cache (`ReachCache`, `entry_key`). Nothing ever called it, and by its own notes it
was unsafe to adopt: from policy 14 on an answer depends on OTHER entries (`outside.rowed_waters`), which a per-entry
key cannot see, and the key also missed `undrawn_part`, `standing`, licensing records and tidal splits. It was deleted
(T561, 2026-11-10); a full reach run resolves the corpus in about 0.1 s, so there is nothing to cache.

What stays is the version and its changelog: other code and tests cite "POLICY_VERSION n" for when a rule landed, and
`test_reach_builder.test_policy_version_is_bumped_when_classify_changes` pins the classifier's observable policy to it,
so a change to an outcome is written down here. Bump it, with a line, whenever `classify.py` changes an OUTCOME.
"""

from __future__ import annotations

#: Bump whenever `classify.py` changes an OUTCOME (the changelog below). Guarded by a test.
#: 6: the tributary walk lets a lake into a walk through its MAIN outlet even when the lake's
#:    aggregate order runs ahead of the outlet's (Dester Lake / Meldrum Creek) —
#:    `graph.tributaries._is_main_outlet`. It changes which sections a walk reaches.
#: 7: a DETACHED braid piece (touching no section of its water, only unnamed lakes on its own
#:    blue line) is placed by its own line's nearest placed pieces (`extent._bracket`), and a
#:    watershed the registry did not mint (`area:basin:100-342455-`) resolves from the graph
#:    (`extent.area_sections`) where it used to fail.
#: 8: (2026-09-24 rulings) the walk collects streams only; a lake in the middle of the reach is
#:    the river passing through and its inflows are tributaries; a bifurcation's run follows its
#:    FWA code; a Region 7 row's MUs resolve it to its zone (`outside.entry_regions`).
#: 9: `Extent.watershed` — a part of a river's watershed is cut by FWA code position
#:    (`extent._watershed_part`) and joined after the walk, never walked (`classify`).
#: 10: a section touching two region polygons resolves `area:region:*` to its HOME region only —
#:    the one holding most of its area (a waterbody) or length (a line), `registry.regions`
#:    (user ruling 2026-09-25, Mara Lake). It changes which sections a zone rule binds.
#: 11: a LAKE touching two region polygons resolves `area:region:*` to BOTH (stream pieces keep
#:    their home), and a regional row not printed by another region applies along its water's
#:    whole length (`outside.region_limit`, user ruling 2026-09-25, second half).
#: 12: `Extent` op `rest` — "other parts" binds the rule's water minus its named siblings'
#:    sections (`build._build_rest`); unknown when a sibling does not bind, and a sibling's
#:    straddling pieces are withheld (`classify.COMPLEMENT_*`).
#: 13: (2026-09-29 rulings) a cut at a CONFLUENCE keeps the joining water and its subtree out of the
#:    walk unless the rule's words include it (`build.confluence_excludes`); a carve-out may
#:    `walk_past` its water; a water row's walk is not held to any region
#:    (`outside.WATER_ROW_WALKS_CROSS_REGIONS`); a tidal row's water leaves every other binding
#:    (`outside.tidal_sections`); a code-less floodplain lake takes its side of a watershed cut
#:    from the nearest river piece (`extent.CODE_LAKES_PLACED_BY_POSITION`); a designation stops
#:    at national parks (`licensing.DESIGNATIONS_STOP_AT_NATIONAL_PARKS`); a designation still
#:    walked into a confluence cut's joining water (replaced in 14).
#:    Signs the rule's words put BELOW the confluence ("signs located downstream of the Meziadin River
#:    confluence") put it inside a reach running up from them: the joining water is walked
#:    (`build._signs_below`, review 2026-09-29).
#: 14: (2026-09-30 ruling) a confluence cut's joining water with NO ROW OF ITS OWN goes with the cut
#:    on BOTH sides; one with its own row stays out (`outside.rowed_waters`,
#:    `tributaries.CONFLUENCE_WATER_WITHOUT_A_ROW_GOES_WITH_THE_CUT`); licensing takes the same test
#:    (replacing "a designation always walks in"); signs ABOVE a confluence put the joining water
#:    inside a reach running down from them (`build._signs_above`). NOTE: the answer now depends on
#:    OTHER entries (which waters have rows), which `entry_key` does not see — a cache keyed on one
#:    entry must also key on `rowed_waters` before it is ever used.
#: 15: (FIX round 2026-10-06) a walk continues across water outside B.C.: order 0 (FWA computes no
#:    order past the border) is unknown to the Strahler guard (`tributaries.ORDER_ZERO_IS_UNKNOWN`);
#:    a lake is book-known steelhead water only as a steelhead row's own water
#:    (`steelhead.LAKES_ONLY_BY_OWN_ROW`).
#: 16: (RULES round 2026-10-07) a row printing "(also in M.U. X)" in its name binds in X's region too
#:    (`outside.region_limit`, the Canim River rows, user Q24).
#: 17: (RULES round 2026-10-07) a rule held as an undrawn part never walks tributaries
#:    (`classify.UNDRAWN_PART_DOES_NOT_WALK`: Dinosaur, Deena, Mamin).
#: 18: (D5, owner 2026-10-12) a piece the neighbour pass leaves unplaced is placed by its FLOW PATH
#:    through lakes, ponds, other waters and water outside B.C. to the first section of its own water
#:    (`extent.OFF_LINE_PIECES_FOLLOW_THEIR_FLOW`); what stays unplaced carries a typed reason.
#: 19: (R10, owner 2026-10-16) a braid straddling a cut takes the side holding most of its length,
#:    projected onto the cut's line, and is marked (`extent.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE`);
#:    a piece joining it follows it; an even split or no geometry stays unplaced.
#: 20: (owner 2026-10-22) an ambiguous cut — one split id at two measures on one line, a lake's outlet
#:     and inlet — takes the candidate nearest the rule's OTHER end, not the one nearest the mouth
#:     (`extent.AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`). It moves `upstream_of` and some `between` cuts.
#: 21: (reviewer 2026-10-22) `between` across two lines takes the nearest candidate pair that still
#:     keeps the upper cut above the lower one — a tributary joining the lower line on its lake piece
#:     no longer leaves the cuts unordered.
#: 22: (T90 2026-10-22) the mouth seeding happens only at the reach's lower cut and compares a measure on
#:     the line it belongs to (`tributaries.MOUTH_SEEDS_ONLY_AT_THE_CUT`); a water carrying a code the
#:     reach drains into is never seeded (`tributaries.THE_RECEIVING_RIVER_IS_NEVER_A_SEED`) — the
#:     Chehalis closure no longer binds two Harrison side channels.
#: 23: (T102 owner 2026-10-22) the WALK never steps into a water whose code is a strict ancestor of the
#:     reach's — the receiving river or its side channels, at any order
#:     (`tributaries.THE_WALK_NEVER_ENTERS_A_PARENT`); a seed takes the same test (`_refusal`), so
#:     does a lake's code run (a run's piece of the receiving river is left out), and
#:     T90's seeding-only `THE_RECEIVING_RIVER_IS_NEVER_A_SEED` is removed.
#: 24: (T117 owner 2026-10-22) every walk that climbs a water steps through `tributaries.may_enter`:
#:     a confluence cut's joining water (`tributaries.subtree`) no longer climbs across a code run's
#:     divide, into a canal, a bigger river or the river it drains into — the Chilko row's designation
#:     loses North Tsuniah Creek's 355996084:17, which FWA codes to Tsuniah, not Brittany Creek.
#: 25: (T111 owner 2026-10-22) EVERY CUT REACHES THE WALK: the tributary seeding takes every extent's
#:     lower cut as `resolve_extent` used it, not the first extent's window
#:     (`tributaries.EVERY_CUT_REACHES_THE_WALK`), and the confluence test reads the same cuts (the
#:     candidate an ambiguous cut's reach used, never every candidate).
#: 26: (T115 owner 2026-10-22) ONE REGION MEMBERSHIP: a per-region row's region, the steelhead twin's
#:     `area_id` and the `area_kind: region` family are a region's HOME members
#:     (`registry.regions.region_sections`, `ONE_REGION_MEMBERSHIP`) as the zone rules' always were —
#:     a stream piece straddling a region line is no longer the other region's row's.
#: 27: (T153, owner Q124 open — its stated default built) an UNNAMED lake polygon the river's main flow
#:     runs through, between two pieces a rule's extent selected, is the river for that rule
#:     (`extent.river_widenings`, `extent.UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER`); a named lake never
#:     takes the river's row.
#: 28: (T153 follow-up, 2026-10-23) A STEELHEAD RIVER'S WIDENING IS ANADROMOUS: a pond a steelhead row's own
#:     river takes as its widening is `anadromous` (a rainbow over 50 cm is a steelhead, p.80) where steelhead
#:     rules apply (`steelhead.Presence.anadromous`, `A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS`) — the
#:     Kitimat's pond kept a wild 60 cm rainbow under the zone's lake quota.
#: 29: (T67, owner 2026-10-24) E15 IS GONE: a braid straddling a cut is no longer placed on its majority
#:     side and marked. The atlas cuts it (`splits.braid_sweep`); the reach reads the cut on every line it
#:     crosses (`extent.A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`), and the run refuses what still straddles
#:     (`build.straddle_gate`).
#: 30: (T67) the D5 flow path never crosses the window's cut either: it does not step between pieces of a
#:     line meeting at the cut, nor climb through the lake the cut is the edge of (`extent._flow_reach`).
#: 31: (T187) a detached braid piece cut at BOTH ends by the window's own two cuts (lower end the lower
#:     cut, upper end the upper cut) lies inside the window
#:     (`extent.A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE`): the Peace's 1.84 km piece under Site C.
#: 32: (T183, owner 2026-10-24, REVERSES 26/T115) A STRADDLER CARRIES BOTH REGIONS' RULES: the one region
#:     membership is raw (`registry.regions.STRADDLERS_IN_EVERY_REGION`) — a stream piece in two region
#:     polygons takes both zone tables, both regions' per-region rows and both steelhead twins.
#: 33: (T152) NOT FISHING WATER: a sewage settling basin, mine tailing pond or dump (the polygon's FWA
#:     FEATURE_CODE, stamped by the atlas as `StreamNode.not_fishing`) is taken out of every binding
#:     (`classify.apply_limits`, limit `not_fishing_water`).
#: 34: (T179, owner 2026-10-24) ONE CUT-PLACEMENT PASS: the atlas places every cut of a blue line together
#:     (`sectionizer._plan_blk`) — a gauge joins a name within 400 m on EITHER side, two authored cuts are
#:     never merged by distance, `at_edge` replaces the per-split pickup radii — so the sections a rule
#:     binds move. No classify.py outcome changed; the bump marks the binding diff.
#: 35: (T468, owner 2026-11-07) A LAKE ROW'S THROUGH-RIVER ABOVE THE LAKE IS ITS TRIBUTARY when the river
#:     has no row of its own (`tributaries.A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY`, retired at 52): Powell Lake's
#:     closure takes the Powell River above the lake. Depends on OTHER entries (`rowed_waters`), as 14 does.
#: 36: (T546) A ROW WHOSE SCOPE DOES NOT RESOLVE BINDS NOTHING: a scope extent that fails, or a scope placing no
#:     section, holds every rule of the row unresolved `scope_unresolved` (`build.A_FAILED_SCOPE_UNBINDS_THE_ROW`) —
#:     it used to bind unclipped (the whole water) or narrowed to the parts that resolved.
#: 37: (T665, owner 2026-11-11) A LINE THE ATLAS LISTS AS TIDAL WHOLE IS TIDAL WATER (`outside.A_LINE_THE_ATLAS_LISTS_INSIDE_
#:     IS_TIDAL`, owner `inside:<blk>`, from `inside_tidal.json`): 215 lines wholly inside the DFO tidal polygon leave
#:     every binding after the walk; the shore cut moves to the polygon exit (`atlas.splits.shore`), so the crossings
#:     below it change. No classify.py outcome changed; the bump marks the binding diff.
#: 38: (T679, owner 2026-11-12) D5: AT A DFO PLACE'S CONFLUENCE CUT THE JOINING WATER STAYS OUT unless DFO's words add it
#:     (`build.DFO_JOINING_WATER_STAYS_OUT`; every joining water used to go with the cut, a DFO place having no rows).
#:     D4: a word read that matched before and no longer matches stops the run (`build.A_WORD_READ_THAT_STOPS_MATCHING_
#:     STOPS_THE_RUN`, `reach.word_reads`). D1 (the river's widening pond is the river) is decided in the atlas registry
#:     (`registry.flowing.fold_widening_ponds`), so no classify.py outcome changed for it; the bump marks the diff.
#: 39: (T743, 2026-11-15) A DFO LAKE PLACE'S THROUGH-RIVER ABOVE THE LAKE STAYS OUT unless DFO's words name it
#:     (`build.DFO_THROUGH_RIVER_STAYS_OUT`, retired at 53; D5's reason applied to T468: a DFO place has no rows, so every through-river
#:     read as unrowed). Redsand Lake 2,639 -> 4 sections, Kitsumkalum Lake loses the 1,643 of the river above it; no
#:     place of the 11-13 reach run changes. No classify.py outcome changed; the bump marks the binding diff.
#: 40: (T753, owner 2026-11-15) A LAKE'S LOOP IS THE LAKE'S: a nameless line that leaves a lake and comes back into the
#:     same lake leaves its river's item and joins the lake's (`registry.lake_loops`, `LAKE_LOOPS_LEAVE_RIVER_ITEMS`,
#:     `LAKE_LOOPS_JOIN_THEIR_LAKE`; the build lists every move in `lake_loop_report.json`). Sproat's narrows take Sproat
#:     Lake's rows, not the lower river's. Decided in the atlas registry, so no classify.py outcome changed; the bump
#:     marks the binding diff.
#: 41: (T753/T743, 2026-11-15) A LOOP NEVER DIVIDES ITS LAKE: a lake loop joins its lake only when it is in exactly the
#:     lake's cut areas (`registry.lake_loops.LOOPS_NEVER_DIVIDE_A_LAKE`): Kotcho Lake's loop, outside the ecological
#:     reserve the lake is in, stays with the Kotcho River. A CONFLUENCE ANCHOR'S TRIBUTARY IS ITS LINE THAT MEETS THE
#:     PARENT (`splits.anchors.A_TRIBUTARY_IS_ITS_LINE_THAT_MEETS_THE_PARENT`): the Fraser's Seton River cut is built. Both
#:     decided in the atlas, so no classify.py outcome changed; the bump marks the binding diff.
#: 42: (2026-11-15, the Stikine part-name stop) A DEAD END OF A COLLECTED WATER IS COLLECTED: an unreached sink (a
#:     lake's second outlet that ends, a closed loop, an unconnected piece) whose watershed code a walk otherwise
#:     collected wholly goes with that water, with what feeds it (`tributaries.A_DEAD_END_GOES_WITH_ITS_WATER`).
#:     Buckley, Iskut, Chukachida, Mess (Stikine) and Horsefeed, Sutlahine (Taku) take their place's salmon whole.
#: 43: (T761, owner 2026-11-15) A DFO "REST OF" HAS ITS OWN BOUNDS AND ITS OWN LIST: a DFO place's rest is the rest of
#:     its `bounds` (`Extent.bounds`; `build.DFO_REST_HAS_ITS_OWN_BOUNDS`) less its LIVE, drawn listed places (its own
#:     `siblings`, else every other live place of the water: `entries.rest_listing`); a dormant listed place takes
#:     nothing (Nanaimo, Somass, Stamp), one that does not bind is skipped and named, never the whole rest
#:     `complement_unknown` (`build.DFO_REST_SKIPS_A_LISTED_PLACE_THAT_DOES_NOT_BIND`). A rule-book rest is unchanged.
#:     The listed places' sections inside the bounds ride the reach row (`behind`): the salmon chain answers them
#:     after the listed places ("areas and times", `deliver.salmon.rest_after`).
#: 44: (T778, 2026-11-15) A PIECE WHOSE WATER ENTERS THE WINDOW'S CUT LAKE IS JUST ABOVE THAT CUT: the flow path of
#:     a piece no neighbour places stops at a lake that is one end of the window (a lake-end cut) and takes "just
#:     above that cut", instead of walking through the lake to its far end
#:     (`extent.A_PIECE_INTO_THE_CUT_LAKE_IS_JUST_ABOVE_ITS_CUT`). The Shuswap's delta side channel 355998688:691
#:     into Mabel Lake is inside "between Shuswap Falls and Mabel Lake" (both DFO wordings), no longer straddling.
#: 45: (T798 item 1, owner 2026-11-16, RULINGS Li5a) A PART A DESIGNATION EXCEPTS AND WALKS PAST IS NO STOP: a water
#:     named in a designation's `tributary_excludes` with `walk_past` does not stop that designation's walk under
#:     Li5 (`licensing.A_WALKED_PAST_EXCEPTION_IS_NO_STOP`). Coal Creek above the old MF&M Railway bridge (190
#:     sections) is the Elk's Class II again; Line and Grave creeks (rows of their own, no walk_past) still drop.
#: 46: (T833, 2026-11-17) A CUT ON A LINE'S END IS ACROSS ONLY FROM THE LINES MEETING AT ITS POINT: the junction seed
#:     takes an edge of the end piece only when its junction is the cut's point (`extent.A_CUT_ON_A_LINE_END_IS_ACROSS_
#:     ONLY_AT_ITS_POINT`; "at" within `tolerances.STUB_M`, 100 m, as `bundle.continuation`), not every tributary and
#:     side channel along the piece. The Gitnadoix coho place upstream of the mouth alias binds the river's 1,000
#:     sections, not 991; the Chilliwack, joining 25.4 m below the Vedder Crossing Bridge cut on the Vedder line's top,
#:     stays across it. No classify.py outcome changed; the bump marks the binding diff.
#: 47: (T804 + T807, owner 2026-11-16) THE WATER AT A CONFLUENCE CUT IS THE ONE THAT BRINGS ITS NAMED WATER TO THE LINE
#:     (`build.THE_WATER_AT_A_CONFLUENCE_CUT_IS_THE_ONE_THAT_BRINGS_ITS_NAMED_WATER`): the Morice's "Gosnell Creek to
#:     Lamprey Creek" cut finds the Thautil River (Gosnell joins it 222 m up), which has a row of its own, so T9 keeps it
#:     and Gosnell Creek out of the stretch's two rules. And A DFO PLACE'S SIDE STREAMS ARE DFO'S WORDS FOR IT
#:     (`regs.dfo_salmon.side_streams`): its own line, else the exact heading it sits under, else river only; a saved
#:     value is a stated exception; the merged water's flag never decides (11 live places go river-only). And (T772) A
#:     THROUGH-RIVER THAT STAYS OUT OF A LAKE'S WALK STAYS OUT WITH ITS SIDE CHANNELS
#:     (`build.A_THROUGH_RIVER_STAYS_OUT_WITH_ITS_SIDE_CHANNELS`): Kitsumkalum Lake no longer takes the Kitsumkalum
#:     River's side channels above the lake.
#: 48: (T944, 2026-11-20) A DEAD END FLOWS WHERE ITS WATER FLOWS: a sink `A_DEAD_END_GOES_WITH_ITS_WATER` hands a walk
#:     goes on, for every pass that walks down from it, where the rest of its watershed code leaves that code
#:     (`tributaries.dead_end_outlets`, `A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS`), never "nothing before the sea".
#:     `licensing.first_classified_downstream` (RU-13) gives Shea Creek's dead end (773423) its water's answer —
#:     Gosnell Creek's row, no unit — instead of both the Bulkley's and the Morice's. No classify.py outcome changed;
#:     the bump marks the binding diff.
#: 49: (T973, owner 2026-11-20 12:10/12:11) A UNIT CARRIES THROUGH A ROWED NO-UNIT WATER, AS A "MAY NEED"
#:     (`licensing.A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT`): a section LI-2 drops (`own_row_not_classified`: the
#:     Endako under the Stellako, Gosnell Creek under the Morice) keeps, beside the placements, the unit of the nearest
#:     classified water below it with the rowed waters passed (`licensing_carried.jsonl`). The placements, the
#:     licensing tables and `licensing_digest` are unchanged by it. And ONLY A UNIT THAT REACHES ITS TRIBUTARIES CLAIMS
#:     THEM (`licensing.ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM`, owner 2026-11-20, default pending his confirmation):
#:     a walked section whose walk down first meets a designation printed for its own water only is passed through it
#:     (`unit_not_on_tributaries`) and keeps the nearer tributary-reaching unit below as a carried "may need" — this one
#:     moves placements (counted in T973). No classify.py outcome changed.
#: 50: (T1053, owner 2026-11-24 12:33 "Climb through every head lake") A WHOLE WATER'S WALK CLIMBS THROUGH THE LAKE IT
#:     FLOWS OUT OF (`tributaries.A_WHOLE_WATERS_WALK_CLIMBS_ITS_HEAD_LAKE`, `build.climbs_head_lakes`): a book rule whose
#:     every extent is `whole` enters its water's head lake as a tributary lake — the lake walked, never collected (T1),
#:     every stream feeding it the rule's (Eagle Creek's closure now takes Dick Creek above Dick Lake). A stretch rule, a
#:     DFO place and a licensing record stay as they were; each head lake is reported (`head_lake` diagnostic). No
#:     classify.py outcome changed; the bump marks the binding diff.
#: 51: (T1146, owner 2026-11-25 "Fix the binding in 0.10.0") A SIDE CHANNEL GOES WITH ITS WATER
#:     (`tributaries.A_SIDE_CHANNEL_GOES_WITH_ITS_WATER`): a walk that collects a stream item takes its no-main-flow
#:     pieces (not a barrier, not carved out, not own water) that touch only collected pieces of that item, and climbs
#:     on from them — the distributaries and bigger-order channels the upward walk never met (Forsyth, Koksilah,
#:     Halfway, MacMillan, Manson, Omineca, Shelagyote). No classify.py outcome changed; the bump marks the binding diff.
#: 52: (T1177, owner 2026-11-26 17:10 "everything in is a tributary and only shouldnt be if an exception is listed saying
#:     otherwise"; O2, O7) EVERY INFLOW OF A LAKE THE RULE BINDS IS ITS TRIBUTARY
#:     (`tributaries.EVERY_INFLOW_OF_A_RULES_LAKE_IS_ITS_TRIBUTARY`, replacing T468's
#:     `A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY`): the river on the lake's own line is taken whether or not it
#:     has a row of its own, and the walk climbs on through every lake above (Chehalis Lake's "No Fishing" closes the
#:     Chehalis River above the lake; Connor Lake's takes Forsyth Creek; the Campbell and Arrow reservoir chains climb).
#:     Only a printed exception keeps water out, and a lake row's printed exception also stops every walk through that
#:     lake (`build.A_LAKES_PRINTED_EXCEPTION_STOPS_EVERY_WALK_THROUGH_IT`, the narrower reading: Kinbasket's "Does not
#:     include Columbia River upstream" holds for Lake Revelstoke's and Lower Arrow Lake's walks). No classify.py outcome
#:     changed; the bump marks the binding diff. With it (review): a stop holds only from where a walk ENTERS its lake,
#:     and a lake at a stretch's top is no lake the rule binds (`build.A_LAKE_AT_A_STRETCHS_TOP_IS_NO_LAKE_THE_RULE_BINDS`).
#:     Round 2: a stop holds for the water a walk reaches THROUGH its lake, never for the rest of the walk, so the edge
#:     order cannot decide it (`tributaries.A_STOP_HOLDS_THROUGH_ITS_LAKE`); the book run's bindings are unchanged by it.
#: 53: (T1177, owner 2026-11-27 ~11:00, with the maps) WATER JOINING THE ROW'S OWN STREAM ABOVE ITS LAKE IS THE LAKE'S
#:     TRIBUTARY (`build.WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY`, Whiteswan: "Include
#:     them: No Fishing all year (Recommended)"): a lake-only reach's walk climbs PAST the entry's own stream flowing into
#:     its lake, collecting what joins it, never the own water. And A RIVER STRETCH IN A LAKE'S TRIBUTARIES ROW IS THE
#:     LAKE'S OWN UPPER END (`build.A_RIVER_STRETCH_IN_A_LAKE_ROW_IS_THE_LAKES_OWN_UPPER_END`, Upper Arrow: "yes ... this
#:     is what we do"): the river above the drawdown stretch's top (the Hwy 1 bridge) is the lake's through-river inflow,
#:     climbed to Kinbasket's printed exception. Classify outcomes change (Whiteswan's r5 gains the 14.0 km). 53 is the
#:     walk's (it merges first); the DFO branches that also set 53 move to 54. Review round 3 guards (the rule's words keep
#:     an own stream out; only a stretch on the lake's own river line is its upper end) change no corpus binding.
#: 54: (T1177, owner 2026-11-26: "unless you have exact dfo words saying what they mean by tributaries we should have a
#:     single tributary logic that is the saame across the board") ONE TRIBUTARY LOGIC FOR THE BOOK AND DFO
#:     (`build.ONE_TRIBUTARY_LOGIC_FOR_DFO_PLACES`, retiring T743's `DFO_THROUGH_RIVER_STAYS_OUT` and T1053's DFO
#:     refusal in `climbs_head_lakes`): DFO prints no definition of "tributaries", so a DFO place's lake takes its
#:     through-river and a whole-water place climbs its head lake as the book's rows do, and the lake rows' printed
#:     exceptions stop its walks too (`dfo_salmon.resolve_all(stops=)`). No classify.py outcome changed; the bump marks
#:     the DFO places' binding diff (on top of the walk's 53, which merges first).
#: 55: (T1250, owner 2026-11-27 "Pacific streams now, barriers in 0.11.0 (Recommended)"; RULINGS S14) A PACIFIC
#:     STREAM OF THE STEELHEAD REGIONS IS STEELHEAD WATER (`steelhead.PACIFIC_STREAMS_ARE_STEELHEAD_WATER`): every
#:     "possible" stream of Regions 1, 2, 3, 5, 6 whose FWA basin reaches the Pacific (`steelhead.pacific_basin`) is
#:     known (source "Pacific stream"), so a rainbow over 50 cm there is a steelhead where steelhead rules apply. The
#:     Arctic and Yukon drainage (basins 200, 800) stays "possible"; lakes stay out. Only the steelhead presence
#:     table and its digest change; no rule binding, licensing placement or classify.py outcome changed. On top of
#:     the DFO branch's 54.
POLICY_VERSION = "55"

