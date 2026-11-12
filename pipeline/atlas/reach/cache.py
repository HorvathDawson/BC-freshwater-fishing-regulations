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
#:     has no row of its own (`tributaries.A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY`): Powell Lake's
#:     closure takes the Powell River above the lake. Depends on OTHER entries (`rowed_waters`), as 14 does.
#: 36: (T546) A ROW WHOSE SCOPE DOES NOT RESOLVE BINDS NOTHING: a scope extent that fails, or a scope placing no
#:     section, holds every rule of the row unresolved `scope_unresolved` (`build.A_FAILED_SCOPE_UNBINDS_THE_ROW`) —
#:     it used to bind unclipped (the whole water) or narrowed to the parts that resolved.
#: 37: (T665, owner 2026-11-11) A LINE THE ATLAS LISTS AS TIDAL WHOLE IS TIDAL WATER (`outside.A_LINE_THE_ATLAS_LISTS_INSIDE_
#:     IS_TIDAL`, owner `inside:<blk>`, from `inside_tidal.json`): 215 lines wholly inside the DFO tidal polygon leave
#:     every binding after the walk; the shore cut moves to the polygon exit (`atlas.splits.shore`), so the crossings
#:     below it change. No classify.py outcome changed; the bump marks the binding diff.
#: 38: (T679 #5, owner D1 / Q154 answered 2026-11-11) A RIVER'S UNNAMED WIDENING IS A STREAM (`extent.A_RIVERS_
#:     WIDENING_IS_A_STREAM`): a pond `river_widenings` takes for a stream item's whole course has water kind `stream`
#:     (`extent._kind_of`), so whole-river AND "in streams" rules both hold on it.
POLICY_VERSION = "38"

