# T228 — No-access land and salmon both break a water's pieces; dormant gauges attach to nothing

Status: DONE (a757756b)

Source: owner 2026-10-28. (1) Coquitlam River: "why does coquitlam not separate the section in no access and the section
below the no access? when navigating sections they got combined. no access should not allow adjacent sections to
merge." Add-on, same day: "adjacent sections are merged in the front end only when BOTH the standard regulations AND
the salmon regulations match. Salmon comes from the live DFO feed, so the grouping changes with it, with no release
rebuild." (2) Whonnock Lake 08MH100 (discontinued 1964, no real-time data): keep it MATCHED but use it nowhere. Make that
a general state. (3) Beattie Lake / Q87: a "Trout" quota in an entry that does not mention char separately means trout
AND char.

## 1. No access is a hard break (bundle `part`)
- `part.no_access` (0/1, last in the UNIQUE key and the order key): a section MOSTLY inside no-access land
  (`access.mostly_inside`: no-access permille summed > 500, the app's `MOSTLY_INSIDE`, pinned by
  `test_mostly_inside_is_the_apps`) never shares a part with one outside. `section_access` is now written before the
  derived tables (`bundle/build.py`).
- Measured on the rebuilt bundle against live r-a871447c48ba: 34 waters gain a part (20,475 → 20,509 parts): Coquitlam,
  Capilano, Seymour, Goldstream, Jordan, Leech, West Leech, Buntzen, Noons, Slesse, Salmon River and 23 creeks. 125 waters
  have a no-access part (243 sections in them). **Bindings unchanged**: every section's rule set (1,956,935) and
  licensing set are byte-equal. The reach run was not re-run, so `binding_diff` has nothing to compare.
- Coquitlam: Lougheed Highway Bridge → Coquitlam Watershed boundary (1 section) is now its own part. The no-access part
  (9 sections) sits above it, and the mark between them is labelled "Coquitlam Watershed boundary".
- The answers picker still folds every closed-all-year part into one choice ("Closed all year · N stretches"). That is
  a picker group, not the map's navigation, and it is unchanged.

## 1b. Salmon also breaks, live (bundle `part_stretch` + `@app/core` `mergeNav`)
- Pipeline (`topology.by_chain`): each part's stretch is cut wherever the DFO salmon chain changes
  (`salmon_section.chain`). Two pieces of one part meet at a `part_mark` labelled with the cut's split name, else unnamed.
  The salmon tables are now written before the part topology. A side channel with no `section_touch` goes with its
  chain's main-stem piece.
- App (`mergeNav`): two pieces of one part join when a mark joins them AND their sections read the same salmon year
  (`salmonKeyOf`: the chain's species verdicts, gear lines and map status, from `loader.salmon`, which `withLiveSalmon`
  lays over the release). A feed change re-groups the pieces in memory, with no new release. With no salmon read, every
  piece of a part joins, which gives the pre-T228 stretches. `useSalmonKey` (ui) reads the water's regions' salmon
  slices. `WaterMapView` passes the key to `useWaterMap`. An unnamed piece boundary reads "Salmon rules change here".
- Tests: `waterNav.merge.test.ts` (a feed opening salmon on the middle piece splits it out; reverting joins it back;
  no-access part never joined), `test_part_geo.py::test_t228_*`, `test_derived_tables.py::test_no_access_*`.
- T227 (salmon lane) adds `salmonTimelineKey` to core with the same three fields. Once it is committed,
  `salmonKeyOf` should call it.

## 1c. Follow-up (owner 2026-10-28): the key is the regulations, and every cut is named
- One helper: `@app/core` `salmonTimelineKey` (T227's, extended). It compares each species' verdict AND its rule
  content (DFO's rule lines and notices: limits, sizes, hatchery-marked or wild, gear and method conditions, windows),
  normalised for case and spacing, plus the gear lines and the map status. It resolves the file's indexes to content,
  so "coho 2 a day" and "coho 4 a day, hatchery-marked only" split even though both are open. The ui's own copy is gone.
  Tests in `waterNav.merge.test.ts`: a limit difference splits, hatchery-only vs any splits, identical content joins
  (across chains, with case or spacing apart), a feed change re-groups.
- Every boundary is a named split (owner 2026-10-28). The build cuts a part only where the salmon chain changes AT a
  named split (`topology.cut_between`). Where the chain changes with no split there, the build neither cuts nor names
  the point: it is a DATA GAP, a DFO place edge not yet placed as a split. Each gap goes into the bundle table
  `salmon_edge_gap`, is printed by the build, and is put on the review's DFO queue by `pipeline.regs.dfo_salmon.edge_gaps`
  (into `dfo-suggestions.json`; `suggest.run` folds it in too). "Salmon rules change here" and "Edge of DFO area" are gone.
- The 6 unnamed cuts, by cause:
  - **Atnarko**: Bella Coola River "(not including Atnarko River)" has tributaries=true, and its walk covers the
    Atnarko's 38 main-stem sections; it missed only 2 side channels. This was a data defect, not a missing split. The
    queue now has an extent-fix item: add gnis:17209 to the Bella Coola place's `tributary_excludes`.
  - **Tomahnous Creek**: "Tatshenshini … including tributaries" covered the creek's stem but not its side channel. The
    side channel was missed by the placement, so the place now covers the whole water. Rule (`salmon.carry_side_channels`):
    a water-bound place that covers every main-stem section of a water also covers that water's side channels. No cut.
  - **Klinaklini** (3 cuts, 1 gap): the 5B catch-all is bound to the MU 5-6 to 5-11 polygon, and the river leaves MU
    5-6 into 5-12. That edge is a real line. The queue item carries a proposed new split, "Management Units 5-6 to 5-11
    boundary", at (-124.99676, 51.978791) on gnis:3273. Until it is placed, no cut there.
  - **Tusulko**: the same MU-group edge, but a named lake split stands there, so it is cut and named by that split.
  - After the change the build reports exactly **1 gap** (Klinaklini). A first try (carrying side channels for every
    water-bound place) created 3 new gaps (Morice ×2, Skeena) because partial places crept past their own cuts. It is
    now limited to waters the place covers whole.
- A piece is named by its own ends ("Mouth → white triangle boundary sign …"), never "stretch 1 of 2". Shot:
  `T228-shots/cowichan-sandy-pool-cut.png`.

## 2. Matched but not used (`liveness.dormant`)
- A station that is neither ECCC-active nor on the roster's real-time snapshot stays in `matches.json` and keeps its
  `gauge` row (`matched_by`, `match_m`, `mag`). Its `gauge.item_id` and `sid` are NULL, and it gets no `lake_gauge`,
  `section_gauge`, donor-panel or basin row.
- Counts: 1,618 of 2,081 matched stations are dormant (1,601 of them placed on this graph). Rebuilt bundle vs live:
  lake_gauge 179 → 31, panel_member 15,238 → 4,316, section_panel 248,800 → 124,006 (those sections' donors were all
  dormant, so they could only ever say "not a real-time gauge"), basin_member 1,954 → 459, gauge rows attached to a
  water 2,032 → 453. section_gauge is unchanged (107,799): sheds already came from active stations only.

## 3. Trout includes char (RULINGS C4)
- The catalogue already does this. Of 116 counted bare-"trout" quotas, 108 hold char. The 8 that exclude char are all in
  entries that name char with a related rule of their own: Region 1's table ×4, Region 6's table, Ross Lake 2-2,
  Flathead River and its S tributaries 4-1. No water reads differently. `trout_scope_problems` refuses a bare "trout"
  line that drops char with no char rule in its row.

## Commits
a757756b (code + tests). Release r-c331a7a3c2cf promoted (data/release/LATEST). Owner re-cuts: OWNER-TODO.
