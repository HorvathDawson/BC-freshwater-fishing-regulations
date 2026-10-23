# T121 — Water kind in reach and gauges: read `water_kind.kind_of`, not the graph `NodeKind`

Status: DONE — c5ce4298, 48869e83 (adversarial review: wiring pinned). Reach: no binding moved; bundle: +18 gauged sections (river polygons and sloughs).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings reach 8 and
the Python half of cross 8 (PLAUSIBLE).

## Decision being unified
"Is this water a stream, lake or wetland?" (already one function, `water_kind.kind_of`, pinned by
`test_one_water_kind` for other packages).

## Where it is decided today
- `pipeline/regs/reach/build.py:839,845` (stops at a river's own polygon).
- `pipeline/regs/reach/extent.py:156,1191`.
- `pipeline/gauges/consume/shed.py:153` `is_lake_station`.
- `build_panels.py:89` (that part is T55's; do not duplicate it).

How they differ: the graph's `NodeKind` calls a river's own polygon and sloughs/canals a lake, while `kind_of` follows
the curated kind. A walk can stop, or a station be treated as a lake station, where the page calls the water a stream.

## Work
- Call `water_kind.kind_of` at each place; extend `test_one_water_kind`'s pin to `pipeline/regs/reach/` and
  `pipeline/gauges/`.

## Acceptance
Pure refactor where the kinds agree: per-rule binding diff and gauge shed diff, both reviewed (expected small; each
change named).

Rebuild: reach + deliver. Depends: T55 for `build_panels.py`. Size: S. Owner: no.

## As built (2026-10-23)
The water kind is the registry's answer (`reach.water_kind.kind_of`, through `extent._kind_of`) at every place
the ticket lists. (The paths are `pipeline/atlas/reach/…`; the ticket's `pipeline/regs/reach/` is the old name.)

- `build._stem_to_first_lake(graph, mouths, registry)`: RU-14's stem stops at the joining water's first LAKE by
  the registry's kind, so a river's own polygon or a slough on the stem is the river going on. The caller in
  `_expander` passes the registry. `NodeKind` is no longer imported by `build.py`.
- `extent._watershed_part(…, reg)`: the river's code is read from the pieces the registry calls stream, and so are
  the "touching nothing" lakes placed by position.
- `shed.is_lake_station(graph, node_id, registry=None)`: standing water is `kind_of(...) in STANDING_WATER`
  (`("lake", "wetland")`, as `regions.lakes_among` reads it). `build_gauge_sheds(…, registry=)` and
  `lake_gauge_links(…, registry)` pass the registry through.
- The bundle hands both the registry's kinds from `_kind_registry(registry_items)`. That is one helper, also used by
  `_water_places`, which built the same namespace inline.
- With `registry=None` (fixtures), `kind_of` reads the node's kind, as it always did.
- `build_panels.py:89` is left to T55, as the ticket says.

Tests: `pipeline/tests/test_t121_water_kind_in_reach_and_gauges.py`.
- The stem runs on through the river's own polygon and stops at a real lake.
- A station on a river polygon is a river station: it sheds over the river and is not lake-linked.
- A real lake's station is lake-linked only, and a wetland is standing water.
- GATE (the extension of `test_one_water_kind`'s pin to `pipeline/atlas/reach/` and `pipeline/gauges/`): no
  `NodeKind`, no `n.kind` / `src.kind` / `node.kind` / `nodes[…].kind` / `nodes.get(…).kind`, and no
  `endswith("lake")` outside `water_kind.py`.

Mutations, each run:
- The stem reading the node's kind fails `test_the_stem_runs_on_through_the_river_s_own_polygon`.
- `is_lake_station` ignoring the registry fails `test_a_station_on_a_river_polygon_is_a_river_station` and
  `test_a_river_polygon_station_sheds_like_a_river_station`.

Updated: `test_reach_confluence_cuts.py`, for the monkeypatched `_stem_to_first_lake` signature.

**Per-rule binding diff.** This was measured in the combined T141 + T121 scratch reach run against
`data/generated/reaches/full` (`b9be92114323aa22`). **No binding moved**, and the run digest is identical (see
T141). Why, from an audit of the live atlas:
- 146 sections have a water kind that differs from their node's. All of them are polygons (`NodeKind.lake`)
  owned by a STREAM item: river polygons and sloughs.
- Not one of them carries a `continuation` edge. Their edges are only `lake_in` (251) and `lake_out` (154), so the
  stem walk, which follows `continuation` only, met none of them.
- The 8 watershed extents read the same river code either way.

**Gauge shed diff.** A scratch bundle was built with this branch's code from the live atlas + live reach run and
diffed against the live `bundle.sqlite` (r-941dcc149b17). The only changes are `section_gauge` +18 rows and
`section_down` +18 rows; nothing was removed and `lake_gauge` is unchanged. Every added row is a polygon section
(`section_span.shape` = 1) of a stream item. The walk used to skip these as "a lake's level"; they now take their
river's gauge:

| water (item) | sections | station | trust |
|---|---|---|---|
| Nation River (gnis:7893) | 4 | 07ED001 Nation R. near Fort St. James | 3 weak, 1 fair |
| West Road (Blackwater) River (gnis:26104) | 3 | 08KG001 West Road R. near Cinema | 2 weak, 1 good |
| Blind Slough (gnis:13674) | 2 | 08MH147 Stave R. above Stave Lake | weak |
| Catbird Slough (gnis:37665), Sturgeon Slough (gnis:16259) | 1 each | 08MH035 Pitt R. near Port Coquitlam | weak |
| Bowman Slough (wbk:329103932) | 1 | 07EE010 Pack R. at outlet of McLeod Lake | fair |
| Tupper River (gnis:29883) | 1 | 07FD007 Pouce Coupe R. below Henderson Cr. | good |
| Stellako River (gnis:7836) | 1 | 08JB003 Nautley R. near Fort Fraser | good |
| Alouette River (gnis:9630) | 1 | 08MH005 Alouette R. near Haney | good |
| Vedder River (gnis:3062) — the Vedder Canal's polygon | 1 | 08MH024 Fraser R. at Mission | weak |
| Sumas River (gnis:26216) | 1 | 08MH029 Sumas R. near Huntingdon | good |
| Honna River (gnis:33832) | 1 | 08OA004 Honna R. near the mouth | fair |

No station changed side: no station sits on a river polygon in the frozen match, so `lake_gauge` is identical.

Rebuild that ships it: the gauge rows ship at the next deliver. The reach code ships with any reach rerun and
changes no binding today.

## Left / follow-ups
- `pipeline/atlas/gauges/build_panels.py:89` still reads the node's kind. It is T55's to change, as the ticket
  says.
- 08MH024 (Fraser at Mission) now speaks, weakly, for the Vedder Canal's polygon. That follows from the walk and
  the drainage test (the canal drains through Mission), and it is weak by the band. It is listed in case the owner
  wants the Vedder's own gauge to win there. That is T105/T76 territory (one gauge speaker), not this ticket's.

## Adversarial review (2026-10-23) — 48869e83
**Defect: the change that ships was not pinned by any test.** Each reader takes the registry as an optional argument,
and `None` means the node's shape. So a production caller that forgot it would silently read the shape again, and
the unit tests (which pass the registry themselves) would stay green. Each of these mutations was run, and each
failed **no** test:
- dropping `registry=kinds` from `bundle.build._gauges`' `build_gauge_sheds` call (this takes the 18 gauge rows back
  out);
- dropping `kinds` from its `lake_gauge_links` call;
- dropping `registry` from `_expander`'s `_stem_to_first_lake` call;
- dropping `reg` from `resolve_extent`'s `_watershed_part` call (the watershed half had no behavioural test at all).

Fixed with `test_every_production_reader_of_the_water_kind_is_handed_the_registry`. It is an AST scan of every
non-test module under `pipeline/`. Each call of these functions must pass a registry argument that is not the
literal `None`: `_stem_to_first_lake`, `_watershed_part`, `is_lake_station`, `build_gauge_sheds`, `lake_gauge_links`.
The scan also asserts that it sees the five production callers. Each of the four mutations above now fails it. So
does `shed.py`'s walk calling `is_lake_station(graph, sec)` without the registry, which also fails
`test_a_river_polygon_station_sheds_like_a_river_station`.

Also checked, with no change needed:
- **The gate's reach.** It found no other node-kind read in `atlas/reach` or `gauges`.
  - `steelhead._kind` reads an ITEM's kind. `steelhead.kind()` asks `_kind_of`.
  - `gauges/consume/place.py` skips `lake:` nodes when projecting a coordinate onto a line. That is the shape (a
    point can only be projected onto a line), not the water kind.
- **A wbk-matched (level) station that would now be called a river.** All 196 `wbk` matches in
  `data/curated/gauges/matches.json` were checked against the registry. No stream item owns any of their
  polygons, so `lake_gauge` cannot change. The table was identical in the builder's diff, which agrees.
- **The bundle rows.** Spot-checked in the live bundle. Every polygon (`shape` = 1) section of Nation (4), West Road
  (4), Stellako, Vedder, Blind Slough (2) and Alouette has no `section_gauge` row today, while their line sections
  carry the stations the table above names. That matches the +18. West Road shows 4 polygons against 3 added rows;
  this review did not check why the fourth gains no row.

The scratch reach run of this review (with the T141 fix) is in T141's review: the binding diff is empty. No bundle
was rebuilt, because the fix is tests only.

## Owner questions
- Standing water for a gauge station is `lake` OR `wetland` (default built). Before, only a lake-shaped node
  counted. No live station sits on a wetland, so nothing changed.
