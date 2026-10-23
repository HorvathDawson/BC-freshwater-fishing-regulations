# T140 — Bundle regions of a water + area families ask the one region membership (G3)

Status: DONE — b08d32f2. Measured: no bundle row changes today (latent defect closed).

Source: T115's leftovers (owner Q99, 2026-10-23: "yes, as T140 with a measured diff").

## Decision being unified
"Which sections are in region R?" / "which sections are in area family K?" — one answer,
`pipeline/atlas/registry/regions.py` (T115).

## Where it was decided outside that module
- `deliver.bundle.rules.regions_of_waters` called `outside.water_regions(…, registry)` with NO graph, so it read
  plain polygon membership: a stream piece straddling a region line counted as being in both regions. This is G3's
  "the regions a water lies in", which decides which neighbouring region a printed exemption carries to
  (`equivalent_regions`).
- `reach.licensing.national_park_sections` read the `area:national_parks:` family itself.
- `deliver.bundle.licensing` (`province_except`) read the `area:{kind}:` family itself.

## As built (2026-10-23)
- `regions.HomeView`, `regions.home_view(graph)` and `regions.home_view_from(build_dir, registry)` — the home map
  without the graph. The bundle build already attaches the home map in `_water_places`. It now keeps
  `home_view(graph)` before the 5 GB graph is dropped and hands it to `rules.write(home=)`. `home_view` raises when
  no map is attached, because a view of nothing would silently be plain membership. `rules.write` without `home`
  (unit fixtures, direct callers) reads it from `build_dir` through `home_view_from`.
- `rules.regions_of_waters(registry, docs, home)` passes the view to `outside.water_regions` and so to
  `regions.section_regions`: a straddling stream piece is in its home region, and a lake is in both. Policy flag
  `rules.WATER_REGIONS_BY_HOME`.
- `reach.licensing.national_park_sections` returns `regions.area_family_sections(registry, NATIONAL_PARKS) or
  frozenset()`. `bundle.licensing` `province_except` reads `regions.area_family_sections(registry, kind)`.
- No `cache.POLICY_VERSION` bump: no reach outcome changes (the reach-side edit is a refactor that gives the same
  set).

Tests: `pipeline/tests/test_t140_water_regions_by_home.py`. They are whole-value tests:
- A creek whose only Region 5 presence is a piece homed in Region 3 lies in Region 3 only.
- A straddling lake lies in both regions.
- G3 end to end through `equivalent_regions`.
- The view answers the same as the graph for `region_sections` and `section_regions`.
- A view of a graph with no home map is refused.
- The area families come from the one reader.
- Wiring: `bundle.build` calls `_rules.write(…, home=)`, and `regions_of_waters` is called with the home.
- No second copy: an AST scan of `atlas/reach/*.py` and `deliver/bundle/*.py` finds no `f"area:{kind}:"` family
  prefix.

Mutations, each run:
- `WATER_REGIONS_BY_HOME=False` fails `test_a_straddling_piece_homed_elsewhere_is_not_a_region_of_the_water` and
  `test_the_lift_does_not_carry_into_a_region_the_water_only_straddles_into`.
- Re-adding `f"area:{NATIONAL_PARKS}:"` to `reach/licensing.py` fails
  `test_no_second_copy_of_an_area_family[reach/licensing.py]`.

**Measured diff.** Scratch bundle built with this branch's code from the live atlas + live reach run
(`reaches/full` digest `b9be92114323aa22`), diffed table by table against the live `bundle.sqlite`
(release r-941dcc149b17):
- Every table that T140 touches is row-identical: `rule` (its `exempts` regions), `province_except` and the
  licensing tables.
- The only changed rows were T121's: `section_gauge` / `section_down`, +18 each.

Why nothing moved, measured on the live atlas:
- 58 sections change membership under the home map, 46 of them on named waters.
- Each of those waters also lies in the other region through whole pieces.
- So of 1,367 water rows, **0** have a different region set, and no exemption carries differently.
- The defect was latent. A water reaching a region ONLY through a straddling piece homed elsewhere would have
  carried its lift there.

Rebuild that ships it: deliver (no reach, no atlas). It changes no output today.

## Left / follow-ups
- `pipeline/tools/unnamed_trib_sweep.py` keeps its own `area_sections` (a tool that reads its own build), carried
  over from T115.
- `place_names` / `read` / `export_ui_rules` classify `area:region:` ids by string. That is id parsing, not
  membership, so it was left alone.

## Owner questions
- none.
