# T115 — One region membership: Region 5 rules bind on Fraser pieces whose home is Region 3

Status: DONE — lane-T111 commit "T115: one region membership". 12 Fraser pairs + 3 steelhead-twin pairs + 1 licensing pair removed.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding reach 1
(CONFIRMED in `rule_section.jsonl`).

## Decision being unified
"Which sections are in region R?"

## Where it is decided today
- `pipeline/regs/reach/extent.py:41` `area_sections` → `regions.in_region`: home-filtered.
- Raw polygon membership instead:
  - `pipeline/regs/reach/outside.py:140` `region_sections` → `region_limit`
  - `_membership`
  - the steelhead twin's `area_id` (`pipeline/regs/reach/build.py:326`)
  - `pipeline/regs/reach/extent.py:995` and `:1066`

How they differ: section `356364114:418405` (home R3, inside polygons 3 and 5) carries `r5:fraser_river@5-2` rules
r2/r3/r4. That is 12 rule–section pairs on 4 Fraser pieces getting another region's rules.

## Work
- One home-filtered `region_sections(regions, registry, graph)` in `pipeline/registry/regions.py`, plus
  `area_family_sections(reg, kind)` for the steelhead twin. Every place above calls them.
- Pin: a grep test that `region_limit` / raw polygon membership is read only inside `regions.py`.

## Acceptance
Proof is a reviewed per-rule binding diff from a side-dir reach run: exactly those 12 pairs removed, nothing else
(anything else is listed with names and why before promoting).

Rebuild: reach + deliver (no atlas). Depends: none (can share a reach rerun with T102/T111, diffs separate).
Size: S. Owner: no.

## As built (2026-10-23)

"Which sections are in region R?" now has one answer, in `pipeline/atlas/registry/regions.py` (the module that
already owned the home map):

- `region_members(registry, graph, key)` — one `area:region:*` item's sections through `in_region` (a stream piece
  straddling the line is its home region's; a lake is in every region it touches). `extent.area_sections` answers
  every `area:region:*` key with it.
- `region_sections(regions, registry, graph)` — the union for the named regions (`7` = 7A + 7B), cached on registry,
  graph AND the attached home map (re-`attach` is never served stale). `outside.region_limit(entry, registry, shared,
  *, graph)` calls it — the per-region row limit is now the zone rules' membership. `outside.region_sections` is
  deleted.
- `area_family_sections(registry, kind, graph)` — every `area:<kind>:*` item; the `region` family by home. Used by
  `Extent.area_kind`, `outside_area_kind` (extent), the steelhead twin's `outside_area_kind` (build;
  `_area_kind_sections` deleted) and `outside.outside_bc`'s "inside any region" (same union either way).
- `section_regions(registry, graph=None)` replaces `outside._membership`; `outside.water_regions(entry, registry,
  graph=None)` reads it. `region_items` / `zones_of` replace `outside._region_items` / `_zones_of`
  (`common.entry_id` calls `regions.zones_of`). `outside.REGION_PREFIX` is gone.
- The steelhead twin's `area_id` / `outside_area` resolve through `extent.area_sections` (home members for a region
  key) instead of the raw item.
- Policy flag `regions.ONE_REGION_MEMBERSHIP`; `cache.POLICY_VERSION` 24 → 25, policy pin updated.

Tests: `pipeline/tests/test_one_region_membership.py` — the Fraser fixture (a piece in polygons 3 and 5, home 3):
`region_limit` and `build_reach` for `r5:fraser_river@5-2` leave it out, a straddling lake stays in (MUTATION
`ONE_REGION_MEMBERSHIP=False` → the piece comes back); the row limit and the zone rule are the same set; families,
`section_regions`, `water_regions`, `outside_bc` read the same membership; no home map = plain membership; a
re-attached home map is not served from cache; NO SECOND COPY (AST scan of every `pipeline/atlas/reach/*.py` and
`pipeline/common/entry_id.py`: no `"area:region"` string outside docstrings, no bare `REGION_PREFIX`, no
`in_region` call, none of the deleted helpers defined — re-adding any fails). Updated: `test_region_homes.py` (the
"still limited by the polygons it touches" test now asserts the one membership: lake in both, a homed stream piece
not), `test_reach_limits.py` (`regions.region_sections`, `graph=`).

**Binding diff (side-dir reach run; code = base f57b94c1 + this ticket's code only, so the diff is T115's alone;
against `data/generated/reaches/full`): 12 rules + 1 licensing record: 15 rule–section pairs + 1 record pair, ALL
REMOVALS, all on the 4 Fraser pieces that straddle a region line.** `reach-T115.json` in the lane's scratchpad.

The 12 pairs the ticket names — each straddling Fraser piece now carries only its HOME region's Fraser row (as its
zone rules already did; its home row still binds it):

| piece | polygons | home | removed (another region's row) |
|---|---|---|---|
| 356364114:418405 | 3, 5 | 3 | `r5:fraser_river@5-2` r2, r3, r4 |
| 356364114:434930 | 3, 5 | 5 | `r3:fraser_river@3-14` r1, r4, r12 |
| 356364114:674465 | 5, 7A | 5 | `r7:fraser_river@7-9` r1, r2 |
| 356364114:699253 | 5, 7A | 7A | `r5:fraser_river@5-2` r2, r3, r4, r5 |

(3 + 3 + 2 + 4 = 12.) Anything else, named: **356364114:699253 also leaves the provincial steelhead twins
`zp:steelhead` r1b, r2b, r4b and the licensing record `steelhead_targeting_known`.** Why: a section is BOOK-KNOWN
steelhead water when a steelhead-printing row's rule binds it (AGENTS 54); it was known only through
`r5:fraser_river@5-2` binding it. Homed in Zone 7A, it is no longer that row's, so it drops from the book-known set
(73,103 → 73,102 sections; `steelhead_waters` diagnostics `known`/`kept` 16 → 15) and the twins, which bind
book-known waters the base leaves, lose it. Its `steelhead_presence` row goes from `r5:fraser_river`, `rules: true`,
`anadromous: true` to `curated list`, `rules: false`, `anadromous: false` — exactly AGENTS 54's stated reading of
"the Fraser in Zone 7A" (known, no steelhead rule). Diagnostics: the `region_clip` counts of the three Fraser rows rise
by the pieces removed. No unresolved change, no rescoped section, no gain anywhere.

Rebuild that ships it: reach + deliver (no atlas).

## Left / follow-ups
- `deliver.bundle.rules.regions_of_waters` calls `outside.water_regions(…, registry)` with NO graph, so it keeps
  plain polygon membership (unchanged output; G3's "the regions a water lies in"). Passing the attached graph would
  make it the one membership too, but it is a deliver-side behaviour change (which neighbouring region a printed
  exemption carries to) and was not measured here — a ticket of its own if wanted.
- `reach.licensing` reads the `national_parks` family itself (`f"area:{NATIONAL_PARKS}:"`); not a region question,
  left alone — it could call `regions.area_family_sections`.
- `pipeline/tools/unnamed_trib_sweep.py` keeps its own `area_sections` (a tool, reads its own build); not unified.

## Owner questions
- none (the extra 3 steelhead-twin pairs + 1 record follow from AGENTS 54 as written; default built: they go).
