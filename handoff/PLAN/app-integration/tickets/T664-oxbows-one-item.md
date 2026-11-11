# T664 — Okanagan River Oxbows: one water, not 31 items

Status: PARTIAL — 25471b393; left (Claude): the one side build per `handoff/PLAN/app-integration/next-rebuild.md` (with T665 + the nine DFO cuts): matched restamp, the in-season reference patch, reach + binding diff (oxbow rule 89 = 89), promote

Source: owner 2026-11-11: "the oxbows should be combined as one item. Right now a shit load come up in the search etc..
when they are the same with same rules".

Since T637 each oxbow channel is its own registry item (31 `wsc:` items, all named "Okanagan River Oxbows", same rule).
Search and the map list each one.

Want: one "Okanagan River Oxbows" item holding every channel (one search hit, one card, one name on the map), bound by
the one rule as today.

Notes for whoever takes it:
- The registry groups by watershed code; a curated group across codes is needed (look for an existing mechanism before
  adding one: `name_variants.json`, `book_name_links.json`, the merge report).
- Needs an atlas rebuild, a restamp of `r8:okanagan_river_oxbows@8-1`, a reach run and binding diff (the rule must bind
  the same sections as build 27: 89).
- Ask before merging any other same-name group on the same pattern; this ticket is the Oxbows only.
- Not in build 27 (owner did not hold the release for it); rides the next atlas rebuild, with the nine waiting DFO cuts.

## Built 2026-11-11 (25471b393), not yet in an atlas
- Mechanism: a `name_variants.json` name record may carry `"one_water": true` (set on the T637 oxbow record). The
  registry pass `pipeline/atlas/registry/one_water.py` (`merge_one_waters`, run in `atlas/build.py` after the MUs and in
  `atlas/sidecars.py`) makes every STREAM item of that name one item: the first line item by id survives, and absorbed
  ids go into its `aliases` and `ref_ids` (rule 55's three read points map them, everything else refuses them). A lake
  edge onto its own polygon is dropped; one carrying a cut alias is refused; a name in two regions is refused.
  Tests: `pipeline/tests/test_one_water.py` (8).
- Measured in memory on the live atlas (no write): 21,331 → 21,287 items; 45 → 1 (`wsc:300-432687-459615`, 80
  sections; 30 line items + 14 polygon items absorbed); no boundary dropped; `okanagan_river_oxbows.r1` live 89 = the
  merged 80 + 8 still-water polygons + `lake:329461947` (river widening): the same set.
- Curated ids to move (patch `T664-pending/t664-references.patch`): `r8:okanagan_river_oxbows@8-1` `matched` 53 → 9
  (`backfill_matched --restamp` makes the same change on the new registry), in-season `is-bb5426c61ff3` 53 → 9
  extents. No DFO, gauge, cut or book-link reference names an absorbed id.
- Owner calls (defaults taken): (1) the 8 LAKE/WETLAND oxbow items stay apart (search: 1 + 8). Folding them in makes
  them streams and moves zone rules (`ONE_WATER_TAKES_STILL_WATER`). (2) The survivor id is the first line item.
- Not verified: the build itself, search and card in the app, tile labels.
