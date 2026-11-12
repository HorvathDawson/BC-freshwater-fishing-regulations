# T664 — Okanagan River Oxbows: one water, not 31 items

Status: PARTIAL — 25471b393, a3518f5d2, 56b18e8bb; left (Claude): promote the side build (`handoff/PLAN/app-integration/next-rebuild.md` steps 7 to 9) after the owner's go on `next-rebuild-results.md`

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

## Side build 2026-11-11 (the rebuild lane; `app-integration/next-rebuild-results.md` §3)
- Owner ruling 2026-11-11: the 8 lake / wetland oxbow polygons are streams ("They should have been converted to stream
  polygons like Nicomen Slough so it all becomes one. They aren't lakes, they are considered streams."). Built as a
  list by id on the name record (`one_water_takes`, `registry.one_water`; proposal patch
  `T664-pending/oxbow-still-water.name_variants.patch`, applied on the side copy only). The blanket switch stays off.
- Measured on the side atlas: ONE item `wsc:300-432687-459615`, kind stream, 88 sections (52 items absorbed).
  `okanagan_river_oxbows.r1` 89 → 89, the same sections. On the 8 polygons the lake rules come off the two lakes
  (`zp:bait` r5, `zp:barbless_single_hook_streams` r2) and the stream rules go on all eight, the Region 8 spring stream
  closure among them, with the oxbow row's exemption bound there as on the rest of the water (7 of 8 carry exactly a
  line section's rule set; the eighth keeps one area closure it already had).
- `lake:329461947` (unnamed polygon on an oxbow channel) is left a lake: owner item in the results file.
- References: `matched` 53 → 1 and in-season `is-bb5426c61ff3` 53 → 1 extent (`T664-pending/t664-references.patch`,
  remade from the side build; `resync_notice.py` makes the notice half). `atlas.promote` now refuses while a curated
  file names an absorbed id.
- Not verified: search, card and label in the app (no tiles / bundle built on the side atlas).
