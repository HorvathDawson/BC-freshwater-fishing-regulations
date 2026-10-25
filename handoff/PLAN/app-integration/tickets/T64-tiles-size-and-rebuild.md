# T64 — Tiles: size wins (S2) + the pending tile rebuild

Status: DONE — ebfbba7d, 05096efd (built and promoted with r-9b51c28ce0fe, 2026-10-25; the map look is on OWNER-TODO).
and the S2 easy size wins. Never replace the tiles dir: promote pmtiles + meta + layers/*.geojsonl file by file.
Tiles and bundle share a vintage (rebuild both or check the handle digest). Acceptance: size table before/after;
private land visible from z11; no basin seams; strict + smoke.

2026-10-22 (release size, not tiles): `store.common.train_zdict` is now best-of-8 sample offsets (keeps the dictionary
that packs the table smallest; deterministic). One offset let a 0.6% rows_row change swap the dictionary (regs-3 gz
319,894 → 427,578) and refuse the release at 14,027,828 B; same answers now pack to 13,844,550 B gz (gate 14,000,000).
Store step 8.4 s → ~31 s. Pinned in `test_regs_store.py` (two-build identity + a 0.6% append swings < 3%, old rule +9%).

2026-10-23 (release size, the dictionary trainer again): the T128/T103/T104/T127 answers (sha 821bc30d) were refused at
14,010,163 B (regs-3 319,894 → 450,423). REAL content cost, packed with the live release's own dictionaries
(r-36fbc5326506): old answers 13,844,550 (= live, exact) → new 13,859,022, **+14,472 B**. The other +151 KB was all
`rows_row`'s dictionary: its 8 candidates (6 whole rows each) all packed it to ≥1,044,024 B where the live one did
890,072. Fix (`store.common.train_zdict`): prescore every sample offset (≤512) on every 16th blob, and score the first 8
plus the prescore's top 4 on the whole table (the first 8 always stay finalists, so never worse than best-of-8); a
region-shard table's rows are weighted by how many shards ship them (`release.shards.row_regions`, the release's own
cut — pinned equal to `shard_rows`). Weighting changes nothing on today's data (every shard row ships, ~1.02 copies);
it is there so a table whose bulk ships nowhere is not trained on it. Result: new answers **13,843,064** (−156,936
under; regs-3 319,120), live answers 13,817,723 (content cost under the new trainer +25,341). Store 30 s → ~60 s,
release 9 s. Q55 (raise the gate / drop private land from access) is not forced by this change; still the owner's.

## Owner addition (2026-10-23): drop unnamed wetlands with no direct regulation
"in the tile optimization ticket we could consider removing all unnamed wetlands without direct regulation? (including
dropping ones only regulated by admin area regs like liard) this might reduce size a bit".
Scale: the FWA wetlands layer has **375,178 polygons, 77 named** (gpkg `wetlands`) — most of them carry no rule of their
own, only zone rules or an area entry (e.g. "Liard River watershed").
- KEEP (never drop): named wetlands; any wetland that is part of a stream item (sloughs/side channels folded by
  flowing.py, T55/T149 folds); one a water's own row or an extent binds; one with a gauge (lake_gauge); wetlands a stream line runs through that the walk/panels need as a node (keep the GRAPH,
  drop only the DRAWN polygon — the atlas and reach are unchanged; this is a tiles-only cut).
- DROP from the tiles: every other wetland polygon — unnamed, and bound only by zone rules or area-only entries
  (watershed / admin-area rules like the Liard). That includes unnamed wetlands inside no-access land (owner
  2026-10-23: "this could get dropped if it has no regs") — the no-access LAND fill (`noaccess.fill`) still shows the
  area, so the wetland adds nothing. They are still covered by those rules; the app answers a tap there
  from the land/area, not a wetland feature.
- Measure first: wetland layer bytes in atlas.pmtiles by zoom, how many polygons each KEEP class retains, and the size
  win; a before/after shot of a wetland-heavy area (Fort Nelson, the Chilcotin) so the map still reads (a muted wetland
  fill from the basemap's landuse layer can stand in visually if the look suffers).
- Status colours: a dropped wetland never had a status of its own; check no section's colour disappears (the drop list
  holds no section a status index row colours).

## Outcome
Code done 2026-10-24 (tiles window); the build, the shots and the after-column are the owner's run. Size table:
`handoff/PLAN/app-data/S2-tile-size-easy-wins.md` "Measured 2026-10-24".
- Already in code before this window, and shipping with this build: parcels from z11, basins as one 100 m coverage.
- **Unnamed wetlands** (owner addition): `tiles.export.wetland_is_drawn` keeps a wetland in a registry item (named,
  or folded into a river: a slough/side channel), a stream-kind polygon, or one with a gazetted name; every other
  wetland is left out of the tiles and listed in `layers/undrawn.json` (counts in `atlas.meta.json` `undrawn`). The
  graph, atlas and reach are untouched. Measured on the live bundle: 333,526 wetland features (111.1 MB of the 548 MB
  atlas); 333,471 go (332,937 zone-only, 460 area-row-only — Liard watershed etc., 74 with no rule set), 55 stay
  (54 wetland items, 1 folded into a stream). No wetland is bound by a water row (reach or walk) or gauged today.
  26,079 of those dropped carry `section_access` rows (no-access land); the land fill still draws there.
- **The gate:** `bundle.undrawn.check` (in the bundle build, after access) refuses a bundle in which an undrawn section
  is in an item, gauged, or bound by a water row — so a later data release that binds one asks for a tile rebuild
  instead of drawing nothing. Note: the 460 area-row-only wetlands DID carry the status index's "own regulations"
  colour (an area row counts as own); dropping them is what the owner asked ("including ... admin area regs like liard").
- Tests: `test_wetland_drop.py` (keep classes, the undrawn list, zone/area may go unseen, water row/item/gauge refuse,
  the sidecar/list pairing).
- Left for the build: wetland-heavy shots (Fort Nelson, the Chilcotin), z11 private land, basin seams, strict + smoke.
  If the look suffers, the basemap keeps OSM `wetland` landuse (T85 filter) as a stand-in one style layer away.

## Outcome — DONE (2026-10-25, built and promoted with r-9b51c28ce0fe)
- After: atlas.pmtiles 548.3 -> 228.2 MB, basemap.pmtiles 918.4 -> 875.8 MB (T85 alongside); `undrawn` 333,471 wetlands.
- Knock-on (expected): access is measured on the tiles' own sections, so undrawn wetlands carry no `section_access` rows —
  no-access rows over 50% 4,028 -> 3,603 (Camp Chilcoten 1,052 -> 743, Greater Victoria WSA 590 -> 492, the three
  watersheds -13). `test_section_access` re-pinned (>= 3,423, 95%); the per-watershed floors still hold.
- Left with the owner (OWNER-TODO "Map look"): the wetland-heavy shots (Fort Nelson, the Chilcotin).
