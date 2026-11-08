# T493 — A creek shows a gap where its small lake drops out zoomed out

Status: IN REVIEW — slough fix `fill_zooms` (41bf8e309, 0031f1997) + side build + shots done; left: owner look at the shots, ships with the next tiles rebuild (on T497's atlas). Was: PARTIAL (reopened 2026-11-08) — owner phone: Nicomen / Camp Slough still gap; the 8494959a2 link covers lakes only. Owner 2026-11-07 (Foley Lake on Foley Creek).

Zoomed out, a small lake leaves the tiles before its creek does. The creek's line stops at the lake's shore and starts
again on the far side, so the creek looks broken.

## Owner decisions (2026-11-07)
- **Fix:** at zooms where the lake is hidden, draw the creek's line through the lake (along the lake's own centre
  line / the FWA connector through the polygon). At its usual zoom the lake is drawn as it is now.
- **Colour:** that stretch takes the LAKE's status (it is the lake's water), not the creek's.
- **Tap:** a tap on that stretch opens the lake.

## Do
- Find the lake's min zoom in the tile build and where the stream layer drops the in-lake connector (FWA's
  through-lake lines). Count how many lakes on a drawn stream are hidden at each zoom (z8–z12) to size it.
- Ship the connector as a stream-layer feature from the lake's min zoom down, carrying the lake's item (its
  status key and tap target), shown only below the lake's min zoom (one source of truth: tiles, no app geometry —
  AGENTS 28, layers only via `layers.source.json`).
- Shots before/after: Foley Lake/Creek and 2–3 others, at the zooms where the gap shows. Build beside; never rebuild
  live tiles to test (AGENTS 17). Ships with a tiles rebuild (with T489, same stage).
- Fast-fail (AGENTS 20a): a tile check that no drawn stream breaks at a hidden lake, if cheap.

## Measured (2026-11-07, live tiles 269777be.55f8a4cfc1b5, before any change)
- A lake's first zoom: `tiles/ladder.zoom_for_area` (its side >= 3 px at BC's mid latitude), stamped in
  `export_waterbodies`. A creek's: `ladder.zoom_for_magnitude` (FWA magnitude ladder) in `export_streams`. The route
  through a lake was only ever drawn as the dotted, id-less `under_lake` (z10+), never in `stream`.
- Lakes hidden between a creek drawn on BOTH sides (one hop; chains not counted):
  z6 122 · z7 825 · **z8 2,446 · z9 5,149 · z10 13,216 · z11 22,554 · z12 27,816**.
- Foley Lake (`lake:329083356`, 10.3 ha): lake from z10, Foley Creek above (mag 128) and below (mag 240) from z8 — gap
  at z8-z9. Others shot: Hayward Lake (Stave River; lake z8, river z6), Nahatlatch Lake (z8 / z6), Morris Lake (z11 / z8).

## Done (IN REVIEW)
- `pipeline/deliver/tiles/export.py` `lake_link_zooms` + `export_streams`: a lake whose drawn water on both sides
  (looking through neighbouring lakes, so chains link too) comes in before the lake gets its ROUTE written to a new
  layer file `layers/lake_link.geojsonl`, drawn INTO the `stream` tile layer (`layers.DRAWN_INTO`) under the LAKE's
  section id, `ord` of the creek, minzoom = both sides drawn, maxzoom = lake's zoom − 1, no name (no label).
  `under_lake` for a linked lake starts at the lake's own zoom. Own file so the bundle's section access / run-timing
  readers of `stream.geojsonl` never see a lake as a line. Not in the tile contract (no new tile layer, no style change).
- `tippe.py`: the link rides the water pass (its ends stay shared nodes with the creek); a per-zoom pass is no longer
  fed features whose `maxzoom` is below it, and the top pass gets no link (else the link would pin every `under_lake`
  vertex).
- App: no change needed, checked in code — colour labels are per section id on every per-section source-layer
  (`colouring.ts` `label(section)`; native `watchTiles(LABEL_SOURCE_LAYERS)`), pinned by a new `colouring.test.ts`
  case; a tap in the stream group returns the MVT id → `openTap` → `itemForSection` → the lake.
- Tests: `pipeline/tests/test_tile_lake_link.py` (Foley shape, no-gap cases, chains, a loop, the pass wiring).
- Fast-fail: none added — a gap check needs a built layer set, nothing cheap fits `scripts/fast-fail.sh`; the
  unit tests are in smoke.txt.

## Side build (2026-11-07, scratch, both tickets; live tiles untouched)
- 36,702 links. Hidden-lake gaps after: z6 18 · z7 24 · z8 17 · z9 6 · z10 2 · z11 1 · z12 0 (left: lakes whose route
  is dropped as > 3x their outline, cut-into-parts ghosts, no-polygon lakes). `stream.geojsonl` re-exported
  byte-identical to live.
- Archive 199.90 -> 201.50 MB (+1.6 MB, +0.8%), both tickets; per zoom: z4 +7.0% · z5 +8.3% · z6 +10.9% · z7 +13.7%
  · z8 +13.7% · z9 +12.9% · z10 +8.5% · z11 +1.7% · z12 +0.8% · z13 0. Stream layer (links) z8 +16%, z10 +14%, z12 +5%.
- Shots (before | after, centre crop x2): scratchpad `t493/shots/pairs/` — foley-lake, hayward-lake, nahatlatch-lake,
  morris-lake at z6-z10. Not shown: status colour (the shot tool draws no feature-state).

## Reopened — found 2026-11-08 (read-only investigation)
- Nicomen and Camp Slough are not lakes: each is a STREAM's own polygon (`water:"stream"`, drawn `lake_flowing`), its
  route in the stream layer. `lake_z` only holds `_kind == "lake"`, so the T493 link never sees them.
- The real defect: inside one water every piece gets its own first zoom (lines by magnitude, routes by the max of their
  neighbours, polygons by area), so a water can have holes. Nicomen: 59594 / 1454487 / 59593 hidden z7–z10 between two
  z7 pieces. Camp: z11 polygons with z12/z13 connectors.
- Province-wide ~6–39 gap runs per zoom (z7–z12), 12 stream polygons; mostly lower-Fraser sloughs.
- Fix: `fill_zooms` in export.py — a piece of water I hidden at z, with drawn water OF I on both sides (through hidden
  pieces of I), is drawn from z. Same water only (any-water would draw every lake-outlet creek). Two floods per zoom,
  zooms only lowered, no new layer. Lake links computed on the filled zooms. Tiles-only rebuild, AFTER T497's atlas
  (membership from registry.json). Tests: Nicomen shape, Camp shape, one-side negative, other-item negative, loop,
  never raised, deterministic.

## Slough fix — done 2026-11-08 (41bf8e309 … 0031f1997)
- `export.fill_zooms`: a hidden piece of water I is drawn from zoom z when drawn water OF I lies upstream AND
  downstream of it (through hidden pieces of I only) AND the water visibly breaks there: either the two sides are
  separate drawn runs of I, or one side is a drawn piece that DANGLES into the hidden stretch (no drawn piece of I
  next to it) and the other side is a different drawn piece. A stream's own polygon counts as drawn from its polygon
  zoom. Zooms only lowered; no new layer; `export_streams` buffers, fills, then writes; lake links use the filled zooms.
- The agreed rule ("drawn water of I on both sides") was too wide: on the T497 atlas it filled 16,754 pieces — braids
  and side channels of big rivers (Stikine, Liard, Swanson) pulled from z13 to z4–z7. "Only where I ends" still gave
  5,382. "Two separate runs" gave 204 but missed Nicomen: as built, 1266795 → 59599 → 59592 is drawn, so its hidden
  channel 59594 / 1454487 / 59593 is a parallel channel of ONE run, ending at 1454621, which dangles. Final: 242.
- Tests `pipeline/tests/test_tile_fill_zooms.py` (Nicomen schematic + as built, Camp, one side, other item, braid
  beside a river, Swanson side channels, loop, never raised + fixed point, deterministic, lake link on filled zooms,
  wiring); mutation-checked; in smoke.txt.
- Side build ON the T497 atlas (APFS clone `data/generated/side/t493/atlas`), tiles `data/generated/side/t493/tiles/`
  (version 269777be890cc2b3.c0ed226b8ed7, 196.0 MB; admin layers copied from live, water layers re-exported). vs live
  `stream.geojsonl`: 242 minzooms lowered (none raised), 0 geometry / `ord` changes; plus T497's renames (6 Columbia /
  Fraser / Kootenay side channels → their own names) and one new feature (Baldy Channel). Nicomen 59593 / 59594 /
  1454487 z11 → z7; Camp Slough's connectors z12–z13 → z11.
- Shots (before | after, centre crop x2): `shots/T493-nicomen-slough-z8.png`, `-z10`, `shots/T493-camp-slough-z11.png`,
  `-z12`.
- Ships: a tiles rebuild after T497's atlas is promoted (the side tiles are on T497's side atlas, not live).

