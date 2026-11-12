# T678 progress (Claude, 2026-11-11 night)

Ticket: `T678-tidal-in-the-build.md` (owner answers at its top are binding). Nothing promoted, nothing released.

## Done (commits on redesign/stream-sections)
- `addbfd819` step 1: `pipeline/atlas/splits/coast.py`, the cut rule. Rank 1 = DFO-written split rows (`coast.authority`:
  a split row's `source`/`authority` field starting `dfo`; a row with neither is UNCLASSIFIED, governs as today and is
  listed in `coast_owner.json`; any other value = no authority) + the owner's `whole` rows. Rank 2 = coast crossing: top
  of the line's lowest edge-1450 run, cross-checked with the geometric crossing (sea = outside the FWA land mass, plus
  tidal rivers' polygons); over 50 m apart, or no geometry, = the owner's line. Stub under 25 m at the top = tidal
  whole. Fraser code `100-000000`: named non-slough lines joining tidal water = tidal whole; sloughs and unnamed = owner.
  FWA rivers + land mass: `fetch_data.py` `fwa_rivers` / `fwa_land` into `data/source/fwa_coast.gpkg` (own file:
  the main gpkg's stamp is an input of every shipped atlas). Config key `data_accessor.coast_gpkg_path`.
- step 2: the build. `build.cut_shores` = the coast cut, now BEFORE `cut_length`. `waters/shore_held.json` (184 lines,
  today's result) written by `python -m pipeline.atlas.splits.coast_held --atlas data/generated/atlas/full --out
  data/curated/waters/shore_held.json` (a `decided` row survives a rerun). `build_registry` writes
  `tidal_sections.json` (THE record) and proves rank 1 covers the same water (`coast.coverage`). Reach CLI, tiles,
  reach stage key and review app read the record on a coast-cut atlas (`coast.built_by_coast_rule`); an older atlas
  is read the old way, printed. `sidecars --tidal-only` removed.

## Not done (next, in order)
1. Read the candidate build's diffs (below) with the owner; promote only on the owner's word.
2. At promotion: move `inputs.T678_INPUTS_AT_PROMOTION` into `ATLAS_INPUTS` (+ `--backfill`), drop the
   `NOT_ATLAS_INPUTS` shore_held row. Added earlier, every shipped atlas reads as behind and release preflight stops.
3. Plan 5: `tidal` as a field on the graph's nodes (today only the record carries it: graph.pkl is written before
   the registry the record needs).
4. Plan 2: the rank-1 resolvers (`reach.outside.tidal_cut_owner` / `tidal_whole_owner`) still live in the reach and the
   atlas still builds the mid-build registry (`water_registry(areas=False)`) to ask them before the coast cut.
5. Plan 6: a record row for polygon lines the prune took (`shore.py:535` silent skip) — not written.
6. Plan 8: store fact for key-0 tidal sections (deliver/store) — not started (T679 lane owns deliver/bundle).
7. Retire `shore.polygon_rule`, `HELD_*` constants, `atlas_tidal`, `inside_tidal.json` readers once no pre-T678 atlas
   is read (the build no longer calls them).
8. The other lane's split classification: `coast.authority` reads `source` (or `authority`) on split rows; confirm the
   field name when their commit lands, then re-run `coast_held` (San Juan's new split makes 354087703 rank 1).

## The candidate build (one run)
Snapshot of `data/curated` at the step-2 commit (`CURATED_ROOT`), script in the session scratchpad `t678/run_build.sh`:
`python -m pipeline.atlas.build --full --out data/generated/atlas/full_t678` then
`python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_t678 --out <scratch>/reaches`, via
`scripts/heavy.sh t678-atlas 50 -- …`. Diffs: scratchpad `t678/diffs.py LIVE CAND LIVE_REACH CAND_REACH OUT`
(cut_diff.json, tidal_diff.json, rule_length_diff.json).
