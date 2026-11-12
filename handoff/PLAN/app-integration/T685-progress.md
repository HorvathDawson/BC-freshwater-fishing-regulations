# T685 progress — T685 + T682 + T683 + T684, one atlas rebuild

Restart from here. Nothing committed yet by this lane (2026-11-12, second agent took over the first's uncommitted tree).
Scratch (session scratchpad `t685/`): `dry.py <atlas> <out>` runs the coast rule on a built atlas (read-only, ~90 s,
via heavy.sh with `PYTHONPATH=<repo>`), `ana.py <dry dir>` gives per-group before/after against the proposal,
`probe.py <atlas> <blk…>` prints which FWA river polygons / sea each line runs through.

## Why each file changed
- `pipeline/atlas/splits/coast.py` (T685): rule `coast/2`. The cut is the EDGE of the water the line enters, read off
  the polygons: sea = outside the FWA land mass (rule a: a line ending on the coastline gets no cut; only a stretch
  drawn over the sea is tidal, X2); a creek entering tidal water is cut where it leaves the FWA river polygon(s) its
  mouth sits in (`CoastGeometry.river_at`, chosen by location, never by the receiving river's key; the creek's own
  polygon is excluded, `own=`). FWA's edge-1450 vertex is recorded and used only when within 1 m. Sloughs and unnamed
  Fraser-code lines follow rule (b); the "owner cannot decide" path and `missing_held` are gone. `RULES` keeps coast/1
  atlases readable.
- `pipeline/atlas/splits/coast_held.py` + `shore_held.json` (T685): only dated owner rulings; the ~188 "today's
  result" rows are gone. Rows: 356242813 whole (owner A named it), 360848528 fresh (X3), and the 13 G4/G5 lines at
  today's cut (E22 "keep today's cuts"; each row records where the rule's river edge falls).
- `tidal_limits.json` (T685/E24): Deas, Woodward, McDonald, Tamboline, London as `whole`; Ewen moved here (owner A).
- `build.py`, `inputs.py`, `curated.py`, `config.yaml`, `README.md`, `MAP.md`: wiring + wording for the above.
- `test_coast_cut.py`, `test_tidal_cut.py`: pins for the above (own-polygon guard has a near-miss test).
- T682: `matches.json` (two rows only: 08MH028 blk 355994324->356364114, 08NK016 356366042->356570562, notes),
  `match_calls.json` (`line` on the two calls), `gauges/review.py` (`Decision.line`), `generate/promote.py` (writes it),
  `matches.py` (doc), `consume/place.py` `owner_line` + `bundle_tables.py` (place the station at the cut on that line),
  `test_gauge_splits.py`.
- T683: `dfo_salmon/locations.py` note pattern "from the centre line to the X bank" (half width: whole channel bound,
  note carried); `region-6.json` Nakina place gets that note + `spatial_caveat`; `test_dfo_salmon.py`.
- T684: nothing to edit — the five held "item C" rows left `shore_held.json`; the rule makes no cut on them (dry run:
  no row for four; 354087703 carries only the owner's Deering Road `cut` row, rank 1).

## Dry run on the live atlas (dry4, current code)
G1 21.1->0.0 km; G2 1.65->0.0; G3 11.82->8.25; G4 6.65->6.65 (owner rows); G5 0.49->0.49 (owner rows); G6 36.07->0.0;
G7 0.53->0.47 (X1 174.9 m, X2 130.3, X7 168.1; X3, X4, X5, X6 fresh); sloughs 8.6->26.9 (five whole rows).
Outside the proposal: Port Moody added lines Hutchinson (-2200000293) 5.4 m and Turner/Noons (-2200000392) 21.3 m drawn
over the sea go tidal (rule a, like X2); 356242813 and Ewen whole (+45 m each, owner A); the four creek pieces
360586479/360665787/360675720/360812872 go fresh (their river is now fresh to the shoreline, so rule b does not apply).

## Build 1 (2026-11-12 13:43, `full_next`, not promoted) found two defects; fixed, build 2 running
- X1 Power House Creek cut at 113.9 m instead of the river's edge 174.9 m: `cut_shores` read the edge on `b.chains`,
  whose merged geometry for a key carrying Burnaby added pieces (two tributary branches) is not one mouth-to-top line.
  Now reads the graph's own lowest section run (`shore.graph_lines`), as the dry run does
  (`test_the_stage_reads_the_edge_on_the_graphs_own_line_never_the_merged_chain`).
- The reach run stopped at the unplaced gate on `-2200000392:21`: Port Moody's Turner Creek line is Noons Creek in the
  registry, which the owner's railway-bridge split governs, but `shore.owner_limited` joined lines by gnis only, so
  the coast rule cut it. `owner_limited(..., waters)` now also joins by registry stream item
  (`test_owner_limited_takes_an_added_line_of_the_governed_water`). Dry run: governed 93 -> 713 lines, the only
  change in outcome is that Noons/Turner cut gone (other dropped rows were no-cut rows).
- Baseline reach run on the live atlas with this tree: `data/generated/reaches/t685_base` (ok). Pre-existing there:
  dfo place 6:skeena-river:mainstem-waters-near-the-kitsumkal unplaced (straddles_the_cut), not from this lane.

## Owner 2026-11-12 ("these are all good") — RULINGS E25
Accepted the changes outside the proposal: Hutchinson -2200000293 5.4 m tidal and Turner/Noons -2200000392 21.3 m tidal
(drawn over the sea), and the four creek pieces going fresh. Built: Hutchinson and the four as accepted. CONFLICT,
reported, not decided: Turner/Noons is in the Noons Creek water the owner's railway-bridge split governs, so build 2
leaves it fresh (as live). Making the 21.3 m tidal needs either that piece accepted in
`waters/unplaced_piece_calls.json` (owner's call) or a way for rank 1 to cover it; the cut alone stops the reach run.
X5/X6 (Nelson Creek, 356045296): fresh because the Brunette River there is drawn as a single line (no FWA river polygon
within 1 m of either mouth; the nearest polygon is the Fraser's, 980 m away), so the river's edge is the creek's mouth
and nothing is tidal. Not a miss: the rule has no drawn water to cut at. Today's 38 m / 34 m were the DFO polygon exit.

## Next
1. fast-fail; ONE atlas build `python -m pipeline build --atlas` via heavy.sh; parity; reach run; binding diff.
2. Promote atlas file by file; ticket files + README ledger; check-tickets; commit own paths; push; CI.
