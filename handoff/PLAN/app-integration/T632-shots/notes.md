# T632 maps: notes

All images are lines over Esri World Imagery, 800x1100. Section geometry is the atlas's own
(`data/generated/atlas/full/geometries.pkl`, loaded once by one extraction run under `heavy.sh`). Section sets are the
reach reruns of 2026-11-10 on that atlas (before = T546 code, after = T632 code with the tidal subtraction). Cut points
are the ends of the atlas sections at the cut's route measure (`splits.resolved.json`). Rail lines are OSM, read from the
repo basemap (`data/source/basemap.pmtiles`, layer `roads`, kind `rail`). Measurements only; no recommendations.

## Set A: the 5 unplaced pieces

### A1-noons.png: Noons Creek, piece -2200000392:0 (2,198 m)
- Rule noons_creek.r1, "No Fishing upstream of railway bridge"; cut `noons_creek__railway_bridge` at m 58.4 on blk 360880849.
- The railway bridge: the OSM rail line (CPKC / Cascade Subdivision) crosses the creek line at -122.83407, 49.28408.
  This is the same point as the curated cut and the atlas cut (within 1 m). Not read off the imagery.
- The piece does NOT run along the creek line. It is a separate line 678 m (at its closest) west of the creek's own line,
  ending on the shore at the head of Port Moody Arm. Its ends: -122.84289, 49.28744 (shore) and -122.83136, 49.30407.
- Upstream or downstream of the bridge: **2,072 m of the piece lies on the land (upstream) side of the rail line, 126 m on
  the sea side.** The piece crosses the rail line itself, at a point 726 m from the railway bridge, not at the bridge.
- The rule's sections: 21 (15.2 km of lines incl. tributaries and two lakes). Below the bridge (m 0 to 58) is tidal.
- Not established: whether the piece's own rail crossing is a bridge or a culvert; whether the piece and the FWA line are
  the same creek drawn twice (the piece is an added municipal stream line).

### A2-seymour.png: Seymour River, piece -2000000390:0 (216 m)
- Rule seymour_river.r1, verbatim "No Fishing downstream of Seymour Lake"; the only bound is
  `seymour_river__seymour_lake` (entry r2:seymour_river@2-8 has no other extent). The reach run reports it as an ambiguous
  cut (candidates m 19,055 and m 25,947, the lake's two ends); it used m 19,055 (the lake's lower end, -122.96710, 49.44030).
- The stretch then ends at the tidal limit `seymour_river__cn_railway_bridge_tidal_boundary` (m 212, -123.02450, 49.30357):
  the river below it (212 m) is tidal and is not in the rule. Rule sections: 191 (incl. tributaries).
- The piece lies on the SOUTH shore of Burrard Inlet (ends -123.01820, 49.29323 and -123.01937, 49.29160), across the inlet
  from the Seymour mouth: 1,068 m from the Seymour's own line, 1,238 m from the CN rail bridge cut.
- Inside the stretch: **no. The piece is not on the Seymour River between Seymour Lake and the mouth; it is across Burrard
  Inlet, on the opposite shore, and drains straight into the inlet.**
- The registry item gnis:6105 carries a second name source, "Tunnel Creek" (City of Burnaby waterways layer). Not
  established: that the piece is Burnaby's Tunnel Creek (its own name source was not read).

### A3-skeena.png: Skeena, DFO place near the Kitsumkalum mouth
- Place 6:skeena-river:mainstem-waters-near-the-kitsumkal, binding "between" `skeena_river__zymagotitz_river_into_skeena_river`
  (m 115,773, -128.73220, 54.48094) and `skeena_river__kitsumkalum_river_into_skeena_river_u1500m` (m 123,961,
  -128.64193, 54.51296; = top of Hell's Gate).
- Kept sections (blue): 360887278:115772 (8,188 m, the mainstem between the two cuts), 360222215:0 (627 m),
  360222216:0 (1,628 m), 360230482:0 (1,402 m).
- The three side channels (magenta, numbered): 360220078:0 1,488 m; 360220098:0 3,263 m; 360220099:0 2,701 m.
  Shortest distance from each to the Kitsumkalum cut point: 2,888 m, 2,485 m, 1,479 m. They lie south of the main channel,
  on the upstream (east) side of the Kitsumkalum cut.
- Not established: where along the mainstem each channel joins (the graph's junction measures were not re-read here; the
  unplaced-piece note gives m 119,499 and m 125,470-127,295 for 099).

## Set B: the 21 tidal places (B01..B21, sheet-1..4)

- Source: `T632-tidal-examples.json` (place order = file order = B number). Red = sections the subtraction removed
  (reach546 minus reach632, every one listed in the run's `tidal.jsonl`); blue = the place's sections after the
  subtraction; grey = other atlas water; cyan shade = the `tidal_water` tile layer; yellow = the tidal limit.
- The yellow point is the upstream end of the removed section on the limit's own stream (atlas lines run mouth to source),
  labelled with the split's label from `splits.resolved.json`. Pieces too short to see (13 to 46 m: B11, B12, B13, B14)
  are ringed in red.
- Overviews: B03 (All Region 1 waters, 11 limits, 800 x 1100) and B09 (Region 2 waters into Areas 28 and 29, 28 limits,
  800 x 1400 with a Lower Mainland zoom). Numbers on the map match the list under it; the list gives the water, the metres
  removed and the limit. In B09, ten limits are whole waters with no limit point (`water:gnis:...`: channels and sloughs
  of the Fraser delta, e.g. Annacis Channel 9,077 m, Morey Channel 8,598 m); their number sits on the water itself.
  The largest single removal is the Fraser River below the CPR Bridge at Mission, 138,190 m (B09 no. 22).
- B03 no. 9 is Nitinat Lake (lake:329504244), removed whole; it counts 0 m because a lake section counts 0.
- B15 (All Region 6) and B18 (section C) have several limits each; they are shown as one small panel per limit in one image.
- Sheets: 6 per sheet, 2 columns, each tile 760 px wide, titled "n. place" and "metres before -> after (difference)".
- The many parallel straight lines across the Skeena estuary (blue in B15 to B17, grey in B20/B21) and in the Nass/Chambers panels are
  atlas lines as they are in `geometries.pkl` (checked by plotting the raw geometry), not a drawing error.
- Not established: the yellow point is the atlas's cut, not checked against the sign or bridge on the imagery; the
  `tidal_water` shade is the tile layer, which is not what the reach run uses to decide tidal water (it uses the limits).
- The old 01..21 png files named in the brief were not in this folder or in git, so nothing was deleted.
