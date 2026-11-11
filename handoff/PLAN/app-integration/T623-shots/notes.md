# T623 map questions: notes (F2, F9, F16, F21)

All four images are drawn from repo data over Esri World Imagery (800x1100). River lines come from the release atlas tiles
(data/release/tiles/d5242c0c70509877...pmtiles, z13, about 5 m resolution); cuts from data/curated/waters/cuts.json; falls from the
FISS/OSM layers; the park and reserve from the parks_bc layer. The part colours are drawn from the curated cut coordinates, not
read out of the staging app (the staging map could not be driven to these spots cheaply). No answers are given here, only measurements.

## F2-campbell-elk-falls.png
- Red = part 5 (closed all year) from the Elk Falls cut up to the John Hart Dam / lake-outlet cut. Blue = part 4 (fly only) from the power-line cut up to the Elk Falls cut.
- River distances: dam cut to Elk Falls cut 1.02 km; Elk Falls cut to the generating station 1.86 km; station to the app's power-line cut about 80 m (station 1.94 km below the falls).
- The app's Elk Falls cut (OSM, -125.33018, 50.04111) and the FISS falls point (-125.33119, 50.04319) are 220 m apart; the dam is at the lake outlet, the canyon runs east.
- Station location is NOT in any repo layer. I read it off the imagery (powerhouse, substation and tower at the canyon's lower end, about -125.3103, 50.0437): approximate, within about 100 m.
- Not established: the app's actual part colouring; the book's exact "station" point (powerhouse vs tailrace).

## F9-campbell-quinsam-powerline.png
- Quinsam mouth is the end of the Quinsam line in the tiles (-125.29409, 50.0379). The app cut is 1,332 m up the river from it (1,277 m straight line). Point 200 m up the river from the mouth is drawn in magenta (-125.29679, 50.03818).
- The 5.54 and 4.2 km in the audit give the same gap (1.34 km); my own km values count from the town-end of the line, so do not compare them directly.
- A cleared strip that looks like a power-line corridor meets the river about 320 m up from the mouth (my reading of the imagery, approximate); the transmission tower and substation sit beside the generating station near the app's cut.
- Not established: where the power line actually crosses (no line layer in the repo); the Quinsam mouth cannot be seen on imagery under the trees, so it is the data's mouth.

## F16-little-qualicum-falls.png
- Falls along the river (flow south to north, upstream first): Upper Waterfall (cut, OSM -124.5555, 49.31362), Lower Waterfall (-124.55635, 49.31614), "Little Qualicum Falls" the large falls (-124.55564, 49.3177). The distances along the river: upper to lower 475 m, lower to large falls 202 m, upper to large falls 677 m.
- Red = the app's Dec 1 to May 31 closure, starting at the Upper Waterfall and running downstream; so it covers the lower falls and the large falls too.
- The park boundary (yellow) meets the river at the large falls (the proposed "park downstream end" cut is 11 m below it). Two FISS falls records (cyan) sit between the OSM falls.
- Not established: which falls the book means by "the falls" (the cut's note says lowest); the imagery cannot show the gazetted sign positions.

## F21-peace-clayhurst.png
- Yellow = Clayhurst Ecological Reserve (4.0 km2, from parks_bc); red = the Peace River centreline in the FWA stream layer, the same line the app builds from.
- Measured: 2.70 km of the centreline lies inside the reserve of a 3.0 km stretch from its first to last point inside; the remaining 0.30 km is outside by at most 13 m (the line wiggles across the boundary).
- On the imagery the reserve covers the north bank and about half the channel; its south-east edge follows the river's middle, so the centreline is on or just inside that edge.
- Not established: the river's surface polygon is not a repo layer, so the share of water area inside the reserve is not computed; the centreline is not the bank.
