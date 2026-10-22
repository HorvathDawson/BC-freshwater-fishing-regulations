# water_continue: every row, reviewed (2026-10-21)

The owner asked: "it should be able to navigate up if one stream comes directly from another, like vedder from chilliwack." The release ships `water_continue (ord, part, next_ord, next_part)`. Each row says that water `ord` continues as water `next_ord`. The rule is in `pipeline/deliver/release/continuation.py`. It is pinned in `pipeline/tests/test_water_continue.py` and, on the real data, in `test_release.py::test_the_chilliwack_continues_as_the_vedder_and_no_tributary_does`.

## Verdict on the first cut: 92 rows, 89 of them wrong

The first cut (`1f1ffb11`) decided "A is the only inflow at B's top" by reading the bundle's `section_down`. That table holds only the sections inside gauge sheds (`pipeline/gauges/consume/shed.py::downstream_map`). Most inflows are therefore invisible to it: lake outlets, the second river at a confluence, and every tributary outside a shed. Four kinds of error came from this:

- **Lake outlets.** Oliver Creek was shipped as continuing into the Cowichan, but Cowichan Lake's outlet is the Cowichan's top. The same happened to Chase Creek and the South Thompson (Little Shuswap Lake), Poquette Creek and the Quesnel (Quesnel Lake), Fairmont Creek and the Columbia (Columbia Lake), and Zetu Creek and the Iskut.
- **Confluences.** The Sproat was shipped into the Somass, but the Stamp joins 64 m away. The South Thompson was shipped into the Thompson, but the North Thompson joins 145 m away. The Puntledge was shipped into the Courtenay, but the Tsolum joins 39 m away.
- **Plain tributaries.** In most rows the receiving line goes on for kilometres above A's mouth. For example, Burgess Creek goes on 9.9 km above Arbuthnot Creek, and the Akue goes on 18.8 km above Gammer Creek. A piece is cut only where something splits it, so a tributary flows into B's top piece somewhere along it. The old rule checked which piece A flows into, but never where along that piece A joins.
- **Missed continuations.** Name changes outside any gauge shed were never seen. The fixed rule finds 15 continuations that the first cut did not ship.

## The fixed rule (in the atlas graph, `graph.pkl`, which holds every inflow)

Water A continues as water B when all of the following hold:

- Both are streams, and `item_down` already says A flows into B.
- A's main line begins at A's lowest piece, and that piece flows into the top piece of B's main line.
- One of these is true:
  - A's mouth lies on B's own blue line, or
  - all three of these hold:
    - A joins within **100 m** of that piece's top. The Chilliwack joins 25 m below the top (snapping). The genuine tributaries that were shipped joined 238 m or more below it.
    - No other stream and no lake outlet joins within **200 m** of A's mouth, or above it. This covers the confluences seen in the data: 64 m (Somass), 77 m (Fort Nelson), 90 m (Taku) and 145 m (Thompson).
    - No piece of B itself joins lower down. The upper Cottonwood joins 448 m below the Swift, so the Cottonwood continues as itself.
- The FWA watershed codes do not say the flow is backwards (added 2026-10-21 late, owner: "the issue seems to be eagle trib 5-3 turns into 5? we could just trim out those bad ones?"). The rule reads the code of A's mouth piece and of B's top piece from the graph. If B's code extends A's (A `100-1`, B `100-1-2`), B is a tributary of A by code, so "A flows into B" is the data reversed and the link is refused. Padding (`-000000`) is trimmed first. A missing code is no evidence. A's code extending B's (A is B's tributary by code) is kept: that is every real row below, and the municipal ones the owner kept ("we can just leave them"). Pinned in `test_water_continue.py` (`test_a_link_into_a_water_the_codes_call_a_tributary_of_it_is_refused` and its neighbours).

The release builder loads `graph.pkl` from the bundle's atlas, using the same digest check as `section_handles.txt`. It uses the graph only for this step and then frees it. The load takes about 6 to 16 s, and peak RSS is about 6 GB.

## The rows the fixed rule ships: 18 (release r-f579fe5eac3e), now 17

The code rule (above) drops one row, Eagle Trib.5 → Eagle Trib.5-3. Every other row's codes agree with its direction: A's code extends B's (Chilliwack `100-064535-057628-998041` → Vedder `100-064535-057628`; Jeperson Side Channel is `100` on both items). Checked from the registry's `wsc:` ref ids, not from a rebuilt release; `test_release.py::test_the_chilliwack_continues_as_the_vedder_and_no_tributary_does` pins the 17 on the next release build.

| water (ord) | continues as | note |
|---|---|---|
| Chilliwack River | Vedder River | the owner's case. The Chilliwack joins 25 m below the end of the Vedder line, and nothing else joins there. (A's mouth is 25 m below B's top) |
| Hatzis Slough | Lower Hatzic Slough | a name change on the same slough (A's mouth is 15 m below B's top) |
| Chilqua Creek | Chilqua Slough | the creek becomes the slough (A's mouth is 6 m below B's top) |
| Page Creek | Matsqui Slough | the creek becomes the slough. The next inflow is 656 m lower. (A's mouth is 23 m below B's top) |
| Marblehill Creek | Semmihault Creek | a name change. The next inflow is 1.2 km lower. (A's mouth is 12 m below B's top) |
| Davidson Creek | Tomat Creek | a name change. Nothing else joins the Tomat's top piece. (A's mouth is 28 m below B's top) |
| Pass Creek | Screw Creek | a name change. An unnamed stream joins 245 m lower. (A's mouth is 28 m below B's top) |
| Jeperson Side Channel | Jeperson Side Channel | two registry items with the same name (gnis:11481 and blk:355994562), one channel (A's mouth is 0 m below B's top) |
| Aliceville Trib.1-1 | Aliceville Trib.1 | municipal stream. By its name it is a tributary, but the receiving line ends at its mouth. (A's mouth is 83 m below B's top) |
| Beecher Trib.4 | Beecher Creek | municipal stream, as above (A's mouth is 27 m below B's top) |
| Crayfish Creek Trib.3-1 | Crayfish Creek Trib.3 | municipal stream, as above (A's mouth is 39 m below B's top) |
| Eagle Trib.4-1 | Eagle Trib.4 | municipal stream, as above (A's mouth is 99 m below B's top) |
| ~~Eagle Trib.5~~ | ~~Eagle Trib.5-3~~ | DROPPED by the code rule (2026-10-21 late). The flow is REVERSED: by its watershed code Trib.5-3 (`100-019698-794100-757900-384000`) is a tributary of Trib.5 (`100-019698-794100-757900`), yet the data says Trib.5 flows into it. The added-streams orientation defect stays in the data; the link no longer ships. (A's mouth is 0 m below B's top) |
| Jr 4-1-2 | Jr 4-1 | municipal stream, as above (A's mouth is 15 m below B's top) |
| Noble Creek Trib.1 Trib.1 | Noble Creek Trib.1 | municipal stream, as above (A's mouth is 96 m below B's top) |
| Rochester Creek | Holmes Creek | municipal stream. Its watershed code ends in 999999, which puts it at the top of the Holmes. (A's mouth is 0 m below B's top) |
| Westway Avenue Drainage Ditch | Magnolia Creek Trib 1 | a municipal ditch (A's mouth is 54 m below B's top) |
| Finch Connector (Unnamed) | Logger's Lane Connector | a municipal connector (A's mouth is 50 m below B's top) |

Ten of the 17 are municipal added streams (the `Trib` names, a ditch and a connector). Topologically each one is the only water above its receiver. By name, most are tributaries. The owner kept them (2026-10-21 late: "we can just leave them I think"). Eagle Trib.5 → Trib.5-3 was a reversed flow direction in the added-streams data and is now refused by the code rule.

## Rename rivers checked for misses

All 1,572 stream→stream pairs whose mouth meets the top piece of the receiver were checked, along with every pair whose names share a stem (North/South/East/West/Little/Upper/Lower X → X). None of these was rejected wrongly. In each one, either the receiver goes on above the mouth (kilometres, for example North July → South July 2.2 km, Widgeon Creek → Widgeon Slough 1.1 km), or a second river or a lake outlet meets it there. The near-misses below are confluences, so neither river continues the other: North Thompson and South Thompson → Thompson, Sproat and Stamp → Somass, Puntledge and Tsolum → Courtenay, Atnarko and Talchako → Bella Coola, Nakina and Inklin → Taku, Fontas and Sikanni Chief → Fort Nelson, and Nahlin and Sheslay → Inklin. Swift → Cottonwood is rejected because the Cottonwood continues as itself. Most of BC's other well-known name changes happen through a lake (by this rule a lake is never a continuation), so they cannot appear here.

## The first cut's 92 rows, each judged against the full graph

| A | B | verdict | why |
|---|---|---|---|
| Westway Avenue Drainage Ditch | Magnolia Creek Trib 1 | right | the only water at B's top |
| Davidson Creek | Tomat Creek | right | the only water at B's top |
| Chilliwack River | Vedder River | right | the only water at B's top |
| Pulley Creek | Akolkolex River | WRONG | B's line goes on 13,151 m above A's mouth; also joining there: an unnamed stream |
| Gammer Creek | Akue Creek | WRONG | B's line goes on 18,838 m above A's mouth; also joining there: an unnamed stream |
| Twentyone Mile Creek | Alta Creek | WRONG | B's line goes on 869 m above A's mouth; also joining there: Alta Lake (lake outlet), an unnamed stream |
| Cashmere Creek | Ambusten Creek | WRONG | B's line goes on 8,500 m above A's mouth; also joining there: an unnamed stream |
| Salmon River | Bedford Channel | WRONG | B's line goes on 3,645 m above A's mouth; also joining there: Fraser River |
| Mellin Creek | Berry Creek | WRONG | B's line goes on 1,907 m above A's mouth; also joining there: an unnamed stream |
| Boundary Trib.2 | Boundary Creek | WRONG | B's line goes on 435 m above A's mouth |
| Laundry Creek | Brewer Creek | WRONG | B's line goes on 8,790 m above A's mouth; also joining there: an unnamed stream, an unnamed stream (lake outlet) |
| North Brown Creek | Brown Creek | WRONG | B's line goes on 3,392 m above A's mouth; also joining there: Brown Creek, an unnamed stream |
| Arbuthnot Creek | Burgess Creek | WRONG | B's line goes on 9,911 m above A's mouth; also joining there: an unnamed stream |
| Young Grouse Creek | Burnt Creek | WRONG | B's line goes on 1,706 m above A's mouth; also joining there: an unnamed stream |
| Thomas Creek | Cameron Creek | WRONG | B's line goes on 1,112 m above A's mouth; also joining there: Xenia Lake (lake outlet) |
| South Canyon Creek | Canyon Creek | WRONG | B's line goes on 12,898 m above A's mouth; also joining there: an unnamed stream |
| Cody Creek | Carpenter Creek | WRONG | B's line goes on 9,078 m above A's mouth; also joining there: an unnamed stream |
| Brett Creek | Cartmell Creek | WRONG | B's line goes on 6,047 m above A's mouth; also joining there: an unnamed stream |
| Leigh Creek | Chehischic Creek | WRONG | B's line goes on 10,035 m above A's mouth; also joining there: Shesta Lake (lake outlet), an unnamed stream |
| Gough Creek | Clack Creek | WRONG | B's line goes on 5,973 m above A's mouth |
| Fairmont Creek | Columbia River | WRONG | B's line goes on 2,992 m above A's mouth; also joining there: Columbia Lake (lake outlet), Columbia River, Geary Creek, an unnamed stream |
| McKian Creek | Cooper Creek | WRONG | B's line goes on 12,769 m above A's mouth; also joining there: South Cooper Creek, an unnamed stream |
| Rubie Creek | Corne d'elan Creek | WRONG | B's line goes on 3,232 m above A's mouth; also joining there: an unnamed stream |
| Puntledge River | Courtenay River | WRONG | also joining there: Tsolum River |
| Oliver Creek | Cowichan River | WRONG | B's line goes on 635 m above A's mouth; also joining there: Cowichan Lake (lake outlet) |
| Littlemoor Creek | Dalzell Creek | WRONG | B's line goes on 4,496 m above A's mouth |
| Beady Creek | Dease River | WRONG | B's line goes on 4,381 m above A's mouth; also joining there: Dease Lake (lake outlet), Dease River, Thibert Creek, an unnamed stream |
| Janzé Creek | East Murray Creek | WRONG | B's line goes on 5,059 m above A's mouth; also joining there: an unnamed stream |
| Robie Creek | Findlay Creek | WRONG | B's line goes on 2,903 m above A's mouth; also joining there: an unnamed stream |
| Froggers Trib.2 | Froggers Creek | WRONG | B's line goes on 1,659 m above A's mouth |
| Crossman Creek | Gardom Creek | WRONG | B's line goes on 2,456 m above A's mouth; also joining there: Gardom Lake (lake outlet) |
| Connors Creek | Gladys Creek | WRONG | B's line goes on 136 m above A's mouth; also joining there: Gladys Lake (lake outlet) |
| Glen-Lyon Trib.1 | Glen-Lyon Creek | WRONG | B's line goes on 306 m above A's mouth |
| Gray Trib.1 | Gray Creek | WRONG | B's line goes on 1,581 m above A's mouth |
| Gray Trib.1-1 | Gray Trib.1 | WRONG | B's line goes on 507 m above A's mouth |
| Simlock Creek | Harveys Creek | WRONG | B's line goes on 6,131 m above A's mouth; also joining there: an unnamed stream |
| Russell Creek | Hascheak Creek | WRONG | B's line goes on 5,201 m above A's mouth; also joining there: an unnamed stream |
| Sergeant Creek | Henderson Creek | WRONG | also joining there: an unnamed stream |
| Fred Wells Creek | Hobson Creek | WRONG | B's line goes on 13,134 m above A's mouth; also joining there: an unnamed stream, an unnamed stream (lake outlet) |
| Hogget Creek | Hogranch Creek | WRONG | B's line goes on 6,321 m above A's mouth; also joining there: an unnamed stream |
| Bornais Creek | Holland Creek | WRONG | B's line goes on 2,477 m above A's mouth; also joining there: an unnamed stream |
| South Hospital Creek | Hospital Creek | WRONG | B's line goes on 12,441 m above A's mouth; also joining there: an unnamed stream |
| North Hurst Creek | Hurst Creek | WRONG | B's line goes on 4,603 m above A's mouth; also joining there: an unnamed stream |
| Zetu Creek | Iskut River | WRONG | B's line goes on 1,044 m above A's mouth; also joining there: Kluachon Lake (lake outlet), an unnamed stream |
| Jr 4-1 | JR Trib. 4 | WRONG | B's line goes on 605 m above A's mouth |
| Snowshoe Creek | Keithley Creek | WRONG | B's line goes on 13,517 m above A's mouth; also joining there: Donaldson Creek, Honest John Creek, Rabbit Creek, an unnamed stream |
| Cup Creek | Koosawu.áa Héen | WRONG | B's line goes on 10,724 m above A's mouth; also joining there: an unnamed stream |
| Simpson Creek | Koprino River | WRONG | B's line goes on 11,856 m above A's mouth; also joining there: Kempson Creek, Koprino River, an unnamed stream |
| North Seaskinnish Creek | Ksi Sgasginist | WRONG | B's line goes on 16,180 m above A's mouth; also joining there: Boulder Creek, an unnamed stream |
| Herbert Creek | Lakit Creek | WRONG | B's line goes on 3,234 m above A's mouth; also joining there: an unnamed stream |
| Ottawa Creek | Lardeau Creek | WRONG | B's line goes on 5,986 m above A's mouth; also joining there: an unnamed stream |
| Chum Creek | Little River | WRONG | B's line goes on 3,306 m above A's mouth; also joining there: Shuswap Lake (lake outlet), an unnamed stream |
| Little Stawamus Creek Trib 3 | Little Stawamus Creek Trib 2 | WRONG | B's line goes on 289 m above A's mouth |
| Nicol Creek | Lussier River | WRONG | B's line goes on 6,454 m above A's mouth; also joining there: Fish Lake (lake outlet), an unnamed stream |
| Deiner Creek | MacIntyre Creek | WRONG | B's line goes on 2,883 m above A's mouth; also joining there: an unnamed stream |
| Magnolia Creek Trib 1 | Magnolia Creek | WRONG | B's line goes on 758 m above A's mouth |
| Bayview Creek | Manitoba Creek | WRONG | B's line goes on 3,823 m above A's mouth; also joining there: an unnamed stream |
| Hilder Creek | Marvel Creek | WRONG | B's line goes on 10,925 m above A's mouth; also joining there: an unnamed stream |
| Thule Creek | McCumber Creek | WRONG | B's line goes on 14,040 m above A's mouth; also joining there: an unnamed stream |
| Dardanelles Creek | McGuigan Creek | WRONG | B's line goes on 3,007 m above A's mouth; also joining there: an unnamed stream |
| West McInnes Creek | McInnes Creek | WRONG | B's line goes on 5,000 m above A's mouth; also joining there: an unnamed stream |
| Dixie Creek | Monitor Creek | WRONG | B's line goes on 1,163 m above A's mouth; also joining there: an unnamed stream |
| Gleason Creek | Moore Creek | WRONG | B's line goes on 3,887 m above A's mouth; also joining there: an unnamed stream |
| Tseehee Creek | Moosehorn Creek | WRONG | B's line goes on 2,478 m above A's mouth; also joining there: an unnamed stream |
| Connor Creek | Morris Creek | WRONG | B's line goes on 610 m above A's mouth; also joining there: Morris Lake (lake outlet), an unnamed stream |
| Toneko Creek | Moxley Creek | WRONG | B's line goes on 753 m above A's mouth |
| East Murray Creek | Murray Creek | WRONG | B's line goes on 11,869 m above A's mouth; also joining there: an unnamed stream |
| Johnson Draw Creek | Nelson Creek | WRONG | B's line goes on 7,823 m above A's mouth; also joining there: an unnamed stream |
| Sunken Creek | Norbury Creek | WRONG | B's line goes on 265 m above A's mouth; also joining there: Norbury Lakes (lake outlet) |
| Magog Creek | Og Creek | WRONG | B's line goes on 5,369 m above A's mouth; also joining there: an unnamed stream |
| Kobes Creek | Panorama Creek | WRONG | B's line goes on 11,701 m above A's mouth; also joining there: an unnamed stream |
| Poquette Creek | Quesnel River | WRONG | also joining there: Quesnel Lake (lake outlet) |
| Willie Phillipps Creek | Reserve Creek | WRONG | B's line goes on 8,874 m above A's mouth; also joining there: Conner Creek, an unnamed stream |
| Deschamps Creek | Riddle Creek | WRONG | B's line goes on 12,178 m above A's mouth; also joining there: an unnamed stream |
| James Creek | Roberts Creek | WRONG | B's line goes on 5,122 m above A's mouth; also joining there: an unnamed stream |
| Douglas Creek | Round Creek | WRONG | B's line goes on 2,818 m above A's mouth |
| Silk Creek | Shakan Creek | WRONG | B's line goes on 13,518 m above A's mouth; also joining there: Bar Creek, Barrie Creek, Dud Creek, Maka Creek, Platt Creek, Saatin Creek, Tent Creek, an unnamed stream |
| Skulaow Creek | Shingle Creek | WRONG | B's line goes on 9,298 m above A's mouth; also joining there: an unnamed stream |
| Tomat Creek | Smith Creek | WRONG | B's line goes on 7,237 m above A's mouth; also joining there: Westbank Creek, an unnamed stream |
| Sproat River | Somass River | WRONG | also joining there: Stamp River |
| North July Creek | South July Creek | WRONG | B's line goes on 2,182 m above A's mouth |
| Chase Creek | South Thompson River | WRONG | B's line goes on 369 m above A's mouth; also joining there: Little Shuswap Lake (lake outlet) |
| Speculator Creek | Springer Creek | WRONG | B's line goes on 4,816 m above A's mouth; also joining there: an unnamed stream |
| High Falls Creek | Squamish Powerhouse Channel | WRONG | B's line goes on 313 m above A's mouth; also joining there: Squamish River |
| Strimboldh Creek | Tachek Creek | WRONG | B's line goes on 14,941 m above A's mouth; also joining there: an unnamed stream |
| South Thompson River | Thompson River | WRONG | B's line goes on 144 m above A's mouth; also joining there: North Thompson River |
| Craig Brook | Tindill Creek | WRONG | B's line goes on 5,418 m above A's mouth |
| West Twenty Mile Creek | Twenty Mile Creek | WRONG | B's line goes on 2,216 m above A's mouth; also joining there: an unnamed stream, an unnamed stream (lake outlet) |
| Nordic Creek | Ventego Creek | WRONG | B's line goes on 2,348 m above A's mouth; also joining there: Ventego Lake (lake outlet), an unnamed stream |
| Amadis Creek | Watt Creek | WRONG | B's line goes on 3,786 m above A's mouth; also joining there: an unnamed stream |
| Sakwi Creek | Weaver Creek | WRONG | B's line goes on 2,393 m above A's mouth; also joining there: Weaver Lake (lake outlet), an unnamed stream |
| Brierly Creek | Wolverine River | WRONG | B's line goes on 8,236 m above A's mouth; also joining there: an unnamed stream |
