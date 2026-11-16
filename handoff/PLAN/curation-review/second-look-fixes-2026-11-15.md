# Fixes after the second look (2026-11-15 night)

Owner, 2026-11-15 21:40: "you should also double check all the extents for dfo and make sure they all look sane (match to
proper bound with proper tributary logic or second part etc etc....) build on other review and we want to fix items form
other review". Two fix lanes applied the second look's recommended fixes (report:
`final-marks-second-look-2026-11-15.md`) as marks saved "unsure" for the owner to re-tick, each noted "saved by Claude at
the owner's request 2026-11-15 night", and checked what every place BINDS on the side build
(`data/generated/side/rebuild-input-2`). Scripts and tables: the session scratchpad, `fixA/` and `fixB/`.

Tags: **[ran]** a script, **[read]** code or data read, **[inferred]** reasoning.

## Lane A: Region 1

### Marks appended (verdict unsure; `dfo.mark_for` + `marks.append`; lines 1-2770 untouched)

| mark | place | change | sections | km on own river |
|---|---|---|---|---|
| 2771 | Somass "Except in those areas and times listed below" | rest → whole | 0 → 5 | 0 → 9.5 |
| 2772 | Stamp "Except in those areas and times listed below" | rest → whole | 0 → 10 | 0 → 21.5 |
| 2773 | Little Qualicum "all waters unless indicated below" | rest → whole | 5 → 8 | 8.8 → 22.8 |
| 2774 | Cowichan, Sandy Pool sign to the tidal boundary | tributaries off | 550 → 9 | 24.8 |
| 2775 | Cowichan, 66 Mile Trestle to Sandy Pool | tributaries off | 43 → 2 | 11.1 |
| 2776 | Nanaimo, Cedar Road bridge to the Hwy 1 overpass | tributaries off | 123 → 4 | 9.2 |
| 2777 | Nanaimo, 500 m below Bore Hole to Boulder Creek | tributaries off | 37 → 2 | 9.9 |
| 2778 | Nanaimo, power line at Bore Hole to Boulder Creek (dormant twin of 2777) | tributaries off | 36 → 1 | 9.4 |

Every extent was resolved with the review app's resolver on the side atlas: 0 unplaced pieces [ran]. Counts are after
the tidal cut, as the build ships them.

### Why "whole river" is right for the three "except" places [read `pipeline/deliver/salmon.py`]
- Every place that binds a section is in that section's chain (`covering`, lines 768-788), sorted by `chain_order`
  (348-359): precedence, own region, fewer sections first, riders, id.
- `verdict_on` (249-303) returns at the first place with a limit held for that fish that day, and MOVES ON to the next
  place when a place has no rule for that fish on that day (and no `check` / unread row; none of these siblings has one).
- So a narrower listed stretch answers on its own dates, and the whole-river place answers on every other day: DFO's
  "except in those areas and times".
- Somass: the "except" place and `somass-river:whole` (a gear-only bait ban, Jun 1-Aug 24) now cover the same 5 sections
  and tie by id; one has only limits, the other only a gear line, so no conflict. Chinook and coho limits now show
  everywhere except inside the 0.5 km finfish closure on its dates (Aug 25-Nov 15).
- Stamp: the stretches close on their own dates (Stamp Falls Jun 15-Nov 15, lagoon Aug 25-Oct 31, powerline Oct 1-14,
  above the Ash Jan 1-Apr 30); the limits show on every other day.
- Little Qualicum: in Sep-Nov the falls-to-hatchery stretch (closed Dec 1-May 31 only) gets fly-only, chinook 2, coho 1;
  the hatchery fence to 35 m stays closed all year.
- The places computed from siblings do not change (Little Qualicum all-open 8, Qualicum 5, Somass / Stamp tributaries
  5 / 10). The Nanaimo rest place was not marked; it gets the dropped side streams back (about 1,116 → 1,275 sections).

### Side streams (the tight rule)
Changed only where ALL hold: the place's tributary choice is blank, the water's flag is true, the extent is a bounded
stretch, neither the place's words nor the water's printed heading say tributaries, and the side run showed side-stream
sections bound. Dropped: Cowichan Sandy Pool 541 side-stream sections (the Koksilah among them, 408 shared with its own
place), 66 Mile Trestle 41, Nanaimo Cedar Road 119 (Haslam, North Haslam, Hokkanen, Thatcher), Bore Hole 35 (Stark,
Elkhorn, Berkley). Left alone: Nanaimo Cedar Road to Hwy 19 (no side stream joins in 257 m), three dormant places where
the owner saved tributaries as true, the whole-water and rest places. Full table: scratch `fixA/tributaries-table.md`
(17 places).

### Cut labels (ids and positions unchanged; `curated_write.write`; `check_curated --waters` 0 errors)
- Stamp, upstream cut: "fishing boundary signs 200 m upstream of Stamp Falls (50 m downstream of site 17 in Stamp River
  Provincial Park)"
- Stamp, downstream cut: "signs 500 m downstream of Stamp Falls (225 m downstream of the confluence of Deer Creek)"
- Sproat: "fishing boundary signs about 350 m downstream of Hwy 4 (downstream end of the pool below the rapids at the
  Sproat River Fishway)"
- Deer Creek sits at about 6,150 m and the lower cut at about 5,988 m: about 165 m below Deer Creek, not DFO's 225 m
  (about 60 m short) [inferred from the watershed code].

### What every Region 1 place binds (83 placed places) [ran]
Clean: every `between` has both ends on its own river line, in order, non-zero; named distances match (Stamp Falls
700 m; Nitinat 200 m and 100 m; Quatse 200 m; Little Qualicum 35 m; Puntledge 75 m, 200 m, tailrace stretch 553 m
against "approx. 500 m"; Stamp powerline 495 m); both parts bound where the words name two (Cayeghle + Colonial, Adam +
Eve, Cowichan + Cowichan Lake, both Puntledge pieces); the only overlap between live places of different waters was
Cowichan / Koksilah, fixed by 2774.

### New findings (code, not data)
1. **Gear lines do not stack.** `gear_on` (salmon.py 306-316) shows only the narrowest place's gear lines that day. On
   the Little Qualicum in Sep-Nov "fly fishing only" hides the all-year hook rule. Suggested: gear lines of every place
   in the chain show.
2. **Chain size counts side-stream sections.** The whole Stamp (10) now sorts before the Ash stretch (37, of which 33
   are side streams). No answer changes today (dates never overlap). Suggested: order by own-river sections.

### Questions for the owner
1. Chemainus tidal boundary at 2.1 km (low confidence; it takes 16 of 24 sections as tidal): keep, check imagery?
2. Cowichan: the water's "including tributaries" flag looks wrong (only a dormant wording says tributaries): turn off?
3. Sproat (2468) and Stamp Falls (2469) still end on "wrong" saves though their fixes are in: re-save as right.
4. The dormant Stamp "southern portion of the lagoon" is drawn as the 522 m river stretch, though its words describe
   part of the lagoon: leave while dormant?

## Lane B: Regions 2-8

Every extent was checked with the builder's own resolver on the side atlas; on current bindings it reproduced the side
run exactly [ran].

### Marks appended (place marks unsure, for the owner to re-tick)

| mark | place | change | sections |
|---|---|---|---|
| 2779 | Tatshenshini, Blanchard River place | the whole Blanchard (`gnis:13637`) with tributaries | 984 → 287, 0 unplaced, 0 outside BC |
| 2780 | Tatshenshini, Kwatini / Stanley / Goat creeks | the three creeks whole | 984 → 211 |
| 2781 | Morice, lake outlet signs to Gosnell Creek | upstream of the Gosnell confluence cut on the Morice item, tributaries on; reaches m 91,755 (the lake edge) | 61 → 72 |
| 2782 | Skeena "mainstem waters only" (pink, printed under B(i)) | upstream of the CNR bridge, no tributaries | 222 → 94 |
| 2783 | Sheldens Creek | the whole creek (owner: "i think this is tidal bound maybe") | 1 → 4 (0.29 → 3.36 km) |
| 2784 | Babine Lake sockeye place (~3) | the whole lake, tributaries off | 0 → 1 |
| 2785 | Thompson, Juniper closed area (`closed_area`, verified) | lower signs to 4 km up (owner: "if the book says 4km then we put it 4km unless i had another split it already matched??"); no cut existed near m 97,016, so two new cuts | new |

- Tatshenshini: this was the save that made the side reach run REFUSE (19 pieces leaving BC). After the fix the three
  upper places (Blanchard, the creeks, "mainstem and all other tributaries" with 773) share no section with each other or
  with the downstream place; creeks + mainstem place = exactly the 984 the owner's saves had bound.
- Morice: takes neither Morice Lake nor the Nanika; shares nothing with Gosnell → Lamprey.
- Sheldens: nothing crosses the creek below today's cut in the forest roads, the culvert / obstacle layers or FWA; the
  creek has no tidal piece.
- Files: `marks.jsonl` (append only), `data/generated/review/dfo_closed_area_geometry.json` (the Juniper piece's note
  and its upper-split note).

### New cut proposals after both lanes (`proposals build`)
`split-kasiks_river_hwy_16_bridge_over_the_kasi`, `split-kasiks_river_signs_below_the_kasiks_uppe`,
`split-shuswap_closed_station_as_printed`, `split-shuswap_river_shuswap_falls_wilsey_dam`,
`split-thompson_deadman_upper_signs`, `split-thompson_bonaparte_mouth`, `split-thompson_ashcroft_hwy_97c_bridge`,
`split-thompson_juniper_upper_signs`, `split-thompson_juniper_lower_signs` (the last two new tonight).

### Stopped or held
- **Kitsault tidal boundary name** (note 2682): the row is a mark-written `mouth` row and every mouth row gets the fixed
  label "tidal boundary"; the name needs a code change or a new cut. Nothing changed.
- **Tatshenshini "downstream of the BC/Yukon border" (~2)**: the only other place matching the tight side-stream rule;
  held back: turning side streams off would take it from 6,261 to 44 sections, the owner verified it today (2719), and
  its older wording said "including tributaries".
- **Maria Slough needs no mark**: all six Mission-area Fraser places already bind Maria Slough (8 sections) and the
  gazetted Jeperson line on the side run, "Fraser only" or not (`reach/channels.py` adds them). Strawberry, Nicomen and
  Nelson sloughs stay out (not named by the owner).
- Little Campbell needs no change (it binds no side streams).

### What every place in Regions 2-8 binds: new findings
1. **Kitsumkalum Lake "including tributaries" takes 16 side-channel pieces (14.6 km) of the Kitsumkalum River above the
   lake**: the through-river rule drops only the river's main line. A code fix (the lake must not take its through-river's
   own side channels).
2. **Kitimat west bank**: the 750 m all-year closure also covers 11.9 km of side streams (its tributaries are on), and
   closes the whole width (T754 not built). Follow-up mark below.
3. Minor: the Nass-near-Meziadin lower end is the confluence rather than DFO's signs; the Sumas part runs to the mouth
   rather than to the signs near the Fraser.
Everything else: two-part places bind both parts, nothing outside BC, cut ends and lengths match the words.

### Follow-ups asked of the lane after its report
- Kitimat west bank: held undrawn (a card note in DFO's words) until T754, tributaries off. (result below)
- Skeena sockeye pair, owner 22:12: "this iss handled by rest of option in extents". (result below)
