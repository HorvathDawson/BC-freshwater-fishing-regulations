# Gotchas: things that look like a bug, a change or a duplicate, and are not

A gotcha earns a place here when the evidence misleads: the data says one thing and means another. What the code
handles belongs in its comments. Data files and tests cite these headings: keep them.

## Which "including tributaries" rule a stream answers to

The RULINGS T4, T14, T18 amendments (T1053; all-year part proposed) and T19 (T1177: item 1 is "most upstream wins"):

1. **A stream's own row, or the nearer ✱ row, wins on each thing both speak to** (a quota, a release, gear, bait):
   the Duncan's tributaries keep the Duncan's bull trout 2, not Kootenay Lake's tributaries' release
   (`read.ladder.OWN_ROW_BEATS_INHERITED`, `NEARER_ROW_BEATS_FARTHER`, `OWN_ROW_REPLACES_INHERITED`).
2. **A DATED closure inherited from further down still closes**, even a stream with its own row, unless that stream
   is exempt by a printed lift: the Granby is closed Jul 25-Sep 15 by the Kettle's tributaries' closure.
3. **An ALL-YEAR inherited "No Fishing" yields** to the stream's own water-table row (`r<region>:`, never a zone line)
   or a nearer ✱ row, per fish that row speaks for: Dove Creek reopens for the fish Anderson Lake's ✱ release names
   and stays closed for the rest; Dick Creek, with no row, still closes under Eagle Creek's ✱ "No Fishing"
   (`read.ladder.ALL_YEAR_INHERITED_CLOSURE_YIELDS`; which row counts as "speaking" is still open).
4. **Tributary rules cover streams, never lakes** (RULINGS T1). A walk climbs through a lake to the streams feeding it
   and never covers the lake, except a pond the registry holds as a piece of a stream (an oxbow, a slough lake, an
   unnamed widening): it is the stream. Bowman Slough takes Williston Lake's Zone A rule with its streams.
5. **A whole-water ✱ rule climbs through its head lake** (RULINGS T18): Eagle Creek's "No Fishing" reaches Dick Creek
   above Dick Lake. A stretch rule, a DFO place and a licensing record stop at the lake.
6. **A lake's tributaries are EVERY inflow, through every lake above** (RULINGS T19): its own line's river too, even
   with a row of its own: Chehalis Lake's "No Fishing" binds the Chehalis above it. A stretch "A from x to y" never takes
   A above x; inlet rows = the walk. **A lake row's printed exception also stops the rows of the lakes below it** (owner
   11-27), only on water reached through it: Lake Revelstoke's and Lower Arrow's bull trout release add nothing above
   Kinbasket; John Hart's and Lower Campbell's closures stop at Buttle's "except Thelwood Creek" (16.3 km, Buttle's r2 closes it all year).
   **Kootenay Lake's "to the U.S. border" is read as all upstream of the lake** (36,007 km back in B.C.: Koocanusa, Elk,
   St. Mary): it looks like an over-wide carve-out; it is the owner's reading (11-27). Water joining a lake row's own stream
   ABOVE its lake is the lake's inflow (Whiteswan's feeders, Hatzic Slough's creeks); Upper Arrow's drawdown stretch is the
   lake, so the Columbia above the Hwy 1 bridge is its inflow, climbing to Kinbasket's exception.

## Questions the owner will put to BC Fish and Wildlife

The questions (the book read two ways; the app is built on the stricter reading) are `pipeline/docs/BOOK-QUESTIONS.md`.
The printed errors are `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`.

## Harrison Lake's lake trout show "hatchery only" (a bare "trout" line covers char)

The row reads "No wild trout over 50 cm, 1 bull trout over 60 cm". A "trout" rule covers char unless char are left out,
and RULINGS C2 leaves out only the char the same row gives its own line, here bull trout. So `harrison_lake.r1` is
`TROUT_CHAR` except `DV`, wild, take 0 over 50 cm: it covers lake trout and brook trout too. Every lake trout the zone
lets you keep is over 50 cm, so a keepable one is a hatchery fish. It looks like a parse error and is the book read the
agreed way (owner 2026-11-23). If "trout" here is ever read as trout only: add `LT` (and `EB`) to `species_except` in
region-2.json.

## An "All" closure against a species row on the same DFO place (the Tatshenshini, the Cowichan)

DFO's table puts an `All` row and a species row on one place with no words saying which wins (`pipeline/deliver/salmon.py`
`_all_closure_wins`, RULINGS L24). **The All CLOSURE wins unless the species row is dated inside the All row's dates.**

- Tatshenshini below the border: `All | Jul 24 to Aug 14 | No fishing for salmon` beside `Coho | Apr 1 to Mar 31 | 2
  per day`. The coho row is DFO's all-year default written loosely; the closure wins.
- Cowichan, 66 Mile Trestle to the Sandy Pool: `All | Sep 1 to July 14 | Open for salmon catch and release` beside
  `Coho | Nov 1 to Dec 31 | 1 per day`. Dated inside the All row, the coho row speaks.

Do not reopen the Tatshenshini's coho because its own row says all year, and do not let an All row that is no closure
swallow a short species opening.

## Kept city-layer names that are not real names

Names are shown as sourced (owner rule): city layers keep any feature that carries a name, so some are asset codes or
designations, and some labels sit on the wrong FWA line. Do not drop or rename them.

- Designations: 154 Burnaby "X Trib.N" ("Stoney Trib.3-1-1-2-2-1"); 17 Port Moody and 16 Squamish "Trib" names.
- Codes: Slvt2; Jr 1-1, Jr 4-1, Jr 4-1-1, Jr 4-1-2, JR Trib. 4; Crabtown Creek T1 (Burnaby).
- Not streams or odd spellings (Squamish): Mamquam Blind Channel Wetland; Logger's Lane Connector and Loggers Lane
  Creek; Mashiter Side Channel.
- Labels on the wrong line: Evans Creek on the Squamish River; Dragonfly Creek on Still Creek; Lost Trib.1 on Austin
  Creek; Stoney Trib.4 on Stoney Creek. Several names on one unnamed line: Thunderbird + Newport (`360305166`); Beaver
  Creek + Beaver Creek BioPond North + Deer Lake Brook (`356297118`); Beecher + Sunken Engine (`356093488`). Tunnel,
  Turner and Capitol creeks were filed under the Seymour, Noons and "Squatters" creeks by a code fault (T1246).

## Book errata: an MU printed wrong (Mackenzie Creek "4-21" is 4-31)

The book prints MACKENZIE CREEK ("No Fishing", p.38) under MU 4-21; the only Mackenzie Creek is in MU 4-31
(`gnis:12042`). The fix is an override in `data/curated/regulations/book_name_links.json` keyed on the row as printed;
the entry id keeps the printed MU (`r4:mackenzie_creek@4-21`). Do not "correct" the id or the MU in the corpus. Known
errata: **Mackenzie Creek MU 4-21 -> 4-31.** Not found and not errata (`not_found` overrides): Frog Lake (MU 5-6),
Redfern Lake (MU 5-15), Squirrel Lake (MU 6-1).

## Two rows the province's newer book lost (Little Qualicum River, Lizard Lake): dropped, T1168, T1179

Our copy (2025-11-18) prints LITTLE QUALICUM RIVER (1-6) and LIZARD LAKE (1-3); the gov.bc.ca copy edited 2026-08-25
does not (rendered pp. 18-20; no notice says why) and prints PUNTLEDGE RIVER twice (pp. 19, 20). Owner, 2026-11-26: adopt
the new book by hand, "drop the dropped rows by adding a drop boolean to the entries witrh a note why". So the two
entries stay in the corpus with `drop: true` and a note (when the row was last printed, which book lost it); nothing
deletes them and a re-read must not mint a second Puntledge. Until T1179 lands they are still live. The Province is
asked in `BOOK-ERRORS-FOR-THE-PROVINCE.md` (entry 17).

## Book errors we found, for the Province

Every printed error or loose print is one entry of `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md` (region, row, page,
what is printed, what is wrong, what we do meanwhile, evidence, date, status). Add one there first; the entries below
say how the DATA treats the ones that look like a bug.

## Stellako River opens May 15 (its dates replace the spring closure), P7a

The row prints "No Fishing Nov 15-May 14"; the neighbours print "Nov 15-June 30", so May 14 is read as a replacement
for the spring closure. Owner: "yes to open since it seems to be a replacement closure time to the springf closure."
The Fraser-watershed (Region 6) and Zone A (7A) closures lift from May 15: built in T1177 (RULINGS L27), checked on every section of both parts.

## Region 5's spring closure is lifted only by a row's own closure or "open all year" (P7b)

The closure (p.42) excepts "the mainstem of the Fraser River and other streams listed in the tables". We do not know which
rows count: the tables print no "exempt" line. Owner, strictest reading: lifted ONLY where a row prints its own closure or "open
all year" (whole or part), on that stretch (RULINGS L28); a row with only gear or quota rules keeps it, so Cariboo, the Quesnel below
the Cariboo, Moffat, McKinley, Blue Lead, Little Horsefly, Mitchell and Canim are closed Apr 1-Jun 30 (about 430.8 km). What we are unsure of, asked of the Region 5 office
(BOOK-ERRORS entry 19): whole stream or only the stretch the row describes; park and chain-of-lakes rows; tributaries of a
listed stream. Looks like a wrong closure on a listed stream: it is the strict reading, not a bug.

## Hevenor, Station and Two Mile creeks: trout 1 all year, and the Nov 1-Jun 30 release stays (P7c)

"Open all year; trout daily quota = 1 (no minimum size)" sits under Region 6's "release trout from streams Nov 1-Jun 30".
On Jan 1 both "keep 1" and "release all" speak. Owner: "keep release add to gotchas". The release holds Nov 1-Jun 30.

## Goat River's garbled catch-and-release line is read as the stretch between two creeks (O10)

The row prints "Trout/char catch and release (mainstem only) Leadville Creek Cameron Creek" (Region 4, p.37) with no linking word; the
Findlay Creek row prints "(mainstem only) from ... to ...". Owner: "between the 2 only with a gotcha added". Built (T1177): catch and release
ONLY on the main stem between Leadville Creek (km 33.1) and Cameron Creek (km 42.6); the rest of the main stem takes the regional quota; the
creeks get nothing extra. Before this the whole Goat (86.3 km: the 77.3 km main stem plus 9.0 km of side channels) was catch and release; now 9.5 km keeps it and 76.8 km (67.8 km main stem + 9.0 km side channels) takes the region's quota. A reading, not the book's words: asked of the Province
(BOOK-ERRORS entry 11); evidence `one-offs-evidence/A-goat-river.md`.

## Mine Lake takes Main Lake's rules because FWA calls it Main Lake (O11)

On Quadra Island FWA gives the Mine Lake polygon (wbk:329095512) the name "Main Lake" and the same GNIS id (12126) as Main Lake proper; the gazette's
Mine Lake is an alternate name for that part of Main Lake. So the Main Lake row (bait ban, single barbless hook Nov 1-Apr 30, engine 7.5 kW) binds
both polygons, plus Mine Lake's own engine row. Owner: "keep on both add to gotchas". Not a bug (`test_haynes_and_mine_show_the_engine_limit_once` pins it).

## Hay River basin keeps the stricter rule against the p.63 map (O12)

The Region 7B row "LIARD RIVER WATERSHED (see map on page 63)" (p.65) points to a map that draws everything north of its
dotted line, the Hay basin's Kotcho Lake and Kotcho River included, as "Liard River Watershed" (Province list entry 12). Data uses the true river basin, so all bull trout are released there. Owner: "we
will not add hay basin in for now. leave it stricter". Not an error in the data.

## Two Maka creeks, one rule on both (O14)

"No trout under 25 cm" (Region 3) is bound to both Maka creeks, which are in the same MU. The curated "most likely" is
the Spius Creek one. Owner: "keep on both because they are same MU and everything". Do not remove it from the other.

## Nanaimo River's struck-through "15" (O17)

The row prints "Sept 1-Oct ~~15~~": a 0.14 pt black line (a drawn line, not text) runs through the 15, nothing in its place. Data: Oct 15. Owner: "it is
probably a format issue and is okay". A real end date from the Province would replace it.

## A water's undated hatchery-steelhead quota does not lift the zone's Nov-Jun release (the Kitimat)

The Kitimat prints "Hatchery steelhead (adipose clipped, >50 cm) daily quota = 2" with no dates (p.51); Region 6
prints "you must release: … Trout of any size from streams, Nov 1-June 30" (p.49). Owner ruling: not lifted. Hatchery
steelhead there are catch and release Nov 1-Jun 30 and the 2 applies outside it; only "all year" lifts a dated zone
release (`test_answers_kitimat_steelhead_lift.py`). Keeping one also counts toward the 1-over-50 cm trout limit.

## A row's closed season does not shorten the zone's (the Stein, the Nahatlatch)

The Stein prints "No Fishing Jan 1-May 31"; Region 3 closes every stream Jan 1-June 30. Both hold, closed on the union.
The p.28 line listing "Nahatlatch River … and Stein River: from Jan 1-May 31" sits under Steelhead Management Changes
and is no exception list. A row beats a zone closure only where the book prints an exemption, or a dated release,
opening or quota inside the closure: then on exactly those dates, for those fish (AGENTS 57). Every overlap is listed in
the export's `guide.gotchas.closures_combine`, and the reader decides what each note claims. Before adding a lift, find
the printed words; a row's own season is never them.

## A dated bait ban DOES replace the zone's all-year one (Quatse, Somass, Sproat, Stamp)

The opposite of the entry above. Region 1's bait ban is "all streams of Region 1, all year, with some important
exceptions. Check the tables."; the Quatse prints "Bait ban, May 1-Nov 30", so bait is allowed there Dec 1-Apr 30
(`quatse_river.r4x`). A closure never works this way; a bait ban with "check the tables" does (RULINGS G5; export
`gotchas.dated_bait_ban_replaces_zone`).

## The same closure appears twice, under two sign counts (Skeena @ Kispiox)

DFO words the closure at the Kispiox confluence three ways over the years: "the 4 triangular", "3 white triangular"
and "three white triangular" signs. **They are one zone**, bound to one pair of cuts
(`skeena_river__kispiox_sign_zone_lower` / `_upper`). A DFO locator id is a fingerprint of `section|water|specific_area`,
so re-worded scope text (even a trailing `.`) mints a new locator that looks like a new regulation. Same water, same
section, same rules, landmark in the same place: one zone, recorded with `duplicate_of` + `duplicate_confirmed`. The
400 m either side of the confluence is an assumption (the radius of comparable Region 6 mouth closures);
`spatial_caveat` stays set until someone pins the signs.

## Sheldens Creek is a name we assumed

DFO's Region 6 rules name "Sheldens Creek" (Moresby Island); no FWA water carries the name. Blue line 360846083
(`wsc:950-016367`) into Sheldens Bay carries it, because the forest roads around it do ("SP 19(1) TO SHELDONS",
SHEL100-103). The owner accepted it at low confidence; no map, gazetteer or DFO source names the line. It is a
`name_variants.json` record (so the line and the hand-drawn estuary channel are one item); the
`found_name_calls.json` accept stays. If a source names another stream "Sheldens Creek", recheck this first; never
treat it as confirmed in audits or matching.

## The Kasiks "Upper Pool" is a place we inferred

DFO splits the Kasiks for coho at "fishing boundary signs located below the Upper Pool"; no source names or places the
pool. The proposed cut (`kasiks_upper_pool_signs`) takes it as the pool at the foot of the river's one barrier, the
canyon falls at FWA km 17.6-18.4, with the signs at its tail-out (km 17.65 on blk 360879632; medium confidence; the
weaker alternative is the confluence pool about 300 m lower). A mismatch with signs on the ground means the inference
was wrong. Replace it only with a stated sign position (DFO Terrace or Prince Rupert), never another guess.

## DFO place words that do not fit the ground, kept as they are on the owner's word

Each looks like a labelling mistake when measured; each was shown to the owner and kept. A change needs a new source
and his word.

- **Kanaka Creek, "112th Street bridge"** (2): the only crossing is 112 Avenue; the cut sits there.
- **Kispiox, "signs near the Kispiox River Resort"** (6): placed at the Kispiox River Lodge, 13.7 km up, low
  confidence, until DFO Smithers confirms.
- **Lower Shuswap closed stretch, "one km"** (8): the printed points are 1,738 m apart in a line and 2,075 m along the
  river; the printed points stand.
- **Stamp Falls** (1): "500 m below the fishway" and "225 m below Deer Creek" disagree; the fishway measure is kept.
- **Somass, "Paper Mill Dam upstream approximately 0.5 km to Collins Farm"** (1): the named ends, about 1.3 km apart,
  win over the distance.
- **Thompson closed area at Juniper Beach, upper end** (3): "approximately 4 km" above the lower signs is used, not the
  point 1.5 km below Juniper Beach Park (it errs toward closed).
- **Bulkley, two "all tributaries" rows** (6): one leaves out only the Suskwa, the other the Morice, the Suskwa and Two
  Mile Creek. They are two places, read word for word.
- **Sheldens Creek, the bridge** (6): the creek is drawn on by hand (added stream `-2400000001`, source `manual`,
  `data/source/municipal/manual_streams.geojson`) from the FWA line's mouth through the salt marsh to the SPUR 14 road
  crossing near the sea (53.14746 N 131.76003 W), with the tidal limit a `cut` at that bridge
  (`waters/tidal_limits.json`, low confidence). Which marsh channel carries it is a reading of imagery. The literal Spur
  19 crossing, 3.4 km up, keeps the unbound cut `sheldens_creek__spur_19_bridge_site` should DFO place the signs there.
