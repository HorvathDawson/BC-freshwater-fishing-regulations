# Gotchas: things that look like a bug, a change or a duplicate, and are not

A gotcha earns a place here when the evidence misleads: the data says one thing and means another. What the code
handles belongs in its comments. Data files and tests cite these headings: keep them.

## Which "including tributaries" rule a stream answers to

The RULINGS T4, T14 and T18 amendments (T1053; the all-year part is proposed until the owner reads its list):

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

## Questions the owner will put to BC Fish and Wildlife

The questions (the book read two ways; the app is built on the stricter reading) are `pipeline/docs/BOOK-QUESTIONS.md`.

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
  Creek + Beaver Creek BioPond North + Deer Lake Brook (`356297118`); Beecher + Sunken Engine (`356093488`). The Seymour
  River item holds the city's Tunnel Creek (`-2000000390`).

## Book errata: an MU printed wrong (Mackenzie Creek "4-21" is 4-31)

The book prints MACKENZIE CREEK ("No Fishing", p.38) under MU 4-21; the only Mackenzie Creek is in MU 4-31
(`gnis:12042`). The fix is an override in `data/curated/regulations/book_name_links.json` keyed on the row as printed;
the entry id keeps the printed MU (`r4:mackenzie_creek@4-21`). Do not "correct" the id or the MU in the corpus. Known
errata: **Mackenzie Creek MU 4-21 -> 4-31.** Not found and not errata (`not_found` overrides): Frog Lake (MU 5-6),
Redfern Lake (MU 5-15), Squirrel Lake (MU 6-1).

## Two rows the province's newer book lost (Little Qualicum River, Lizard Lake), T1168

Our copy (2025-11-18) prints LITTLE QUALICUM RIVER (1-6) and LIZARD LAKE (1-3); the gov.bc.ca copy edited 2026-08-25
does not (rendered pp. 18-20; no notice says why) and prints PUNTLEDGE RIVER twice (pp. 19, 20). We keep both rows: a
re-parse of the new copy must not drop them or make a second Puntledge. Sent to the province with the other questions.

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
