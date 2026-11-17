# General audit, checker "labels": does the released data hold what the owner's DFO labels say?

Saved by the main chat from the checker's hand-back (the checker could not write files). Read-only. Head d0d584159,
release `data/release/r-46e4355a7277` (LATEST), reach run `data/generated/reaches/full` (digest 11eb6853dff81815),
atlas handles 55032563b98ee973. Regions 1, 2, 3, 4, 5A, 5B, 7, 8, plus the nine Region 6 places of the Claude-saved
marks. Scripts and evidence: this folder (`base.py`, `cmp_marks.py`, `answers.py`, `scan_all.py`, `dossier.py`,
`ranges.py`, `show.py`; outputs under `tmp/`). `graph.pkl` was never loaded.

Tags: **[ran]** a script run on the files, **[read]** read in data, code or DFO's cached page, **[inferred]**.

## Count checked
- 124 places read one by one: 75 with a last save on or after Nov 13, 40 older (seeded random sample), 9 Region 6
  places of the Claude-saved marks.
- Machine checks: file against save for all 309 marked places; release against stored DFO rows on every section,
  5 fish, 366 days, for 203 places; cut-to-cut ranges for 104 extent pieces; all 39 rows the reach table did not
  bind; the 4 bound closed areas and Babine Lake.

## The short answer
- The place files hold what the last saves say: 0 differences over all 309 marked places. [ran]
- The release carries the reach table: 366 of 369 rows identical (plus `behind`); the 3 others explained. [ran]
- **No place was found where the app says open and that place's own stored DFO row says closed**, except two where
  a narrower DFO opening answers first (Fraser Mission to Hope in November; the Atnarko coho exception). [ran]

## Findings, most serious first

### 1. Fraser, Mission to Hope: open now, on an "unsure" save (toward open; the owner's choices; no defect)
- `2:fraser-river:mainstem-waters-downstream-of-the` (live): coho Nov 7-30 "2 per day, hatchery marked only"
  (FN1217); chum Nov 3-30 "2 per day" (FN1179). `2:fraser-river:mainstem-waters-upstream-of-the-cp~2` (live):
  "All | Apr 1 until further notice | No fishing for salmon".
- Line 2568 **unsure** is the opening, between the CPR bridge at Mission cut (m 76,506) and the Hwy 1 bridge at Hope
  cut (m 158,751), Fraser item, tributaries off. Line 2569 verified is the closure, Mission to Spuzzum Creek
  (m 196,967). Both files equal their saves. [ran]
- Release: the opening binds 62 sections, the closure 66; the 62 are a strict subset, so the opening answers coho and
  chum on its dates, the closure every other day and fish. On `356364114:76506`: coho closed to Nov 6, open
  Nov 7-30 ("2 a day, hatchery-marked only" in the limit tree), closed from Dec 1; chum open Nov 3-30; chinook
  closed all year. The 4 sections from Hope to Spuzzum stay closed. [ran]
- 25 of the 62 open sections are not the main channel: Maria Slough 8, Herrling Island Side Channel 7, Jeperson Side
  Channel 6, Seabird Island North Side Channel 4. DFO's words are "mainstem waters". The owner's recorded choice
  (mark 2578 note; OWNER-TODO "Add all five (Recommended)"). [read]
- DFO's words for the opening are garbled ("downstream of the CPR Bridge at Mission to the Highway 1 Bridge at
  Hope", page dated 2026-11-06), read as Mission up to Hope; the binding note says the owner confirmed 2026-11-03.
- Recommended: nothing to fix; put line 2568 FIRST on the re-tick list (the one open answer in these regions that is
  live this month and rests on an unsure save).

### 2. Regions 7 and 5A: stored in-season openings are shown closed all year (reverse direction; a decided rule)
- `7:nechako-river:from-the-downstream-edge-of-the-fo`: sockeye Aug 22-Sep 21 "2 per day, bait ban"; pink "4 per
  day" (FN0851); save 2644 verified. `5a:quesnel-river:that-portion-of-the-quesnel-river`: sockeye Aug 22-Sep 15;
  pink Aug 22-Sep 30 (FN0846); save 2580 unsure. `5a:quesnel-lake:that-portion-of-quesnel-lake-known`: sockeye
  Aug 22-Sep 15; not drawn, no mark.
- Release: closed every day, text "There is no fishing for salmon in Region 7 until further notice." (5A the same).
- Why: `deliver/salmon.py` `location_rules` replaces a place's rows with the header closure when the page header
  carries one (`quota.header_closure`, "owner 2026-10-31: the header closure wins"). [read]
- DFO's page carries both sentences: the header's "no fishing … until further notice" and, above the table, "There
  is no fishing for salmon in Region 7 except for the opportunities listed in the table below". The page cached
  Aug 29 shows the header and the open rows together, inside the opening's dates. [read]
- Effect: none today. Next Aug 22 the app would say closed where DFO opens sockeye and pink.
- Owner decision. Recommended: a dated table row that carries a fishery notice answers on its own dates; the header
  closure answers every other day and fish. One ticket; the live feed must read the same way.

### 3. Atnarko below the Tweedsmuir Park Lodge sign: the coho exception also opens 17 side-stream sections
- `5b:atnarko-river:downstream-of-the-fishing-boundary` ("Coho | Jan 1 to Oct 15 | 4 per day, only 2 over 50 cm").
  Line 2584 **unsure**: downstream of the lodge-sign cut (m 2,339), tributaries on. File = save. [ran]
- Release: 18 sections: the river's lowest 2.3 km and 17 side-stream sections (16 unnamed, Corbould Creek 1). Coho
  open on them Jan 1-Oct 15; under the whole-river row they would be closed. [ran]
- DFO's heading is "Atnarko River including tributaries", so keeping side streams is a fair reading; the exception's
  own words name only a point on the river.
- Owner decision. Recommended: side streams off for this stretch (errs closed, 17 sections).

### 4. Shuswap (lower): the cut is the Mara Bridge; DFO says "signs above Mara Bridge"
- `8:shuswap-river:lower-upstream-from-white-triangul` ("Chinook | Aug 16 to Sep 12 | 4 per day, only 2 over
  50 cm"). Line 2689 verified; cut `shuswap_river__mara_bridge` at m 24,787, the bridge itself.
- Release: chinook open Aug 16-Sep 12 from the bridge to Mabel Lake (18 sections, the Trinity Valley bridge 100 m
  closed inside). If the signs stand above the bridge, the water between is shown open and is not. [inferred]
- Recommended: the owner says whether the signs are at the bridge; if not, a cut at the signs.

### 5. The four bound closed areas stand on "verified" saves written by Claude; two are outside lines 2770-2794
- Lines 2748 (Thompson, Deadman), 2749 (Thompson, Ashcroft), 2770 (Shuswap), 2785 (Thompson, Juniper): verdict
  "verified", reviewer "Claude (for Dawson)". `closed_areas.json` equals the four saves. [ran]
- Other Claude-written last saves before line 2770: 2672 (Stave, unsure), 2730 and 2731 (Bella Coola, **verified**);
  Region 6: 2728 and 2732-2736 (unsure), 2738 and 2739 (**verified**).
- All four closed areas err closed. Two are low confidence by their own notes: Juniper's upper end is 4 km above
  DFO's printed lower point, by the words; the Shuswap stretch is 2,075 m between DFO's two printed points, where the
  words say "one km".
- Recommended: the re-tick list takes every Claude-written last save: add 2672, 2730, 2731, 2748, 2749 and Region
  6's 2728, 2732-2736, 2738, 2739.

### 6. Sheldens Creek (Region 6, line 2783): 3.4 km went from coho open to coho closed on a hunch (reverse)
- DFO: "upstream of white triangular fishing boundary signs located at the Spur 19 Bridge site", "Coho | 0 per day".
  Save 2783 (unsure): the whole creek, on the owner's "i think this is tidal bound maybe".
- Release: coho closed all year on all 4 sections. Before, only the top 0.29 km was closed; below the cut, section
  D's "Coho | Apr 1 to Oct 31 | 4 per day, only 1 over 50 cm" answered. Needs the owner's tick.

### 7. Three live lake parts are not drawn and have no mark (lake splits deferred)
Mabel Lake (a chinook opening shown only as a note; the lake reads closed), Osoyoos Lake north basin, Quesnel Lake
Horsefly Bay. None in the dangerous direction.

### 8. Known items, confirmed (not new)
- Audit tool false positives (T776): the "not_last_save (5)" rows are false; the files hold the bounds and listed
  places exactly as lines 2790-2794 say. [ran `cmp_marks.py`]
- Four "wrong" last saves whose extent ships: Sproat (2468) holds the asked fix (offset 350 m, built at m 1,874);
  Stamp Falls (2469): the cut names carry "site 17" and "Deer Creek"; the dormant Thompson wording (2521) is
  answered by the three closed areas; the dormant Region 3 Fraser mainstem (2578) is the owner's open question.
- Somass: the finfish closure above Papermill Dam is 1,304 m; DFO says "approximately 0.5km". The owner's pin, noted
  "do not move it". Errs closed, Aug 25-Nov 15.
- Little Qualicum: the "falls to the hatchery fence" closure starts at the park's lower boundary, not the falls. No
  answer changes.

### 9. For the answers checker
A verdict's text `t` drops "hatchery marked only": Fraser coho reads `"t": "2 per day (Nov 7–30)"`; the condition
is in `tree.says` ("2 a day, hatchery-marked only"). If any screen shows `t` alone, the condition is lost. [ran on
the release; not checked in the app]

## The Claude-saved marks 2770 to 2794, each against DFO's words (file = save for every one)

| line | place | stored and bound | release (Jul 15 / Sep 15 / Nov 15) | read |
|---|---|---|---|---|
| 2770 | Shuswap closed stretch ("one km") | gauge 08LC003 (m 135,155) to DFO's printed point (m 133,080): 1 section, 2,075 m | closed all year, all fish | as the owner said; low confidence |
| 2771-2773 | Somass, Stamp, Little Qualicum "except" | superseded by 2787-2789 | | |
| 2774 | Cowichan, Sandy Pool sign to tidal boundary | downstream of the sign cut m 20,037, tributaries off: 9 own sections | no row / closed / closed (All Sep 1-Nov 15); coho from Nov 16 | fits |
| 2775 | Cowichan, 66 Mile Trestle to Sandy Pool | between m 30,530 and m 20,037: 2 own sections | no row / release / coho open, others release | fits |
| 2776 | Nanaimo, Cedar Road bridge to Hwy 1 | between m 2,979 and m 10,455: 4 own sections | coho closed / closed / open (Oct 16-Dec 31) | fits |
| 2777 | Nanaimo, 500 m below Bore Hole to Boulder Creek | between m 12,485 and m 22,348: 2 sections | closed all year | fits |
| 2778 | dormant twin of 2777 | between m 12,985 and m 22,348: 1 section | says nothing | fits |
| 2785 | Thompson, Juniper closed area | m 93,016 to m 97,016: 1 section | closed all year | upper end by the words; low confidence |
| 2787 | Somass "Except in those areas and times listed below" | rest, no bounds: 0 own; behind the 3 live places on all 5 sections | chinook and coho open Aug 25-Dec 31; the 1.3 km above the dam closed Aug 25-Nov 15, open from Nov 16 | fits |
| 2788 | Stamp "Except…" | rest: 2 own, behind 5 listed | closed / open / open; Stamp Falls closed to Nov 15 | fits |
| 2789 | Little Qualicum "all waters unless indicated below" | rest: 5 own, behind 2 listed | closed / chinook open / chinook and coho open | fits |
| 2792 | Nitinat "downstream of Parker Creek, except…" | rest, bounds downstream of m 40,925, 3 listed: 5 own, behind 3 | closed / chinook and coho open / coho open; bridge 100 m closed to Oct 15 | fits |
| 2793 | Dean "Crag Creek … to the tidal boundary, except for the canyon" | rest, bounds downstream of m 47,833, 1 listed: 20 own, behind the canyon's 2 | open / open / closed; canyon closed all year | fits |
| 2794 | Puntledge below the hatchery signs (dormant) | rest, bounds downstream of m 3,926, Morrison closure listed: 2 own | says nothing | fits |
| 2779 | Tatshenshini, Blanchard River | Blanchard whole, tributaries on: 287 sections | coho open Aug 15-Jul 23; chinook and sockeye closed | no answer depends on the side streams |
| 2780 | Tatshenshini, Kwatini / Stanley / Goat creeks | the three creeks whole: 211 sections | closed all year | fits |
| 2781, 2782, 2784, 2786, 2790, 2791 | Morice, Skeena pink, Babine Lake, Kitimat west bank, Skeena sockeye pair | file = save | | the Skeena cascade review, Part 2 |
| 2783 | Sheldens Creek | the whole creek: 4 sections | coho closed all year | finding 6 |

## Not bound (Regions 1 to 5B, 7, 8)

| place | status | reach | why | owner's choice? | what the release does |
|---|---|---|---|---|---|
| `1:nanaimo-river:in-subarea-17-14-tidal-portions-of` and `~2` | live | `tidal_water` | wholly below the Cedar Road bridge | yes: marks 2473, 2474 | in no chain; `off` as tidal |
| `1:somass-river:except-in-those-areas-and-times-li` | live | `unresolved` / `no_sections` | a rest-of whose listed places take all of it | yes: mark 2787 | in 4 chains behind its listed places; it answers |
| `1:san-juan-river:…` (2) | dormant | `no_extent` | never placed | no mark | undrawn part; no rules |
| `2:birkenhead-river`, `2:brunette-river`, `2:inch-creek`, `2:pitt-river` (3), `2:sakinaw-lake` (2), `2:seymour-river:from-the-seymour-river-dam` | dormant | `no_extent` | never placed | no mark | undrawn parts; no rules |
| `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` | dormant | `no_extent` | part of a lake | lake splits deferred | undrawn part; no rules |
| `5a:quesnel-lake:that-portion-of-quesnel-lake-known` | **live** | `no_extent` | part of a lake | lake splits deferred | undrawn part; closed all year by the 5A header (finding 2) |
| `8:mabel-lake:northerly-of-a-line-drawn-from-a-w` | **live** | `no_extent` | part of a lake | lake splits deferred | the lake reads closed; "Chinook Aug 16-Sep 12, 4 per day" is a note |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` (and dormant twin) | **live** | `no_extent` | part of a lake | lake splits deferred | the lake reads closed; "no fishing for sockeye" is a note |

All 352 places are in the payload (a chain's `l` or `u`, or `off`). No place is "carried" in these regions.

## Closed areas: a section inside each

| closed area | section | span | chinook, coho, sockeye on the three days | from |
|---|---|---|---|---|
| Thompson, Deadman | `356364690:112992` | m 112,992 to m 116,608 | closed, all nine | the piece |
| Thompson, Juniper | `356364690:93015` | m 93,016 to m 97,016 | closed, all nine | the piece |
| Thompson, Ashcroft | `356364690:78341` | m 78,342 to m 80,683 | closed, all nine | the piece |
| Shuswap | `356363194:133080` | m 133,080 to m 135,155 | closed, all nine | the piece |
| Babine Lake, 13 areas | `lake:329026676` | each `closure_carried`, 0 sections | lake closed on the three days; sockeye **open Aug 1-27** | each area an undrawn part; the lake's drawn answer does not change inside them |

Each bound piece is first in its chain (`piece: 1, bound: 1`). The Thompson place around the pieces reads "check"
for chinook and sockeye ("To be determined") and closed for coho.

## Checked and found clean
- File against save, all 309 marked places: 0 differences (op, cuts, item, tributaries, tributaries-only, excepted
  tributaries, rest-of bounds and listed places; 60 new cuts within 60 m of the stamped point, except one Region 6
  dormant place built on another blue line of the same confluence).
- `apply --check`: every mark in force has landed, apart from the two T359 tidal waits.
- Cuts and ranges: 104 extent pieces; the bound sections run exactly from cut to cut (within 2 m); 10 rows read by
  hand (6 tidal subtractions, De Boville Slough, 3 two-line cases incl. the Dean canyon, about 1.5 km as the words).
- Lake-end cuts (Harrison, Shuswap lower and middle, Stave, Thompson): each took the lake end nearest the stretch.
- Release against the stored DFO rows, 203 places, 5 fish, 366 days: 164 agree everywhere; 21 not drawn; 18 differ,
  all explained (6 defaults under their waters; 8 narrower closures answering first; 2 narrower openings: Fraser,
  Atnarko; 2 closed by the header closure). No wider place ever answers over a narrower one.
- Side streams under an open rule (11 live places): each grounded in DFO's words, except the Fraser's own channels
  (the owner's choice).
- Words with an exception (15 live places): each held as a rest-of, a closed-area piece, or by leaving the excepted
  water out.
- Live places with no typed rule: 0. Dormant places with a rule: 0.

## Not checked
- Typed rows against DFO's live pages today (cached pages read for a few waters only).
- Where signs really stand (Mara Bridge, Juniper, Deadman's CN bridge, the Atnarko lodge sign, Goldpan, Lillooet).
- The app's screens. Gear lines; pink and chum by eye.
- The 51 marked places outside the sample, beyond the machine checks and the 22 that open a fish.
- Region 6 beyond the nine places named; the live feed and worker.
