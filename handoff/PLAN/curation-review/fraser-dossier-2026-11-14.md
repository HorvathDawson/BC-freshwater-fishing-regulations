# Fraser DFO dossier (2026-11-14, T743)

For labelling the Fraser's DFO places one at a time. Read-only on marks: nothing here changed a mark,
`marks.jsonl`, `cuts.json` or `dfo-suggestions.json`. River km are FWA route measures on the Fraser main line
(blk 356364114), from the atlas the release was built on (`atlas/full`, release r-f749475d0962).

## Do these first

1. **Re-mark the three "Mission to Hope" wordings as CPR Bridge at Mission → Highway 1 Bridge at Hope**, no
   tributaries ("mainstem waters"). All three now say Mission → Spuzzum Creek, which is 38 km too far (Hope is
   km 158.8, Spuzzum km 197.0). Two of them were written by one group click (lines 2554, 2555), the third by hand
   (line 2552). Wordings: `mainstem-waters-downstream-of-the` (the LIVE one), `from-the-downstream-side-of-the-cp`,
   `mainstem-waters-from-the-downstrea`. Your own earlier answer (line 2546, Mission → Hope, unsure) was right.
2. **Decide before the next release**: the places file now draws the live "Mission to Hope" place (chum 2 a day
   Nov 3–30, coho 2 hatchery a day Nov 7–30) on Mission → Spuzzum. It ties with "upstream of the CPR Bridge"
   (no fishing for salmon) on the same sections and wins the tie by name order, so **Hope → Spuzzum (about 38 km)
   would show chum and coho open where DFO says no fishing**. The Hope cut does not exist yet, so the right
   extent cannot be drawn until an atlas build (see 4). See "Rule side" below.
3. **Re-check the two "upstream of the CPR Bridge at Mission" wordings** (lines 2547/2548, one group click):
   Mission → Spuzzum is the reading the suggester now gives too (a Region 2 page ends at Region 2's line).
   Both are now "contested" and in no group (line 2560 wiped their real link, T742 file defect 4).
4. **Accept the Highway 1 bridge at Hope cut** (and decide Alexandra, below). Accepting a new cut writes it into
   `cuts.json`; the atlas is then behind its inputs and the next release needs an atlas build + tiles before the
   place can be drawn.
5. Owner's calls listed under "Calls only you can make".

## The places (12: 10 on the Fraser mainstem, 2 on Nicomen Slough)

Dates are the page's own "date modified"; the history cache holds 12 Region 2 and 2 Region 3 page copies
(`cache/dfo_salmon/history/`). No Fraser mainstem place exists on DFO's Region 5 (5A/5B) or Region 7 pages in any
cached copy; DFO's Region 6 page has one closure for the "Fraser River Watershed" in Region 6 (area, not the river).

| # | location id (short) | status | DFO's wording | pages that carried it (date modified) | what DFO said |
|---|---|---|---|---|---|
| 1 | `2:…upstream-of-the-cp~2` | **active** | mainstem waters upstream of the CPR Bridge at Mission | 2026-11-02, 2026-11-06 (now) | all salmon: no fishing, Apr 1 until further notice |
| 2 | `2:…upstream-of-the-cp` | dormant | mainstem waters upstream of the CPR Bridge at Mission, BC | 2025-04-01 → 2026-10-16 (5 copies) | the same rule |
| 3 | `2:…mainstem-waters-downstream-of-the` | **active** | mainstem waters downstream of the CPR Bridge at Mission to the Highway 1 Bridge at Hope | 2026-11-02, 2026-11-06 (now) | chum 2/day Nov 3–30 (FN1179); coho 2/day hatchery marked Nov 7–30 (FN1217) |
| 4 | `2:…mainstem-waters-from-the-downstrea` | dormant | mainstem waters from the downstream side of the CPR Bridge at Mission, BC to the Highway 1 Bridge at Hope, BC | 2024-11-08 | bait ban, chum 2/day, coho 2 hatchery/day, Nov 3–30 (FN1164/1165) |
| 5 | `2:…from-the-downstream-side-of-the-cp` | dormant | from the downstream side of the CPR Bridge at Mission upstream to the Highway 1 Bridge (bridge, 2022) at Hope | 2022-10-28, 2023-09-13, 2025-08-28, 2025-11-03 | 2022: Nov 3–30 chum/coho 2, rest released (FN1226); 2023: Sep 1–21 pink 2, bait ban (FN0915); 2025: pink 4 Aug 22–Sep 21 (FN0841), coho 2 hatchery Nov 3–30 (FN1171) |
| 6 | `2:…from-the-downstream-side-of-the-cp~2` | dormant | from the downstream side of the CPR Bridge at Mission upstream to the Highway 1 Bridge at Hope **and** from the Highway 1 Bridge at Hope to the downstream side of the Alexandra Bridge | 2025-08-28, 2025-11-03 | sockeye 2/day Aug 22–Sep 1, daylight (FN0841) |
| 7 | `2:…from-the-highway-1-bridge-at-hope` | dormant | from the Highway 1 Bridge at Hope to the downstream side of the Alexandra Bridge | 2023-09-13, 2025-08-28, 2025-11-03 | 2023: Sep 13–25 pink 2, bait ban (FN0990); 2025: pink 4 Aug 22–Sep 25 (FN0841) |
| 8 | `2:…upstream-from-the-highway-1-bridge` | dormant | upstream from the Highway 1 Bridge at Hope | 2022-10-28 | no fishing for salmon until further notice |
| 9 | `3:…mainstem-waters-of-the-fraser-rive` | dormant | Mainstem waters of the Fraser River (Region 3 page) | 2025-04-01 | no fishing for salmon Apr 1–Mar 31 |
| 10 | `3:…in-that-portion-of-the-fraser-rive` | **active** | from the confluence with the Seton River downstream to the fishing boundary signs … approximately 4 km downstream of the town of Lillooet | 2026-08-14 (now) | sockeye "to be determined"; pink no fishing |
| 11 | `2:nicomen…siddle-bell` | active | Nicomen (including Dewdney) Slough: from the confluence of Siddle (Bell's) Ck. downstream to the Fraser | 2022-10-28 → 2026-11-06 | (slough rules; not reviewed here) |
| 12 | `2:nicomen…hwy-7-bridge-at-dewdn` | active | Nicomen Slough: from the Highway 7 bridge at Dewdney downstream to the Fraser | 2022-10-28 → 2026-11-06 | (slough rules) |

### Their binding, suggestion and mark now

"Suggestion now" is the live `dfo-suggestions.json`; "with T743" is the fixed suggester (below). Mark = the latest
mark in force (marks.jsonl line numbers, 1-based, as in the T742 file).

| # | binding in the places file | live suggestion | with T743 | latest mark | should be |
|---|---|---|---|---|---|
| 1 | Mission → Spuzzum | upstream of Mission (low) | none (bound) | 2547 verified Mission → Spuzzum, **group** | Mission → Spuzzum (owner's call: Region 2 only) |
| 2 | Mission → Spuzzum | upstream of Mission (low) | none (bound) | 2548 verified Mission → Spuzzum, **group** | same as 1 |
| 3 | Mission → Spuzzum **wrong** | Mission → Hope (low) | none (bound) | 2555 verified Mission → Spuzzum, **group**, replaced 2546 | **Mission → Hope**, no tributaries |
| 4 | Mission → Spuzzum **wrong** | Mission → Hope (low) | none (bound) | 2552 verified Mission → Spuzzum | **Mission → Hope**, no tributaries |
| 5 | Mission → Spuzzum **wrong** (DFO dup of 3) | Mission → Hope (low) | none (bound) | 2554 verified Mission → Spuzzum, **group** | **Mission → Hope** |
| 6 | none | Mission → Alexandra (low) | Mission → Alexandra (medium; Mission now high) | 2551 unsure Mission → Alexandra (new cut) | Mission → Alexandra (Alexandra: your call) |
| 7 | none | Hope → Alexandra (medium) | the same, + "Alexandra is 4.3 km past Region 2's line" | none | Hope → Alexandra |
| 8 | none | upstream of Hope (high) — to the Fraser's top | **Hope → Spuzzum** (medium) | 2558 unsure Hope → Spuzzum | Hope → Spuzzum |
| 9 | none | **whole Fraser** (medium) — would bind all 1,407 km | **none: leave undrawn** | none | undrawn (it already rides only Region 3's Fraser, km 197.0–452.7) |
| 10 | none | Seton mouth → signs ~4 km below Lillooet (low) | unchanged | none | the same; both ends new cuts |
| 11 | downstream of the Siddle Creek cut | (bound) | (bound) | 2539 unsure | fine as drawn (13 sections) |
| 12 | downstream of the Hwy 7 Dewdney cut | (bound) | (bound) | 2540 unsure | fine as drawn (1 section) |

Bound places get no suggestion, so a regenerated `dfo-suggestions.json` would show nothing on 1–5. That is why the
live file should NOT be regenerated before you re-mark 3–5: its pre-fill for them (Mission → Hope) is the right one.

## Cuts

How checked: each coordinate against OpenStreetMap (Overpass query for bridge ways around each landmark,
2026-11-14), distances by haversine; river km from `splits.resolved.json` / the proposals' own snap.

| landmark DFO names | cut | where | km | check | verdict |
|---|---|---|---|---|---|
| CPR Bridge at Mission ("downstream side of") | `fraser_river__the_cpr_bridge_at_mission` (curated) | -122.30047, 49.12606 | 76.5 | 79 m from OSM "Mission Railway Bridge" (way 327365721, CPKC/CN); the Hwy 11 road bridge is 470 m downstream | **right bridge**. Same cut is the Fraser's tidal limit (`tidal_limits.json`) and the app's part 0/1 break: one point. The Tidal tab's proposal `tidal-356364114` is this same cut, not a second one. "Downstream side" = metres below the cut; nothing to move |
| Highway 1 Bridge at Hope | **missing** in cuts.json; proposed twice: `fraser_hope_bridge` (split hunt, high) and `fraser_river__highway_1_bridge_at_hope_bill_hartley_fraser_hope` (your mark 2546) | snapped -121.452186, 49.386489 | 158.8 | 27 m from OSM Trans-Canada Hwy bridge way 30689321 (MoTI "Bill Hartley / Fraser-Hope"); the CN rail bridge is 1.6 km away | **right bridge; one place under two proposal ids** — accept one |
| Alexandra Bridge ("downstream side of") | **missing**; proposed twice: `fraser_alexandra_bridge` (hunt, medium) and `fraser_river__alexandra_bridge_hwy_1` (mark 2551) | snapped -121.409692, 49.701209 | 201.2 | 39 m from OSM Hwy 1 bridge way 30689318; the OLD Alexandra suspension bridge (footway, way 132578983) is 855 m upstream | **which bridge is your call**: DFO says only "the Alexandra Bridge". The proposal reads it as the Hwy 1 bridge (the lower one). Either way it is **4.3 km above the Region 2/3 line** |
| Spuzzum Creek confluence = Region 2/3 line | `fraser_river__spuzzum_creek_into_fraser_river` (curated, FWA mouth) | -121.40912, 49.66513 | 197.0 | FWA creek mouth; the app's part 3/4 break and the region change in the release sit on it | **right**; the region line and the cut are one point |
| Region 3/5 line | none (area boundaries `area:5` / `area:3`) | — | 418.4 and 452.7 (both-regions stretch between) | atlas part 14 is in Regions 3 and 5 | no cut; none needed unless you want a Region 3 place drawn to the line |
| Region 5/7A line | none (`area:7A`) | — | 674.5 and 703.7 (both-regions stretch) | part 18 | no Fraser DFO place needs it |
| Seton River confluence | none; suggester proposes the FWA mouth (Seton gnis:6291) | -121.926856, 50.681805 | 329.1 | FWA mouth, exact | proposal (high). Note: 250 m below the curated Hwy 99 bridge cut (km 329.4); accepting it makes a 250 m piece |
| signs ~4 km below Lillooet | none; hunt proposal `fraser_lillooet_4km` (low) | -121.904312, 50.655955 | 325.5 | 4 km below the Bridge of the 23 Camels; if DFO's "town" is its south edge the signs are ~2 km lower | **low**: needs your reading |
| Hell's Gate, signs 2800 m below; signs 6500 m below Boston Bar; CNR bridge ~1 km below the Thompson; Thompson; tail race; Hwy 99 Lillooet; BC Railway; Bridge River Rapids; Chilcotin; Williams Lake River; Cottonwood | curated | — | 209.1, 211.9, 217.0, 266.7, 268.0, 327.9, 329.4, 333.6, 338.4, 481.9, 538.4, 673.6 | not named by any DFO Fraser wording | not checked against OSM here (synopsis cuts) |

Nothing was added to `cuts.json`: both missing cuts are already proposals (split hunt + your marks), which is this
repo's form for proposed-not-accepted cuts; adding them live would put the atlas behind its inputs. The entry your
acceptance writes (through the review app) is a `point` cut on FRASER RIVER gnis 39325 at the coordinates above.

## What the suggester got wrong, and the fix (commit 319a447a0, unpushed)

1. **"Whole water" and open ends ran out of the page's region.** A DFO place's extent is read as its own words
   (`regional=False`), so "upstream of the Hwy 1 Bridge at Hope" drawn as suggested would bind the Fraser to its
   top, and Region 3's "Mainstem waters of the Fraser River" drawn whole would bind all four regions. Now an open
   end stops at the page region's line when a curated cut stands there (Hope → Spuzzum), says so when the line is
   only an area boundary, a whole water across regions is not suggested whole, and an end past the line
   (Alexandra) or into a both-regions stretch is named as your call.
2. **The double wording took its half's extent.** "Mission to Hope and Hope to Alexandra" reads 80% like "Mission
   to Hope", so the rewordings cluster handed it Mission → Spuzzum. Back-to-back reaches are now one reach (A to C)
   and never clustered with a half.
3. **"CPR Bridge at Mission" matched "low"** because the Tidal tab's proposal on the same cut counted as a rival.
   It is now high.

Diff on today's marks (scratch run, `--marks` a snapshot): of 136 suggestions, 3 change, all Fraser (rows 6, 8, 9
above). Scratch file: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/51a6025f-10ca-4232-b730-e92b1263ce2f/scratchpad/fraser-suggestions-2026-11-14.json`.
Against the live file, 44 rows differ, mostly because 38 places got bound since it was written (they drop out).

Apply, after you have re-marked 3–5 (not before):
`PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.suggest`

## Rule side: what the app shows on each Fraser part

**Release r-f749475d0962 (now):** no Fraser DFO place is drawn. Parts 0–3 (Region 2, km 0–197.0) list all eight
Region 2 Fraser places as "location not yet mapped", including part 0, the tidal Fraser below Mission (km 0–76.5,
20 sections: the mainstem below the CPR bridge plus its arms), where none of them applies; the answer there comes
from the Region 2 default (parts 1–3) or nothing (part 0, tidal). Parts 4–14 (Region 3 and the 3/5 stretch) list the
two Region 3 places as not mapped. Parts 15–19: region defaults only. Nothing wrong is *drawn*.

**Next release, if built from the places file as it is:**

| part | km | what would show | right? |
|---|---|---|---|
| 1 | 76.5–156.4 | "Mission to Hope" (chum/coho open) first, then "upstream of Mission" (no fishing) | right |
| 2 (Landstrom Bar signs) | 156.4–156.5 | the same | right |
| 3 | 156.5–197.0 | the same, **on Hope → Spuzzum too** | **wrong for km 158.8–197.0: chum and coho shown open; DFO says no fishing** |
| 0, 4–19 | — | as now | — |

Fix: re-mark rows 3–5 as Mission → Hope (needs the Hope cut built). Until then the safe choice is to keep those
three places undrawn, as the release has them; that is your call (it means taking back saved marks).

## Calls only you can make

- Does a Region 2 page's "upstream of the CPR Bridge at Mission" end at Region 2's line (Spuzzum)? Suggested yes.
- Alexandra Bridge: the Hwy 1 bridge (suggested) or the old suspension bridge 855 m upstream? And keep the 4.3 km
  above Spuzzum (DFO's words) or stop at the region line?
- Region 3 "Mainstem waters": leave undrawn (suggested; it already holds only on Region 3's Fraser) — and does it
  include the both-regions stretch km 418.4–452.7? Undrawn, it does.
- Signs "approximately 4 km downstream of the town of Lillooet": which edge of town.
- Which of the two proposal ids to keep for Hope and for Alexandra.
