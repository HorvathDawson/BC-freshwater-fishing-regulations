# T608A findings: does the live release say what the sources say?

Read-only audit, 2026-11-10. Release `r-a178299f6005` (LATEST, also what staging serves). Nothing was changed.

## Summary

- **48 waters checked** across all regions (38 with their own printed row, 10 on regional rules only), each for
  **2026-11-10** and **2026-07-15**, plus season-edge dates where a rule starts or ends.
- **34 clean. 14 with something to look at.** No wrong value was found in any regional (base) table.
- **21 findings: 5 high, 7 medium, 9 low.** Only 5 are certain; 16 need the owner's reading.
- By group: **printed synopsis 13**, **DFO content 3**, **in-season notices 3**, **wording only 2**.
- **DFO placement and coverage: 15 waters listed separately** (waiting on the owner's labelling; not ranked).
- The worst three:
  1. **Campbell River** shows closed July 15 to August 31 under the Region 1 summer stream closure, although the
     book lists it in unit 1-10, outside the closure's units 1-1 to 1-6 (F1).
  2. **Campbell River salmon**: DFO prints "to be determined" for chinook only on one stretch; the app shows "to be
     determined" for all five species there, hiding the coho and pink openings (F3, certain).
  3. **Shuswap Lake and Quesnel Lake**: closures that cover one bay or arm are not drawn, so the card gives keep
     limits lake-wide and the closure rides only as a "not yet mapped" note. Salmon Arm Bay is closed today (F4).

## How it was done

- Rules were read from the release files through the pipeline's own reference reader (`province_store`), the same
  pre-resolved answers the app looks up. No cascade logic was rewritten.
- Printed pages come from the synopsis PDF in the release (text pulled with local tools). Page numbers are PRINTED.
- Salmon is the release's placement read through the live feed on `data-staging.canifishthis.ca`, composed as the
  app composes it, and its words compared with DFO's pages (the repo's cached copies).
- In-season is the staging feed (32 notices, all matched) and its links file.
- Six reading lanes, one per region group. Every high finding was re-checked against both texts afterwards.

## Ranked findings

"Stage" is where it most likely went wrong. "Sure?" is Certain, or Owner (needs the owner's reading).

### High

| # | Water, stretch | Shown | Source | Likely right; stage | Sure? |
|---|---|---|---|---|---|
| F1 | Campbell River `gnis:39532`, parts 1-4 and 7-10, Jul 15 | "status=closed", closed by `z1:summer_stream_closure.r1` "No fishing in streams, Jul 15-Aug 31" | p.13: "No Fishing in any stream in Management Units 1-1 to 1-6 from July 15-Aug 31". p.14 row: "CAMPBELL RIVER 1-10", whose own closure is only "from the boundary sign at the end of Maple Street downstream to the boundary sign at the cement block, Aug 1-Oct 31" | The book: the row is in unit 1-10, and DFO opens pink there from Jul 15. Follows ruling E9 (a piece on a unit line takes both sides). Cascade / area membership | Owner |
| F2 | Campbell River, parts 4 and 5 | Part 4 "From the power line ~200m up of Quinsam up to the Elk Falls" is open, fly only. Part 5 "From the Elk Falls up to the John Hart Dam power station" is closed all year | p.14: "No Fishing between Elk Falls and John Hart Dam Power Station"; "From John Hart Dam Power Station to power line crossing approximately 200 m upstream of Quinsam River confluence: fly fishing only year-round" | The book's order puts the power station BELOW the falls. The cut sits at the John Hart Lake outlet, above them; the cut's own note says "decide dam-face vs tailrace". If the station is below, the canyon shows open and the wrong stretch shows closed. Placement | Owner (map) |
| F3 | Campbell River salmon, parts 2-3 (Maple Street to the Quinsam), both dates | chinook, coho, sockeye, pink, chum all "To be determined (as printed: To be determined)" | DFO Region 1: "downstream of the confluence with the Quinsam River to the fishing boundary sign at the end of Maple Street. Chinook: To be determined". The wider place: "Coho, Sept 1 to Dec 31, 4 hatchery-marked per day"; "Pink, Jul 15 to Oct 15, 4 per day" | DFO. A one-species row answers for every species. Feed typing / chain | Certain |
| F4 | Shuswap Lake `wbk:329518145` (Nov 10) and Quesnel Lake `wbk:329480766` (Mar 1-May 31) | Shuswap: keep 1 rainbow, 1 char, lake-wide. Quesnel on Apr 15: open, normal limits. The closures are held as "not yet mapped" notes, not as closures | p.32: "No fishing in Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point ... Sept 1-Dec 31"; "rainbow trout and char catch and release, May 15-Mar 14, in the waters lying west of a line between signs at Henstridge Road and Wharf Road". p.46: "No Fishing southwest of a line ... Horsefly Bay, Mar 1-May 31"; "No Fishing in North Arm, north of a line between Watt and Service Creeks, Mar 1-May 31" | The book. Known deferral (lake splits, ruling K5): the lake is one piece. A reader who misses the note is told he may keep fish in closed water. Placement | Owner |
| F5 | Nechako River `gnis:16605`, part 5 "From Region 6 boundary up to the Knewstubb Lake outlet", May 1 | "status=own", "LT,EB=keep:2", no closure | p.49: "No fishing: in any stream ... in the Fraser River watershed in Region 6, Apr 1-June 30". The Nechako's "Exempt from spring closure" is printed on the Region 7 row (p.60) | Unclear: either Region 6's closure is not placed here (it does close the Stellako next door) or a Region 7 exemption lifts it. Cascade / placement | Owner |

### Medium

| # | Water, stretch | Shown | Source | Likely right; stage | Sure? |
|---|---|---|---|---|---|
| F6 | Peace River `gnis:14619`, part 9 "From Dinosaur Lake up to the Williston Lake outlet", both dates | "release ['KO']" by `z7b ... species_quotas.r5` "Release kokanee from streams" | p.64: "Kokanee: 10 (none from streams, except Peace River)" | The book: keep 10. The exception is honoured only where a Peace row repeats it. Zone rule parse | Certain |
| F7 | Fraser River `gnis:39325`, parts 16-19 (above the Chilcotin, Regions 5 and 7), all year | Banner `is-14c1a6f972a3`: "No fishing for steelhead in the Fraser River (mainstem) upstream of Hells Gate January 1 to December 31", linked from Spuzzum Creek to the source. The cards there say steelhead release | p.30 bounds the same rule: "from Hells Gate upstream to the Region 3 boundary Jan 1-Dec 31"; p.45 closes steelhead only "from the confluence with the Chilcotin River downstream to the Region 5 boundary" | The card. The Region 3 notice should stop at the Region 3 line. Notice link | Owner |
| F8 | Fraser River, part 18 "Where Region 5 and Region 7 meet" | Both speak: `r5 ... fraser_river.r3` "Bait ban, Sep 15-Jul 15" and `r7 ... fraser_river.r2` "Any bait may be used, upstream of the Cottonwood River confluence, lifts Bait ban" | p.45: "bait ban Sept 15-July 15"; p.59: "also EXEMPT from bait ban upstream of Cottonwood River"; p.58: "Bait ban: applies to all streams of Zone A, all year" | Under ruling Z4 (stricter of both) the Region 5 ban should hold; the Region 7 exemption should lift only Zone A's ban. No cut at the Cottonwood. Cascade | Owner |
| F9 | Campbell River, parts 2-4 | The "power line ~200m up of Quinsam" cut is at km 5.54; the Quinsam joins at km 4.2 | p.14: "power line crossing approximately 200 m upstream of Quinsam River confluence" | The cut is 1.3 km above the confluence, so about 1.1 km shows ordinary gear where fly only may apply. Placement | Owner (map) |
| F10 | Babine River `gnis:17687`, part 7 "From the Fort Babine Bridge up to the Babine Lake outlet" | "1 line with a single barbless hook ... (one hook, lure or fly)": no fly-only rule | p.50: "Fly fishing only (a) from the Fort Babine Bridge downstream to signs 100 m upstream of the adult fish counting fence ... includes "Rainbow Alley"" | The release follows clause (a) to the letter. Does "includes Rainbow Alley" pull in the water above the bridge? Parse / placement | Owner |
| F11 | Similkameen River `gnis:26324` and its tributaries (236 waters, e.g. Allison Creek), Jul 30-Sep 15 | On Aug 15: "status=own", "GB,LT=keep:4". The notice is a banner only | Notice `is-6a32433916ac`: "No fishing July 30 to September 15, including tributaries." The book (p.71) has no such closure | By design a notice never changes the card, so in that window the card says open and the banner says no fishing. Same for Okanagan River, Salmon River (R3), Bessette Creek, Granby. Not on either audit date. Design | Owner |
| F12 | Stellako River `gnis:7836`, parts 0-4, Jul 15 | Part 0: "Steelhead=1 hatchery rainbow trout over 50 cm"; parts 1-4 "ST=keep:1/release" | p.53 and p.61: "Rainbow trout catch and release" | Likely the book: the water is marked "steelhead known", so a hatchery rainbow over 50 cm is treated as a steelhead and kept. Follows rulings S6/S9, but is the Stellako a steelhead water? Curation / cascade | Owner |

### Low

| # | Water | What | Sure? |
|---|---|---|---|
| F13 | Squamish River `gnis:25671`, coho, Nov 10 | Short line "1 per day (Sep 15-Jan 31)" drops DFO's "hatchery marked only" (DFO: "1 per day, hatchery marked only"). The detail line keeps it. Feed wording | Certain |
| F14 | Fraser part 14, Nechako part 5 (region straddles) | The card mixes two regions' words: "No more than 5 trout and char ... across Region 3" beside "all Region 5 streams combined". Numbers are right (ruling Z4). On the Nechako the bucket names only brook and lake trout though rainbow and cutthroat are kept at 1. Display | Certain |
| F15 | Cowichan Lake, Stamp River, Elk Lake (R1), Chilko River | "on parts" is lost from rule titles: "No vessels", "Engine power restriction 7.5 kW (10 hp)", "Speed restriction (8 km/h)" read as whole-water. Book p.15, p.18: each is "on parts". Chilko's "speed restriction (5 km/h) on part" shows on all three parts. Display | Certain |
| F16 | Little Qualicum River `gnis:10886`, parts 2-3 | The Dec 1-May 31 closure runs to the Upper Waterfall; the cut's own review note says "LOWEST falls". Errs on the closed side. Placement | Owner |
| F17 | Lakelse Lake `wbk:329310750` | No bait ban shown. p.49: "Bait ban: In the Skeena River, including tributaries" is bound to streams only. Does it reach a lake in the watershed? Parse | Owner |
| F18 | Babine River salmon, Aug 17-Mar 31 | DFO prints "No fishing for salmon" (FN0869) in a Sockeye row; the feed closes sockeye only, so coho reads open to Oct 15. Not on either audit date | Owner |
| F19 | Stellako River, part 4 (François Lake outlet) | p.53: "See restrictions for François Lake's outlet area" (p.51: "No Fishing Nov 15-May 31; char daily quota = 2 ... bait ban"). Nothing of it on the river's top stretch. Does map 1 reach into the river? | Owner |
| F20 | Fraser River, part 0 (tidal, below Mission) | The Region 2 steelhead banner `is-8cd37f6e5bf9` is linked from the mouth, so it also sits on the tidal stretch that has no freshwater card. Notice link | Owner |
| F21 | Peace River, part 2 "Within Clayhurst Ecological Reserve" (3 km of mainstem) | Closed all year by "Fishing is prohibited in Ecological Reserves" (p.9). The rule is real; is the river channel inside the reserve or only beside it? Ruling E8 treats a straddle as inside | Owner |

### Rules shown that no source supports, and source rules not shown

- Shown without support: F1 (summer closure on a unit 1-10 river), F3 (four species "to be determined"), F12 (a
  steelhead to keep on a rainbow-release river), F21 if the reserve only touches the river.
- Printed but not shown as a rule: F4 (bay and arm closures), F5 (Region 6 spring closure), F6 (Peace kokanee
  exception), F10, F17, F19.
- Carried as "not yet mapped" notes, checked in the data (their look on the card was not checked): Cowichan Lake
  "overall 10 km/h speed restriction within 60 m of shore" (p.15); Columbia River "no power boats in wetlands"
  (p.35); Okanagan Lake "Speed restrictions or no vessels (as buoyed and signed), various locations" (p.70).

## In-season notices

- 32 notices on the feed, 32 matched. On the sampled waters the dates are right: Cowichan River's 2025 closure and
  the July 30 to September 15 windows show on neither audit date.
- Peace River "No fishing between Peace Canyon Dam and Hwy 29 Bridge": banner on the right stretch (part 8); the
  card is closed there too, and the book already prints it (p.66). No conflict.
- Fraser River: the rescinded Region 2 trout closure is not on the card (parts 1-3 keep 2 hatchery trout on Nov 10)
  and the book no longer prints it. The Region 2 and Region 5 steelhead notices land on the stretches they name.
  Only the Region 3 notice over-reaches (F7).

## DFO: waiting on the owner's labelling

Not defects now. Listed so they can be checked again after labelling. Where a place is not drawn, the stretch
falls to a wider place or to "not opened by DFO: closed".

| Water | DFO place | What is missing or misplaced |
|---|---|---|
| Cowichan River | "downstream of the weir on Cowichan Lake to 66 Mile Trestle"; "from CNR Trestle at mile 70.2 to Skutz Falls"; "including tributaries and Cowichan Lake" | Not drawn. Parts 2-4 show every salmon closed on both dates |
| Stamp River | "Except in those areas and times listed below" and 8 other places | Not on parts 0 and 4-7: chinook and coho show closed Nov 10 though DFO opens them. "Stamp River tributaries" is drawn on the mainstem |
| Little Qualicum River | "all waters unless indicated below" | Not drawn (T607): chinook and coho show closed Nov 10; DFO has 2 chinook, 1 hatchery coho. Its fly-only line is absent. The falls-to-fence place ends at the park boundary |
| Campbell River | (see F3, a content matter) | Placement otherwise fine |
| Fraser River | 8 Region 2 mainstem places (Mission, Hope, Alexandra Bridge); Region 3 "Mainstem waters of the Fraser River" and the Seton stretch | None drawn: all 20 parts show salmon closed on both dates |
| Harrison River | Harrison Lake outlet to Hwy 7 bridge; Hwy 7 bridge to the Fraser | Not drawn: all closed Nov 10; DFO has 4 hatchery coho and 2 chum |
| Thompson River | Kamloops Lake outlet to Gold Pan signs; Gold Pan signs to Skihist | Not drawn; the answer happens to match on both dates |
| Squamish River | "downstream of the powerline crossing" | On the Squamish's chain, but DFO's Squamish entry does not list it |
| Dean River | Crag Creek to tidal; the canyon; upstream of Crag Creek | Not drawn: parts 0-4 take the 5B default and show chinook and coho open Nov 10 (DFO: no fishing Oct 1-May 31; canyon closed all year). Parts 5-9 are closed by the wrong place |
| Quesnel Lake | Horsefly Bay sockeye place (Aug 22-Sep 15), under two wordings | Not drawn; no effect on the audit dates |
| Spatsizi River | "Stikine River including tributaries" | Not attached: chinook shows "4 per day, only 1 over 65 cm" (DFO: non-retention); coho shows closed (DFO: 4 per day) |
| Bulkley River | "downstream of the Morice River confluence excluding tributaries"; two tributary places | Not drawn: coho and pink show closed Jul 15; DFO opens both |
| Kispiox River | "downstream of fishing boundary signs near Kispiox River Resort" | Not drawn: pink shows closed Jul 15; DFO has 2 per day |
| Zymoetz River | "upstream of Highway #16 bridge" and a note place | Not drawn; no change to the answer |
| Skeena, Babine | Places kept in the feed that the current DFO page no longer words that way | No answer on the audit dates |
| Nechako River | Two Foothills Boulevard Bridge places | Not drawn; closed matches DFO. Caution: on DFO's Region 7 page the Nechako row sits beside a stray "-->" and may be commented out |

One wider fact, not a defect on these dates: 101 of the 350 DFO place ids the release's chains name are absent
from the live feed (among them every region default). Composed the app's way, none of the 455 chains changes its
open/closed answer on either audit date against the release's own file.

## Waters checked

| Region | Water | Id | Result |
|---|---|---|---|
| 1 | Cowichan River | `gnis:12227` | Clean |
| 1 | Cowichan Lake | `wbk:329101302` | Minor (F15) |
| 1 | Campbell River | `gnis:39532` | Wrong (F1, F2, F3, F9) |
| 1 | Stamp River | `gnis:14533` | Minor (F15) |
| 1 | Little Qualicum River | `gnis:10886` | Minor (F16; T607) |
| 1 | Elk Lake | `wbk:329676313` | Minor (F15) |
| 1 | Kennedy River (base only) | `gnis:2263` | Clean |
| 1 | Eagles Lake (base only) | `wbk:329676481` | Clean |
| 2 | Fraser River (R2, R3, R5, R7) | `gnis:39325` | Minor (F7, F8, F14, F20) |
| 2 | Chilliwack River | `gnis:8634` | Clean |
| 2 | Vedder River | `gnis:3062` | Clean |
| 2 | Harrison River | `gnis:15333` | Clean |
| 2 | Cultus Lake | `wbk:329083342` | Clean |
| 2 | Squamish River | `gnis:25671` | Minor (F13) |
| 2 | Skagit River | `gnis:27159` | Clean |
| 2 | Orford River (base only) | `gnis:24864` | Clean |
| 3 | Thompson River | `gnis:39492` | Clean |
| 3 | Adams River | `gnis:39257` | Clean |
| 3 | Shuswap Lake | `wbk:329518145` | Deferred (F4) |
| 3 | Kamloops Lake | `wbk:329563838` | Clean |
| 3 | Avery Lake (base only) | `wbk:329650455` | Clean |
| 4 | Columbia River | `gnis:37414` | Clean |
| 4 | Kootenay Lake, Main Body | `wbk:-20` | Clean |
| 4 | Elk River (classified) | `gnis:16880` | Clean |
| 4 | St. Mary River (classified) | `gnis:2619` | Clean |
| 4 | Blaeberry River (base only) | `gnis:13624` | Clean |
| 5 | Dean River (classified) | `gnis:16075` | Clean |
| 5 | Chilko River (classified) | `gnis:13741` | Clean |
| 5 | Quesnel Lake | `wbk:329480766` | Deferred (F4) |
| 5 | Horsefly River (classified) | `gnis:24531` | Clean |
| 5 | Pelican Lake (base only) | `wbk:329601600` | Clean |
| 6 | Skeena River (classified) | `gnis:2936` | Clean |
| 6 | Kispiox River (classified) | `gnis:2308` | Clean |
| 6 | Bulkley River (classified) | `gnis:2195` | Clean |
| 6 | Babine River (classified) | `gnis:17687` | Unsure (F10, F18) |
| 6 | Lakelse Lake | `wbk:329310750` | Unsure (F17) |
| 6 | Zymoetz River (classified) | `gnis:3137` | Clean |
| 6 | Spatsizi River (base only) | `gnis:27329` | Clean |
| 7A | Nechako River | `gnis:16605` | Unsure (F5, F14) |
| 7A/6 | Stellako River (classified) | `gnis:7836` | Unsure (F12, F19) |
| 7A | Osilinka River (base only) | `gnis:24882` | Clean |
| 7B | Peace River | `gnis:14619` | Minor (F6, F21) |
| 7B | Charlie Lake | `wbk:329264876` | Clean |
| 7B | Kiskatinaw River (base only) | `gnis:2307` | Clean |
| 8 | Okanagan Lake | `wbk:329459116` | Clean |
| 8 | Similkameen River | `gnis:26324` | Clean (F11 outside the dates) |
| 8 | Kettle River | `gnis:11984` | Clean |
| 8 | Allison Creek (base only) | `gnis:9613` | Clean |

What "clean" covered, in short: every printed closure with both its edge dates, quotas and size limits, catch and
release, bait and hook rules, fly-only stretches, boat rules on the stretch they name, classified-water periods
and steelhead stamps, weekday rules (Skeena, Kispiox, Bulkley, Babine, Zymoetz, tested on a weekend date), and
the regional table of each region. Readings that follow a ruling were not counted (white sturgeon shown closed,
W1; a regional seasonal release holding over a water's own quota, Q6; region straddles taking the stricter, Z4;
brook trout apart from trout in Region 6, C2/C4).

## What could not be checked, and why

- **The card as drawn.** The audit read the answers the app looks up, not the screens. Not seen: how a "not yet
  mapped" note looks (F4), whether "only 1 over 50 cm" lines print, which salmon string prints (F13), how the
  rescinded-closure banner reads (its window field holds the old closure's dates, Sep 1 to Nov 15).
- **Positions on the ground.** Signs, bridges, power lines, unit lines and reserve edges were checked for order and
  length only (F1, F2, F9, F21, the Cottonwood confluence in F8). No map was opened.
- **The government's live pages.** In-season was read from the staging feed, not from gov.bc.ca. DFO's words were
  read from the repo's cached pages (Regions 1 and 2 from Nov 9; the others older), not fetched fresh.
- **The PDF text.** Pulled with a local tool; in table pages a row's name sits in the middle of its lines, and PDF
  page 17 carries a hidden second layer of text. Quotes were cross-read against the pipeline's stored row text.
- **Other days.** Two dates plus the season edges of the rules found. Salmon was read on the two dates only.
- **Campbell River part 11** (top run, into what looks like Buttle Lake): whether "BUTTLE LAKE'S TRIBUTARIES ...
  Fly fishing only" (p.14) should apply was not settled. **Bulkley part 7** (a side channel, closed all year): the
  data gives no distance, so it could not be confirmed to lie above the Morice confluence.
- **Pelican Lake's salmon answer** (the 5B default, 4 a day): its management unit was not confirmed.
