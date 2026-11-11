# T608 D follow-up — digital depth maps, and the matching rules laid out

Read-only, 2026-11-10. Follows `T608D-findings.md`. No code or data changed. Live release read: `r-a178299f6005`.
Counts come from small read-only scripts kept outside the repo (how each was counted is in "How I counted").

## Summary
- **The 697 is real, and it is not the same thing as the 28.** The Province made a vector file (depth lines with
  depth values, plus sounding points) for 697 sheets around 2000-2001. The 28 are a newer, separate project that
  placed contours on the map. We never fetched either. Our fetch kept the flag and threw away the link to the file.
- **The vector files are public today.** I downloaded 3 and probed 20 more: 22 of 23 flagged sheets have a file.
  None of 8 unflagged sheets do. They cover **495 lakes in the app** (114 with their own rules).
- **They are not placed on the earth.** Each file says "diagrammatic - no projection". Sizes are true (metres).
  Each lake needs to be slid and turned onto our lake outline. That is far less work than tracing a scan.
- **Licence is the open question.** The files sit at the same address as the open PDFs, and 696 of the 697 are
  sheets in the open table. But the open dataset's words name only the PDFs. One email settles it.
- **Largest number of digital maps without tracing anything: 508 lakes** (495 vector + 28 placed, 15 overlap).
  1,732 lakes are scan only. Tracing those is about 15 minutes each.
- **Matching:** the code join is strong. Laid out as rules below: 1,944 chart ids are clean, 350 go to you.
  Three things in the current code break your standing rules and should change before any match file is frozen.
- **Stocking:** still zero rows. GoFishBC refused my one small request today (HTTP 403). The rules are written,
  with counts from the lakes table and from the chart rows as a stand-in.

## Part 1 — Why 697 "vectorized" but 28 lakes with contours

### What the flag is
| Question | Answer |
|---|---|
| Where is it | Field `VECTORIZED_FLAG` on the Province's sheet layer `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW` |
| Where here | `data/source/bc_bathymetry_sheets.json` (`vectorized_flag`), written by `data/fetch_data.py:1236-1239` |
| What it means | "A vector file exists for this sheet." The same layer has a field `VECTOR_FILE_URL` |
| What we dropped | `fetch_data.py:1236-1239` keeps the flag but not `VECTOR_FILE_URL` or `TIF_FILE_URL`. The link is still in our big source file (`bc_fisheries_data.gpkg`, layer `bathymetry_polygons`) |
| Counts | 697 sheets, 684 files, 517 lake codes. 2,046 sheets say N. 1 blank |
| Catch | Every sheet has a link filled in, flagged or not. Only flagged ones return a file. Unflagged return a page saying "File not found" |

The catalogue gives no description for the field (all column comments on that layer are empty). I confirmed the
meaning by fetching, not by reading it anywhere.

### What is inside a vector file (3 opened: Tuck, Beaver, Quality lakes)
- A zip, 200 to 400 KB, holding two ArcInfo export files (`.e00`, a 1990s text format). Dated September 2001.
- File 1: lines. Each line has a type code and a `CONTOUR` value (0 for shore, then -2, -4, -6 ... for depth).
  Also the lake outline, islands, text labels.
- File 2: sounding points, each with a `DEPTH` (Tuck Lake: 271 points, deepest -15.5).
- Its own notes: "RESOLUTION Scale 1:2500; diagrammatic - no projection", "Map produced from soundings plotted
  within the lake outline", "Provisional - Shore Outline from Air Photo", source "BC FISHERIES (BCF)".
- Coordinates are sheet coordinates in metres at true size. Tuck Lake measures 43.3 ha in the file and 43.6 ha
  in our lakes layer. So no stretching is needed, only a shift and a turn.
- **Not confirmed:** whether depths are feet or metres in every file. Old charts are often in feet. It has to be
  read off each chart.

### What the 28 are
Layer `WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP`, "Bathymetric Maps of Surveyed Lakes". 137 shapes over 28 lakes
(re-counted today; T218's figures hold). Each shape is the lake sliced at one depth, already placed on the map,
with max depth, mean depth, volume and survey date. 27 were made by the Province in ArcGIS from the scanned
PDF. 1 (Wahleach Lake, 2012) is from a modern sonar survey. Regions: Omineca 15, Peace 11, Skeena 1, Lower
Mainland 1. Examples: Fraser, Pinchi, Tezzeron, Charlie, Cunningham, Gwillim, Maxhamish lakes.

All 28 are lakes in the app. 15 have their own rules. 15 of the 28 also have a 2001 vector file. So the two sets
are different work on partly the same lakes, 20 years apart.

### Every source checked today
| Source | What | Lakes | Format | Licence, quoted | Access |
|---|---|---|---|---|---|
| Vector files (FIDQ) `a100.gov.bc.ca/pub/fidq/downloadBathymetricMap.do?filename=<file>.zip` | Depth lines + sounding points, not placed | 517 codes, 495 app lakes | zip of `.e00` | **Not stated for these files.** Open dataset says only: "a reference table that includes URLs to PDF bathymetric map files published under the OGL" | Public, no login. Not fetched |
| Placed contours `catalogue.data.gov.bc.ca/dataset/bathymetric-maps-of-surveyed-lakes` | Depth slices on the map | 28 | WFS / WMS / KML; also ArcGIS layer 12 | **"Access Only"** | Public to view. Reuse needs permission |
| Sheet layer `.../dataset/bathymetric-maps` | Lake outline + links to PDF, TIF, vector zip | 2,265 codes | WFS / WMS / KML; ArcGIS layer 19 | **"Access Only"**. Open dataset warns it "is not to be considered an Open Government Licence - British Columbia catalogue, as it also contains lakes that are not Province of BC ownership" | Public |
| Open table `.../dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2` | PDF links | 2,245 codes | CSV + PDF | **Open Government Licence - British Columbia** | Fetched (PDFs on this Mac) |
| TIF scans (same FIDQ address, `.tif`) | The scan as an image, 1-bit | all sheets | TIFF | Same as the PDF it was made from (my reading, not stated) | Public. Not needed: we have the PDFs |
| Lake Inventory Surveys `.../dataset/lake-inventory-surveys` | Max depth, mean depth, area per survey. No shapes | 5,318 survey rows with a max depth | WFS; ArcGIS layer 21 | **Open Government Licence - British Columbia** | Public. Not fetched. Feeds the Depth colouring, not a map |
| Cariboo depth lines `.../dataset/bathymetric-lakes-for-the-cariboo-region` | "Bathymetric line work ... for various lakes in the Cariboo Region", feet and metres | unknown | Oracle only: "export is not enabled" | **"Access Only"** | By request to the Province only |
| GoFishBC (FFSBC) | "Depth Map" link per stocked lake (PDFs; their page says more are on the Ministry site) | not listed | PDF | "All Rights Reserved"; "written consent" | No feed. Likely the same scans |
| CHS charts (federal) | Paper/raster charts 3050 Kootenay, 3052 Okanagan, 3053 Shuswap (from a retailer list and CHS's chart table) | about 10 big lakes | Raster charts, paid | Crown copyright, licensed | Not usable without a licence |
| CHS NONNA (federal) | 10 m / 100 m depth grids | **could not confirm any B.C. lake** | GeoTIFF / grid | Open data licence plus a separate "CHS NONNA LICENCE", "NON-NAVIGATIONAL USE ONLY" | Free with login |
| Okanagan Basin Water Board | 2021 depth lidar: Okanagan, Kalamalka, Wood, Ellison, Skaha, Vaseux, Osoyoos | 7 | Report found; data download not found | Unknown | Ask OBWB |
| Navionics, Garmin, Backroad Mapbooks | Commercial | many | closed | Proprietary | Do not use (SOURCES.md) |

"Access Only" still means what the first audit said: the Province's copyright page says such material "may not be
reproduced or redistributed without the prior written permission of the Province of British Columbia".

### What it takes to make each kind digital
| Kind | Lakes in app | Work | Realistic |
|---|---|---|---|
| Placed contours | 28 | Permission, then load. T218 as written | 28 |
| Vector files | 495 (480 beyond the 28) | One email on licence. A reader for `.e00` (S). Slide-and-turn each lake onto our outline: automatic fit, you tick each one beside the PDF. Tick feet or metres | 308 are one sheet for one lake: expect most to fit by program (T219 fit 4 of 5 scans; these are cleaner). The other 187 are multi-sheet or shared sheets: placed by hand, about 10 min each, about 30 h |
| Scan placed as a picture over the map | 1,732 | No tracing. Fit the scanned shoreline to our outline (T219: 4 of 5 simple sheets; fails on reservoirs and partial sheets) | 1,390 are one sheet for one lake; at T219's rate about 1,100. A picture, not depth lines: it cannot colour the map or answer "how deep here" |
| Scan traced into depth lines | 1,732 | T219: about 2 weeks to build, then about 15 min a lake | All 1,732 is about 430 hours. The 423 that have their own rules: about 105 hours |

### Count table (lakes in the app; 2,240 have some depth product)
| State | Lakes | Note |
|---|---|---|
| Digital now (in a release or in tiles) | **0** | Nothing fetched, nothing shown |
| Digital, needs permission | **28** | Placed contours, "Access Only" |
| Digital, public download, not fetched | **495** | Vector files; 480 not counting the 15 also in the 28. Licence unstated |
| Scan only | **1,732** | PDF on this Mac; 43 more chart ids land on lakes the app does not list |

| Step | Running total of digital maps | Needs |
|---|---|---|
| 1. Placed contours | 28 | Province says yes (D1 email) |
| 2. Vector files | **508** | Province confirms the vector files fall under the open licence, or grants use. Then reader + placing + your ticks |
| 3. Trace scans for lakes with their own rules | 931 | T219 tool, about 105 hours of checking |
| 4. Trace every scan | 2,240 | About 325 more hours. Not recommended |
| Side step: scans as placed pictures | about 1,100 extra lakes get an overlay | No permission needed (open PDFs). Not "digital" in the depth-line sense |

**508 is the largest number reachable without tracing.** It needs one yes from the Province and no new source.

## Part 2 — Matching rules, for you to challenge

### What each record carries
| Signal | Chart sheet | Stocking row (GoFishBC, as seen by the first audit) | Our lake |
|---|---|---|---|
| Lake code (WBID, e.g. `00034SANJ`) | yes | yes (`wbid`) | yes, the Province's 50K code on the lakes layer |
| Name | two: the title printed on the chart, and a "gazetted name" column | lake name | up to 3 official names |
| Official name number (GNIS id) | no | no | yes. So we match on the name and record our lake's number in the match file |
| Position | the Province's own outline for the sheet (not in the open table) | **none seen** (rows have a town) | outline |
| Lake area | area of that outline | none | yes |
| Watershed code (50K) | yes | none seen | yes |
| Region / MU | no | no (town only) | yes. Not usable as a test; shown to you in review |
| Our durable id | — | — | `wbk:<waterbody key>`. What the match file stores. Never a section id |

One caution that shapes the rules. The "gazetted name" column and the outline were both filled in by the Province
**from the code**. So they cannot prove the code is right; they only repeat it. The one thing on a chart that was
not made from the code is the **printed title**. (This is the lesson in the memory note about checks seeded from
their own input.) The position test still earns its place: it proves our lakes layer hangs the code on the lake
that sits where the Province drew the sheet.

### (a) Chart -> lake. Rules in the order tried. 2,294 chart ids today.
A chart id is one lake code with all its sheets. "Clean" means proposed with a tick already suggested; you can
accept those in bulk. Nothing enters the frozen file until you accept it.

| # | Rule | Result | Ids | Clean example | Hard example |
|---|---|---|---|---|---|
| A0 | The row has no lake code | Review, by eye. Never matched by name alone | 44 sheets (outside the 2,294) | — | "DINOSAUR LAKE" (3 sheets), "COBBLE HILL QUARRY" |
| A1 | The code is on no lake in our lakes layer | Review. If the reservoir or wetland layer has the code, that lake is offered | 17 (4 offered, 13 with nothing) | — | Goose Lake `00112OKAN` is a reservoir in our data. Lower Arrow Lake `00099LARL`, McIvor Lake: code found nowhere |
| A2 | The code is on several waterbodies | Review. You pick which ones the chart covers | 54 | — | `00001LNRS` covers Natalkuz Lake and Intata Reach. `00082MFRA` covers Dester Lake (102 ha) and two unnamed ponds of 0.1 and 0.2 ha a kilometre away. `00334SALM`: two lakes both named Twin Lake |
| A3 | The lake has no official name | Review | 140 (97 on a lake in the app, 43 on a lake the app does not list) | — | "ROSE LAKE" `00020FRAN` lands on an unnamed 32 ha lake. Our own name for it may have come from this chart, so agreeing with it proves nothing |
| A4 | No name on the chart agrees with the lake | Review, with the evidence shown. Refused until you accept | 12 | — | Chart "CRIPPLE LAKE" on Nendatoo Lake (153 ha, position and size agree: a rename?). "TOQUART LAKE" on Toquaht Lake (spelling) |
| A5 | The lake is not listed in the app | Held and listed. Not dropped | 0 after the rules above (the 43 in A3) | — | — |
| A6 | Position, size or watershed check fails: sheet outline centre more than 100 m outside the lake's box, or outline area under half or over double the lake, or watershed codes differ, or no outline to test | Review | 41 (14 fail, 27 are in the open table only so there is no outline) | — | Takla Lake `00023MIDR`: outline 24,629 ha, our piece 2,660 ha, 28 km apart (our lake is in pieces). Quennell Lake: chart is "CHEMAINUS FULLERS & QUENNEL LAKES", outline a tenth of the lake |
| A7 | Only the gazetted column agrees; the printed title does not | Review | 86 | — | Title "TRUDA LAKE", lake Tranquille Lake. Title "P.G.E. RESERVOIR", lake Reservoir Lake. Many are word order only: "MAXWELL LAKE" vs "Lake Maxwell" |
| A8 | One code, one named lake in the app, printed title agrees, position, size and watershed agree | **Clean** | 1,944 (1,706 title equal, 238 lake name inside a longer title) | Bobbs Lake `00001BRID`: title "BOBB'S LAKE", 106.9 ha vs 106.6 ha, 0 m | "WILSON L. LITTLE WILSON L." is one sheet for two codes; each code is clean on its own lake |

Name test, exactly: lower case, punctuation dropped, "(formerly)" dropped, one closing word of lake / lakes / L.
/ pond / ponds / reservoir dropped. Then equal; or, for a title, the lake's name appears as whole words inside it.
Thresholds: 100 m; 0.5x to 2x area. Among name-verified ids 2,086 of 2,095 sit at 0 m, and 1,970 are within
0.8x to 1.25x, so the limits are loose on purpose and catch only the odd ones.

Other cases you asked about:
| Case | Rule | Today |
|---|---|---|
| Several charts for one lake | All kept, in sheet order, each with its own title and year. None chosen over another | 383 ids have 2 to 10 files |
| One chart file covering several lakes | The file is attached to each lake it names. Shown with its full title so the angler sees it covers more | 122 files shared by 236 ids |
| Same-name lakes | A name never picks a lake. Only the code does. 811 chart ids carry a name used by 2 or more B.C. lakes ("Jennings" 72, "Twin" 31, "Summit" 25) | 0 matched by name |
| Lake split into parts in the app | The chart attaches to the whole lake; every part shows it | 0 today (Williston, Kootenay, Shannon have no chart id) |
| Renamed lake | Shows up as A4 or A7. You accept, and the chart keeps its printed title. We never rename it | 98 |
| Unnamed lake | A3 | 140 |
| Two sources disagree (open table vs sheet layer) | Both kept. 49 ids are only in the Access Only layer and are held for licence, not for matching | 49 |
| Broken PDF | Matched as usual, flagged "file missing". Not hidden | 6 |

Check against an outside answer: the Province's own placed-contour layer names the waterbody key for its lakes.
For the 25 that are in our sheet list, the rules above pick the same key 25 of 25.

### (b) Stocking row -> lake. Rules in the order tried. 0 real rows today.
| # | Rule | Result | What I can count today |
|---|---|---|---|
| B0 | The id is not a lake code (FFSBC's own ids such as `FFSBC3893`, hatchery ponds) | Review once. You mark it "not a public lake" and it stays marked | "A few hundred" per the old fetch notes. Not re-counted |
| B1 | The code is on no lake | Review | 163 of 7,580 app lakes carry no code at all, so a stocked lake among them can only arrive here |
| B2 | The code is on several waterbodies | Review. No nearest-one pick: the rows have no position | 8,016 of 185,958 lake codes. 53 carry two different names: Buntzen Lake and McCombe Lake share `00943SQAM` |
| B3 | The lake has no official name | Review | Chart stand-in: 140 of 2,294 (6 %) |
| B4 | The name on the row disagrees with the lake | Refused until you accept | Chart stand-in: 12 (0.5 %) |
| B5 | One code, one named lake, the row's name agrees | **Clean** | Chart stand-in: about 9 in 10 |
| B6 | The lake is not listed in the app | Held and listed | unknown |
| B7 | Several codes for one lake | All their rows attach to that lake | 5 app lakes have more than one code (Williston has 3) |
| — | Name-only match | **Never.** Not even as a fallback | — |

Weaker than charts in two ways. There is no printed title: the row's name may itself come from the code. And
there is no outline, so no position or size test. So for stocking I propose you spot-check clean rows harder
(plan below), and that a lake stocked for the first time always comes to you once.

### What the code does today, and what I propose to change
| Where | Today | Proposed |
|---|---|---|
| `pipeline/deliver/lake_facts.py:80-94` (`bind`) | Code, then name check. The base of rules A1, A4, A8 | Keep as the one rule for both kinds |
| `lake_facts.py:89` | A code whose lakes are all unnamed binds **every** one, with no review | Proposal only (A3). You accept |
| `lake_facts.py:93` | Unnamed bits under a named code ride along (Dester Lake's two ponds) | Proposal only (A2) |
| `lake_facts.py:62` | Drops a closing "lake"; "Lake Maxwell" vs "Maxwell Lake" fails | Keep strict; these land in A7 for a tick. Your call (decision 4) |
| `pipeline/stocking/generate/match.py:115` | Several lakes under one code: picks the nearest to a point the feed does not give | Remove. Review (B2) |
| `match.py:119-123` | A code hit with no name support is still "matched", only flagged | Refused until accepted (B4) |
| `match.py:126-165`, `:60`, `:140` | Fallback: same name within 2,000 m, and "name contained in the other" counts ("Fish Lake" inside "Little Fish Lake") | Remove the automatic fallback. This is a silent fallback, and the note `lake-must-not-answer-to-its-river` is this exact failure |
| `match.py:68` | Stores a node id (build output) | Store `wbk:<key>` (gauge lesson) |
| `pipeline/stocking/generate/identifiers.py:49` | If the code column is missing, returns nothing and "the caller falls back to names" | Stop with an error |
| nowhere | No printed-title, position, size or watershed test | Add as A6 / A7 |
| `config.yaml:201`, `:204` | Both match files `null` | `data/curated/bathymetry/matches.json` and `data/curated/stocking/matches.json`, one each |
| both READMEs | Say an audit script exists | It was deleted; fix the words |

### How the frozen file works (your standing rules)
- One file per kind. Each row: the source id, our `wbk:` id(s), the lake's official name number, the rule that
  proposed it, the evidence (names, metres, areas), and who accepted it and when.
- The program writes **proposals** to a separate file. Only your accept moves a row into the frozen file.
- The build and the app read the frozen file. Neither matches anything itself. An id not in the file shows nothing
  and goes on the proposal list. It is never guessed.
- A re-run never rewrites accepted rows (`gauge-regenerate-drifts`: a re-run once demoted 25 reviewed gauges).
  It only reports: new ids, ids that vanished, and accepted rows whose evidence changed. Those come back to you.
- Refusals are rows too ("not a public lake", "no such lake"), so the same question is not asked every year.
- Sourced words are kept as sourced: chart titles and stocking lake names are stored and shown unchanged.

### Ways these rules could be wrong, and the spot check that would catch each
| Risk | How it would show | Check |
|---|---|---|
| The code is stale and the chart is of another lake with the same name nearby | Clean row, wrong drawing | Open the PDF beside the lake outline: do the shapes match? |
| "Name inside title" accepts too much ("Wilson" in "Little Wilson") | Chart on the wrong one of a pair | Look at every pair that shares a file |
| Area limits too loose (0.5x to 2x) | A pond accepted as a lake | Sample the outer bands |
| The title was typed from the code's lake name, not read off the chart | Title agrees, drawing differs | Same shape check; read the title on the PDF itself |
| Our lakes layer hangs the code on the wrong polygon | Position test passes anyway if both came from one table | The 25-of-25 check above; add the shape check |
| Reservoirs changed shape since the survey | Clean, but the chart misleads | Sample the largest lakes and all reservoirs |
| Stocking: name repeats the code, nothing independent | Clean row on a wrong lake | Compare town on the row with the lake's nearest town |

Sample plan for charts (about 160 looks, each a few seconds in the review app with the PDF beside the outline):
| Stratum | Size | Look at |
|---|---|---|
| Clean, title equal | 1,706 | 60 at random |
| Clean, name inside a longer title | 238 | 40 at random |
| Clean, area ratio in the outer bands (0.5-0.8 or 1.25-2) | about 100 | 30 |
| Clean, name used by 6 or more B.C. lakes | up to 201 | 20 |
| Clean, 10 largest lakes and any reservoir | 10+ | all |
| Every review row (A0-A7) | 350 ids + 44 sheets | all, one by one |
If 160 clean looks show 0 wrong, the clean set is very likely under 2 % wrong. One wrong: stop, tighten the
rule that let it through, re-run, sample again. Stocking, once rows exist: 100 clean rows at random, all rows
for lakes whose name is used 6 or more times, and every lake in the top 50 by fish released.

## What you must decide
1. Add the vector files to the permission email (D1)? Ask: "do the vector .zip files served beside the open PDFs
   fall under the Open Government Licence?" This is what unlocks 508.
2. While waiting: may I fetch the 684 vector zips (about 200 MB) to this Mac, outside git, to build and test the
   reader and the placing? Nothing would be published.
3. Do you want the placed-picture overlay for scan-only lakes (about 1,100), or only true depth lines?
4. A7 (86 ids): keep strict so you tick "Maxwell Lake = Lake Maxwell" by hand, or let word order pass as clean?
5. A2 small unnamed ponds under a named lake's code: always ask (my default), or bind only the named lake?
6. Are the limits right: 100 m, 0.5x to 2x area?
7. Stocking with no position: is "code + name, never name alone, first-time lakes always reviewed" enough?

## How I counted
- Chart ids: union of `bc_bathymetry_sheets.json` / gpkg layer `bathymetry_polygons` (2,265 ids) and
  `wsa_bathymetry_maps.csv` (2,245 ids with a code; 44 rows have none) = 2,294.
- Lakes: `bc_fisheries_data.gpkg` layers `lakes`, `manmade`, `wetlands`, read without shapes (box only).
- App lakes: table `item` in `core-74d7f6dce1aa.sqlite.gz` of `r-a178299f6005` (7,595 lake items, 7,580 whole).
- Name test: the repo's own `lake_facts.core` and `bind`, imported and run, not re-written.
- Position: centre of the sheet outline's box against the lake's box. A box, not the true shape; a real matcher
  should use the shape.
- My lake total is 2,237 from charts plus 3 placed-contour lakes with no sheet row = 2,240.

## Could not confirm
- The licence of the vector files. Not stated anywhere I read.
- That all 684 vector files exist. 22 of 23 probed did; the miss was a second sheet (`00289702`).
- Feet or metres in the vector depth values.
- Any stocking count on real rows. GoFishBC answered 403 to one small request; I did not retry or work around it.
  So I also could not re-check which fields a stocking row carries (taken from the first audit).
- Whether NONNA covers any B.C. lake; whether OBWB shares its lidar; what the Cariboo layer holds.
- How well the placing fit works on vector files. The 4-of-5 figure is T219's, on scans.
- `curation-review/` was not opened.
