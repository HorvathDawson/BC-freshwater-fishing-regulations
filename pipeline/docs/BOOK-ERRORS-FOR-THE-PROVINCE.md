# Book errors for the Province: one entry each

Owner, 2026-11-26: "also could add to gotchas in a way that if we wanted to compile errors we found to send to gov it was
easy." One entry per printed error or loose print, one shape. Pages are the printed pages of the 2025-2027 synopsis, then the page of
`data/source/fishing_synopsis.pdf` (PDF = printed + 2 in Regions 1-4, + 6 from Region 5). Quotes are the PDF's text layer, exact. Ways to read a row two ways are
`BOOK-QUESTIONS.md`; what the app does about each is `GOTCHAS.md` and `docs/OWNER-RULINGS.md`. **Add an entry the day a
new error is found; never delete one: change its status.** Status: to send / sent / answered. Sections A, B, C and E are the lists to send; section D holds cross-links and entries that are not errors (not for sending). Entries stay in number order within and across sections; a late find takes the next number and goes in section E.

Shape: **Region, water / row, page** - Printed: "verbatim" - Wrong or unclear: ... - Meanwhile: ... - Evidence: ... -
Found: date - Status.

## A. The unit is printed wrong (seven rows, owner 2026-11-26, P10: all kept bound to the real water)
Every override note in `data/curated/regulations/book_name_links.json` carries the same evidence. "VORR" = the federal
Vessel Operation Restriction Regulations, SOR/2008-120 (Transport Canada TP 15587E since SOR/2025-158). Stocking = FIDQ.

1. **R7A, Lynx Lake, p.60 (PDF 66).** Printed: "LYNX LAKE 7-15 Electric motor only - max 7.5 kW". The only Lynx Lake in BC is
   in MU 7-10 (20 km from 7-15; wbk:329547767). Meanwhile: bound to it. Evidence: VORR Sched. 3 item 90 "Lynx Lake, south
   of Prince George, 53 37 29 N 122 54 24 W"; 7-15 in every edition since 2006-07; stocking 00728TABR. Found 2026-11-26. To send.
2. **R7A, Chubb Lake, p.59 (PDF 65).** Printed: "CHUBB LAKE 7-10 Engine power restriction - 7.5 kW (10 hp)". The lake is in 7-8
   (13 km off; wbk:329097617). Meanwhile: bound to it. Evidence: VORR Sched. 4 item 38 "Chubb Lake, Prince George, 53 16
   38 N 122 33 54 W, 7.45"; Omineca 2004 stocked-lake report 00681COTR ("engine power restriction of 7.5 KW or 10 HP").
   Found 2026-11-26. To send.
3. **R4, Bridal Lake, p.35 (PDF 37).** Printed: "BRIDAL LAKE 4-7 No powered boats". The lake is 80 m inside 4-8 (Kootenay Pass, a
   true line case; wbk:329262703). Meanwhile: bound to it. Evidence: VORR Sched. 2 item 11 "Bridal Lake, near Creston,
   49 03 37 N 117 02 24 W"; stocking 00411LARL ("BRIDAL", "SUMMIT"); BC Parks (Stagleap Park). Found 2026-11-26. Optional to send.
4. **R4, Hiawatha Lake, p.37 (PDF 39).** Printed: "HIAWATHA LAKE 4-3 Yellow perch daily quota = unlimited". The only Hiawatha Lake
   is in 4-4 (6.9 km from 4-3; wbk:329246408). Perch in it are not confirmed by coordinates. Meanwhile: bound to it.
   Evidence: first printed 2009-11 beside Haha Lake 4-3; Kootenay Region Angling Regulations Review 2009-10 lists it as a
   perch lake; stocking 00637KOTL. Found 2026-11-26. To send, with "are perch confirmed there?".
5. **R7A, Emerald Lake, p.59 (PDF 65).** Printed: "EMERALD LAKE 7-15 No powered boats", directly under Eena Lake 7-15. The lake is
   in 7-16 (22 km off; wbk:329102641). Meanwhile: bound to it. Evidence: VORR Sched. 2 item 61 "Emerald Lake,
   approximately 57 km north of Prince George, 54 25 35 N 122 36 32 W"; Omineca 2004 report 01100CRKD. Found 2026-11-26. To send.
6. **R7A, Nation River, p.60 (PDF 66).** Printed: "NATION RIVER 7-30 See Nation Arm (Williston Lake)". The river is in 7-28 and
   7-29, none in 7-30; 7-30 is the Nation Arm's unit, so the pointer is right. Meanwhile: the pointer is kept. Evidence:
   BC Geographical Names 9120; the Nation Arm row's closure line sits at the Nation River Bridge (55.3727 N 123.6897 W,
   7-29), 16.4 km up the river (T1178). Found 2026-11-26. Optional: "does the Apr 1-June 30 closure cover the river?"
7. **R8, Ellison Lake, p.69 (PDF 75).** Printed: "ELLISON LAKE 8-8 Yellow perch daily quota = 20", out of order under Eneas Creek 8-8.
   The Province's own proposal page says MU 8-10 (wbk:329459126; 23.7 km from 8-8). Meanwhile: bound to it. Evidence:
   "Ellison (Duck) Lake proposed angling regulations ... Management unit (MU): 8-10 ... quota of 20" (archived 2025-02-09).
   Found 2026-11-26. To send.

## B. A distance or a place that does not fit the water
8. **R8, McRae Creek, p.70 (PDF 76).** Printed: "MCRAE CREEK 8-15 No Fishing downstream of falls situated approximately 4 km upstream of Christina Lake".
   The real falls are 2.05 km up, not about 4 km. Meanwhile: the cut is at the falls, 2.05 km (owner: "that is the real falls location").
   Evidence: the FISS obstacle at 2.05 km. Found 2026-11-26. To send.
9. **R2, Tamihi Creek, p.26 (PDF 28).** Printed, two lines under "TAMIHI CREEK 2-3": "No Fishing downstream of the falls situated approximately 200 m
   upstream of Chilliwack River" and "No Fishing upstream of the falls situated approximately 200 m upstream of Chilliwack River, May 1-June 30".
   The falls we found (FISS) are 0.66 km up, not about 200 m. Meanwhile: cut at the falls; below it closed all year, above it May 1-June 30
   (the curated rules `tamihi_creek.r1` / `.r2`). The data matches both lines; only the distance differs, so the owner's O15 ("Our cut is the falls")
   stands. Found 2026-11-26. To send.
10. **R8, Okanagan River Oxbows, p.70 (PDF 76).** Printed: "OKANAGAN RIVER OXBOWS 8-1 ... These isolated oxbows adjacent to Okanagan River downstream of the
    McIntyre Dam and upstream of Vaseux Lake are EXEMPT from the spring closure". No water is both: McIntyre Dam is below Vaseux Lake. Meanwhile: both
    stretches (49.5 km) stay exempt (owner: "Keep both exempt, list it for the Province"). Evidence: the dam and lake positions on the map. Found 2026-11-26. To send: "which oxbows?".
11. **R4, Goat River, p.37 (PDF 39).** Printed: "GOAT RIVER 4-6 Trout/char catch and release (mainstem only) Leadville Creek Cameron Creek" (no linking word; the Findlay Creek row
    prints "(mainstem only) from ... to ..."). Meanwhile (owner 2026-11-26, "between the 2 only with a gotcha added"): catch and release only on the main stem between
    Leadville Creek (km 33.1) and Cameron Creek (km 42.6); the rest of the main stem takes the regional quota. Evidence: `one-offs-evidence/A-goat-river.md`. Found 2026-11-26. To send: "a stretch between the two creeks?".
12. **R7B, Hay River basin, row p.65 (PDF 71), map p.63 (PDF 69).** Printed, the row (its MU cell says only "7-53"): "LIARD RIVER WATERSHED (see map on page 63)"
    "Bull trout catch and release from all lakes and streams, Aug 15-Oct 15" / "Bull trout daily and possession quotas = 1 (none under 30 cm or over 50 cm) from all lakes and streams, Oct 16-Aug 14".
    Zone B's list, p.64 (PDF 70): "Bull trout from the Liard River watershed Aug 15-Oct 15, and from the Peace River watershed all year (See tables for exceptions)".
    The map is vector art with a text layer (no raster image on the page). Its "Key to Peace Watersheds" names "Liard River Watershed" above a dotted line and "Peace River Watershed"
    below it; on the main map that line runs east to the Alberta border south of MU 7-47, so everything north of it, the Hay basin included (the map labels "Kotcho L" and "Kotcho R" in
    7-56; it prints no "Hay"; the audit's count: Kotcho Lake, Hay, Kotcho and Shekilie rivers, 1,547 km), reads as "Liard River Watershed". Meanwhile: the true basin is kept, so all
    bull trout must be released (owner: "leave it stricter"). Evidence: the map and its key, read from the rendered page. Found 2026-11-26. To send: is the Hay basin in the Liard Watershed?
13. **R1, Healy Lake's outlet stream, p.16 (PDF 18).** Printed: "HEALY LAKE'S OUTLET STREAM 1-5 No Fishing". Which stream, from where? Meanwhile: cut below Healy Lake only (owner O13, a global extent). Evidence: the
    whole South Englishman River (26.7 km) was closed. Found 2026-11-26. To send.
14. **R3, Maka Creek, p.31 (PDF 33).** Printed: "MAKA CREEK 3-13 No trout under 25 cm". Two Maka creeks exist in the same MU. Meanwhile: the rule is kept on both (owner: "keep on both because they are same MU").
    Evidence: BC Geographical Names; the curated "most likely" is the Spius Creek one. Found 2026-11-26. To send: which one?
15. **R1, Nanaimo River, p.17 (PDF 19).** Printed (text layer): "No Fishing from the Cedar Road Bridge upstream to the Hwy 19 bridge, Sept 1-Oct 15"; on the rendered page the 15 is visibly struck through with nothing in its place. The strike is a drawn black line 0.14 pt wide (x 392.1-397.8 pt, 477.6 pt
    from the top of PDF 19) across the middle of the "15", not a character, so the text layer does not show it.
    Meanwhile: Oct 15 stays (owner: "it is probably a format issue and is okay"). Found 2026-11-26. To send, low priority.

## C. Rows the newer book lost, or loosely printed
17. **R1, Little Qualicum River (1-6) and Lizard Lake (1-3), p.17 (PDF 19).** Printed in our copy (2025-11-18): "LITTLE QUALICUM RIVER 1-6 ..." and "LIZARD LAKE 1-3 electric motor only - max 7.5 kW".
    Missing from the gov.bc.ca copy edited 2026-08-25 (rendered pp. 18-20), with no notice; the new copy also prints PUNTLEDGE RIVER twice (pp. 19, 20). Meanwhile: dropped with a `drop` flag and a note when
    the new book is adopted (owner P11, T1179). Evidence: T1157's page comparison. Found 2026-11-25. To send: an error?
18. **R6 and R7A, Stellako River, p.53 (PDF 59) and p.61 (PDF 67).** Printed: "No Fishing Nov 15-May 14; Class II water when open (Steelhead Stamp not required)"; the neighbours (Endako, Nithi)
    print "Nov 15-June 30". Meanwhile (owner P7a): the row's Nov 15-May 14 replaces the spring closure; it opens May 15. Found 2026-11-26. To send: is May 14 intended?
19. **R5, spring closure "other streams listed in the tables", p.42 (PDF 48).** Printed: "Spring closure: No fishing in any stream in Fraser River Watershed of Region 5 (including the Thompson River Watershed)
    from Apr 1-June 30, EXCEPT the mainstem of the Fraser River and other streams listed in the tables. See definition of “streams” on page 80." The tables never print "exempt from spring closure", only "Open all year" and
    similar. The four rows, read from the PDF (2026-11-26): "MOFFAT CREEK 5-2 Trout/char catch and release and bait ban downstream of falls 8 km from Horsefly River"
    (p.46, PDF 52); "BLUE LEAD CREEK 5-15 No Fishing downstream of falls situated approximately 2 km from Quesnel Lake, Aug 15-Oct 31" (p.43, PDF 49); "CHIMNEY CREEK 5-2
    Open all year downstream of Brunson Lake" (p.44, PDF 50); "MITCHELL RIVER" (with the "Includes tributaries" symbol) "5-15" "No Fishing within 100 m radius of the weir at the
    outlet of Michell Lake" / "No Fishing from Michell Lake to Cameron Creek" / "No Fishing downstream of Cameron Creek (including Cameron Cr.), Oct 1-June 30" / "Trout/char catch and
    release, bait ban, artificial fly only downstream of Cameron Creek (including Cameron Creek)" (p.46, PDF 52; the row prints no words for the river above Mitchell Lake).
    Unclear: (1) where a row names a stream but its words describe only a stretch (Moffat below the falls, Blue Lead below the falls, Chimney below Brunson Lake, Mitchell from the
    lake down; the river above Mitchell Lake has no words), is the whole stream excepted, or only that stretch? (2) Are the streams inside Bowron Lake Park
    ("Bowron Lake Park waters other than Bowron Lake") and Beaver Creek ("Beaver Creek chain of lakes") "listed"? (3) Does the exception extend to the tributaries of a listed stream marked with the tributaries symbol,
    such as Mitchell River? Meanwhile (owner, strictest reading): lifted only where a row prints its own exemption or "open all year", whole or part; closes 330.9 km in spring (Cariboo 143.7, Moffat 88.1, McKinley 38.5,
    Blue Lead 18.2, Little Horsefly 17.8, Mitchell 15.4, Canim 9.2). Evidence: audit note `P7b-region5-spring-closure`; the quotes above are the PDF's. Found 2026-11-26. To send to the Region 5 office.

## D. Not for sending: known and tracked in the gotchas or questions (cross-links, not copies)
16. **R1, Mine Lake and Main Lake, Quadra Island (MU 1-15), both p.17 (PDF 19).** Printed: "MINE LAKE 1-15 Engine power restriction - 7.5 kW (10 hp)" and
    "MAIN LAKE (Quadra Island) 1-15" "Bait ban and single barbless hook, Nov 1-Apr 30;" / "Engine power restriction - 7.5 kW (10 hp)" (LITTLE MAIN LAKE (Quadra Island) is p.16, PDF 18). Not an error in the book: the Main Lake row binds both polygons because FWA labels the Mine Lake shape "Main Lake" (same GNIS id 12126); the gazette's
    Mine Lake is an alternate name for that part of Main Lake (owner 2026-11-26). Kept on both; see GOTCHAS. Not for sending (it sits here, not in the lists to send).

20. **R4, Mackenzie Creek, p.38 (PDF 40).** Printed: "MACKENZIE CREEK 4-21 No Fishing"; the only one is in 4-31. Built: 4-31 (GOTCHAS "Book errata"); owner erratum 2026-10-26, kept (2026-11-26 audit). Evidence added 2026-11-26: the row sits under LUSSIER RIVER 4-21, a copied unit (entry 41); HILL CREEK 4-31 "No Fishing" is 3.6 km away on the same Galena Bay; no Mackenzie Creek exists in 4-21 (the nearest is 145 km off). To send; wrong since at least 2015-17.
21. **R2, McLennan Creek, p.24 (PDF 26).** Printed: "MCLENNAN CREEK 2-8 No Fishing". McLennan Creek (gnis:26380, 9.9 km) is in 2-4 (Abbotsford, 1.7 km from 2-8) and McLean Creek (gnis:37586, Coquitlam, about 2.7 km) in 2-8, which fits the run of small 2-8 urban closures printed with it. Built: both closed (BOOK-QUESTIONS). Owner 2026-11-26: "Both creeks closed, ask the Province (Recommended)". To send: "McLennan (2-4) or McLean (2-8)?"; found 2026-11-26.
22. **R5 Frog Lake "5-6" p.45 (PDF 51), R5 Redfern Lake "5-15" p.46 (PDF 52), R6 Squirrel Lake "6-1" p.47 and p.53 (PDF 53, 59).** Printed: "FROG LAKE 5-6 No Ice Fishing; trout daily quota = 1; bait ban, single barbless hook",
    "REDFERN LAKE 5-15 Trout daily quota = 2; bait ban, single barbless hook", "SQUIRREL LAKE 6-1 Trout daily quota = 2; bait ban, single barbless hook". No water found in the unit (GOTCHAS "Book errata"; BOOK-QUESTIONS). Built: bound nowhere.
23. **Unit disagreements with the WMU map (examined 2026-11-26):** Crooked River Park Bear and Square 7-16 (lakes lie in 7-24, 0.05 and 1.2 km from the line: line cases, kept), Bridge and Jack of Clubs 5-2, Chilliwack Lake 2-4, Stawamus and Lucille 2-9 and Lakelse River 6-10 (entries 25-38 below); Lost Lake near Whistler 2-8 (a misprint for 2-9; owner O3: moved to the Whistler lake). Source: the audit's question 3, then the out-of-unit full audit. The Crooked River Park pair goes with entry 40's kind of question.

## E. Found later: more printed units off (numbers continue; found 2026-11-26 evening)
The out-of-unit full audit looked at all 60 row-and-water pairs bound outside the printed unit; 14 rows are new "book's unit is off" finds. Owner: "Keep all 14 and add each to the Province list with its evidence (Recommended)". For each, the water is the only one of its name near (BC Geographical Names, "BCGNIS"), kept bound to it. "VORR" = the federal Vessel Operation Restriction Regulations. Quotes are the PDF's text layer as the audit read it; the printed page then the PDF page (PDF = printed + 2 in Regions 1-4, + 6 from Region 5). Notes `out-of-unit-full-audit` (per-pair evidence, maps).

24. **R5, Mitchell River, p.46 (PDF 52).** Printed twice in the row: "No Fishing within 100 m radius of the weir at the outlet of Michell Lake" and "No Fishing from Michell Lake to
    Cameron Creek"; the row above it is "MITCHELL LAKE 5-15 No Fishing within 100 m radius of the weir at the lake’s outlet". A misspelling (Michell for Mitchell). Meanwhile: read as Mitchell
    Lake (the cut `mitchell_river__mitchell_lake`). Found 2026-11-26 (re-reading entry 19). To send, low priority.

25. **R1, Prior Lake, p.17 (PDF 19).** Printed: "PRIOR LAKE 1-2 No powered boats". Wrong or unclear: the lake (wbk:329676437, 1.3 ha) lies in 1-1, 4.8 km from 1-2. Meanwhile: bound to it. Evidence: VORR Sched. 2 item 45 "Prior Lake near Victoria 48°28′35″ 123°28′01″" lies in the lake; BCGNIS 26972 "NE of Thetis Lake" and the book prints THETIS LAKE 1-1 0.9 km away; the other Prior Lake (5-2) is 453 km off. Found 2026-11-26. To send (with entry 39).
26. **R1, Prospect Lake, p.17 (PDF 19).** Printed: "PROSPECT LAKE 1-2 Smallmouth bass daily quota = 4; speed restriction on parts (8 and 60 km/h)". Wrong or unclear: the lake (wbk:329676317, 68.2 ha) lies in 1-1, 6.7 km from 1-2. Meanwhile: bound to it. Evidence: VORR Sched. 6 item 44 "all of Prospect Lake near Victoria", 8 km/h with an interior part at 60, which is the row's own "speed restriction on parts"; its point lies in the lake; BEAVER, ELK and DURRANCE (3-4 km away) are printed 1-1; the other Prospect Lake (6-13) is 811 km off. Found 2026-11-26. To send (with entry 39).
27. **R2, Chilliwack Lake, p.22 (PDF 24).** Printed: "CHILLIWACK LAKE 2-4 No wild trout over 50 cm, 1 bull trout over 60 cm". Wrong or unclear: the lake (wbk:329083341, 1,180.6 ha) lies in 2-3, 24.3 km from 2-4. Meanwhile: bound to it. Evidence: the only Chilliwack Lake in BC (BCGNIS 13745, "near BC-Washington boundary"); FOLEY LAKE beside it is printed 2-3; also printed 2-4 in 2015-17 and 2019-21 (T1118). Found 2026-11-26. To send.
28. **R2, Hope Slough, p.23 (PDF 25).** Printed: "HOPE SLOUGH 2-8 Electric motor only - max 7.5 kW". Wrong or unclear: the slough (gnis:24496, 14.4 km) lies in 2-3 and 2-4, 3.6 km from 2-8. Meanwhile: bound to it. Evidence: VORR Sched. 3 item 7 "Hope Slough near Chilliwack" lies on it (0.0 km); BCGNIS 24496 "in Chilliwack" is the only one; the Cheam and Sardis rows beside it agree with the map. Found 2026-11-26. To send.
29. **R2, Lucille Lake, p.24 (PDF 26).** Printed: "LUCILLE LAKE 2-9 Electric motor only - max 7.5 kW". Wrong or unclear: the lake (wbk:329532512, 10.6 ha) lies in 2-6, 15 km from 2-9. Meanwhile: bound to it. Evidence: VORR Sched. 3 item 19 "Lucille Lake near Garibaldi 49°57′52″ 123°09′47″" lies in the lake; BCGNIS has one (Lake Lucille 37388); the Squamish rows around it are printed 2-6. Found 2026-11-26. To send (with entry 40).
30. **R2, Stawamus River, p.26 (PDF 28).** Printed: "STAWAMUS RIVER 2-9 Bait ban, Dec 1-Sept 30". Wrong or unclear: the river (gnis:39479, 19.6 km) lies in 2-8, 32.6 km from 2-9. Meanwhile: bound to it. Evidence: the only Stawamus River in BC (BCGNIS 39479, "into head of Howe Sound"); the book prints its own tributary LITTLE STAWAMUS CREEK 2-8. Found 2026-11-26. To send (with entry 40).
31. **R5, Bridge Lake, p.43 (PDF 49).** Printed: "BRIDGE LAKE 5-2 Lake trout daily quota = 1". Wrong or unclear: the lake (wbk:329060770, 1,360.4 ha) lies in 5-1, 34.6 km from 5-2. Meanwhile: bound to it. Evidence: the only Bridge Lake in BC (BCGNIS 33293, "SE of 100 Mile House"); all nine neighbouring rows (Faulkner, West King, ...) are printed 5-1 and lie in 5-1; the lake-trout rule fits (stocking code 00903BRID is on this lake). Found 2026-11-26. To send.
32. **R5, Jack of Clubs Lake, p.45 (PDF 51).** Printed: "JACK OF CLUBS LAKE 5-2 WARNING: lake trout over 45 cm may contain elevated mercury levels. Limit your consumption." Wrong or unclear: the lake (wbk:329677094, 122.3 ha) lies in 5-15, 29.6 km from 5-2. Meanwhile: bound to it (an advisory only, so the wrong unit moves no rule). Evidence: the only Jack of Clubs Lake (BCGNIS 16935, "at Wells, W of Barkerville"). Found 2026-11-26. To send, low priority.
33. **R6, Dunalter Lake (Irrigation Lake), p.50 (PDF 56).** Printed: "DUNALTER LAKE (Irrigation Lake) 6-9 Electric motor only - max 7.5 kW". Wrong or unclear: the lake (wbk:329064543, 22.2 ha) lies in 6-8, 3.6 km from 6-9. Meanwhile: bound to it. Evidence: VORR Sched. 3 item 152 "Dunalter Lake (Irrigation), Houston" 0.36 km from the lake; BCGNIS 2687 "just N of Houston". Found 2026-11-26. To send.
34. **R6, Lakelse River, p.52 (PDF 58).** Printed: "LAKELSE RIVER CW 6-10" with "Bait ban, no powered boats" and the guided-angling line below it. Wrong or unclear: the river (gnis:4903, 21.8 km) lies in 6-11, 9.4 km from 6-10; the row names Lakelse Lake, whose own row is printed 6-11. Meanwhile: bound to it. Evidence: VORR Sched. 2 item 63 "Lakelse River, Terrace" (no power-driven vessels, the row's "no powered boats") 0.2 km from the river. Found 2026-11-26. To send.
35. **R6, Wahla Lake, p.54 (PDF 60).** Printed: "WAHLA LAKE 6-2 Rainbow trout daily quota = 1 (none over 50 cm)". Wrong or unclear: the lake (wbk:329613022, 505.5 ha) lies in 6-1, 6.2 km from 6-2. Meanwhile: bound to it. Evidence (medium): the only Wahla Lake in BC (BCGNIS 22471, "S side of Eutsuk Lake"); stocking code 01758UEUT links to it; no federal or locator evidence, and no other book row nearby to compare. Found 2026-11-26. To send, as a question: "is 6-1 meant?".
36. **R7A, La Salle Lakes (two polygons), p.60 (PDF 66).** Printed: "LA SALLE LAKES 7-3" with "Note: La Salle Lake “west” is only stocked". Wrong or unclear: both polygons (wbk:328981666, 13.0 ha; wbk:328981670, 12.7 ha) lie in 7-5, 2.6 and 2.7 km from 7-3. Meanwhile: bound to both. Evidence: VORR Sched. 2 item 65 "La Salle Lakes, McBride" 0.24 km away; BCGNIS has one (2482); the stocking codes on both polygons fit the row's "west only stocked". Found 2026-11-26. To send.
37. **R7A, Tsitniz Lake, p.61 (PDF 67).** Printed: "TSITNIZ LAKE 7-9 No powered boats". Wrong or unclear: the lake (wbk:329677206, 10.5 ha) lies in 7-8, 2.2 km from 7-9. Meanwhile: bound to it. Evidence: VORR Sched. 2 item 5 "Tsitniz Lake, expansion of Tsitniz Creek" lies in the lake (0.0 km); BCGNIS has one. Found 2026-11-26. To send.
38. **R8, Swan Lake (8-26), p.71 (PDF page not confirmed: 77 by the +6 rule; the audit's page text for this row came from another page).** Printed: "SWAN LAKE 8-26 Yellow perch daily quota = 20". Wrong or unclear: the lake (wbk:329459120, 412.1 ha) lies in 8-22, 2.6 km from 8-26. Meanwhile: bound to it. Evidence: BCGNIS 20909 "between Vernon and Armstrong", the only Swan Lake in Region 8 apart from one in 8-6 (130 km); stocking code 00099OKAN is on this lake; KALAMALKA LAKE nearby is printed 8-22, the lake's own unit. Found 2026-11-26. To send.

The three patterns the audit lists (one question each to the Province, covering the entries named):
39. **R1, Victoria: 1-1 and 1-2 mixed (Langford p.16 (PDF 18); Prior, Prospect p.17 (PDF 19)).** Printed: "LANGFORD LAKE 1-2", "PRIOR LAKE 1-2", "PROSPECT LAKE 1-2". Wrong or unclear: all three lie in 1-1 (Langford within 50 m of the line); their neighbours THETIS, ELK, BEAVER and DURRANCE are printed 1-1, and Glen Lake 1-2 lies in 1-2. Meanwhile: bound to the lakes. Evidence: entries 25 and 26; the audit's table. Found 2026-11-26. To send: "are the Victoria 1-2 prints meant to be 1-1?".
40. **R2, Squamish: 2-9 printed on 2-6 and 2-8 waters (Lucille p.24 (PDF 26); Stawamus p.26 (PDF 28)).** Printed: "LUCILLE LAKE 2-9", "STAWAMUS RIVER 2-9". Wrong or unclear: they lie in 2-6 and 2-8; their own neighbours (LITTLE STAWAMUS CREEK 2-8, the Squamish River 2-6) are printed correctly. Meanwhile: bound to the waters. Evidence: entries 29 and 30. Found 2026-11-26. To send: "is 2-9 a typo for 2-6 and 2-8?". The same kind of question applies to BEAR and SQUARE lakes of Crooked River Park (printed 7-16, lying in 7-24: entry 23).
41. **Copied units: a row prints the unit of the row above (R7A Emerald p.59 (PDF 65), R8 Ellison p.69 (PDF 75), R4 Mackenzie Creek p.38 (PDF 40)).** Printed: "EMERALD LAKE 7-15" directly under "EENA LAKE 7-15"; "ELLISON LAKE 8-8" under "ENEAS CREEK 8-8"; "MACKENZIE CREEK 4-21" under "LUSSIER RIVER 4-21". Wrong or unclear: each water lies in another unit (7-16, 8-10, 4-31). The Mackenzie case is new in this audit and supports the owner's erratum. Meanwhile: bound to the real waters. Evidence: entries 5, 7 and 20. Found 2026-11-26. To send: "a copied unit from the row above?".
