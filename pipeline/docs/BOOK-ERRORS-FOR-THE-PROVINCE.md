# Book errors for the Province: one entry each, ready to send

Owner, 2026-11-26: "also could add to gotchas in a way that if we wanted to compile errors we found to send to gov it was
easy." One entry per printed error or loose print, one shape. Pages are the printed pages of the 2025-2027 synopsis, then the page of
`data/source/fishing_synopsis.pdf` (PDF = printed + 2 in Regions 1-4, + 6 from Region 5). Quotes are the PDF's text layer, exact. Ways to read a row two ways are
`BOOK-QUESTIONS.md`; what the app does about each is `GOTCHAS.md` and `docs/OWNER-RULINGS.md`. **Add an entry the day a
new error is found; never delete one: change its status.** Status: to send / sent / answered.

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
16. **R1, Mine Lake and Main Lake, Quadra Island (MU 1-15), both p.17 (PDF 19).** Printed: "MINE LAKE 1-15 Engine power restriction - 7.5 kW (10 hp)" and
    "MAIN LAKE (Quadra Island) 1-15" "Bait ban and single barbless hook, Nov 1-Apr 30;" / "Engine power restriction - 7.5 kW (10 hp)" (LITTLE MAIN LAKE (Quadra Island) is p.16, PDF 18). Not an error in the book: the Main Lake row binds both polygons because FWA labels the Mine Lake shape "Main Lake" (same GNIS id 12126); the gazette's
    Mine Lake is an alternate name for that part of Main Lake (owner 2026-11-26). Kept on both; see GOTCHAS. Not for sending.

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

24. **R5, Mitchell River, p.46 (PDF 52).** Printed twice in the row: "No Fishing within 100 m radius of the weir at the outlet of Michell Lake" and "No Fishing from Michell Lake to
    Cameron Creek"; the row above it is "MITCHELL LAKE 5-15 No Fishing within 100 m radius of the weir at the lake’s outlet". A misspelling (Michell for Mitchell). Meanwhile: read as Mitchell
    Lake (the cut `mitchell_river__mitchell_lake`). Found 2026-11-26 (re-reading entry 19). To send, low priority. (Numbered after section D's entries: numbers are never reused.)

## D. Already known and tracked in the gotchas or questions (cross-links, not copies)
20. **R4, Mackenzie Creek, p.38 (PDF 40).** Printed: "MACKENZIE CREEK 4-21 No Fishing"; the only one is in 4-31. Built: 4-31 (GOTCHAS "Book errata"). To send; wrong since at least 2015-17.
21. **R2, McLennan Creek, p.24 (PDF 26).** Printed: "MCLENNAN CREEK 2-8 No Fishing". McLennan Creek is in 2-4 and McLean Creek in 2-8. Built: both (BOOK-QUESTIONS).
22. **R5 Frog Lake "5-6" p.45 (PDF 51), R5 Redfern Lake "5-15" p.46 (PDF 52), R6 Squirrel Lake "6-1" p.47 and p.53 (PDF 53, 59).** Printed: "FROG LAKE 5-6 No Ice Fishing; trout daily quota = 1; bait ban, single barbless hook",
    "REDFERN LAKE 5-15 Trout daily quota = 2; bait ban, single barbless hook", "SQUIRREL LAKE 6-1 Trout daily quota = 2; bait ban, single barbless hook". No water found in the unit (GOTCHAS "Book errata"; BOOK-QUESTIONS). Built: bound nowhere.
23. **Unit disagreements with the WMU map, unexamined:** Crooked River Park Bear/Square 7-16; Bridge, Jack of Clubs 5-2; Chilliwack Lake 2-4; Stawamus, Lucille 2-9; Lost Lake near Whistler 2-8 (a misprint for 2-9; owner O3: moved to
    the Whistler lake); Lakelse River 6-10. Source: the audit's question 3. To send as one question.
