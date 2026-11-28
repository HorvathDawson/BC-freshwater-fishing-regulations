# The owner's standing rulings

One place for the answers that set a pattern (T1158). Each ruling is in his words (verbatim, his spelling), with the
date and the ticket. Before a default is offered or built, read this page: a default that contradicts a ruling is not
a default. How the printed book is read, once BUILT, is `pipeline/docs/RULINGS.md`; a ruling below moves there in
the change that builds it.

**Nothing in "Data rulings" is built yet.** The fixes ride ONE future data build, together, and nothing from the
audits is built before the owner's audit review ends. His words, 2026-11-26: "okay perfect we will review in morning
no changes will happen from audit until after that review by me"; and "this will go into the mornign bundle to and
any fixes we end up making including other region issues will go into a future build with tthose other match issues".

## How the work runs

- Everything Claude adds for review is a proposal, never a decision (2026-10-29).
- No silent fallbacks: a failure is shown, never hidden behind a callback (2026-11-03).
- The app is fully free; the goal is access (2026-11-19).
- Production commands are only his: he alone types `CONFIRM_PROD` (2026-11-22).
- Parser runs are his alone: Claude never starts the LLM parser or anything that spends credits (CLAUDE.md).
- Part names: every part of a water has a real name or a distance, never a number: "there should be no partt x of n
  nameas anywhewre in the app" (T1158; T780, 2026-11-20).
- Questions after midnight wait for the morning, unless urgent: "questions uo to midnight is okay then batch them
  after (unless urgent)" (2026-11-25, T1159).

## Data rulings

### A river on a unit or region line is in both (Campbell River), 2026-11-25
"campbell river is correct it straddles so is included in mu 1-10 and 1-6 so is in closure". He confirmed ("yes to
this") that this covers unit and region LINES only, not an area rule (a WMA, a park) reaching far outside its area by
a small overlap (T1149). Keep as built.

### Rancheria River: tributaries only, by the row's title, 2026-11-26
"rancheria is tribs only..." (with a picture of the row "RANCHERIA RIVER'S TRIBUTARIES 6-25"). The audit's finding
was wrong. Keep as built; an auditor reads a row's title for "'S TRIBUTARIES".

### Name bindings outside the printed unit (T1118), 2026-11-26
All seven overrides date from the first commit of `data/curated/regulations/book_name_links.json` (2026-03-14); nothing recent broke them.
"wait how the fuck did this happen they all used to link properly"; on the build check: "a lot of these ... should
mention it in notes" (each override's note says why).
- **Mosquito Creek = the Arrow Park one:** "miquot creek yes should be the arrow park one." (gnis:7335; BC
  Geographical Names keeps "Arrow Park (Mosquito) Creek" as its rescinded name.)
- **Bear (Mahood) Creek = the Surrey creek only:** "bear mahood creek should only link to the one its splits
  match." (gnis:12114; drop gnis:12117 near Harrison.)
- **Twin Lakes 4-34 = the pair inside MU 4-34 only:** "yes to this (The research recommends the pair in the printed
  unit...) bind to thwe new one only... add as note" (wbk:329094185 + wbk:329094397; drop Yankee and Canuck lakes;
  the note gives the evidence: Botts Lake 4-34 beside them prints the same rule). This replaces his earlier lean, "it
  should match to premier lake".
- **Hall Lake 4-34 = the lakes in the unit:** "twin lakes if there is a set in the unit should match to those. same
  with hall lake."
- **McDonnel Lake = the 6-9 lake only:** "maybe the mcdonnel one was because we couldnt tell if it was right or not?
  is the proper name in the right unit?" (it is: the 6-9 lake near Smithers; drop the 6-4 McDonell Lake).
- **Panther Lake points at Healy Lake:** "fix the pointer for panther lake and make sure it is in name variants."
- **McLennan Creek 2-8: bind both for now, and ask the province:** "should match to both maybe for now then make sure
  it is in gotcha as a question to ask." (McLennan Creek in MU 2-4 and McLean Creek in MU 2-8; the question is in
  `pipeline/docs/BOOK-QUESTIONS.md`.)

### P1: "includes tributaries" keeps walking through a lake, 2026-11-26
"it will keep walking but somehting like lakes own tributaries will replace it IF IT TARGETS THE SAME ITEM. for
example it wont remove the lower rules unless it is effecting the same thing like if lower one says no fishing for
trout then lake says 2 trout of x size that will replace the no fishing for trout. bvut if it was sayign 2 kokanee
then it wont because they are orthogonal."
- Read as: the 2026-09-24 rule stands (keep walking). A row higher up (the lake's own tributaries row, a water's own
  row) replaces an inherited rule only where both speak to the same thing (species, subject); other rules stack.
- Open (asked): Trout Lake's tributaries row speaks only of bull trout, so the Lardeau and Duncan rainbow exemption
  would still reach Trout Lake's 51 creeks; Cameron River stays closed.

### P2: the Classified Waters licence climbs like the fish rules, 2026-11-26
"this should follow the same rules as other tributary climbs. if a cw doesnt say includes tribs it is only for itself
but only a few streams are like that such as thompson... otherwise the license climbs the same as what we just said
and gets overridden by upstream cw titles the same way too."
- Read as: with the row's tributaries mark, the licence climbs through head lakes like the fish rules; with no mark,
  only the water itself; a Classified row upstream overrides it. Open: Babine's "from the weir downstream".

### P3: a printed unit never limits a row, 2026-11-26
"the listed mu in the book are just for identification not authority so keep as it is now. it is not limited to its
unit ever. (region sometimes is like with the fraser)"
- Read as: keep as now. A region limits a row only sometimes (the Fraser). Open: region BASE rules on a lake across a
  region line (Kinbasket carrying the 7A base).

### P4: tributaries of excepted rivers (the Squamish), 2026-11-26
"Keep it as now".

## Audit rulings, batch 2 (2026-11-26, T1177)

His words, verbatim. The data change is the line after "Build:"; all of it rides the one audit data build inside the
0.10.0 re-cut (T1177), with an answers diff per change. His answer to "when": "Inside the 0.10.0 re-cut".

### Principles that came out of the audit
- **The walk continues through lakes** (P1, above); an upstream row replaces an inherited rule only if it targets the
  same item; orthogonal rules stack (P1 follow-up: "Yes, the ruling stands").
- **The printed MU identifies, it does not authorise** (P3, above). A row is never limited to its unit; a region
  limits a row only sometimes (the Fraser).
- **Fixes go in at the extent level, no special cases** (P5, P6b, O13): a change to an extent op, never a code path
  for one water.
- **An MU-boundary river is in both units** (Campbell River, above; D1 of the audit).
- **One tributary logic across the board, book and DFO**: "unless you have exact dfo words saying what they mean by
  tributaries we should have a single tributary logic that is the saame across the board." Build: DFO places climb
  head lakes like the book's rows, unless DFO's own written definition says otherwise (look for DFO's exact words first).

### P5: a tributary at a row's boundary point
"yes to leave out x unless including is what i want but we need to implement it in the extent level like by having
changes to op maybe? need it to be low level". Build: X (and its tributaries) is left out of "upstream / downstream of X"
unless the row says "including"; an extent op, not a special case (16 cases by the audit's count).

### P6a, P6b: notes and spot rules stop walking
P6a: "Facilities and 'please note' lines (Recommended)". Build: they stop inheriting the tributaries mark.
P6b: "yes to recommended but please only change extents to make it happen". Build: the 4 spot closures and the Bulkley
method rule stay inside the spot; boat rules keep walking; extents only.

### P7a: Stellako opens May 15
"yes to open since it seems to be a replacement closure time to the springf closure. add to gotcha too and make sure it
if lifted properly". Build: the row's Nov 15-May 14 replaces the spring closures; open May 15; GOTCHAS entry; verify the lift.

### P7c: Hevenor, Station, Two Mile
"keep release add to gotchas". Build: the Nov 1-Jun 30 trout release stays; GOTCHAS entry.

### P8: steelhead on lakes
"Rows + the region box + Tenas (Recommended)". Build: the lake copies bind to lakes whose rows mention steelhead, the
lakes a region's steelhead box names, and Tenas.

### P9: marshes are lakes
"Treat marshes as lakes". Build: the 16 marsh and swamp waters of 7A (and any other region's) take the lake rules.

### P10: the seven out-of-unit bindings stay
First: "we should have an agent audit these and build evidence in all directiosn and look for alternatives then i will
review". After the evidence: "i agree with the 7 items can you add all the info to the notes? so we dont have to
refined it. also could add to gotchas in a way that if we wanted to compile errors we found to send to gov it was easy."
Done: the seven overrides (Lynx, Chubb, Bridal, Hiawatha, Emerald, Nation River, Ellison) keep their bindings and carry
the evidence in their notes; the errors are listed in `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`.
P10b, the cross-unit check: "Yes, report only (Recommended)" (per row-and-water pair, an exceptions file, report only
until the rebinds land).

### P11: the August 2026 book
"adopt it eexcept we dont need a parser run you can run a subagent to produce the changes then youi can audit it and
make sure it parsed it right. drop the dropped rows by adding a drop boolean to the entries witrh a note why. also can
we add gotchas for them being dropped with info." Build (T1179): no parser run; a sub-agent applies the changes by
hand, the planning chat audits; Little Qualicum River and Lizard Lake are dropped with `drop` plus a note; GOTCHAS entry.

### DFO tributaries at head lakes
See the principle above (one tributary logic). Build: T1177.

### T1084: creeks with no bank in the data
"Record the method, build nothing (Recommended)". The 98 %-accurate geometry method is kept for the day a case appears. A one-bank place's creek whose side cannot be read (T1029, 2026-11-27): "keep it closed (for trib rules i assume) but one thing to note we should add a thing that warns us if this is ever a thing".

### Side-channel labels
"all of them now. also we need to make sure th ui still says if it is in an area in the subtitle in the app so a user
knows eco reserve or park or whatever in those cases. (like how sections use start and end to create subtitle. we need
this to be universal and cleanly produced the same wat the a to b labels are". Also: "we want consistent naming for
anyhting so have consistent rules"; "No verb for a leaving channel (Recommended)"; on a unique channel: "most seperate
ruled side channel shouldnt need addition differentiation". Read as: a distance only when the water has more than one
such part; the area (park, eco reserve, WMA) shows in the app subtitle for every part inside one, built by the same path
as the A-to-B labels. Server side: PR #312. App side: T1181. Old Kootenay: "Shorter: '... inside Creston Valley WMA'";
Thelwood: "yes name it but this is something we need to fix generally."; channels take the river's rules: "Yes, after the
park-rule fix (Recommended)" (T1149).

### The one-offs, "One by one, with the book's words (Recommended)"
- **O1 Little River:** "Row dates replace the spring closure (Recommended)". Build: both rules bind the whole river.
- **O2 Upper Chehalis River:** "close it and check pattern. we need to think about this because tributaries have a
  closure so in this case it needs to override." Build: closed; a lake-tributary closure overrides.
- **O3 Lost Lake near Whistler:** "Move it to the Whistler lake (Recommended)". Build: the electric-motor rule moves.
- **O4 Hatton Creek:** "Add fly-only to Hatton Creek (Recommended)". Build: fly only ALL YEAR, as the book prints it (no dates); his batch-1 answer: "it is all year".
- **O5 Gwillim, Muncho:** "Keep the zone limit (Recommended)". Build: none.
- **O6 McRae Creek:** "that is the real falls location". Build: none; the cut stays at 2.05 km; the book's "about 4 km" is on the Province list.
- **O7 Connor Lake's tributaries:** "it needs to apply to all tributaries of the lake need to make sure this is true for
  all items like this". Build: every "X Lake's tributaries" row binds ALL the lake's inflows; a province-wide sweep.
- **O8 Robb Lake:** "Keep the zone dates (Recommended)". Build: none.
- **O9 Okanagan River Oxbows:** "Keep both exempt, list it for the Province (Recommended)". Build: none; on the Province list.
- **O12 Hay River basin:** "is this the basin alogn the alberta boarder on the right side of liard river watershed? im
  not sure here this should be added to gotchas I think and we will not add hay basin in for now. leave it stricter".
  Build: none (the true basin, all bull trout released); GOTCHAS and the Province list.
- **O13 Healy Lake outlet:** "yes to bleow the lake but make sure you use global extent for this not rule level".
  Build: the closure is cut below Healy Lake as a global extent on the water.
- **O14 Maka Creek:** "keep on both because they are same MU and everything make sure the gotcha is there for this". Build: none; GOTCHAS.
- **O15 Tamihi Creek:** "Our cut is the falls". The book prints two lines: "No Fishing downstream of the falls situated approximately 200 m upstream of Chilliwack River" and "No Fishing upstream of the falls situated approximately 200 m upstream of Chilliwack River, May 1-June 30". The data matches both lines (below the falls closed all year, 0.66 km; above it May 1-Jun 30, 13.1 km); only the distance differs (0.66 km, not "approximately 200 m"). So the ruling stands: no data change. Province list entry 9.
- **O16 Shuswap above Sugar Lake:** "it is a better more cautious reading to limit it so that it is not exempoting the
  whole river so exempt here". Confirmed (below): no data change.
- **O17 Nanaimo River:** "it is probably a format issue and is okay add to gotchas." Build: none (Oct 15 stays); GOTCHAS.

### P7b: Region 5 "other streams listed in the tables": the strictest reading (RULED, 16:00)
"i think the other streams listed means the entries with exemptions on a partt or open all year on whole or part etc. we
should be strict here so lets take the strictest reading add it to gotchas with explicit ask of what we are confuysed
about here (what is lifted items etc)". Build (T1177, option C of the P7b audit): the spring closure is lifted ONLY where
a row prints its own exemption or "open all year" (whole or part); rows with only gear or quota rules keep the closure.
Closes 330.9 km in spring before the lower Quesnel ruling below (Cariboo 143.7, Moffat 88.1, McKinley 38.5, Blue Lead 18.2, Little Horsefly 17.8, Mitchell
15.4, Canim 9.2). GOTCHAS entry and the Province question (BOOK-ERRORS entry 19) spell out what we are unsure of.

### O16 confirmed; the Oxbows label (16:00)
O16: "Yes, that is right": catch and release on the whole Shuswap River; the spring exemption only Mara Bridge-Sugar
Lake; no data change. Oxbows label: "Follow the rule, show me in the final table (Recommended)", i.e. the general label rule.

### Lake zones: build nothing now (16:10)
Zone leanings (verbatim, INPUTS not decisions): "i think a mix of cuts and overlays maybe? like cuts for the larger
divisions liek gullwing with full closure but overlays for smaller ones like the stream mouth closures. then we just need
to make the ui map for the card show this better and make it clear to users who open it from map or from a website
link"; year bar: "in recut but we should add a new line type for paretial closure where it can be half red half other
type maybe?"; off-day zones: "not drawn until getting close to regulation change then maybe it could have a. new color
indicating changing soon? im not sure."; Shuswap A: "i think a real cut but (same thing ...)". And: "i think all this
lake stuff we need to just leave it how it is now and really have a few agents look into a clean way to do this and plan
it out and figure out the best way so it isnt janky." So: nothing is built; a separate design chat researches and brings
proposals with pictures of options (T1180). Still OPEN: every zone pick.

### O10 Goat River and O11 Mine Lake: RULED (17:10)
- **O10:** first "add to gotchas and could it be boundaries? maybe it means between 2 confluences? we need to decide on best interpretation"; after the evidence: "between the 2 only with a gotcha added". Build (T1177): catch and release only on the main stem between Leadville Creek (km 33.1) and Cameron Creek (km 42.6); the rest of the main stem takes the regional quota. GOTCHAS and Province entry 11.
- **O11:** first "Show me the map first"; after the evidence: "keep on both add to gotchas because in the gazette mine lake is labelled main lake by fwa but mine lake is a alt name for that part of main lake". No data change; GOTCHAS entry.

### The tributary principle and chains (17:10; goes with O2, O7)
"everything in is a tributary and only shouldnt be if an exception is listed saying otherwise." So a lake's tributaries row covers the river entering above the
lake too, even when that river has its own row. Chains: "climb through everything (a stream is not a trib of itself tho so if stream a from x to y includes tribs then
upstream of x is not included. lakes include all. and tribuatries stack so that if many tribuatry rules are on a water the most upstream ones get priority if they
conflict (for example multiple quotas on same species upstream is the one that takes over) if that makes sense". Read as: the walk climbs through every lake; lakes
include all inflows; a stream "from x to y, incl. tributaries" does not include itself above x; tributary rules stack and on a conflict about the same item the MOST UPSTREAM row wins.
A walk change: needs a reach rerun and a rule-bindings diff (T1177 item 10).
- **Merge order, the walk and the gear wording (2026-11-27 ~10:45):** "Wording fix first, then the walk, both in 0.10.0 (Recommended)" (T1249 first, then PR #365, T1177).
- **A lake's printed exception stops the rows of the lakes below it (2026-11-27 ~10:45, Kinbasket and Buttle):** "wait so you mean kibasket lake will stop other tributaries from below the lake from walking up as well as not havign its own walk up columbia? thatr seems like a fair assumption and add to gotcha" (RULINGS T19, GOTCHAS item 6).
- **Kootenay Lake's exception (2026-11-27 ~11:00, with the map):** "the does not include exception really intends to means upstream of lake (all upstream)": all of the Kootenay above the lake stays out, as built (RULINGS T19).
- **Whiteswan's feeders above the lake (2026-11-27 ~11:00):** "Include them: No Fishing all year (Recommended)" (RULINGS T19, E26).
- **Upper Arrow climbs to Kinbasket (2026-11-27):** "it should climb all the way to kinbasket lake i think?"; confirmed: "yes it doesnt matter anyways because lake revelstoke has same regs for its tributaries but yes this is what we do" (RULINGS T19).
- **DFO places walk like the book (2026-11-27):** "Yes, in 0.10.0" (built on branch `fix/T1177-dfo-one-logic`; RULINGS T19).

### OPEN, not ruled
- **The lake-zones picks** (above). **Licence wording (FWID, WILD):** no answer recorded in this chat.

## Audit rulings, batch 3 (2026-11-26, evening; planning chat, relayed)

His words, verbatim. Answers given in the planning chat and relayed verbatim to the chat that builds count as his yes
(the standing ruling below). Builds ride the same one data build inside the 0.10.0 re-cut unless a line says otherwise.

### Labels (the #312 final table; T1172, T1196, T1191)
- **Snehumption:** "Plain \"2 side channels\" (Recommended)". The rule: if not every channel in the part has a named end, the part reads the plain count.
- **Klinaklini and MacMillan, no "Mouth → Source":** "Plain \"Side channel\" / \"3 side channels\" (Recommended)". The app change (T1196) ships first, then the one-line data change.
- **"Eco Reserve" everywhere:** "Shorten Ecological Reserve to \"Eco Reserve\"", then "it should be \"[name] eco reserve\" everywhere" (names, ends and subtitles alike).
- **Names inside areas** (the naming model, T1191): see the ticket for his words; a part wholly inside an area says so, every separate side-channel part says why it is separate, and lakes partly in a cut area carry a "partly in" chip.

### Tamihi Creek stands (O15)
"Our cut is the falls" is unchanged. The book prints two lines (closed all year below the falls; closed May 1-June 30 above); the data matches both and only the distance differs (0.66 km, not "approximately 200 m"). Told to him 2026-11-26; to be re-confirmed in his audit review. Province list entry 9.

### Standing rules: gauges, feedback, storage, how decisions are put
- **Answers given here count:** "Yes, answers given here count (Recommended)". An answer given in the planning chat and relayed verbatim counts as his yes for another chat's merge. Irreversible deletes still need him directly (the staging-backup release copy, 0.48 GiB, waits for his own yes in that chat).
- **R2 storage is a soft target:** "Whole account, but a soft target: trim the waste, a few GB over is fine (Recommended)" (replaces the hard 10 GB reading; the account measured about 22 GiB). Clean-up he selected: "Compress the big source files, Delete leftover test uploads and old test app files, Delete the old release copy in the staging backup"; not selected: shortening the staging backups to 14 days (they stay 30). The raw basemap: "maybe we should just have the command to refetch the raw pmtile? then we can pin that one as a backup into r2?"
- **Design decisions come as whole options:** "also we need to have just a couple proposals with all the info and then i can select how it is right now having all the exampels seperate makes it hard to make a decision i think we need to make like a option A-... wehere each one is different ways we could handle it. make sense? this is just hard for me to interpret." (Said on the cut rule; it holds for every design question: 2-4 complete options A, B, C on one sheet, each combining the parts that belong together.)
- **The gauge page is one shared piece:** "one shared piece used by both website and app is a yes. ideally we have the colour bar showing current conditions with where it currently is like the condition tab. we want this page to have minimal text all the sources at bottom like all other pages. clean ui etc etc." On the web page: "the web has a big \"live level and flow, nd the weather ...\" at the top this is bloated and can be in sources at bottom. or actually just removed.... the app is much cleaner here. i like app design." Three ranges: "Three ranges: 72 h, 1 week, 1 year (Recommended)".
- **Gauges with no water card are reachable:** "yes as long as they have up to date data" (the conditions map's tap on a gauge, T1194; lower tidal Fraser gauges).
- **"Current" is judged against the feed, not the clock:** "A new ticket, in the 0.10.0 re-cut (Recommended)" (T1205).
- **Tester pictures:** "build A we still will send stuff to reviewwrr after we can send the one proposal to see what they think." The search terms stay: "i like my search terms we should leaves those they are fun." Bigger text: "we just want to make sure bigger text plays nicely in all parts of the app." Towns: "i want you to choose best judgement for users to have the best experience across bc and make it easy to find a location by zooming in from far out". Map key: "Floating \"Map key\" over the map, near the bottom (Recommended)"; legend words: "\"Its own rules / Region rules only\" (picture 21)"; day selector: "One \"Today\" chip that opens the calendar (Recommended)"; phone controls: "definitely drop +/- then we need to maybe rework all the rest of the controls?"; Dease Lake salmon block: "Leave it as it is (Recommended)" (T1202).
- **T1077:** "Yes, bait after gear is fine (Recommended)". **T1078:** "yes do primary closure. the as built is better then mockup. we could combine the salmon and game fish schedules for year round closures tho if they are both always closed (they could split again if it ever changes)." (Primary closure only; one block "Salmon and game fish" with one year bar: "One block \"Salmon and game fish\" with one year bar".) Top tabs shorten their words when large text makes them too wide.
- **T1167 banner:** "Yes, ship as pictured (Recommended)". **T1163:** "just test in emulator instead for now".

### Reserve and park cut rule (T1199, T1240-T1242)
- **Edge water along a reserve or park:** "Closed, like a boundary river (Recommended)".
- **A stream beside an area that never crosses:** "Never inside (Recommended)". With his design words: "also if a stream never enters a eco area at all then it will never get cut and will not get considered inside. it must cross at least once or be very close. ... we dont want tiny artifacts left because of the 25m or whatever distance rule."
- **Timing:** "In the 0.10.0 re-cut (Recommended)". The cut rule built is T1199 (real crossings, 50 m band, no piece under 100 m). **The opens list and the two edge stretches G6 named (2026-11-27):** "Merge with the list; close both edge stretches (Recommended)": T1199 merges into the 0.10.0 re-cut, the Beaverfoot River along Yoho (123 m) is closed as edge water; for the creek along the Klanawa River Ecological Reserve, shown its picture: "Stop at 370 m" (closed to 370.0 m as today, its last 51.6 m open as today) (`waters/area_edge_calls.json`). Both came after his night answer "Hold for the loop-removal trial" (Return batch 4). On the parts named "inside" after the cut (the Skeena side channel at the Skeena River Eco Reserve, 2026-11-28): "yes but we need to make sure the rule is consistent across the board here not just a ranom special case".
- **The water option:** "D: simplify the water itself"; late folds: "Hold the re-cut for all of them"; loop removal: "Run the loop-removal trial first, then I decide" (T1242). Snehumption's fold: "i need to see on map for how to handle it" (T1240).
- **Open water between two crossings of an area (T1199, E36):** shown the Beaverfoot River beside Yoho: "do the recommended the app should auto join them later i think assuming same rules"; then: "nvm it wont but that is okay". It reads "Outside Yoho National Park of Canada (between its boundaries)"; "Within" only where the part carries the area's rule.
- **Side channels named inside a reserve (T1199, E35):** 2026-11-27 on the Skeena's side channel: "how can skeena river inside skeena river eco reseve be 72% inside. we cut on eco reserves so it should be 1005 always for eco reserves"; 2026-11-28, shown its picture: "yes but we need to make sure the rule is consistent across the board here not just a ranom special case". Built for parts of side channels only at cut parks and reserves; main-stem parts and whole waters (named by their ends) are an open question to him.

### Tributary and place picks of the one-offs batch (data chat)
- **Healy Lake outlet:** "Yes, open the 5.05 km above the lake (Recommended)".
- **Goat River:** "Yes, open the 76.8 km to the regional quota (Recommended)" (the rest of the main stem takes the regional quota; entry 11 of the Province list).
- **Mosquito Creek:** "Yes, move it (Recommended)" (to the Arrow Park creek, gnis:7335: closes about 36 km, opens 1.4 km).
- **Lake rebinds:** "Yes to all four (Recommended)". The four: McDonell Lake (6-4); the Twin Lakes 4-34 versus Yankee and Canuck lakes; Hall Lake 4-20 versus the 4-34 Hall lakes; the Malcolm Knapp Lost Lake versus the Whistler Lost Lake. Panther Lake is not one of the four (its pointer fix is the earlier ruling above).
- **Hatton Creek and the Caycuse River (replaces O4's fly-only dates):** "it is all year and make sure this is also to the mainstem and other tribs that they all get all year". Final rulings: fly fishing only ALL YEAR (not Sep 1-Jul 14) on Hatton Creek and on the Caycuse main stem above the Hatton confluence. Other tributaries: "Only Hatton Creek and the main stem, as the book prints (Recommended)". The Caycuse Jul 15-Aug 31 closure scope: "Keep as now" (the closure stays on the whole Caycuse, not on Hatton Creek).

### Rulings made after the first draft of this batch (2026-11-26, evening; planning chat, relayed)
- **Snehumption Creek:** "The whole creek is a Similkameen tributary". The fold opens the 17 km main line in spring (the channel carries the Similkameen tributary rows). Built as a rule: a stream that leaves B.C. and joins a B.C. river downstream of the border is that river's tributary; the other waters the rule touches are shown to him first (T1240). The confluence with the Similkameen is in the U.S. (Province list entry 42).
- **Stellako:** "Yes, open May 15 as built (Recommended)" (P7a stands as built).
- **Little River:** "yes we want the strictest here." (O1 stands as built).
- **The lower Quesnel River, below the Cariboo:** "Yes, close it too (Recommended)": about 100 km more spring closure (99.9 km measured); Region 5 total about 431 km (430.8 km) (T1177 item 8, Province list entry 19).
- **Lake zone cuts (T1180):** "Every line or bay mouth, both pieces at least 0.5 km²: 11 cuts (Recommended)"; approximate edges: "Keep them as approximate marks until located". The card map and search: NOT approved: "more design please just this changes a lot so i want to make it more clear because it effects streams too..." (round 3 designs the card's map for lakes and streams as whole options).
- **Lake zones, his latest words (T1180):** "in terms of the lake zones and cuts etc that is a big change and i dont think we should implement it yet we want to really prepare how it is going to happen first so weshould go through the rest of the prep I mention and we really want to nail down a few proposal of how it qworks with the current workflow and menus. with the card reg condition map items and the actual map." Plain reading: no build of lake zone cuts until he picks a whole proposal; the design lane prepares 2-4 whole proposals covering the card, the regulations view, the conditions map items and the map.
- **The water option D:** "Run the loop-removal trial first, then I decide" (T1242); the parts C and D share are built now.
- **Braid loops (T1242, 2026-11-27):** "i think i changed my mind for now we should just save info here and only remvoe the loops with no stream connecting it (if it isnt something like the stave spawning channels or following our other rules for when they cant be removed...) but we will leave loops that have a stream connecting for now. it is too much to figure out right now". Reading: loops no creek flows into go in the 0.10.0 map build, any size, unless named or kept by structure (E31). Loops with a creek: left for after 0.10.0 (T1260, the planning chat's ticket). The Goat River side channel: "it stays by 150m rule ...".
- **While he is away:** "Collect everything; nothing that opens water or changes a look merges without me (Recommended)".
- **The two T1177 branches (one-offs PR #330, spring closures PR #339):** "Yes, merge both (Recommended)".
- **Away rule from now on** (replaces the line above): "Yes, approved items merge (Recommended)": anything he already picked or answered merges after its full check and a second agent's review; only NEW openings, new looks and open questions wait for him.
- **Blue Lead Creek below the falls** (0.9 km, kept lifted by its Aug 15-Oct 31 closure): "Keep it open in spring (Recommended)" (as built).
- **An annual quota under its own row's release (T1243)**: "Hide the annual quota while release applies (Recommended)".
- **Strathcona-Westmin Park** (part of "Strathcona Park" for the rules that name it?): "Yes, treat it as Strathcona Park (Recommended)" (T1240).

### Out-of-unit audit (T1118)
- The 14 newly examined rows whose printed unit is off: "Keep all 14 and add each to the Province list with its evidence (Recommended)" (entries 25-38 of the Province list).
- **McLennan Creek 2-8:** "Both creeks closed, ask the Province (Recommended)" (the 2-4 McLennan and the 2-8 McLean creeks both closed).
- His question that began the full audit: "what ever happen to the audit a long time ago looking at all waterbodies that were matched to items in a different mu you said there were like 50 but we only looked at 7". All 60 pairs are now audited: no new wrong water.

### Lake zones and the naming model: the direction
- **Lake zones:** the mix is the direction: "i think we do want some to be full cuts because they are big sectiosn liek obvious kooteay lake. however smaller areas should be marks only so i think the mix is the correct way"; every zone bounded by a line becomes a cut: "all line cuts make sense to do" (T1180).
- **Naming:** P3 (area names plus chips), the full first stage inside the 0.10.0 re-cut (T1191).

### Review domains closed (the repo clean chat's seventh question)
He selected Tidal, DFO and Sloughs, and added: "dfo might have more if future places come in so jsut make sure that plays nicely." The three domains' names and entries stay open to new places; the DFO one must accept a place that appears later.

## Owner answers of the night of 2026-11-26/27 (docs batch 5, T1191)

He came back at about 23:50 MST on 11-26 and answered in five batches, then a few single answers, all before 00:30 MST
on 11-27 (the chat's stamps in between were not read from the clock; the order is right). Each answer is in
his words (verbatim, his spelling); "Reading:" is the planning chat's plain reading, not his words.

### Return batch 1 (23:50 MST)
- **The while-away rule:** "Yes, approved items merge (Recommended)" (recorded above).
- **PR #312 (Eco Reserve words):** "Merge; names only (Recommended)". Reading: merged as built; the map area labels,
  the notes, the rules' sentences and the book lines keep the full official name (T1245's owner line). #312 is merged.
- **The two T1177 branches; Blue Lead Creek below the falls:** (both recorded above).

### Return batch 2 (the map look)
- **Phone controls (Q19):** "do tiday with the attribution moved to botton corner now". Reading (the planning chat's
  "my reading"): option A, Tidy, with the attribution "(c)" button moved to the bottom corner.
- **Region border (Q20):** "1 · Dash-dot, heavier, light band (Recommended)".
- **Water colour (Q21):** "Accept cyan water (Recommended)".
- **Haida Gwaii on phones (Q22):** "Leave phones; fit all on wide screens (Recommended)".

### Return batch 3
- **Larger text (Q23, T1203):** "Merge as shown (Recommended)". Four checks on his own phone are still owed:
  Notifications page, walkthrough cards, the tab shortening, glossary rows.
- **The short tab word:** "\"Rules\"". Reading: "Rules", NOT the built "Regs".
- **T1078 banner (all-year closure with no single closure covering the year):** "Approve as built (Recommended)".
- **Little River annual quota (T1243):** (recorded above).

### Return batch 4
- **Lake zones (T1180), round 3 (options A, B, C):** "i like B HOWEVER, it needs a few changes first the search
  having all parts is too much ssay somehting like fraser with 10+ parts will become crazy. maybe it will have a clean
  way in the search to expand or click on it which then opens the map in the card instead where the user can select the
  specific section if that makes sense? like in A? this needs more work but something like that. also lakes arent the
  sam eupstream/downstream it should have different text when navigating. or even better only a parts selector / user
  able to click and hide the up/downstream on lakes?" Reading: B is the direction; round 4 designs a search that does
  not list every part (expand, or a tap that opens the card's map to pick the section) and lakes with no
  upstream/downstream (different words, or a parts selector only). Still design only.
- **The T1199 cut rule:** "Hold for the loop-removal trial". Reading: the rule and its opens/closes list wait for
  the T1242 trial; he sees both in one look.
- **The .env load:** he pasted the names: only GOOGLE_API_KEY and GOOGLE_API_KEY_2..7 (the Gemini keys of the old
  parsing step), and said "so nothing matters". Reading: remove the automatic .env load.
- **Strathcona-Westmin:** (recorded above).

### Return batch 5a, naming
- **No access on the card (Q4):** "i dont understand the cottoinwood river example but i like the layout in p3-5
  access. i dont understand the salmon stretch tag either. also the tag for noons creek is weird too. i think for noon
  creek as a side note since fwa and city have 2 different streams in same area same name they should both get noons
  creek regs for safety". Reading: he likes the chip layout of sheet P3-5; the status-pill version (b) was not chosen;
  the Cottonwood River example, the salmon stretch tag and the Noons Creek tag were not clear to him (the planning
  chat owes him an explanation). Noons Creek: the FWA stream and the city stream of the same name in the same area
  BOTH get the Noons Creek regulations, for safety. Settled 2026-11-27, with a plain sheet of the three: "Yes, settled (Recommended)" (layout as P3-5; the three tags are not built; T1191).
- **Which land counts (Q5):** "all no access in that layer. also we seperatiely want a tag for tags that is like
  indegenous except \"crosses private land\" as another tag". Reading: all closed land in the layer keeps "No public
  access" (no filtering); NEW, a separate tag "Crosses private land", like the Indigenous one.
- **Indigenous chips (Q6):** "chip replaces the banner and no when wholyl yes and make it if the water goes through
  indigenous at all it will get part tag". Reading (the planning chat's "my reading"; the "no" was ambiguous): the
  chip replaces the banner; a chip when wholly on; a part tag on ANY crossing. Confirmed in batch 5b, below.
- **The 14 side parts (Q-R2):** "Fold by the rule, in the re-cut (Recommended)".

### Return batch 5b, naming
- **Klinaklini / MacMillan DFO salmon side channels (Q-R1):** "why do tey not get reached? that seems like an issue not
  something that needs a tag". Reading: no tag; a defect: the DFO salmon stretch must reach the river's side channels
  (the channels take the host's salmon chain). Goes to T1240.
- **Noons Creek / Turner Creek:** "Show me the map first" (then the next section).
- **Indigenous chips, the reading:** "Yes, that is right". Reading: the chip replaces the banner; a chip when wholly on
  reserve land; a "crosses" chip on ANY touch (no 5% floor).
- **Words (Q7 and Q10):** "options one except if it is the only section it doesnt need a locator at all". Reading:
  "Cowichan 1 reserve" and "Main channel"; BUT a water with only one section or part gets no locator line at all.

### Later single answers
- **Lake zones (T1180), round 4 (B1, B2, B3):** "B1 is good except it needs care for making sure buttons dont move
  around navigating up and down etc. otherwise it looks good! it can be explored more". Reading: B1 (the count chip)
  is the direction; the controls must not move while stepping between parts; more exploration welcome; still design
  only, nothing built until he says.
- **Trend word on Level gauges:** "Yes, on Level too (Recommended)".
- **Turner Creek (after the map; a separate creek filed under Noons Creek):** "Make it its own water (Recommended)".
  Reading: Turner Creek becomes its own water (T1246). The data chat then found the cause is Noons Creek's watershed
  code on the city line, not an alias; how to fix it (Turner only, or with Tunnel and Capitol creeks, now or in the
  hardening release) is a new question for him; no data change until he answers. Should Turner take the Noons row "No Fishing upstream of railway bridge"? "No, leave Turner as today (Recommended)" (2026-11-27, T1191).
- **Steelhead water (2026-11-27, option E sheet):** "Pacific streams now, barriers in 0.11.0 (Recommended)". Every stream of
  Regions 1, 2, 3, 5, 6 draining to the Pacific is steelhead water in 0.10.0 (RULINGS S14, T1250); barriers are T1257.
  Buckley Creek: "Release: the row's cap holds (Recommended)" (S15). The Columbia and the Skagit: "Hold both but make
  sure it will be compatible with a larger barrier list" (the barrier list, S14). And: "Also if rainbows over 50cm are
  ever referred to it always implies the rule is on steelhead too" (S12, measured before it is built: a follow-up).
  On the over-50 sheet (S12a): "the rows specific water rules always beat regional for specific quotas like that?"
  (Kitimat's 2 hatchery steelhead stand); "it is 1 trout over 50cm 2 if one is a steelhead (rainbow)" (Regions 1-2,
  Q16); Muchalat's "no wild rainbow trout over 50 cm" "is the same as regiuonal no wild steelhead". His full words: S12a.
  Kitimat's hatchery steelhead against Region 6's winter release: "(b) Leave as today" (released Nov 1-Jun 30). The
  Regions 1-2 box as built: "Yes, as built (Recommended)". The over-50 follow-ups are answered; nothing built beyond S15.
  The three under-closure rows S15 meets (2026-11-27, `under_closure_calls.json`), each answered "Yes, mark it checked
  (Recommended)": "Clearwater River (Region 3). What changes for the angler: NOTHING. Before and after: Jan 1-Jun 30
  closed; Aug 1 steelhead released, rainbow keep 2 (none over 35 cm). OK to mark this row as checked by you?" "Mahood
  River below Goodwin Falls (Region 3). What changes for the angler: NOTHING. Before and after: Jan 1-Jun 30 closed
  (the row's own closure); Aug 1 steelhead released, rainbow keep 2 (none over 35 cm). OK to mark this row as checked
  by you?" "Buckley Creek (Region 6). ONE change, the one you ruled today: Aug 1, hatchery steelhead: keep 1 ->
  RELEASE (the row's "none over 50 cm" holds). Unchanged: May 15-Jun 15 steelhead closed; wild steelhead released;
  rainbow keep 1. OK to mark this row as checked by you?"
- **Turner, Tunnel and Capitol creeks (T1246):** "All three now, in the re-cut". Built: each its own water with a
  code of its own; the build refuses a city line on an FWA code.
- **Their salmon Area (T1246):** "(a) Fix it properly now (Recommended)". Built: a city creek that drains to the sea
  takes the DFO Area its own mouth lies in; 25 more city creeks (21.98 km) come under Area 28's salmon closure.
- **Turner Creek's tidal limit (T1246):** "tidal limit is rail bridge again I think. like noons". Built: the lowest
  126 m, below the Ioco Spur crossing, is tidal (OSM shows no bridge there: it may be a culvert).
- **Tunnel Creek's Seymour leave-out row (T1246):** "(a) Remove the row too (Recommended)". Removed.
- **MacMillan Creek's channel to Diana Lake (360244113:889, 2026-11-27/28):** "not into skeena ..."; "so we jsut decided that macmillan doesnt get skeena riles so this is mute ..."; "i thought we went over this??". It is not Skeena water and does not take the Skeena River's own row; built after 0.10.0 with the outflow guard (T1164 item 2; words in full: OWNER-UNFILED).

## Known, not new (the audit's finds he had already ruled on)
- A lake divided into closure areas is deferred (his lake splits); the lake page already says "Open, except some parts".
- Waters whose rules bind nowhere (Squirrel Lake 6-1, Frog, Redfern) are the known unfound waters.

## Owner
He reads this page once and corrects anything that is not what he meant (T1158); after that a chat adds a ruling the
same turn he gives it.
