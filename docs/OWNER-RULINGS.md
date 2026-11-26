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
unless the row says "including"; an extent op, not a special case (16 cases).

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
"Record the method, build nothing (Recommended)". The 98 %-accurate geometry method is kept for the day a case appears.

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
- **O4 Hatton Creek:** "Add fly-only to Hatton Creek (Recommended)". Build: fly only Sep 1-Jul 14.
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
- **O15 Tamihi Creek:** "Our cut is the falls". Build: none (0.66 km stays); on the Province list.
- **O16 Shuswap above Sugar Lake:** "it is a better more cautious reading to limit it so that it is not exempoting the
  whole river so exempt here". Confirmed (below): no data change.
- **O17 Nanaimo River:** "it is probably a format issue and is okay add to gotchas." Build: none (Oct 15 stays); GOTCHAS.

### P7b: Region 5 "other streams listed in the tables": the strictest reading (RULED, 16:00)
"i think the other streams listed means the entries with exemptions on a partt or open all year on whole or part etc. we
should be strict here so lets take the strictest reading add it to gotchas with explicit ask of what we are confuysed
about here (what is lifted items etc)". Build (T1177, option C of the P7b audit): the spring closure is lifted ONLY where
a row prints its own exemption or "open all year" (whole or part); rows with only gear or quota rules keep the closure.
Closes 330.9 km in spring (Cariboo 143.7, Moffat 88.1, McKinley 38.5, Blue Lead 18.2, Little Horsefly 17.8, Mitchell
15.4, Canim 9.2). GOTCHAS entry and the Province question (BOOK-ERRORS entry 22) spell out what we are unsure of.

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

### OPEN, not ruled
- **O10 Goat River:** "add to gotchas and could it be boundaries? maybe it means between 2 confluences? we need to decide
  on best interpretation". An agent checks where Leadville and Cameron creeks join the Goat. Until he decides: as built.
- **O11 Mine Lake:** "Show me the map first".
- **The lake-zones picks** (above). **Licence wording (FWID, WILD):** no answer recorded in this chat.

## Known, not new (the audit's finds he had already ruled on)
- A lake divided into closure areas is deferred (his lake splits); the lake page already says "Open, except some parts".
- Waters whose rules bind nowhere (Squirrel Lake 6-1, Frog, Redfern) are the known unfound waters.

## Owner
He reads this page once and corrects anything that is not what he meant (T1158); after that a chat adds a ruling the
same turn he gives it.
