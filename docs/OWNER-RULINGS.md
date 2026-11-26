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

## Known, not new (the audit's finds he had already ruled on)
- A lake divided into closure areas is deferred (his lake splits); the lake page already says "Open, except some parts".
- Waters whose rules bind nowhere (Squirrel Lake 6-1, Frog, Redfern) are the known unfound waters.

## Owner
He reads this page once and corrects anything that is not what he meant (T1158); after that a chat adds a ruling the
same turn he gives it.
