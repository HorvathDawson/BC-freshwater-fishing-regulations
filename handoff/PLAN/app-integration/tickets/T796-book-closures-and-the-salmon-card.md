# T796 — A stretch the book closes still shows an open salmon card; a DFO "Finfish closure" closes only the salmon card

Status: OPEN (owner: OWNER-TODO "2026-11-16 morning list" questions 29 and 30; app core salmon card + pipeline).

Source: general audit 2026-11-16, answers checker A5 and A6 (`handoff/PLAN/audit-2026-11-16/answers.md`).

## A5. Closed stretch, open salmon card (errs OPEN)
Rule book p.77: "when fresh waters are closed to fishing, or have gear restrictions outlined in this Synopsis, the
regulations apply to fishing for salmon as well". Seen: Skeena parts 2 and 4 (ecological reserve) on Aug 15, "Closed
all year" beside "Open today" for sockeye; Atnarko parts 2 to 4 on Jul 15 (chinook); Cowichan below Mile 66 on Oct 10
(closed beside "Release only today"). The page's gear rules reach the salmon card (`gearProvincial`); its closures do
not. Read from the app's view model, not seen on a device.
Recommended: the salmon card reads closed when the page is closed by a full closure, with DFO's row kept as text.

## A6. DFO "Finfish closure" (errs OPEN for trout)
Ten DFO rows (Stamp 4, Nitinat 3, Somass, Sproat, Tahltan) say "Finfish closure"; the app closes salmon only. The
Stamp hatchery-lagoon stretch on Oct 10 shows open with a trout card. `typed.py` reads the row as a salmon closure.
Recommended: a finfish closure closes the whole page, trout included, on its dates.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 29, typed: "yes we want to do this but can you bring the example changes to me so i can see why it happened etc? but yes we want it to do this". Question 30: "Closes the whole page (Recommended)". Split agreed with the app chat (11:55): A5 = pipeline adds the book's full closure as the first member of the stretch's salmon chains (a release-side location marked static), the app keeps a static location under a live feed and words the card; A6 = pipeline `typed.py` + deliver + vectors first, then the worker's twin, then core closes the page from the feed. The before / after examples (Skeena reserve sockeye Aug 15, Atnarko chinook Jul 15, Cowichan below the trestle Oct 10) go to him with the why.

## Owner, 2026-11-16 12:53 (typed in chat)
"also we need to make sure the salmon closed because the river is clsoed in synopsis appllies to the map too. so the
colors show it as well." PASS CONDITION for A5: on a book-closed stretch the Salmon view's line colour, the year bars
and the alerts read closed (offline and under a live feed), not only the card. A map picture before and after goes
to him with the three examples.
