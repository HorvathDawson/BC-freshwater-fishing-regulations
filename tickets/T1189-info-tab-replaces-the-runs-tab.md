# T1189 — An Info tab in place of the Runs tab: runs, stocked lakes, depth charts and more (design)

Status: OPEN — design in progress (structure agreed 2026-11-27; prototype rounds 1-3 answered, round 4 next); prototype branch `proto/T1189-info-view` is local and never merged; not for the 0.10.0 re-cut; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1189 | An Info tab in place of the Runs tab: runs, stocked lakes, depth charts and more (design) | OPEN — design in progress; not for the 0.10.0 re-cut; owner: see ## Owner | — | prototype round 4 (the Conditions graph, the map credit) to him; then the build ticket; nothing is built before he picks |

Source: the owner, 2026-11-26 ~17:50 (planning chat), verbatim: "another idea i have we shoudl ticket is changing the runs tab to be info tab where runs will
be part of it but it will also be where the stocked lakes, bathy maps/charts etc etc live. i think that would make more sense as a division of info maybe?"

## What the app holds or plans, and where each lives today (2026-11-26, main)
The bottom bar is `BrowseTab = "map" | "conditions" | "runs" | "spots" | "more"` (`app/packages/ui/src/route.ts:50`).
| kind | state | where it lives today |
|---|---|---|
| Run timing (salmon / steelhead by pentad) | shipped | the Runs tab (T199, T261): species chips and date slider in its foot bar (`RunsSheet.tsx`, `RunsControls.tsx`, `MapLegend.tsx`); the map paints waters by runs |
| Stocked lakes | prepared, not fed | the Map tab's water colouring "Stocked" (`app/packages/core/src/layers.ts:38, 114-120, 150`: offered only when the release carries it); data T200, T634 |
| Depth charts / bathymetry | prepared, not fed | the Map tab's water colouring "Depth" (same file); charts and PDFs planned in the water page's Lake tab (T634 item 6, T201, T218, T219, T1188) |
| Gauges, flows, levels, water temperature | shipped | the Conditions tab (`ConditionsPanel.tsx`, `GaugeTrace.tsx`, `DayCalendar.tsx`) and the water page's lead gauge |
| Weather at a spot | shipped | the spot page (`SpotScreen.tsx`, Open-Meteo) |
| Regulations, licences, gear, keep | shipped | the water page's tabs (`app/packages/ui-native/src/regs/`) and the Map tab's Regulations colouring |
| In-season notices (BC Fish and Wildlife) | shipped | a banner on the water page (`regs/InSeasonBanner.tsx`); alerts and the Notifications page (`menu/NotificationsPage.tsx`, `AlertMe.tsx`) |
| DFO salmon openings | shipped | the water page's salmon card (`regs/SalmonCard.tsx`) and the Map tab's Salmon toggle |
| Access (no-access or restricted land) | shipped | the water page (`regs/AccessNoticeBlock.tsx`) and a map colour; data `pipeline/deliver/bundle/access.py` (T54) |
| Saved spots | shipped | the Spots tab |
| About, sources, glossary, help, licences | shipped | the More tab (`menu/MoreScreen.tsx`, `InfoPages.tsx`, `GlossaryPage.tsx`) |
| Lake zones (Babine circles, sign zones) | design | T1180 |
| Photo fish ID | research | the night note `photo-fish-id.md` |

## Open questions (for the design lane to put to the owner, with pictures)
1. What the tab holds: runs + stocked lakes + depth charts only, or also gauges (today a tab of their own), in-season notices and access?
2. One map with a picker (Runs / Stocked / Depth), like the Map tab's water colouring, or a list of things to read (a lake's depth chart, this year's stocking)?
3. Do the Map tab's "Stocked" and "Depth" colourings move into Info, or stay on the Map tab with Info as their reading page?
4. The name: "Info" beside the More tab's About / Sources pages (`InfoPages.tsx` already uses the word): does More keep those?
5. Order with T200 / T201 / T634: the tab arrives with the first of them, or before as Runs alone under the new name?

## Work
1. Check the table against the app (the rows above are from a code search, not a device).
2. Two or three layouts as pictures (phone and tablet), each answering the questions above; send them with the questions.

## Acceptance
The owner has picked a layout and answered the questions; the build is a new ticket. Model: Opus (design). Size: S (the design).

## The thread of 2026-11-27/28 (his words verbatim, in order; "Reading:" is the planning chat's)
- Opening idea (2026-11-27): "We also should have an agent scope out changing runs into the info map where the stocking layer and bathy/chart layer
  also live alongside runs? Then kap could be turned into rules or similar. Can we think about that would it make sense? We want to figure out the best
  place to display all our info items that might open different cards. Then info could become a new tab in the card between conditions and map? Where
  it will hold runs stock info, bathy map stuff etc? I don't know if this is a good or bad division. Could you ask a fresh agent without being poisoned
  what it thinks is best (ask a few even) then come back with proposals". Three fresh agents; options A-D on one sheet.
- On the sheet: "Why would info not contain runs though?  Also we could work on a better way to have salmon vs game fish switching in the rules view
  etc. could we have the agent try to come to an agreed best option then present that as a proposal with layouts". The three agents agreed one
  structure. Reading: bar Rules · Conditions · Info · Spots · More; pickers Rules [Trout & char | Salmon], Conditions [Flow | Level | Temp], Info
  [Runs | Stocked | Depth]; card Rules · (Conditions) · Info · Map; Stocked and Depth leave the Rules map.
- On the agreed proposal: "Yes this structure except can we have another fresh agent try to integrate this into the app as a proposal to show what
  it looks like (not actually interesting it but like using the real UI and modifying it to show it ?"; the salmon switch: "I don't mind the
  bottom but I would like to see other ideas too"; then "comparre s1 and s2 in real ui look".
- Prototype round 1: "S1, slimmer"; finds: "Tidy the foot of the map, Stronger Depth colours, Drop the lone "Colour the water by", this is in the
  new proposal not implemented yet".
- Round 2: "the s1a is best so far but attribution location is weird and the ay conditions go simplified makes it less functional. that needs more
  work I want it simplified but we need to make the graph good. it wasnt good to begin with but we want it to be useful. 4b is way better then 4a...
  would it be worth making it so runs if multiple fish are selected easier to see what is happening color wise?". Depth colours: "choose what you
  think is best to start and we will see in website later" (the planning chat chose D2, navy + orange, as a proposal).
- Round 3 (2026-11-28): Conditions: "okay so i like the c3 bar with fri mon now on it. but i think we could work on the graph in c1 too. instead it
  could be a bar plot that is in y 0 is median then below is low (goes down) and above is goes up ... also remove a lot of words on all options. then
  give another round of items. the plot is for the watershed or all visible changes not sure the best option to choose. probably visible plus buffer?
  bit water shed might make more sen statistically. either way no expand or collapse"; Runs: "Leave Runs as today"; the credit: "we want it on
  the map so maybe in line with the option selector but have map behind them and no text in the line".
- T634's lakes-only "Lake" tab (2026-11-11) is superseded by this Info tab ("Yes this structure ...", 2026-11-27).

## Owner

- [x] 2026-11-26 — Look at the Info tab layouts (pictures from the design lane) and pick one, with the open questions above. Default: nothing is built; the Runs tab stays as it is. → the structure: "Yes this structure except ..." (2026-11-27; the thread above)
- [ ] 2026-11-28 — Prototype round 4: the Conditions graph (a bar plot centred on the median, fewer words, no expand or collapse) and the map credit in line with the selector; then say the design is done. Default: nothing is built.
