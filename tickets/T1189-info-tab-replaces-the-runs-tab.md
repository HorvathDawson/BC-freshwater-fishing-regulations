# T1189 — An Info tab in place of the Runs tab: runs, stocked lakes, depth charts and more (design)

Status: OPEN — a design ticket; not for the 0.10.0 re-cut; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1189 | An Info tab in place of the Runs tab: runs, stocked lakes, depth charts and more (design) | OPEN — design; not for the 0.10.0 re-cut; owner: see ## Owner | — | a design lane: the inventory below checked, then pictures of two or three layouts for the owner; nothing is built before he picks |

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

## Owner

- [ ] 2026-11-26 — Look at the Info tab layouts (pictures from the design lane) and pick one, with the open questions above. Default: nothing is built; the Runs tab stays as it is.
