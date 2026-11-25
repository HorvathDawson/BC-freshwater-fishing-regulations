# T819 — Spots view cleanup: review and proposals for the owner

Status: DONE 6c18bc2b0 (landed 2026-11-17, CI green 12:26, after 7/7 Android benches; was: app; app chat. A fresh Opus worker reviews the spots area and brings pictures plus a proposed cleanup list;
nothing is built until the owner picks)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T819 | Spots view cleanup: review and proposals for the owner | DONE | 6c18bc2b0 | owner 2026-11-16 |

Source: owner 2026-11-16, in the app chat (~21:40), verbatim: "the spots view also needs a lot of work to be cleaned up maybe put a fresh agent on it?".
Out of scope here: T815 (photo viewer close under the iOS status bar) and T816 (iOS clipped text).

## Review (2026-11-16, fresh Opus worker, branch `review/t819-spots`)

Pictures 01–28 and the same text: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t819/` (REVIEW.md). Built on this branch: the edit-mode photo ✕ now reaches 44 px (`ui-native/src/spot/SpotPhotos.tsx`, hitSlop); nothing else is built until the owner picks.

### In short

The spots area works. You can add a spot, save it, open it, edit it, delete it, rate visits and see spots on the map. The trouble is the spot page. It reads like a report on a gauge, not a note about a place you fished.

- What you wrote and your photos come last. When you add a spot you scroll past three to five screens of numbers before you can type a note.
- Some parts say things that are not true. If your visit was within the last day and a half, the big weather figure is today's weather, not the weather at your visit. Two messages say "the rules" were recorded. They never are.
- On a small creek the page contradicts itself. At the top it gives an estimate from a gauge 676 times bigger than the creek. Lower down, a table full of "not known" says no gauge can speak for this water.
- Too much technical wording: gauge codes (08MH001) instead of names, "magnitude 2110", "share 100.0 %", "10 points lower". Dates come in four formats, and coordinates appear in the header and on every list row.
- The "week before" table is broken at phone width. On the web it opens scrolled to the right, so the first day is cut off. On both platforms the high and low temperatures run together.
- "The next few days" takes three screens: five tall cards of six lines each, often with the same score on every day.
- In dark mode the photo viewer turns light grey.

The map pins, the delete question and the rating stars look fine. There are no sharing or backup words in the Spots area yet (sharing is T714, after closed testing).

I fixed one small thing: the ✕ that removes a photo while editing was 28 px with no extra touch area. It now reaches 44 px, like every other button.

How I looked: the phone layout at 390 px, light and dark, on the web build with real staging data, using the same screens the phone draws. I also reused the Android emulator shots from 11-12 for the native look. I did not start a new emulator run because the heavy lane is queued behind the app chat's benches. The pictures are numbered 01–28 in this folder.

### Proposed cleanups (pick by number or by bundle)

#### Bundle A: wrong or contradictory (recommended first)

1. **The weather headline is "now", not your visit.** Shot 15: visit at 17:00 shows 6 °C "observed 21:30". The visit hour itself was 8 °C. This happens for any visit in the last 36 hours, because the capture takes the live "current" reading. **Change:** use the visit hour's reading, and use "current" only when the visit is within the hour. **S. Do.**
2. **"The rules are recorded" is false.** Shot 08 ("The weather, the flow and the rules are all recorded…") and shot 22 ("…the rules that were in force are recorded nowhere else"). A spot keeps no rules. **Change:** drop "the rules" from both sentences. **S. Do.**
3. **A small creek gets two opposite answers.** Shots 17–18: a 1.4 km² creek shows "Below normal" from a 960 km² gauge ("676× apart"). Below that, a five-row table reads "not known" and says no station drains enough of this water to speak for it. **Change:** hide the reach table when it knows nothing. Separately, whether a gauge 676× bigger may give an estimate at all is your policy call, not this ticket. **S (hide) + a question for you. Do.**
4. **The photo viewer goes light grey in dark mode** (shot 20 against 19). **Change:** the viewer is black in both themes. **S. Do.**

#### Bundle B: a short, readable spot page

5. **Your notes and photos are at the bottom.** Shots 09–10: in the draft, Notes and Photos come after the whole record. **Change:** order the page as name, notes, photos, parking, visits, then the record. In the draft, fold the record into one summary card you can open. **M. Do.**
6. **The "How this spot reaches the gauge" table is jargon** (shot 12: "13 reaches", "magnitude 2110", "share 100.0 %", "so it shows: this water", all in capitals and monospace). **Change:** replace the five rows with the plain sentence it already writes ("The gauge sits on this reach." / "Water here flows about 17 km down to …"), and show nothing when nothing is known. **S. Do.**
7. **Gauges are named by code** (08MH001 in shots 12, 14, 17 and the week-before footer). **Change:** name first ("Chilliwack River at Vedder Crossing"), with the code small after it. **S–M. Do.**
8. **The week-before table is broken at phone width.** Shot 13 (web) opens scrolled right, with "ed 11" and " mm" cut. Shot 14 (Android) shows high/low as "13° / 7° 12° / 7°" running together. **Change:** fit seven days without sideways scrolling, with high over low on two lines and the unit once in the row label. **M. Do.**
9. **"The next few days" is three screens long.** Shot 16: five tall cards with six reason lines each, and four days in a row reading "3.6★ Good". A visit made today also compares "Today" with itself ("100%, everything the same"). **Change:** one compact row per day (day, score, the single biggest difference), with the reasons behind a tap, and no "Today" row when the visit is today. **M. Do.**
10. **Four date formats and coordinates everywhere.** The four formats are "2026-11-15 at 21:37", "Sun Nov 15 2026", "15 Nov" and "Sun 15" (shots 08, 11, 02). "on Chilliwack River" repeats the title when the title is the river (shot 28). **Change:** one date style ("Sun 15 Nov, 5 pm"). Show coordinates only under Show on map and Parking. Hide "on X" when it equals the name. **S. Do.**

#### Bundle C: the list and adding a spot

11. **List rows are dense, and the Add buttons sink to the bottom.** Shots 02–03: three grey lines per row (ISO date, flow, "lower than 8 days in 10", temperature, coordinates). With 14 spots you scroll to the end to add one. **Change:** each row shows the name, the water and date, and one condition word. Add a square "Add" button in the screen's header that is always visible. **S–M. Do.**
12. **Back after saving says "Map".** Shot 28: start in Spots, pick on the map, save, and the spot opens over the Map tab, with back going to the map. **Change:** after a save, open the spot on the Spots tab, with back going to the list. **S. Do.**
13. **"Pick on the map" takes three map steps.** Shots 05–07: drop a pin, then a "New spot" sheet with both Done and Cancel, then pick the water separately. The pin stays where you dropped it, even on land, and an unnamed creek shows no name once picked. The day, Trout/Salmon and legend controls stay on screen while you pick. **Change:** one tap drops the pin and snaps it to the nearest water, which is named, with "Different water?" offered only if it is wrong. Picking hides the map controls. **M–L. Optional.**

#### Bundle D: fit and finish

14. **Small targets and mixed buttons.** Edit's touch area is only 25 px wide (its helper assumes the word is 60 px). Directions, Use my location and Pick on the map are drawn 32 px tall beside 50 px buttons; their touch area does reach 44. A big outlined "Add a visit" sits next to plain red "Remove visit" text. Parking shows raw coordinates (shots 11, 21). The photo ✕ is already fixed on this branch. **Change:** give Edit/Delete a real 44 px touch area, one button style per row, and parking as "Parking saved · Directions". **S. Do.**
15. **The empty Spots tab reads technically** (shot 01: "records the gauge, its standing against the record") and does not say where spots are kept. **Change:** two plain lines, plus "Saved on this phone and in its backup. Not on the website yet." **S. Optional.**

#### Not proposed (other tickets)
- T815 (viewer ✕ under the iOS status bar) and T816 (iOS clipped hour and weather icons): I saw no more of the same on the web. Item 8's cut-off first column is a different cause: the table opens scrolled to the end.
- Sharing a spot: T714, after closed testing.

## Owner, 2026-11-16 ~21:52 (app chat dialog, after the review and its pictures)
Bundles picked (verbatim labels): "B: readable spot page (Recommended)", "A: fix wrong bits (Recommended)", "C: list +
adding", "D: fit & finish": all four, items 1 to 15.
Item 3 (a 1.4 km² creek given "Below normal" from a gauge 676× bigger), verbatim: "Could we just use our gauge network
that colors stuff wtc to determine if it is significant enough? It would drop that in theory. Then ofr small ones still
give a warning if used." Read: a spot takes its gauge from the SAME lead-gauge network the Conditions map colours with
(the release's section → lead gauge, the drainage gate already applied); where that network gives the water no lead, the
spot shows no estimate; where a lead exists but the water is small next to the gauge, the estimate shows with a plain
warning.

## Item 3 corrected, owner 2026-11-16 ~22:00 (app chat dialog)
The map colours the 1.4 km² creek "Below normal" from its only paint panel (08MH001), so "the network that colours the
map" keeps the estimate. Asked whether a gauge that much bigger may speak for a small water at all, on the map and in
spots, he picked (verbatim label) "Allow, with a warning (Recommended)". So a spot keeps the sheet and map lead
(leadOver), and the warning comes from Python (T823, cut-off shown to him with examples).
