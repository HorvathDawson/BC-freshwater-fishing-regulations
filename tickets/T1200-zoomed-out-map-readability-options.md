# T1200 — Zoomed-out map readability: names, towns, boundaries, the tapped water, the legend (options as pictures first)

Status: OPEN — 0.10.0 re-cut (owner 2026-11-26); options prototyped in the web app, pictures to the owner, then to the tester

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1200 | Zoomed-out map readability: names, towns, boundaries, the tapped water, the legend (options as pictures first) | OPEN — 0.10.0 re-cut; options prototyped in the web app, pictures to the owner, then to the tester | — | research the standard, build the option set in the web app, send pictures; build tickets are numbered after the pick |

Source: a closed tester's email, 2026-11-26 (feedback log F47–F50, F52; app 0.9.0, Android, light theme, Trout mode),
with two pictures of the main map (province zoom, 200 km scale bar; and about a 50 km scale bar near Prince George) and
one of another map app the tester uses. Checked against `main` by the tester feedback chat the same day (the style and
screen code, not a run of the app).

## What the tester said, and what the code shows

1. "find it tough to find exact rivers and systems. Limited names when im zoomed out. Tough balance between too cluttered
   with names vs no names." / "i could barely landmark where babine is".
   Found: no lake or river name is drawn below zoom 8. `lake__label` and `stream__label` both have minzoom 8
   (`app/packages/map/style/style.json`, the built style); the basemap's own water names are removed on purpose
   (`app/packages/map/src/runtime-style.ts`). The names are in our tiles; it is a style choice (closed T299 thinned the
   labels).
2. "And show a few more main cities? Doesnt have to be too many more just to help distinguish where the waters are."
   Found: `PLACE_TIER_ZOOM = [0,7,9,10,12]` (`app/packages/map/src/labels.ts`): only places of kind "city" pass below
   zoom 7, so five show at province zoom. NOT YET CHECKED: whether `bc.pmtiles` carries towns at zoom 5–6 (Protomaps
   ships each place with its own `min_zoom`); if not, more towns need a basemap rebuild or our own places layer.
3. "Maybe slightly different color scheme would help accent the system i have highlighted?"
   Found: a tapped stream gets a magenta casing (`color.highlight` `#c2187a`, the line plus 2.5 px each side,
   `app/packages/map/src/paint.ts`), a lake a ring; nothing else dims on the main map. Only a water's own Map view dims
   the others (`opacity.water.others` 0.25).
4. "I also feel ?all the roads? Kinda of makes it tough for me to understand what everything is."
   Found: highways are drawn, but as faint white lines (`#ffffff` on `#e2dfda`, 1.1 px at zoom 6). What fills both
   pictures is the orange "Own rules" rivers (`#D08404`) and the grey region boundaries (`region__edge`, `#7F7C79`,
   2.2 px at zoom 5). Our reading, not the tester's words: those are what read as roads. Ask the tester (the reply).
5. The legend under the map takes three lines (Closed, Own rules, Base rules, No access, Fly only, Bait ban, Not
   fishable). Not the tester's point; the owner's, 2026-11-26: "clean up the legend and make it use less space somehow".
6. "Heres my gaia maps setup which is easier on the eyes (for me anyways) and easier to read where the water systems
   are, but also lacks any labels". That map has terrain shading, one bright water blue and a soft ground. Ours has no
   hillshade layer and three blues (basemap water `#80deea`, lake `#64B5F6`, water `#4A90E2`).
7. The tester finds a lake someone mentions by scrolling and zooming until it shows ("i scroll around and zoom in until
   i find it"): browsing the map, not search. The search box's rotating hint in
   the picture reads "Tackle a town or lake" (`SEARCH_BAR.placeholders`, `app/packages/core/src/search.ts`): intended,
   but it does not plainly say "search". Items 1 and 2 serve the browsing; the hint wording is an option to show.

## Work

1. **Read the standard first** (short page in `docs/research/`): how established maps keep a small-scale map neat —
   label hierarchy by feature size and zoom, how many labels per screen, town selection by rank, how administrative
   boundaries are drawn so they do not read as roads (dash patterns, tint bands, casing), how a selected feature is set
   off (dimming the rest, halo), compact map legends on phones (collapsed chip, only the classes in view, a legend
   button). Name the sources. Say for each choice whether MapLibre and Mapbox GL both do it the same way.
2. **Prototype in the web app, not on a phone** (owner: "prototype all of this stuff using the webapp because it is way
   faster then emulating app changes"). Each option is a style or runtime-style variant behind a dev switch, at phone
   width, same three views every time: province zoom, the region view near Prince George, one water tapped.
   - Water names when zoomed out: two or three densities (the largest lakes and main stems from zoom 5; more at 6–7).
   - Towns: about 15–25 at province zoom; first check `bc.pmtiles` for towns at zoom 5–6 (see 2 above).
   - Region boundaries that read as boundaries: dashed or dash-dot, thinner, a tint band, the label on the line.
   - The rule lines at province zoom: fewer small waters, thinner lines, own/base colours that cannot pass for roads.
   - The tapped water: the others dimmed, a stronger casing or halo.
   - The base: a softer ground and one water blue; a hillshade trial (the tile source must be one we can self-host).
   - The legend: a compact form (one line or a chip that opens; only the classes on screen).
   - The search hint: a plainer wording beside the rotating ones.
3. **Pictures to the owner** (in the chat, not paths): a contact sheet per view, the current look first. The owner
   picks; the picks go to the tester as proposals with the reply (the owner sends it).
4. **Then build**: one build ticket per picked change (T1203 on, this chat's range), each with a render or style test,
   on `main` and cherry-picked to `release/0.10.0`.

Constraints: every option must carry over to Mapbox (T1154's engine seam, T1155/T1108): style-spec features both
engines share, no MapLibre-only expression or plugin; say so per option. Fonts, sprites and any new tiles only from our
servers. Dark theme and satellite get the same treatment or an explicit "unchanged" (T940 holds the satellite line
colours). A tile or basemap rebuild runs through `scripts/heavy.sh`, one at a time, and tiles and bundle keep one
vintage.

## Acceptance

- The research page exists and each prototype option cites the practice it follows.
- The owner has seen every option as a picture and his picks are written in the `## Owner` block.
- Each picked change is a merged build ticket; the feedback log rows F47–F50 and F52 name the release that shipped it.
- At province zoom the largest lakes carry names and no two labels collide (a style test over the label layers).

Depends: none to start; the hillshade and Mapbox notes read T1154. Size: L (research S, prototypes M, builds per pick).
Model: Opus for the style work, Sonnet for the research page. Owner: yes (the picks).

## Owner

- [x] 2026-11-26 — zoomed-out names and towns: what to build? → "could we look at the standard for how to do this neatly? we can generate picture of a few different options then offer them as propsoals to the reviewer and see what they think? we could also try other changes to see if it make it neater too. like making it clear those are the region boundaries. also we can clean up the legend and make it use less space somehow? use industry standard. and make sure it is compatible with future move to mapbox. prototype all of this stuff using the webapp because it is way faster then emulating app changes" (2026-11-26)
- [x] 2026-11-26 — how far to go on the map's look (lines read as roads, the tapped water, a softer base)? → "do the full monty and we can see what feels best after and send options to the reviewer." (2026-11-26)
- [x] 2026-11-26 — which release takes this batch? → "we will put all changes from this on it" (the held 0.10.0 re-cut) (2026-11-26)
- [x] 2026-11-26 — which package to build from (today / light touch / recommended / full, the packages sheet)? → "Recommended (Recommended)" (2026-11-26)
  That is: names at the middle density (a2), our towns with a hand-picked second tier (b3), dashed boundaries with a
  soft band and "Region n" labels (c2), fewer and thinner rule lines zoomed out (d1, d2), the softer base with one
  water blue (f1), the one-line legend (g1), "Search waters and towns" (h1). The tapped look is the next line.
- [x] 2026-11-26 — how should a tapped water stand out (sheet e)? → "it opens the page right over it after the click so i dont see why it is needed toi be to crazy highlighted?" (2026-11-26)
  So: no change to the tapped look (e dropped). The reply asks the tester what "the system i have highlighted" was.
- [x] 2026-11-26 — hillshade: now, later or never? → "Later, own ticket (Recommended)" (2026-11-26). It is T1204.
- [x] 2026-11-26 — what goes to the tester as proposals? → "let me pick the options i want to send give me a bunch of options then i will pick the ones i think have a chance of being good. then we can send them to the reviewer and see what they think while we choose our favourite in the mean time. also the legend rework i think is one of the bigger wins maybe?" (2026-11-26)
- [x] 2026-11-26 — pick which numbered option pictures go to the tester (the second round: single, sendable pictures,
  more legend designs)? → "i think there are a lot of good ideas here the whole pronvince look and zoomed in look are a good starting point. could we have a couple full proposals of those 2 looks basically option A-D with different whole pronvince and mid zoom and zoomed in looks. we can think about other things that would improve it to. i like my search terms we should leaves those they are fun. im sure a lot of other fun improvements could be done tho to make it work better on a phone size. including the date selectors too? those still seem clunky..." (2026-11-26)
  So: nothing goes to the tester yet; four whole proposals A-D at three zooms, the rotating search hints stay, and the
  date selector and other phone-size ideas are shown as their own proposals.
- [x] 2026-11-26 — which legend design (pictures 14-19)? → "either 14 or 15 but maybe the legend should be at bottom? would that be better? or could be a map key at bottom that has map behind it and when clicked opens up the reference or somethign?" (2026-11-26)
  So: only the one-line key with More (14) and the Map key chip (15) stay, shown in three placements (in the bar over
  the map's foot, under the tab bar, and floating over the map).
- [x] 2026-11-26 — the second tier of towns (from zoom 6) is a list we choose, since the basemap cannot rank them
  (Vanderhoof, Fort St. James, Mackenzie, Houston and about 30 more in the prototype)? → "i want you to choose best judgement for users to have the best experience across bc and make it easy to find a location by zooming in from far out" (2026-11-26)
  So: Claude chooses the tiers for even coverage of B.C. (junctions, the last town before a lake or river system, coast
  access, fishing bases), not population alone; the list with a reason per town is a data file, shown as coverage pictures.
- [x] 2026-11-26 — the legend's words for the two rule statuses (round-2 pictures 20, 21)? → "Its own rules / Region rules only" (picture 21) (2026-11-26)
  So: every proposal and legend picture uses them; the build changes them everywhere they appear (legend, help,
  glossary, walkthrough).
