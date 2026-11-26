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
   (`app/packages/map/style/layers.source.json`); the basemap's own water names are removed on purpose
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
7. "the way i use these is usually chatting with a friend around the table, we talk about a lake, then i scroll around
   and zoom in until i find it". The tester browses the map and did not use search. The search box's rotating hint in
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
- [ ] 2026-11-26 — pick from the option pictures (names, towns, boundaries, rule lines, the tapped water, the base,
  the legend, the search hint) when the chat sends them. Default until then: nothing changes in the app.
