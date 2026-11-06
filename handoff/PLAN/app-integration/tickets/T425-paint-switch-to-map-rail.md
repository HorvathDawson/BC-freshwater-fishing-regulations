# T425 — The Trout / Salmon switch moves to the map rail, with clearer icons or words

Status: OPEN — owner 2026-11-05, an addition (screenshot on the Map tab, phone width).

## Ask (owner 2026-11-05)
"Move this like in the picture and change the icon or words that are used to show the 2 options."
- Today: the regulations colouring switch **"Trout & more" | "Salmon"** sits at the top of the bottom legend bar
  (`shell/MapLegend.tsx`; words `core/src/salmon.ts:383` + `core/src/menu/words.ts:193` `modes`; test id
  `regs-paint-salmon`).
- Wanted: on the map's **right rail**, as two stacked square buttons directly under locate + layers (the rail in
  `Chrome.tsx` / `MapRail`, same size, square corners, same card look). The legend bar then holds the legend only.
- **Changes an owner rule:** T367 / `map-ui-layout-rule` (11-04) put "the legend and its control in the bottom bar".
  This ticket moves the control out of it; the rule is updated with it (top = search + day; right rail = locate, layers,
  then the colouring switch; bottom = the legend).

## Work
1. Two rail buttons (one per colouring), the chosen one marked selected (the rail's own selected look); each has an
   accessible name and hint ("Colour by trout and other species' rules" / "Colour by salmon openings"), 44 × 44.
2. Icons / words (**OWNER picks**, proposal below): a rail button has room for an icon and at most a very short
   label. Today's icons are a map (Trout & more) and a fish (Salmon) — a map reads as "the map", not "trout".
   - A. Icons only: a trout and a salmon silhouette from the fish sprite art (the ID pane's thumbnails), label under in
     small caps ("TROUT", "SALMON").
   - B. Short words only: "Trout" / "Salmon" in the rail's type (the "& more" moves into the hint and the legend title).
   - C. Letters: "T" / "S" badges (smallest, least clear).
   Default if not picked: A.
3. Phone, tablet and desktop: the rail exists at every width (check the desktop layout keeps it beside the zoom).
   Runs and Conditions modes: the switch only shows in the regulations mode, as today.
4. One load path: switching from the rail = switching from the legend today (same state, same URL param, same paint).
5. Tests: the rail's switch (press → mode, selected state, a11y names, hidden outside regs mode); the legend no longer
   draws it; `MapRail.shell.test.tsx` + the a11y shell test updated; T269 page-vs-tap pairs unchanged.
6. Screens: before / after at 390 × 844 and 1440 × 900, light + dark.

## Acceptance
The switch is on the rail under layers, the owner's chosen icons/words, the legend bar holds the legend only, tests
and shots as above; the layout rule note updated.

Links: T367 (the layout rule), T409 (water pages; unaffected). Size: S–M. Owner: picks the icons/words (item 2).
