# T425 — The Trout / Salmon switch moves to the map rail, with clearer icons or words

Status: DONE (code 0178b4301; owner look 2026-11-06: good) — owner 2026-11-05, an addition (screenshot on the Map tab, phone width).

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
   **Decided (owner 2026-11-05, asked "unless you can think of a better spot"; Claude's call): the rail, option A** — a fish
   silhouette + a small-caps word on each button. Why: the rail already holds the other "how the map shows" control
   (layers); a word under the icon makes it plain with no tour; the top day row is full on a phone and a second row would
   cover more map; the legend is where it is missed today. The owner reviews the shots and may still change the look.
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

## Outcome (2026-11-05)
Code `0178b4301`. `shell/RegsLayerToggle.tsx` is now the rail control (same file, same `col.setRegsLayer` handler, same
`regs-paint` / `regs-paint-standard|salmon` test ids): a radiogroup card under locate + layers (MapScreen `railExtra` ->
MapRail children, from BrowseHost, only when the regulations map is drawn and Salmon is offered). Each button 44x44: the app's
own fish art (`FISH_IMAGES` thumbs, rainbow trout `RB` / chinook `CH`, handed down as `fishImages`; the `fish` icon when the
art is absent; the spinner while Salmon loads) over a small-caps word ("TROUT", "SALMON"). Square corners, rail card + lift.
Chosen = the rail's tint + a 3 px accent bar + bolder word (not the old filled accent: the art is a picture). Names "Colour by
trout and other species' rules" / "Colour by salmon openings", hint "Changes the colours and the key on the map"; words in
`SALMON_WORDS.toggle` (`rail`, `said`, `hint`, `art`). The legend bar holds the key, DFO's source line and the fold only; its
head has a fixed min height so the bar does not move between modes. No new image assets. Tests: SalmonCard (switch), T377.legend
(no switch in the bar), MapScreen (railExtra stacks after the tools) pass; typecheck, platform, boundaries, colours, knip clean.
No Shell-level test (the fixture has no paired status index, so the regs map never draws there).
Owed: before/after shots at 390x844 and 1440x900, light + dark (not taken: no dev server run); owner reviews the look.
The `map-ui-layout-rule` memory already carries the new placement.

## Seen 2026-11-05 (local dev app, 390 wide, light)
The rail looks right (trout + salmon art, TROUT selected). Left: the legend bar keeps an empty head row holding only the
fold chevron — one wasted line; put the chevron on the key's first row (or the bar's corner). Shot:
`.playwright-mcp/t425-phone-light.png` (not committed). Dark + 1440 still owed.
