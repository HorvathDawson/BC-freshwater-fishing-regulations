# T925 — A water with one section gets its Map view too

Status: DONE (landed on redesign/stream-sections 2026-11-20 01:50 in b5634ba92 34a10b157 70dce488f 2a4bedabb; owner approved the pictures).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T925 | A water with one section gets its Map view too; the tapped part stays marked (tester via owner) | DONE | b5634ba92 34a10b157 70dce488f 2a4bedabb | tester 2026-11-19 via owner |

Source: a tester relayed by the owner 2026-11-19 (`T925-shots/tester-2026-11-19-cheakamus.png`, Cheakamus River's
Regulations card shows only Regulations | Conditions). Tester, verbatim: "when you click on a river it highlights for just a
second and if there is a way to see the section actually on the map as well as the description that would be super helpful.
Like a mini map of just the section" ... "Would it be a bad thing to just make a map for every segment even if it's the only
one? Because there is still water above it that people may get confused with and it's just nice to see the segment you are
referring to". Owner had explained: "That is because chek is only one segment".

## Work
Offer the water's Map view (the T752 split: the part highlighted on the map with its name and ends) for EVERY water, a
single-part one included: the one part highlighted, with where it starts and ends, and the water above/below it drawn
quieter so the reader sees what the rules cover. Same on the app and the website (one load path, T369). Also look at the
tap highlight that shows "for just a second": keep the tapped part marked while its card is open.
## Acceptance
Cheakamus River (one part) shows Regulations | Conditions | Map; pictures (390 light/dark, desktop) to the owner first.
Depends: none. Size: M. Owner: yes (look). Default: the Map tab on every water; the tapped part stays marked while its card
is open.

## Built (lane/t925, not merged)
- The gate was `@app/ui` `frameViews`: a frame flagged `solo` (T221: one part, no continuation, `@app/core` `soloWater`)
  offered Regulations | Conditions only. The gate is now the item alone (named water: all three views; unnamed reach:
  two). The whole solo path is gone: the frame flag and `solo` action, the opener's / start link's "solo?" read
  (`regsSoloOf`), the Shell's group read that dropped the Map view, and `soloWater`. One path for app and web.
- Owner-visible: a search result for a one-part water now opens on its Map (as every other water, T203/T747), not on its
  Regulations. A map tap, a spot and a link open as before (Regulations in front), with Map in the strip.
- The one-part map was already drawn (T875 `PartsOne`): the part lit, every other water grey, the receiving water muted,
  "Flows into …". No end marks: a one-part water's ends are its own mouth and source, and the release places marks only
  at rule boundaries.
- Tap highlight: the browse map is hidden under the water's page on phone AND desktop (the page is full width), so a cue
  kept lit there would not be seen. The persistent marking is the Map view (the tapped part lit while it is open);
  nothing else changed.
- Shots: `T925-shots/before-{390-light,390-dark,1280-light}.png`, `T925-shots/after-{390-light,390-dark,1280-light}.png`
  (staging data, web build).
## Owner 2026-11-20 ~01:10 (main chat): approved, with one change
Verbatim: "It says part 1 of 1 should just say whole water. Also wrong font?? Other wise is good". So a one-part water's
counter reads "Whole water" (core `partWhere`). The font in the shots is the local web preview's fallback (the dev server
does not serve the self-hosted Archivo files that `web-shell.mjs` adds at export), not the app or the site.
