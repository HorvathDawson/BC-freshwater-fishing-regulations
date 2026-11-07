# T459 — Stepping ▲/▼ along a water: grey out the other waters

Status: OPEN (owner 2026-11-06, chat 5).

Owner: "in the section navigation with up/downstream all other rivers should be greyed out I think, except downstream
maybe. That way it is less distracting?"

## What
While a water's map is in step mode (the ▲/▼ bar, T443), only the water being stepped along keeps its colours: the
current part highlighted as today, the water's other parts in their colours. Every other stream and lake is drawn in
one muted grey (a token, e.g. `color.water.context`), at reduced width/opacity. Labels and the basemap stay as they are.

Default for "except downstream maybe": the water this one flows INTO (its receiving water, the first downstream
section past the mouth) keeps its colours, slightly muted, so the reader sees where it goes. Everything else is grey.
The shots show it both ways, and the owner picks.

## Work
1. One switch in the style layer (rule 28: `layers.source.json` + `pnpm style:build`; tokens only). Use a
   feature-state or filter on the focused water's item id (+ the receiving item), not a second source. Colour modes
   still colour every member for the focused water (rule 29). Grey is the one look for "not this water". The status
   colours are never reused for it (rule 23: grey must not read as "closed"/"unknown"; check against the legend).
2. It is on only in step mode, and off when the reader leaves it (closes the water, switches tab). It comes on in the
   same frame as the step's camera, with no flash of full colour (cf. T451).
3. Tests: the style expression for focused / receiving / other. The legend does not list the context grey as a
   status. Emulator shots: Fraser part steps, Chilliwack/Vedder, a small creek into a big river. Light and dark, sent
   to the owner.
Related: T443 (step framing, same screen), T451 (switch flicker). Size: S–M. Owner: pick the downstream option from
the shots.
