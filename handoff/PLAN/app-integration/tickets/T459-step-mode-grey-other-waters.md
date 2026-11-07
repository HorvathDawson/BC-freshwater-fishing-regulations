# T459 — Stepping ▲/▼ along a water: grey out the other waters

Status: IN REVIEW — 77c985859; the owner picks the downstream option from the shots (out/t459/)

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

## Done (2026-11-06, 77c985859)
A water's Map view is focused (`MapProps.focus`, part of the look — on from the water map's first style, so no swap per
step and no flash of colour): every stream/lake/wetland section that is not this water (`part`/`water`) nor the
receiving water (feature-state `near`: the sections of the part its mouth meets, `useReceivingSections`) is drawn in
`color.water.context` at its opacity × 0.55; the receiving water keeps its colours at × 0.7. Their gear edges, closed
stripe, run dash and no-access dots are not drawn. Labels and the ground unchanged. The grey is the quiet grey of water
nothing is said about (`@color.water.ungauged`, which the Map tab never draws); no colour mode paints it, so no legend
lists it; ΔE ≥ 12 from every Map-tab status and from unknown in all three themes (`tools/water-focus.test.ts`, which
also evaluates the paint for part / water / near / other and pins the browse map unchanged). Both maps.
Off when the reader leaves (the water map unmounts with its view/tab). A mouth on tidal water has nothing to keep.
**The owner's pick:** shots both ways, light and dark, in `app/tools/testbed/out/t459/` (`*-receiving-kept.png` /
`*-all-grey.png`): `silverhope-creek-mouth-*` (a small creek's mouth part; the Fraser it meets kept, amber, muted),
`chilliwack-mouth-*` (▼ from the Chilliwack's mouth crosses into the Vedder; the Sumas it flows into kept),
`fraser-hope-*` (the Fraser's Landstrom Bar part near Hope: tributaries grey; the Fraser's mouth is tidal, so the two
options are the same picture there). Test bed run `out/runs/t459c`: the two options differ for both waters with a
receiving water; switched by the link below (8 `[flag]` lines per water). The test-bed link `canifishthis://testbed/flag?name=receiving&on=0|1` switches it
(`ui-native/src/testbedFlag.ts`, test bed only) — remove it with the choice.


## Owner pick (2026-11-06)
"Step mode is good": option A ships (the water it flows into keeps its colours, muted). Remove the test-bed-only
all-grey switch after the 0.6.8 check.
