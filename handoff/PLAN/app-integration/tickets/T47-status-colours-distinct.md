# T47 — status colours you can tell apart: "has its own regulations" vs "closed" (and restricted)

Status: PARTIAL — daf61b67, 5473776d, d6a5a51c, 056d6cc5, e6754333. Palette D questions closed (decisions.md "palette D stands"). Left → T83: boundaries under the water, the warm temperature dot's hue, the sub-20 pairs. Close as DONE when T83 closes.

Phase 4 · Depends on T22 (tokens), the colouring engine (colouring.ts), T39 (palette gate) · Size 1 day

## Owner (2026-10-21)
"also we want to make the regulation color better right now has regs and closed are very similar."

Also (owner, same evening): "the region color being red is confusing for the same too close to regs view issue." The
region boundary (`color.region.edge`, light #8E2F52 dark raspberry at 2.4 px; dark #E29AB4) reads as a closure.

## Today (app/packages/map/style/themes/*.json)
| token | light | dark |
|---|---|---|
| status.own (has its own regulations) | #B37B00 dark amber | #F0E442 yellow |
| status.closed | #940018 dark crimson | #FC4164 |
| status.restricted | #8f5c00 dark brown-amber | #e8a733 |
| status.open | #1D7741 | #48b07a |
| status.base (region rules only) | #0072B2 | #56B4E9 |
| region.edge (boundary, not a status) | #8E2F52 raspberry, 2.4 px | #E29AB4 |
| mu.edge (boundary) | #8A7A62 dashed | #A6947A dashed |
In light mode, own, closed and restricted are all dark warm colours of similar lightness. As thin stream lines over the
basemap they look alike. CVD users lose the red/amber difference too.

## Goal
Every status that sits beside another on the map, and in the Keep tab's status words, is distinct at a glance on the
real map. That means thin lines (1–2 px at z8–12) and lake fills on the light and dark basemaps and on satellite, for
normal vision and for protan, deutan and tritan simulation. Meaning stays one colour per meaning (the tokens' own rule).
Prefer separating by LIGHTNESS and hue together, plus a non-colour cue where cheap (e.g. closed waters drawn dashed or
with a casing, the way the gauge-less dash works today).

REFERENCE LINES ARE NOT STATUSES: region and management-unit boundaries (and any future boundary layer) must not use a hue
from the status family (red/crimson/amber/green/blue as used for status). They are neutral (ink/grey family), told
apart by dash pattern and width, and drawn under the water's status colour. A boundary must never be mistaken for a
closed or regulated water.

## Work
- Measure: ΔE2000 between every pair of status colours, as drawn (line over basemap), for normal + three CVD simulations,
  in light/dark/cvd themes. Contrast of each status word on the card (≥ 3:1 for a status colour, as the tokens require).
  Record the table in `T47-colours.md`.
- Propose two or three candidate palettes (keep Okabe & Ito where it still works). Render each on the same map views
  (Vedder/Chilliwack z11, Kootenay Lake z9, a province view z6) in light, dark and satellite, and simulate CVD on them.
  Show the owner side by side as screenshots; the owner picks.
- Apply the pick through tokens only (themes/*.json + style:build). One colouring pathway, so no tile change and no
  per-component colours. Update the legend.
- Re-colour `color.region.edge` (and check `color.mu.edge`) to neutral boundary tokens; include them in the candidate
  renders so the owner sees boundaries next to closed and own-regulation waters.
- Gate: a test that no boundary token falls within ΔE2000 < 20 of any status token (all themes, normal + CVD).
- Gate: extend tools/cvd.test.ts so every pair of status colours that can be adjacent keeps ΔE2000 ≥ the chosen threshold
  (e.g. 20) under all three simulations, and so own vs closed in particular never drops below it again.

## Acceptance
- The owner has picked from the side-by-side shots. The gate is green in all themes. The Keep tab and map legend use
  the new tokens. T39's census holds.

## As built — part 2 (2026-10-22, revised after review)
Owner (2026-10-22): grey boundaries good; the STATUS colours must be more different than A / B / C. Applied **palette D**
(handoff/PLAN/app-data/status-colours/T47-colours.md "Part 2"; `candidates.json` "D"; renders `candidates/D/`; compare.html
last column): B's neutral boundaries, dotted unit and light lakes 0.6 kept; ALL FIVE statuses re-chosen together by
`scripts/search-d.mjs`, the owner's closed / own pair first, no status within 20 (normal) / 15 (any vision) of body ink,
the temperature pairs measured as the dots they are. Light: closed `#8F0006`, own `#D08404`, base `#608AE5`, restricted
`#421652`, open `#016890`. Dark: `#FF3B6D` / `#FFD307` / `#0368EB` / `#BE8DC2` / `#A7FBB2`. cvd: closed / own as light;
base `#3695D8`, restricted `#451F60`, open `#4361D1`.
- The first D (now `D0`) was replaced the same day: review found its light closed / own line (19.5) under B's 20.3 and
  dark no better than before T47, its light restricted a near-black that read as body ink on the card (18.3 / 10.9),
  its cvd boundary token under 20 (18.9), and no render of restricted or open.
- Gate: `app/tools/status-colours-distinct.test.ts` — owner's pair above B (light 29.4 / 21.2 / 25.2 / 17.8 vs 28.4 / 20.3 /
  23.6 / 16.9) and above before-T47 in dark (21.8 / 21.4 / 22.4 / 19.0 vs 20.8 / 19.6 / 20.6 / 17.6); boundary token 20 in
  light and cvd. Under the ticket's 20 (stated in the test): thin lines in every theme, the permit and temperature pairs
  (light 16.0 line / 15.9 dots), the dark boundary (15, no palette reaches 20), dark closed / base as a line (19.1, under
  before-T47's 22.6). Mutation pins: before T47, B and D0 all fail it.
- Renders: the four map views + a permit-area view (restricted) + a temperature view (open) for D and B, and the
  Regulations card (Keep tab, `scripts/card-shots.mjs`) in light / dark / colour-blind; the card's computed word colours
  are D's hexes (read from the theme tokens).
- Port sync: closed / open / restricted are prototype tokens (regs-tokens parity), so `regs-v11.html` + `watermap.js`
  took D and `port/tokens.mjs --ref <that commit>` regenerated `golden/tokens.json`.
- Not done: boundaries still drawn above water (part 1 question 4, no owner answer); the warm temperature dot is plum
  (it reuses restricted by design; an amber restricted collides with own).
