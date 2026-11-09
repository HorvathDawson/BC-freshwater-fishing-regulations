# T470 — Mid-zoom map (z7–z10) too busy: a calmer hierarchy that keeps the information

Status: DONE 2026-11-07 — 85ee6156d (tokens, themes and the mask ramp in 225ad72a5, swept by a concurrent commit). Variant B (owner: "Ok let's try that"). Pending: stress-pan cost check (T420 test bed); dark closed / own on faded small streams is the owner's to keep or answer. WCAG pass 2026-11-09 (a9d19c7be): `tools/map-contrast.test.ts` (1.4.11, 3:1 on the ground); dark base and cvd selection lifted to 3:1; light / cvd own, base and missing recorded under 3:1, a casing is the owner's call (Q176).

Source: owner 2026-11-07: "on desktop or mobile, when in a middle zoom value it feels very very busy. we still want a lot
of the info but need to find a way to make it a little less abrasive." Their screenshot: web, about z8, Lower Mainland
from Powell River to the Fraser Canyon.

## What draws at z7–z10 today (light theme; read from the live web map, `__map.getStyle()`, 2026-11-07)

Regulations view (Map tab). Colours are tokens (`themes/light.json`); the status colour reaches the water layers through
feature-state `p` (`paint.ts`), so every status layer is a `match` on `p`.

| layer | what | colour | width (px) | opacity | zoom |
|---|---|---|---|---|---|
| `stream` | every stream, by status | closed `status.closed` #8F0006, own `status.own` #D08404, base `status.base` #608AE5, other #7F8C99 | `0.6 + 0.15·ord` at z8 → `0.75 + 0.75·ord` at z11, linear between (`width.stream.base` ramp). ord 5: 1.35 at z8, 3.45 at z10, 4.5 at z11; ord 7: 1.65 / 4.55 / 6.0 | 1 (0.25 while another water is selected) | all |
| `stream-closed` | white dash over closed streams (the red-dashed look) | `color.outside` #F8F8F2, dash [2, 3] | 0.4 × stream width | 1 on closed | all |
| `stream-noaccess` | dotted grey over no-access streams | #5E565E, dash [4, 3] | 1.2 (z6) → 1.6 (z10) | 1 on no access | all |
| `stream-gear-fly` / `-bait` | dark casing (fly only / bait ban) | #0A1116 (bait dashed) | stream width + 1.05 (z8) … + 1.95 (z11) | 0 (z8) → 0.3 (z9) → 0.6 (z11) | z9+ in effect |
| `lake`, `lake_flowing` | lake fill, by status | the same `p` match | — | 0.6 (cvd 0.9) | all |
| `lake__edge` | lake outline, by status (T457) | the same `p` match | by `√area_m2`: z8 `0.9 + 0.00012·√A`, max 3; z12 `1.2 + 0.0003·√A`, max 4.5 | 1 | all |
| `lake-gear-fly` / `-bait` | lake gear outline | #0A1116 | 0.56 (z8) → 1.04 (z11) | 0 / 0.3 / 0.6 as streams | z9+ |
| `not_fishing_water` (+ `__edge`) | not fishable polygons | #B1AF8B fill / #9F9556 dashed edge | edge 0.6 (z9) | fill 0.9 | all |
| `region` / `region__edge` | region fill and border | #7F7C79 | **3, solid, every zoom** (`width.region.edge`) | fill 0.04, edge 1 | all |
| `mu` / `mu__edge` | management units | #7F7C79 (`mu.edge` = `region.edge`) | 1.2, dotted [1, 2.5] | fill 0.04, edge 1 | z7+ |
| `park` | provincial parks | #cfe3b4 | edge from z11 only (T311) | 0.32 | all |
| `park_closed`, `eco_reserve`, `no_access` | closed areas, crimson hatch | #8F0006 | edge 0.6 from z10 | 0.25 (z6) → 0.39 (z9) → 0.55 (z12) | all |
| `no_access_permit` | permit hatch | #421652 | as above | as above | all |
| `indigenous_land` | tint | #8B5A2B | edge z11+ | 0.14 | all |
| `watershed` | regulated watersheds | #2F5E2A | — | 0.16 | all |
| `wetland` | hatch | #81C784 | — | 0.45 | all |
| `tidal_water` | tint + edge | #A9CFCB / #5E9A97 | edge from z12 | 0.32 | z9+ |
| `sign_zone` | posted zones | #8F0006, dashed edge | 0.8 (z9) | fill 0.14, edge 0.85 | all |
| `outside` | outside-BC mask (the dark band) | `color.mask` #505950 | — | 1 (z6) → **0.72 (z8+)** | all |
| basemap `landuse_park` | the basemap's own greens (forest, parks) | #9cd3b4 and kin | — | 0 (z6) → 1 (z11); 0.4 at z8 | all |
| labels | MU numbers, park / lake / stream names | — | — | — | MU, parks, lakes, streams from z8 |

What is on screen at z8 (owner frame, 1440 × 900, distinct `stream` sections from `queryRenderedFeatures`): 446 —
own 220 (49 %), base 129, closed 80, no access 17. By order: ord ≤ 3 = 24, ord 4 = 98, ord 5 = 181, ord ≥ 6 = 143.
Lower Mainland / Fraser Valley / Powell River z8 are the same shape (own 196 / 210 / 220 of 392 / 361 / 405). The tiles
carry almost only order ≥ 4 streams at z8, so "thin the small streams" must mean ord 4–6 here, not ord 1–2.

Why it shouts: every status stream is drawn at full opacity and almost the same width (ord 4 vs ord 8 is 1.2 vs 1.8 px
at z8, then all of them grow 3× by z10); own rules is the most common class and the most saturated colour; the region
border is a 3 px solid line at every zoom; MU dots, the 0.72 mask and the 0.6 lake fills with 3 px outlines all sit at
the same volume as the water.

## The variants (run-time overrides in a scratch harness; nothing committed)

All three keep every layer, every label and every status colour at z12+, exactly as today. All three expressed as
factors on the theme's own values, so they carry to dark and cvd unchanged. Closed is never faded in any variant.

**A — Weight only (colour gate safe).**
- `stream` width ramp (`width.stream.base`, `by: ord`) stops: `[4, .5, .1], [8, .2, .16], [10, .3, .33], [11, .5, .55], [12, 1, 1], [14, 1.5, 1.5], [16, 2, 2]`
  (today `[4, .5, .1], [8, .6, .15], [11, .75, .75], [12, 1, 1], …`). ord 5: 1.0 / 1.95 / 3.25 px at z8 / z10 / z11
  (today 1.35 / 3.45 / 4.5); ord 7: 1.3 / 2.6 / 4.35 (today 1.65 / 4.55 / 6.0). The same ramp drives `stream-closed`
  (×0.4) and the gear casings (+ their offsets).
- Closed streams × 1.3 width from z7 to z11 (z12+ as today), so closed is the heaviest line on screen.
- `lake__edge` width × 0.6 at z ≤ 8, easing back to × 1 at z12 (closed lakes untouched).
- `region__edge` width `[[5, 2.2], [8, 1.6], [11, 2], [14, 3]]` (today 3 everywhere), opacity 0.75.
- `mu__edge` width 1.2 → 0.9, opacity 0.55 (dots kept).
- `outside` mask 0.72 → 0.5 at z8+.
- No status colour or opacity changes, so `status-colours-distinct.test.ts` is unaffected.

**B — Small waters recede.** A, plus own/base streams faded by stream order at mid zoom (closed exempt):

| zoom | ord ≤ 3 | ord 4 | ord 5 | ord 6 | ord ≥ 7 |
|---|---|---|---|---|---|
| z7 | 0.45 | 0.55 | 0.70 | 0.85 | 1 |
| z9 | 0.50 | 0.60 | 0.75 | 0.90 | 1 |
| z10 | 0.60 | 0.70 | 0.85 | 1 | 1 |
| z11 | 0.80 | 0.90 | 1 | 1 | 1 |
| z12+ | 1 | 1 | 1 | 1 | 1 |

(`line-opacity` = today's expression × `["step", ord, …]`, wrapped in a top-level zoom `interpolate`.)

**C — Closed leads.** B with a deeper fade, plus own/base colours washed toward the ground at mid zoom, lighter lake
fills and lighter admin lines:
- order fade: z7 `0.3 / 0.4 / 0.55 / 0.8 / 1`, z9 `0.35 / 0.45 / 0.6 / 0.85 / 1`, z10 `0.5 / 0.6 / 0.8 / 1 / 1`, z11 `0.8 / 0.9 / 1 / 1 / 1`.
- `stream` colour: own mixed toward #F8F8F2 by 0.45 (z7) / 0.40 (z9) / 0.15 (z11) / 0 (z12); base by 0.30 / 0.25 / 0.10 / 0.
  (Light: own #D08404 → about #E1B66A at z8.) A top-level zoom `interpolate` between two `p` matches.
- Closed × 1.5 width z7–z11.
- `lake` / `lake_flowing` fill opacity × 0.45 (z7) / 0.5 (z10) / 1 (z12); closed lakes keep ≥ × 0.8.
- `lake__edge` × 0.5 at z ≤ 8; `region__edge` `[[5, 1.8], [8, 1.2], [11, 1.6], [14, 2.6]]` at 0.6; `mu__edge` 0.8 at 0.4;
  mask 0.4.

## Measured: what the variants do to the colour-blind gate

The gate (`app/tools/status-colours-distinct.test.ts`) models a status line as 0.6 coverage over four light grounds, worst
of normal vision and protan / deutan / tritan (Viénot + Machado). The same maths on the mid-zoom (z8) result of each
variant (`worstSeparation`, scratch `gate.mts`):

| | closed / own line (floor 21) | own / base line (floor 35 light, 34 cvd) | own / base lake (floor 35 / 47) | own vs ground |
|---|---|---|---|---|
| today | 21.2 | 35.9 (cvd 37.2) | 35.9 (47.2) | 16.9 |
| A | 21.2 | 35.9 (37.2) | 35.9 (47.2) | 16.9 |
| B, an ord 5 stream | 21.7 | 21.0 (22.5) | unchanged | 13.1 |
| B, an ord 7 stream | 21.2 | 35.9 | unchanged | 16.9 |
| C, an ord 5 stream | 25.8 | 8.5 (9.9) | 10.9 (21.2) | 4.9 |
| C, an ord 7 stream | 24.2 | 22.8 (25.2) | 10.9 (21.2) | 8.1 |

So: A changes nothing the gate measures. B keeps closed / own (it improves a little) but makes own vs base on SMALL
streams hard to tell apart at z7–z10 for a colour-blind reader; main stems keep today's numbers. C makes closed stand
out most, but own vs base and the lighter lake fill fall well under the floors (the same wall T457 / T467 hit).

## Recommendation

**B**, with A as the safe fallback. A alone already takes most of the shouting out (thinner small streams, thin region
border, quieter MU dots and mask) and costs no colour separation. B adds the hierarchy the owner is asking for: the
named rivers read first, the creek network becomes texture until z11, and the closed red is the strongest line on the
map. Its cost is own vs base on small streams at mid zoom; at z11+ everything is today's map, and a tap still answers.
C is the calmest but trades away own vs base and the lake fill, which the gate (and T467's open question) protect; do
not ship C unless the owner answers T467 "cvd-only" AND accepts own/base blending at mid zoom.

## Work (after the owner picks)
1. Edit `app/packages/map/style/layers.source.json` + tokens (rule 28; no literals): the stream width ramp, closed width
   multiplier, `width.region.edge` → a zoom ramp, `opacity.admin.edge` / mu width, `opacity.outside`, lake edge ramp.
   For B, the order fade belongs where the `p` opacity is composed (`paint.ts`), as a token-driven ramp.
2. `pnpm style:build`, `pnpm check`; extend `status-colours-distinct.test.ts` with the faded case for B (a recorded
   exception or a new floor, the owner's word quoted).
3. Native: `nativeStyle` = `runtimeStyle` + `applyLook` (parity.test.ts), so no second change; shot on the emulator.
4. Frame cost: B adds a `step` on `ord` inside the opacity; no new layer. Measure on the T420 stress pan once.

## Acceptance
Owner picks from the sheets; the chosen variant renders as the sheet did (web + emulator shots, same cameras); gate
green with any new exception recorded; z12+ pixel-identical to today in a before/after diff.

Shots: scratch harness (`overrides.js` + `shoot.mjs`, Expo web + headless Chrome), 4 places (owner frame, Lower
Mainland, Powell River, Fraser Valley) × z7 / z8 / z9 / z11 × 1440 and 390 px, plus a cvd pair at Fraser Valley z8.

Related: T457 / T467 (lake fill vs the colour gate), T47 (status colours), T311 (park edges from z11), T420 (map
performance), T469 (legend). Size: S (A) – M (B). Owner: yes (pick A / B / C or none). Default: none shipped until picked.

## Built: variant B (owner 2026-11-07)

Owner: "Ok let's try that but should add a note that we could tune the parameters on the threshold of magnitude of streams
to zoom".

**Thresholds (stream order × zoom) are tunable; first values are the proposal's.** The knob is ONE block:
`fadeByOrder` on the `stream` layer in `app/packages/map/style/layers.source.json` — `orders` (the step thresholds on
`ord`), `classes` (own, base; closed can never be listed, build-style refuses it) and `stops`
(`[zoom, [factor for ord < 4, 4, 5, 6, >= 7]]`, the table above). Edit it, `pnpm style:build`; build-style checks every
row and that the last row is all 1 (so the close-in map cannot move). A deeper fade fails
`tools/status-colours-distinct.test.ts` until its floor is recorded there.

What shipped (everything from z12 in is today's map, proved by `tools/midzoom.test.ts`: every touched layer's paint
evaluated over zooms 12–20 × orders × lake areas × feature-states, all themes, against the pre-T470 paint frozen in
`tools/midzoom-today.fixture.json`; and 0 changed pixels at z12 in the shots):
- stream width ramp as A; `width.closed` [[6, 1], [7, 1.3], [11, 1.3], [12, 1]] (closed × 1.3 z7–z11, on the day's closed labels).
- lake outline `width.recede` [[8, 0.6], [12, 1]], closed lakes untouched.
- region edge: `width.region.edge` (3) × [[5, .733], [8, .533], [11, .667], [12, 1]] = 2.2 / 1.6 / 2.0 / 3 px;
  opacity `opacity.region.edge` (1) × [[11, .75], [12, 1]].
- unit edge: `width.admin.edge` × [[7, .75], [11, .75], [12, 1]] = 0.9 px to z11; opacity `opacity.mu.edge` × [[11, .55], [12, 1]].
- outside mask: `opacity.outside.mid` (light / cvd 0.5, dark 0.49) from z8 to z11, back to `opacity.outside` at z12.
- order fade (`fadeByOrder`, above) composed into the stream's opacity in `paint.ts` (`fadeByOrder()`), before the
  no-access, dim and focus rules, which wrap it.

Differences from the sheet, all for "z12+ is today's": the sheet's region ramp ran on to 3 px at z14 (now 3 at z12),
and its region / unit opacity and the mask were flat at every zoom (now back to today's at z12). Mid zoom is the sheet's.

Colour gate (`status-colours-distinct.test.ts`, T470 block): main rivers (ord >= 7) and closed are never faded, so they
keep every floor. Small streams, as recorded (worst vision, thin lines): light own / base 21.0 at ord 5 z8 (10.4 at the
deepest fade, ord < 4 z7), cvd 22.5 (11.7) — accepted by the owner for B. **Dark was not on the comparison sheet:** its
own / base holds (37.8), but closed / own on a faded small stream falls to 12.0 at ord 5 z8 (9.2 deepest; floor 21) —
the faded yellow own sinks toward the dark rose closed. Recorded, not hidden; the owner's to keep or answer (e.g. a
shallower fade in dark).

Native: `nativeStyle` = `runtimeStyle` + `applyLook`, so no second change (parity.test.ts green).

Shots (web, today vs B as built, owner / lm / fv × z8 / z9 / z12 × 1440 and 390 px): scratch `t470/after-*.png`, side by
side in `after-pair-*.png`. "Today" is rebuilt exactly on the live map from B's own expressions (scratch `today.js`,
checked against the frozen fixture at every zoom). z12: 0 pixels differ in all six frames.

Stress-pan cost check pending (T420 test bed).

Note: the token / theme / runtime-style (mask) edits were swept into 225ad72a5 (T475) by a concurrent commit; the rest
is in the T470 commit.


## WCAG contrast pass (owner 2026-11-09: "what ever is industry standard")

Standard applied: WCAG 2.x **SC 1.4.11 Non-text Contrast**. Every mark that carries an answer needs ≥ 3:1 (relative
luminance) against the colours next to it. Here that means the Standard view's statuses, the "not answered yet" grey,
the Salmon view's four, and the selection ring, each measured against the basemap ground under it. Legend swatches need
3:1 on the card, and legend words need 4.5:1 (SC 1.4.3).

Checker: `app/tools/map-contrast.test.ts`. It reads the real tokens (`waterStatusColour`, `salmonStatusColour`,
`resolveTheme`) and the real grounds: the Protomaps flavour each theme draws (dark = "black", light/cvd = "light"),
meaning land, water, and the green-space fills `landuse_park` paints at full strength from z11 (wood, forest,
scrub/grass, glacier). Every mark under 3:1 fails unless it is in the test's recorded list. That list must be complete,
and a recorded mark fails if it slips under its recorded ratio. Dark may not gain an exception. Closed must reach 3:1 in
every theme. A mutation pin keeps the old dark base failing (2.52).

**Fixed (before → after, worst ratio over the grounds, the ground that binds):**

| theme | mark | colour | worst before | worst after | binds on |
|---|---|---|---|---|---|
| dark | base (general rules) | `#0368EB` → `#4C70FC` (same blue, lighter) | 2.52 | 3.03 | water |
| cvd | selection ring (`color.highlight`) | `#F55C00` → `#C04400` (same vermilion, deeper) | 1.92 | 3.02 | scrub |

Legend swatch on the card: dark base 3.55 → 4.27, cvd selection is not a legend row.

**Already passing, unchanged:** dark closed `#FF3B6D` 3.67 (on the dark water; 5.35 on land), own 8.77, no access 4.51,
restricted / release 4.68, open 10.28, selection 5.16, missing 4.73. Light closed 5.66, no access 4.14, open 3.63,
restricted 8.37, selection 3.32. Cvd closed 5.66, open 3.18, restricted 7.60, no access 4.14. Every legend swatch reaches
3:1 on its card (the tightest is light own at 3.01), and legend words (ink 15+, sub 4.5+) were already held by T243.

**Dark closed "reads weak":** by the standard it passes everywhere (3.67:1 at worst, on water), so its colour did not
change. The likely cause of the weak look is the closed stripe drawn down the middle of the line at 0.4× width
(`stream-closed`, `color.cue.closed` = the near-black paper in dark). That is a non-colour cue and was left alone, as
asked. If the owner wants closed heavier in dark, the stripe is the knob, not the hue.

**Still under 3:1, recorded (light and cvd), with the owner's call below:**

| theme | mark | colour | worst | binds on |
|---|---|---|---|---|
| light | own (and Salmon "check", which is own) | `#D08404` | 1.76 | scrub |
| light | base | `#608AE5` | 1.96 | scrub |
| light | missing (not answered yet) | `#7F8C99` | 2.01 | scrub |
| cvd | own / check | `#D08404` | 1.76 | scrub |
| cvd | base | `#3695D8` | 1.91 | scrub |
| cvd | missing | `#7F8C99` | 2.01 | scrub |

Why recolouring cannot fix these: on this light basemap (green-space fills at luminance ~0.57), a 3:1 line needs
luminance ≤ 0.155 (about L* 46). I searched sRGB (step 3–5) with each hue kept, against every colour gate in the suite:
- The best amber at 3:1 stands only ΔE2000 14.7 off the closed red for the worst reader. T47's floor is 29. This is the
  owner's own complaint: "has regs and closed are very similar", 2026-10-21.
- The best blue at 3:1 (~`#0060FA`) sits 14.3 from `status.open` in normal vision (the one-meaning floor is 20), and
  falls under the closed/base and no-access floors.

So darkening own and base brings back the bug T47 fixed. The standard remedy that keeps both is a **casing**: a darker
edge under the line (for example a darker shade of the same hue). This is how roads and transit lines meet 1.4.11 on
light basemaps. It is a new layer under every stream and lake outline, a frame cost (T420), and a visible change to the
map this ticket just calmed, so it is the owner's call (Q176). Default: no casing; the exceptions stay recorded.

**What the dark base cost elsewhere** (each recorded floor lowered to the new value rounded down to 0.5, with the reason
in the test files):
- `status-colours-distinct` dark permit (restricted lilac vs base, protan): tokens 15 → 14.5 (14.9), lines 13.5 → 12.5 (12.8).
- No-access grey vs base, normal vision: tokens 33.5 → 31.5, lines 24 → 23, lakes 30.5 → 29.
- Faded small streams, dark own/base: ord 5 z8 37.5 → 36, deepest fade 30 → 24.
- `label-colours`: the dark water-name ink vs base is a new recorded pair, ΔE 7.9 (deutan).
- `midzoom-today.fixture.json`: the two hexes were carried in by hand. Nothing else in the frozen paint moved.

#4C70FC was the searched blue (sRGB step 4, LCh hue 255–295, ≥ 3.02:1 on every dark ground) that gave up the least.

Out of scope, measured: our own translucent area tints (provincial parks at 0.32, watersheds, closures) darken the light
ground further. A line over a park needs luminance ≤ ~0.12. This does not change the conclusion above. The mid-zoom order
fade (B) thins small own/base streams on purpose at z7–z11. The checker measures the solid colour, as drawn from z12 and
on every main river.

Shots (web, local dev on the T517 side release, Fraser Valley z11, 1280 × 800): `shots/T470-wcag-light-fv-z11.png`
(light is unchanged), `shots/T470-wcag-dark-fv-z11-before.png` and `shots/T470-wcag-dark-fv-z11-after.png`. The
"before" is the after frame with the old base hex swapped back into the live paint.
