# T470 — Mid-zoom map (z7–z10) too busy: a calmer hierarchy that keeps the information

Status: IN REVIEW (owner picks a variant from the comparison sheets; nothing in the app changed).

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
