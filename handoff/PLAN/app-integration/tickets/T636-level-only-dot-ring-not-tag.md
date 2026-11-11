# T636 — Conditions map: a "level only" gauge is shown by its ring, not a text tag

Status: IN REVIEW — aca09a946 (ring built, proposal A); the owner picks the ring from T636-shots

Source: owner 2026-11-11: "the level only tag in the conditions map is a lot it should be a border color or something instead to indicate it".

## Now
`gaugeGeoJSON` (`app/packages/ui/src/gaugePoints.ts`, T586) gives a station that is silent in the quantity asked, but reports the other today, a grey dot labelled "level only" (or "flow only" under Level). On the browse map the words repeat on every such dot and crowd it.

## Work
1. The muted dot carries no text label on the map. It is told apart by its border: a ring in one distinct colour (a proposal; the owner picks from shots), grey fill kept.
2. The same rule both ways: "flow only" under Level gets the same ring.
3. The legend gains one line for the ring ("Reports level only" / "Reports flow only", following the quantity shown).
4. The sheet row and the pin keep their words (they are one line each, not repeated across the map).
5. Colour-blind check on the ring against the standing ramp and against satellite.
6. Web and native draw it the same; tests pin: no label on a muted feature, ring property present, legend line.

## Owner
- Pick the ring colour from two or three shots (sent with the question).

## Built 2026-11-11 (aca09a946)
- The muted dot has no label on the map (`gaugePoints.ts`: `muted: true`, `only: level|discharge`, no `label`); the
  label layer takes only features with words, so no empty pill. Sheet row and pin keep their words.
- `gauge-dot` (one runtime style, web and phone): muted → ring in `color.gauge.only`, 2.5 px; coloured dots keep the
  2 px paper stroke. Same under Level ("flow only").
- Legend: one line, a ringed grey swatch, "Gauge reports level only" under Flow / "Gauge reports flow only" under Level,
  while the gauge dots are on (not under Temperature).
- Tests: `app/tools/gauge-ring.test.ts` (both maps, ring paint, label filter, colour-blind floors, mutation pin),
  `T636.legend.test.tsx`, `gaugePoints.muted.test.ts` (no label).
- Colour-blind (worst of normal + protan/deutan/tritan): A (the gauge ink) vs the nearest flow/change ramp colour: light
  18.7, dark 14.7, cvd 10.2; vs its grey fill 58-76. Every hued ring tried collides with some ramp colour in some theme
  (violet 2.0-7.0, yellow 2.2-15.4, cyan 2.7-6.9). Over satellite no single colour works on both dark and pale imagery:
  the ink ring is lost on dark forest (3.4-3.9), plain on gravel and snow; recorded in the test, not held.

Shots (web, 390 px, Conditions, Fraser at Mission, light basemap and satellite) in
`handoff/PLAN/app-integration/T636-shots/`: `current-*` (today's "level only" pill), `A-*` (ink ring, built), `B-*`
(yellow ring #FFE14D), `C-*` (ink ring with a paper halo outside it: reads on dark imagery too; a second circle layer,
not built). Owner question: A, B or C?
