# T636 — Conditions map: a "level only" gauge is shown by its ring, not a text tag

Status: OPEN

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
