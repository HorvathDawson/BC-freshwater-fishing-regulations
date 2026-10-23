# T83 — Map: boundaries under the water's status colour; the warm temperature dot; sub-threshold colour pairs

Status: DONE — 111bd363, cbd3e139, e61a6e7d (shots), 7bb14b79 (review) (lane wf_63a2801d-ce3-5; built with Q14–Q16 defaults).
(1) Boundaries: `region`/`mu` moved before `stream` in layers.source.json; the lake shoreline (a status line) is held
back by the new generator option `outline.drawAfter: "mu"`; the lake selection ring follows the last layer of its
source-layer. Order now: lake fills → region/mu → lake__edge → ring → casing → stream. tools/layer-order.test.ts
(whole order + two mutation pins). (2) Warm dot: `color.temp.warm` (light #E86C48, dark #FC9C28, cvd #BC7864), one
table @app/core `TEMP_BAND_TOKEN` → `tempBandColour` (dots) and `palette.temp` (legend). (3) Sub-20 pairs: temperature
lifted in light (15.9 → 27.9) and cvd (20 → 25.3); every remaining floor under 20 is listed with its reason in
`UNDER_20` (status-colours-distinct.test.ts), a new one fails. Dark temperature went 17.7 → 15.4 (warm vs closed,
deutan): recorded exception — no warm hue light enough for the dark map gets further from the dark rose closed. Review (7bb14b79): all three dark dot pairs are under 20 (warm/critical 15.4, cool/warm 17.8, cool/critical 17.7); the 17.7 was open vs closed, untouched by T83, not the plum (which stood 36.0/22.7) — the reason now says so and a test pins it.
Shots (z7/z10/z13 + temperature legend, light/dark): handoff/PLAN/app-data/status-colours/T83/ via
scripts/t83-shots.mjs. Gates green: style, check-colours, status-colours-distinct, label-colours, water-status-cvd,
typecheck. Nothing to rerun (style only; no tiles/reach/deliver). Left: the ledger rows (T83 DONE, T47 → DONE) for
whoever closes the ledger; the warm dot vs `color.highlight` is not held (dark 3.9 worst-vision; the highlight is a
casing on a line, the dot is a stroked circle) — accept, or ask the owner whether a dark warm should give up the orange. Also in normal vision the cvd theme's warm #BC7864 is 15.9 off `color.highlight` (light 33.2, dark 46.0). Not built: a test of build-style's three `outline.drawAfter` error paths; the wetland selection ring stays under the boundaries (the lake ring is now over them) — harmless, same as before T83.

Source: T47 left (1) boundaries still drawn above the water (part 1 Q4, no owner answer) though the ticket says they
must sit "under the water's status colour"; (2) the warm temperature dot is plum, reusing restricted; (4) pairs below 20
recorded in the test: thin lines, permit/temperature, dark boundary 15, dark closed/base line 19.1. T47's (3) palette D
questions are closed ("palette D stands").

## Work
Order region/MU boundary layers under the water status lines, above the fills; give the warm dot its own hue (not a
status colour); lift each sub-20 pair or keep it as a recorded exception with a reason.

## Acceptance
Layer-order test mutation-pinned; style-valid, status-colours-distinct, label-colours, light/dark/CVD gates pass; shots
at z7/z10/z13 both themes. Then T47 → DONE.

Depends: none (style only, no tile rebuild). Size: S–M.
Owner: delegated (call 11). Default: boundaries under the water lines; warm dot gets its own hue.
