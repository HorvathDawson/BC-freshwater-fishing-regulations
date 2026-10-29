# T283 — "Standard" mode renamed "Trout & more"; map legend text larger

Status: DONE 2026-10-29, commit  (owner 2026-10-29: "the name standard keeps irritating me" → "trout & more i think is good"; "the legend text
should be a bit bigger for each map too. rn it is hard to read").

## Work
1. Every user-visible "Standard" for the map mode becomes "Trout & more" (mode switch, walkthroughs, Help, report form's
   "Map colours", About/Updates, SEO/site copy if any). Internal keys (`standard`) stay; words live in the core words tables.
   Check the switch still fits at 360 px.
2. The legend under each map mode (Trout & more, Salmon, Runs) uses a larger type step (e.g. TYPE.small → TYPE.body or a new
   legend token, ≥15 px) with swatches scaled to match; the legend still overlays the map (T275: map rect unchanged).

## Acceptance
Phone shots per mode before/after at 360 and 390 px; grep shows no user-visible "Standard" for the mode; tests updated.

Depends: T275, T276. Size: S. Owner: no.

## Outcome
- Words: map mode "Standard" is "Trout & more" in the two core words tables (`SALMON_WORDS.toggle.standard`, report form `modes.standard`); the walkthrough/Help read them from there. Internal key `standard` unchanged; grep shows no user-visible "Standard" for the mode left. The switch fits at 360 px (shot).
- Legend: one new token `LEGEND_TEXT = 15` (`ui-native/src/type.ts`, was 11.5) used by `LegendCount` (labels and counts), the Runs foot's title and note; swatches scaled (square 12, line 20x4, dotted 20x4, gear edge 22x10), wrap strip padding 9. Legend still overlays the map: map canvas rect identical in all modes (360x790, 390x790).
- Verified on web, headless Chrome (CDP) against the owner's dev server :39218, 360 and 390 px: `T283-shots/` before/after for Trout & more and Runs (the Runs "before" frames show its tutorial card). Salmon could not be shot: this dev setup's map errors (`Cannot read properties of undefined`) and the salmon/runs feeds do not load, so the toggle never leaves Trout & more; Salmon uses the same `LegendCount`, pinned by a test (15 px). Runs foot data also "could not be loaded" in the shots (layout and sizes are real).
- Tests: LegendStrip pin added; core + ui-native suites pass except `likeVisit.test.ts` (another lane's in-flight work). `tsc -b` errors only in conformance `frozenTidalPage` (not mine).
