# T284 — A saved spot's page, restructured: the day, your notes, how the coming days compare

Status: DONE 2026-10-29, 85e854da2 (owner 2026-10-29: "restructure the spot info. to make it clear this was what the day looked like. this is the
info and images you added and this is how similar that day was to the next few days and why. make it formatted a lot
better for both mobile and desktop. right now it is a hard read").

## Work
Re-lay the spot page (Spots tab → a spot) into three clearly headed blocks, same data, no new sources:
1. **That day** — date, conditions as they were (flow/level vs normal, weather, water temp if any), compact and scannable.
2. **What you added** — notes, photos (grid, tap to open), catch/pin details.
3. **The next few days** — per day a similarity score to that day and the plain reasons ("flow within 10%", "warmer by 4°"),
   most similar first or in date order with the best highlighted.
Phone: one column, cards, no dense paragraphs. Desktop: two columns (That day + What you added | The next few days).
Words in the core words tables; existing components (T260); no look change elsewhere.

## Acceptance
Phone + desktop screenshots before/after for a spot with photos and one without; a11y gate covers the new layout.

Depends: none. Size: M. Owner: no.

## Done
`SpotScreen` (ui-native) now reads as three headed `Section`s (`spot/parts.tsx`): **That day** (water record, day flow, trace,
weather, the week before), **What you added** (notes, photographs: a responsive grid, each named "Photo n of N"), **The next
few days** (`LikeVisitStrip`: one card per day in date order, score + read, one short plain reason per scored factor — "Air
temperature: 4° cooler" — always shown, the highest-scoring day marked "Closest to your visit" with a heavier border).
Phone: one column in that order. From `WIDE_PANEL` (880): That day + What you added on the left, The next few days beside.
Same data, no new fetch. Words: `SPOT_WORDS` (thatDay, added, nextDays), `LIKE_WORDS.best`, `LikeFactor.plain` (core),
`photoLabel` (data/spots words); `LikeDayVM.best` / `.reasons` (ui). The strip's tap-to-open working is gone (the reasons are
shown); `why` stays on the view model unrendered.
Later (owner/coordinator review 2026-10-29): the week before is a forecast-style table (sky icons, tinted rain, sticky row labels); a photo opens full screen (`spot/PhotoViewer`); similarity is tiered — flow + rain (0.25 each), cloud + pressure (0.15), temp + wind (0.10) — with a day no forecast reaches carrying today's reading at half weight, said.
Shots (web, headless, 390 / 1280, with and without photos): `T284-shots/before-*`, `after-*` (`app/tools/spot-shots.mjs`).
A11y gate: `a11y.shell.test.tsx` gains a spot-with-notes-photos-and-days state at both widths (names, 44 px boxes, the
columns' shape asserted).
