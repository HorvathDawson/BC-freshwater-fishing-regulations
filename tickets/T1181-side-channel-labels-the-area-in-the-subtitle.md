# T1181 — Side-channel labels: the area in the app subtitle, universal, built like the A-to-B labels

Status: OPEN — after PR #312 merges (its data side)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1181 | Side-channel labels: the area in the app subtitle, universal, built like the A-to-B labels | OPEN — after PR #312 merges (its data side) | — | the app shows "inside <park / eco reserve / WMA>" in the subtitle of every part inside an area, by the same path as the A-to-B section labels; #312 builds the words and the distance rule |

Source: the owner, 2026-11-26: "also we need to make sure th ui still says if it is in an area in the subtitle in the app so a user knows eco
reserve or park or whatever in those cases. (like how sections use start and end to create subtitle. we need this to be universal and cleanly produced
the same wat the a to b labels are". The data side (the words and "a distance only when the water has more than one such part") is PR #312
(T1172, `fix/T1172-one-section-mouth-and-area-name`). This is the app side.

## Work
1. Read how a section's subtitle is built from its start and end (`app/packages/core/src/regs/header.ts`, `partLabel`); make the area a
   second source for the SAME builder, for every part inside a park, eco reserve, WMA or national park, not only the 26 approved parts.
2. One code path for A-to-B and area labels; no special case per water; a failure to build shows, never a silent blank.
3. Tests with a real release: Old Kootenay (inside Creston Valley WMA), Thelwood (inside Strathcona - Westmin Park), Squamish (Baynes Island ER).

## Acceptance
Every part with an area in the data shows it in its subtitle; a unique side channel shows plain words. Severity: low-medium. Model: Opus. Size: S-M.
Depends: PR #312.
