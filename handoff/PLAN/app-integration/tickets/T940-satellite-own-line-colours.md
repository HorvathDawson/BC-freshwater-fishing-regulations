# T940 — Satellite: one colour set for all water lines, the same in light and dark

Status: IN REVIEW — owner: OWNER-TODO "Look (you): T940 satellite after-shots" (row to add; emulator shot waits for the 0.9.0 APK); owner approved the set 2026-11-22; built (code 7a9213671, PR open); after-shots in T940-shots/. Not released: needs the 0.9.0 app build.

Source: owner 2026-11-20 (dialog after T923), on the satellite outline: "Satellite should maybe just be its own colors?
Best for satellite?"; then picked "Yes, open a ticket" for the stream and lake lines. After this release.

## Work
On satellite the water lines (streams, lakes, edges, the status/rule colours, casings) change with the light/dark theme,
although the photo does not. Give satellite its own colour set, chosen for photographs, the same in light, dark and cvd
(as T923 did for the reserve and private-land outline). Keep the meanings (closed, own rules, base rules, no access, keep
salmon …) and the key's swatches in step. The drawn map stays unchanged. Style via layers.source.json (AGENTS 28) and the
colour checks; pictures first (light and dark the same, over forest, river gravel, town and water).

## Acceptance
Satellite looks the same in every theme; every status reads on the photo; the drawn map is unchanged; the owner approves
the pictures.

## Owner 2026-11-21 12:41
Asked for the 0.8.5 batch: "No, next chat". No side-by-sides exist yet; build them first there.

## Pictures (2026-11-22, lane L3)
`T940-shots/proposal-vs-now.png`: four photo scenes (forest, river town, town, deep water; Esri World Imagery z15) x four
columns — the status colours as they are in light, dark and cvd, and ONE proposed set for all themes (open #6BFF9E,
restricted #E3A6FF, closed #FF3B6D, own rules #FFD307, base rules #8CC3FF, no access #E6E4E2, dotted). A MOCK-UP: the app's
two-edge line drawn over the tiles in each set, not a render of the app. Reading: light-theme colours (dark teal, plum, brick)
sink into forest and water; the dark set already reads, the proposal keeps its meanings and lifts base rules (the dark
theme's #4C70FC is dark on dark). Recommendation: take the dark theme's set with those small lifts for every theme.
Question for the owner: approve this set (then the tokens are built, with the key's swatches)?

## Owner 2026-11-22 23:20 (question round 1)
Approve the proposed set? Verbatim answer: "Yes, use the proposed set (Recommended)".

## Built (2026-11-22, lane L17)
Six non-themeable tokens `color.imagery.status.{open,restricted,closed,own,base,no_access}` = #6BFF9E #E3A6FF #FF3B6D #FFD307 #8CC3FF #E6E4E2, the same in light,
dark and cvd (dark's closed/own are aliases of its own, which already were those hexes; the one-hex-one-meaning guard needs it). On satellite only,
`onImageryTokens` (map `style.ts`) swaps each `color.status.*` — and the salmon colours that are those statuses: keep, release, closed, check — for its satellite token
inside the one painter (`contract.ts` `paint`); the drawn map is untouched. The key's swatches (`MapLegend`, `onSatellite`) read the same swap while satellite is
the ground. `build-style.mjs`: the ΔE-between-tokens guard compares a satellite status token as the status it replaces, and skips the paper, place names and gauge dot
(not drawn beside it). Test `tools/satellite-status.test.ts` (mutation-proved). Not changed: the grey of a section with no status (`color.water.ungauged`) and the
Layers sheet / Help / search-row swatches (they are not the map's key). After-shots: see `T940-shots/after-*`.
After-shots (web build of this branch, staging data, Chilliwack River area, satellite, 390 px): `T940-shots/after-web-satellite-dark.png`, `after-web-satellite-light.png`:
the lines are identical in both, the key's swatches are the same hexes. Emulator after-shots NOT taken: they need an APK, and the APK waits for the 0.9.0 batch.
FINDING for the owner: on the light theme the key's card is white and the No access swatch (#E6E4E2) is nearly invisible on it (see the light shot, bottom row). The key sits on the
theme's card, the lines on the photo. Options if he wants it fixed: a thin dark outline on the key's swatch over satellite, or the key keeps the drawn map's grey there. Not decided.
