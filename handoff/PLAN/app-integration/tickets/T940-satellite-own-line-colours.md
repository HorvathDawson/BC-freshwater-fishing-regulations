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

## Owner 2026-11-22 ~23:55 (round 5)
No-access swatch question, verbatim: "Dark outline on swatch (Recommended)". Built (PR #57 fixes): over satellite the key's swatches under 3:1 on their card (light and cvd: nearly all of them, not only no-access) get a thin outline in the theme's ink (`satelliteKeyEdge`); dark's none. Gate: `tools/map-contrast.test.ts` "the satellite key's swatches".

## Review fixes (2026-11-22, lane L17)
Land closure areas (park, reserve, no-access land, sign zone) keep their theme's colours on satellite: the swap is for water layers only (`IMAGERY_WATER_LAYERS`, guarded by a test). Salmon keep on lakes and wetlands (`color.salmon.open-area`) is in the swap too.

## Owner question: colour-blind (cvd) satellite separation
The satellite set is not the cvd theme's palette, and for the worst colour-blind reader (Viénot and Machado, CIEDE2000) its Salmon pairs sit under the repo's floor of 20 that the drawn cvd Salmon keeps (lowest 20.2):
satellite Salmon: keep/closed 15.4, keep/check 12.3, release/check 7.7, closed/check 21.8, keep/release 39.7, release/closed 26.8. Satellite Standard (the statuses on the map together): own/base 41.2, closed/own 21.8, own/closed fine; base/no-access 21.2, closed/no-access 25.5.
Drawn cvd for comparison: Salmon lowest keep/release 20.2, release/closed 23.5; Standard open/base 13.7 (open is not in the Standard map). No gate fails (the colour gates read the drawn tokens, not the satellite set); this is an unrecorded gap, not an exception. Picture: `T940-shots/cvd-drawn-vs-satellite.png`.
Options for the owner: (1) accept the approved set in cvd too (as built); (2) cvd gets its own satellite set (more pictures, more tokens); (3) satellite keeps the cvd theme's drawn set in cvd only. Not decided.

## Owner 2026-11-23 ~01:30: the cvd question
Verbatim pick: "Own cvd satellite set (Recommended)" (option: the colour-blind theme gets its own bright set on satellite, tuned to keep every pair at or above the floor).

## Proposed cvd satellite set (2026-11-23, lane L17; nothing built, the owner picks the look)
Searched (hill climb, worst-reader CIEDE2000 over Viénot and Machado as `tools/cvd.ts` measures; hue windows so each colour keeps its family) with the constraint that every colour reaches 3:1 against the satellite grounds `color.ui.imagery.water` and `.forest`:
open / Salmon keep #FEFF3A, restricted / release #6FABFA, closed #EA90A1, own / check #98AA08, base #21AAF5, no access #DAFFFF.
Worst-reader ΔE (floor 20), pairs a view draws together: keep/release 33.8, keep/closed 25.3, keep/check 20.8, keep/no-access 21.0, release/closed 22.2, release/check 24.2, release/no-access 21.0, closed/check (= own) 20.9, closed/base 21.9, closed/no-access 21.0, own/base 25.2, own/no-access 26.1, base/no-access 21.0. Not drawn together (excluded): keep/base 33.7, release/base 0.7. Lowest 20.8; the approved set under cvd (for comparison): keep/closed 15.4, keep/check 12.3, release/check 7.7.
The key swatches: yellow, pink and the pale no-access and blue are under 3:1 on the white cvd card, so they carry the same ink outline as light's (`satelliteKeyEdge`, already built). Trade-offs, for the owner: keep is YELLOW and own/check is OLIVE (the hue windows that reach 20 under every reader leave no room for keep to be green); at the 3:1-on-forest floor the search tops out near 20 (19.2 for a more conventional green keep / violet release / pale-yellow own set: #04BC04 #B791F8 #FF85A2 #FFFF8F #5DA7F4 #D8FBFF, not proposed because it is under the floor).
To build it: the six `color.imagery.status.*` tokens become themeable, cvd takes this set, light and dark keep the approved one (the build's ΔE guard passes in cvd; verified on a scratch build). Pictures: `T940-shots/cvd-proposal-standard.png` and `cvd-proposal-salmon.png` (cvd drawn map | satellite now | satellite proposed; a web export, Chilliwack River area). STOPPED here for the owner's pick.
