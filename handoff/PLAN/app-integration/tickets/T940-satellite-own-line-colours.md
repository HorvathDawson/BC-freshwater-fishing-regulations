# T940 — Satellite: one colour set for all water lines, the same in light and dark

Status: IN REVIEW — owner: see ## Owner (row to add; emulator shot waits for the 0.9.0 APK); owner approved the set 2026-11-22; built (code 7a9213671, PR open); after-shots in T940-shots/. Not released: needs the 0.9.0 app build.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T940 | Satellite: one colour set for all water lines, the same in light and dark (owner) | IN REVIEW | | owner approved the set 11-22; code 7a9213671, PR feat/T940-satellite-set; after-shots T940-shots/; ships with the 0.9.0 app build |

Source: owner 2026-11-20 (dialog after T923), on the satellite outline: "Satellite should maybe just be its own colors?
Best for satellite?"; then picked "Yes, open a ticket" for the stream and lake lines. After this release.

## What
On satellite the water lines (streams, lakes, edges, the status/rule colours, casings) changed with the light/dark
theme, although the photo does not. Satellite gets its own colour set, chosen for photographs (as T923 did for the
reserve and private-land outline), keeping the meanings and the key's swatches in step; the drawn map unchanged. Style
via layers.source.json (AGENTS 28) and the colour checks.

## Owner answers (verbatim)
- 2026-11-21 12:41, asked for the 0.8.5 batch: "No, next chat".
- 2026-11-22 23:20, approve the proposed set (`T940-shots/proposal-vs-now.png`, a mock-up): "Yes, use the proposed set
  (Recommended)".
- 2026-11-22 ~23:55, the No access swatch nearly invisible on the light key's white card: "Dark outline on swatch
  (Recommended)".
- 2026-11-23 ~01:30, cvd separation (the approved set's Salmon pairs sat under the floor of 20 for the worst reader:
  keep/closed 15.4, keep/check 12.3, release/check 7.7): "Own cvd satellite set (Recommended)".
- 2026-11-23 ~01:45, the cvd look (`T940-shots/cvd-proposal-standard.png`, `cvd-proposal-salmon.png`): "Build as
  proposed (Recommended)".

## Done (lane L17, code 7a9213671, PR #57 fixes)
- Six tokens `color.imagery.status.{open,restricted,closed,own,base,no_access}`: light and dark = #6BFF9E #E3A6FF
  #FF3B6D #FFD307 #8CC3FF #E6E4E2; cvd = #FEFF3A #6FABFA #EA90A1 #98AA08 #21AAF5 #DAFFFF. On satellite only,
  `onImageryTokens` (map `style.ts`) swaps each `color.status.*` — and the salmon colours that are those statuses
  (keep, release, closed, check; `color.salmon.open-area` too) — inside the one painter (`contract.ts` `paint`), for
  water layers only (`IMAGERY_WATER_LAYERS`: land closure areas keep their theme's colours). The key's swatches
  (`MapLegend`, `onSatellite`) follow; under 3:1 on their card they get a thin ink outline (`satelliteKeyEdge`; dark
  none). `build-style.mjs`'s ΔE guard compares a satellite token as the status it replaces.
- Not changed: the grey of a section with no status (`color.water.ungauged`) and the Layers sheet / Help / search-row
  swatches.
- Gates: `tools/satellite-status.test.ts`, `tools/map-contrast.test.ts` "the satellite key's swatches",
  `tools/satellite-colours.test.ts` (3:1 on `color.ui.imagery.water` / `.forest`, ONE recorded exception: light and
  dark closed #FF3B6D on forest 2.26:1, `UNDER`; cvd pairs drawn together ≥ 20 apart for the worst reader; the
  never-together pairs open/base and restricted/base recorded). Mutation-proved.
- After-shots (web, staging data, Chilliwack River area, 390 px): `T940-shots/after-web-satellite-dark.png`,
  `after-web-satellite-light.png`, `cvd-after-standard.png`, `cvd-after-salmon.png`.

## Work (left)
Emulator after-shots: they need an APK, which waits for the 0.9.0 batch.

## Acceptance
Satellite looks the same in every theme; every status reads on the photo; the drawn map is unchanged; the owner approves
the pictures.

## Owner

- [ ] 2026-11-22 — Look at the satellite after-shots (`handoff/PLAN/app-integration/T940-shots/after-*`) and approve the one line-colour set (`T940-shots/proposal-vs-now.png`; the ticket recommends one).
