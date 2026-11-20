# T940 — Satellite: one colour set for all water lines, the same in light and dark

Status: OPEN — built on lane/le-t940; waits until after this release (owner 2026-11-20 10:05). Pictures to the owner after the release.

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

## Built (2026-11-20, lane/le-t940; not merged)
Over SATELLITE the water reads one colour set, the same in light, dark and cvd: `app/packages/map/style/imagery.json`
(values for themeable tokens; an "@" may name one in the set or one already the same in every theme). `build-style.mjs`
checks it (known themeable tokens, types, "@" resolved) and writes it into `STYLE_META.imagery`; its two colour guards
(ΔE ≥ 12, no shared value) also check each theme as it looks over satellite. No layer added. The adapter's `paint()`
swaps the set in for water-group layers when the ground is satellite (`on-imagery.ts` `onImageryLayer` / `onImageryTokens`),
so the stream marks drawn with it (closed's dash, gear edges, keep casing, issue stripe, selection, water copy, no-access
dots) follow. The map key follows the ground (`groundTheme(theme, ground)`, MapLegend `keyTheme`, RunsKey `ground`).
Land (reserves, closures, parks), labels and the drawn map keep the theme's values.

Named exceptions (still per theme, by design): the Conditions/Runs basin field is opaque, mixed on the theme's paper;
the still-wetland hatch is a per-theme image; the flow-arrow halo is the theme's paper; cvd keeps its dotted route
pattern (`under_lake` dash).

Status colours over satellite (searched: every line status ≥ 3:1 on dark forest, shadow and water; light's closed was
1.2:1 there): open #A7FBB2, restricted #A8749C, closed #EB4658, own #FFF304, base #417CFF, no access #BAB7B6;
selection #FF77B8; casings/cues the T923 pale peach `color.imagery.casing`; issue stripe #3B1E5E. Worst of normal and
protan/deutan/tritan as 0.6-coverage lines over the satellite grounds (`status-colours-distinct.test.ts`, one `SAT_FLOOR`):
closed/own 22.2 (old light 17.8, dark 19.0, cvd 17.8); water pairs 27.3 (35 / 19 / 34); permit 13.4 (15 / 11 / 16);
no access 12.6 (14 / 8 / 14); no access vs permit 10.0 (11.5 / 8 / 9). Better than dark everywhere; a little under light
and cvd on three pairs, traded for being seen on the photograph.

Tests: `native-style.test.ts` (one look in all three themes on satellite for every look the app draws, focused or not,
exceptions named; the set is what a Trout status line draws; emptying the set leaves the drawn map byte-identical),
`status-colours-distinct.test.ts` (satellite rows measure the set), `build-style-errors.test.ts` (two refusals),
`ui-native/src/T940.legend.test.tsx` (the key's swatches follow the ground); `imagery-casing.test.ts` (T594) now measures the opened stream against the drawn map under the set. `pnpm -C app check` green (9016 tests).

Pictures (`T940-shots/`): `sbs-satellite-<place>.png` = light before | light after | dark before | dark after, at
Fraser/Chilliwack z11, Chilliwack z14, Skeena/Terrace z11 and z14; `sbs-drawn-<place>.png` the drawn map (unchanged: the
only pixel differences are the search box's rotating placeholder). Raw shots in `before/` and `after/`.

Look questions for the owner (with the pictures): on the LIGHT card the key's yellow own-rules swatch (#FFF304) is faint,
and so are the pale-peach Fly only / Bait ban boxes and the grey no-access dots (they are the colours on the photo, shown
on a white card). Keep, or give the light card's key a darker rim on those swatches?

