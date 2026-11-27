# T1208 — The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1208 | The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once | IN PROGRESS — 0.10.0 re-cut | 13d06f5cd, 0d625ea71, 663d3a35a, 1e0c1d490, 8ef9734ee, 1ba92dd47, 99aaadffb | built, with the owner's three picks (dash-dot border, cyan water, the whole province on wide screens: it fits, pinned by tools/wide-province-fit.test.ts); owed: re-review, the full pre-PR check, the PR |

Source: T1200 (owner 2026-11-26: "build A ..."; proposal A, round 3 sheet 2). Prototype: branch
`feat/T1200-map-readability-options` (dev-only `?t1200=`, not shipped).

## Work

1. Region boundaries: thinner dashes in a muted violet-grey and a soft band just inside each region (`line-offset`);
   unit (MU) lines in the same dress, lighter.
2. "Region n" named once per region at province zoom from fixed label points (a small data file), the tiles' per-piece
   label from z6 in the same dress. The province view keeps the region names inside their regions (a label point is
   inside its region's main piece, never on a tile edge).
3. Rule lines zoomed out: small streams wait (order 7+ below z5, 6+ to z6, 5+ at z7), thinner below z8.
4. The base: a softer ground and one water blue (the basemap's sea and rivers a light tint of the lake blue).
5. Themes: light and dark get the treatment; satellite unchanged (T940 holds its line colours; the band and dashes are
   drawn the same).
6. The widest view (owner 2026-11-26): phones as they are (z4 cannot hold B.C. there); on a wide screen, zoomed all the
   way out, the whole province with Haida Gwaii fits (1280 x 800 at z4.46, 1440 x 900 at z4.63): it already did, now
   pinned (tools/wide-province-fit.test.ts).

## The practice behind look A (research, T1200)

Ordnance Survey's text-on-maps guide and Imhof (water names blue, italic, biggest first), the radical law of selection
(towns by rank), OSM boundary practice (dashed, quieter than roads, a band inside), the ArcGIS legend and Material chips
(a phone's legend collapses to one control). Both engines draw every built layer the same way (T1154): constant
`line-dasharray`, `line-offset`, `concat`, `text-variable-anchor` + `text-radial-offset` (Mapbox 0.54+),
`symbol-sort-key` (0.53+), inline GeoJSON, `zoom` in filters; MapLibre 5.24 throws on a zoom curve over a feature-state
colour, which look A does not use. The basemap names only 5 B.C. towns at z4 (hence `towns.json`).

## The basemap colours the T47 floors bind (review, for the owner's cyan-or-bluer-water question)

tools/status-colours-distinct.test.ts reads `color.ui.basemap.ground` / `.water` from the themes. The pairs that bind,
worst vision, with what the look's first colours gave:

| theme | pair, as drawn | now | floor | look's first colours |
|---|---|---|---|---|
| light | closed / own, thin line on the ground #E5E2DB | 21.31 | 21 | 20.6 on #EFEDE8 |
| light | closed / own, lake fill on the water #82CDD6 | 25.2 | 25 | 23.5 on #A9C6EE |
| light | closed / base, line and lake fill on the water | 35.5 | 35 | 32.6 on #A9C6EE |
| dark | no access / closed, thin line on the ground #121618 | 8.51 | 8.5 | 8.41 on #141414 |
| dark | closed / own, lake fill on the water #31353F | 22.4 | 22 | 21.9 on #333333 |

A search of every water from L60 to 92 (all hues 150-285) found only the cyan family passing with this palette: a bluer
water needs the status colours moved (T47's palette), not the basemap.

## Acceptance

Style tests over the built `style.json` (each fails without its change); pictures today vs A at three zooms, dark.
Only style-spec features MapLibre and Mapbox share (T1154).

Depends: none. Size: M. Owner: no.

## Owner

- [x] 2026-11-26 — look A built (the built sheet): merge it? → "Merge, and fix the opening frame so Haida Gwaii is fully in view (Recommended)" (2026-11-26)
  So: approved; the same branch also frames the opening whole-province view with Haida Gwaii fully in view.
- [x] 2026-11-26 — the region border (three variants beside a no-access stream)? → "1 · Dash-dot, heavier, light band (Recommended)" (2026-11-26) = as built.
- [x] 2026-11-26 — the basemap water the colour floors allow (cyan) or a bluer one with the status colours moved? → "Accept cyan water (Recommended)" (2026-11-26) = as built.
- [x] 2026-11-26 — Haida Gwaii: z4 cannot hold B.C. on a phone; what then? → "Leave phones; fit all on wide screens (Recommended)" (2026-11-26)
  So: phones as they are; on a wide screen, zoomed all the way out, the whole province with Haida Gwaii fits.
