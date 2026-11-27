# T1208 — The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1208 | The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once | IN PROGRESS — 0.10.0 re-cut | 13d06f5cd, 0d625ea71, 663d3a35a, 1e0c1d490 | review fixes in (dash-dot border, basemap ground and water held to T47); left: the opening frame (Haida Gwaii) waits on a decision: z4 cannot hold B.C. on a phone, the app opens at z10; PR |

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

## The practice behind look A (research, T1200; kept here: the docs line budget is full)

Sources read 2026-11-26: Ordnance Survey, [Text on maps](https://docs.os.uk/more-than-maps/geographic-data-visualisation/guide-to-cartography/text-on-maps);
[Typography (cartography)](https://en.wikipedia.org/wiki/Typography_(cartography)) (Imhof through it); the radical law of
selection in [arXiv 1102.1561](https://arxiv.org/pdf/1102.1561); [OSM boundary relations](https://wiki.openstreetmap.org/wiki/Relation%3Aboundary);
the ArcGIS [Legend](https://developers.arcgis.com/javascript/latest/api-reference/esri-widgets-Legend.html); Material 2
[chips](https://m2.material.io/components/chips); Apple's [pickers](https://developer.apple.com/design/human-interface-guidelines/pickers);
the [Mapbox](https://docs.mapbox.com/style-spec/reference/layers/) and [MapLibre](https://maplibre.org/maplibre-style-spec/layers/) style specs.

| practice | the sources | built as |
|---|---|---|
| Water names blue, italic, biggest first; rivers named along a smoothed course | OS; Imhof | T1210: a label layer in our tiles |
| Towns by rank, then spread; a name above-right, else the next corner | OS; radical law | T1209: three tiers, `text-variable-anchor` |
| Admin boundaries dashed, quieter than roads, a band inside | OSM; OS | T1208: `dash.region`, `line-offset` band |
| A phone's legend collapses to one control; a sheet opens it | ArcGIS legend, Material chips | T1211: the floating Map key |
| A compact day control that opens a calendar | Apple pickers | not built (open with the owner) |

Both engines draw every built layer the same way (T1154): `line-dasharray` (constant), `line-offset`, `concat`,
`text-variable-anchor` with `text-radial-offset` (Mapbox GL JS 0.54+), `symbol-sort-key` (0.53+), GeoJSON sources with
inline data, `zoom` in filters (both read it at whole zooms). One difference met: a zoom `step` or `interpolate` over a
feature-state colour threw in MapLibre GL JS 5.24 (`interpolationFactor is not a function`); look A does not use one.

What the data allowed (measured): the basemap carries 5 BC towns in its z4 tiles, 11 at z5, 30 at z6, with OSM
placeholder populations below that, so the tiers are our own list (`app/packages/map/src/towns.json`, a reason each).
The atlas tiles carry only the 10 largest lakes at z4 and river sections too short to hold a name, so lake and river
names zoomed out need the T1210 tile layer.

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
