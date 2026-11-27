# T1208 — The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1208 | The zoomed-out map look A: borders that read as borders, quieter rule lines, a softer base, region labels once | IN PROGRESS — 0.10.0 re-cut | 13d06f5cd, 0d625ea71, 663d3a35a | built on feat/T1208-map-look-A (with T1209, T1211): PR, review, the owner's look at the pictures; boundaries stay neutral grey (T47) |

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

## Acceptance

Style tests over the built `style.json` (each fails without its change); pictures today vs A at three zooms, dark.
Only style-spec features MapLibre and Mapbox share (T1154).

Depends: none. Size: M. Owner: no.
