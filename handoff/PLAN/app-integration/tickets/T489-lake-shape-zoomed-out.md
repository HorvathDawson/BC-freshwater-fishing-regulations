# T489 — Lakes lose too much shape when zoomed out

Status: IN REVIEW — side build + shots done (8494959a2); left: owner look at the shots, ships with the next tiles rebuild. Owner 2026-11-07 (0.6.9 on the phone).

Owner: "lake polygons lose almost too much shape when zoomed out."

## Do
- Find the simplification per zoom for the lake layer (tile build: tippecanoe/simplify settings in the tiles stage;
  T461 did the same exercise for the tidal polygon) and measure vertex counts + tile sizes per zoom.
- Proposal: gentler simplification at low zooms (e.g. z6–z10) within the tile-size budget; side-by-side shots of a few
  well-known lakes (Harrison, Stave, Pitt, Okanagan, a small lake) before/after. Ships with a tiles rebuild (next
  release); never rebuild the live tiles to test (AGENTS 17) — build beside.
- Fast-fail (AGENTS 20a): a check on minimum shape kept (e.g. area/perimeter ratio vs source) per zoom, if cheap.

## Measured (2026-11-07, live tiles 269777be.55f8a4cfc1b5)
- Lakes were in the per-zoom WATER pass at `WATER_SIMPLIFICATION` = 5 units of the 2,048 grid (~1.25 px), tuned in
  T419 for SMOOTHED stream lines. Points kept per lake (z6 / z7 / z8 / z9 / z10):
  Harrison 28/49/99/228/469 · Okanagan 35/67/187/339/638 · Stave 16/25/78/149/260 · Pitt 10/21/41/87/168 ·
  Chilliwack –/8/9/16/27 · Hicks –/–/5/9/16 (a box at z8) · Foley –/–/–/–/5 (a box at z10).
- Lake-only passes at other tolerances (18,071 lakes drawn by z10), lake bytes z6/z8/z10: S5 10.7/112/1,112 KB ·
  S3 13.4/138/1,310 · **S2 16.7/165/1,505** · S1.5 19.6/189/1,665 · S1 24.6/229/1,923. S2 roughly doubles the points
  (Harrison z6 63, Pitt 22, Hicks z8 11, Foley z10 11); S1 doubles again for +30-70% bytes. S2 chosen.

## Done (IN REVIEW)
- `tippe.py`: up to z10 (`STILL_WATER_GENTLE_MAX_Z`) lake + wetland polygons get their own per-zoom pass at
  `STILL_WATER_SIMPLIFICATION` = 2; z11-12 unchanged (water pass, 5). Cost: a shore vertex at a stream mouth is no
  longer pinned below z11 (moves ≤ 0.5 px). Test: `test_tile_lake_link.py`.
- Fast-fail: none — a shape check needs a built archive; nothing cheap fits `scripts/fast-fail.sh`.

## Side build (2026-11-07, with T493; live tiles untouched)
- Points kept after (z6-z10): Harrison 63/125/255/549/1084 · Okanagan 81/178/441/745/1334 · Stave 36/71/174/374/564 ·
  Pitt 22/56/99/208/359 · Chilliwack –/13/18/37/67 · Hicks –/–/11/20/45 · Foley z10 11.
- Lake layer raw bytes z6 +72% · z8 +60% · z10 +46%; whole archive +1.6 MB (+0.8%) incl. T493 (per-zoom table in T493).
- Shots (before | after): scratchpad `t493/shots/pairs/` — harrison-lake, stave-pitt, okanagan-lake, hicks-lake z6-z10.
