# T1204 — Hillshade on the base map (terrain shading from elevation tiles we host)

Status: OPEN — after the 0.10.0 re-cut (owner 2026-11-26: "Later, own ticket")

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1204 | Hillshade on the base map (terrain shading from elevation tiles we host) | OPEN — after the 0.10.0 re-cut | — | size and build a BC elevation PMTiles on our host, settle the offline download, then the style layer; pictures to the owner |

Source: T1200 (a closed tester, 2026-11-26, feedback log F52: another map app "easier on the eyes" and clearer about
where the water is; it has terrain shading). The T1200 prototype drew a hillshade trial (option f2) from local copies
of open terrain tiles; the owner saw it and said "Later, own ticket (Recommended)" (2026-11-26).

## Work

1. The source: a BC elevation set as terrarium-encoded raster PMTiles on our own host (never a third-party URL at run
   time). Candidates and their licences (the open Terrain Tiles on AWS, the Province's CDEM / lidar products); written
   down with the attribution line each needs on Sources and licences.
2. The size, measured, not guessed: T1200 estimated about 80 MB to zoom 9 and 250–350 MB to zoom 10. Decide the top
   zoom from pictures at zoom 8, 10 and 12, and what an offline region download carries (tiles and bundle keep one
   vintage; the build runs through `scripts/heavy.sh`).
3. The style layer: `hillshade` under the water and the rule lines, tuned on the softer ground T1200 ships, in light
   and dark; off on satellite. Style-spec only, the same on MapLibre and Mapbox (T1154).
4. Speed: frame time on a mid Android phone with and without it (the speed chat's bench, T1144).

## Acceptance

Pictures at province, region and creek zoom in light and dark to the owner; the download size and the frame-time read
are in the ticket; the attribution is on Sources and licences; offline maps show the shading with no network.

Depends: T1200's base (f1) merged; reads T1154. Size: L. Model: Opus (tiles), Sonnet (style). Owner: the look, and the
download size he accepts.
