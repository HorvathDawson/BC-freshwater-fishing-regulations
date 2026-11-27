# T1209 — Towns to find your way: three tiers of towns from our own file (province, z6, z8)

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1209 | Towns to find your way: three tiers of towns from our own file (province, z6, z8) | IN PROGRESS — 0.10.0 re-cut | 13d06f5cd, 0d625ea71, 1e0c1d490 | built; owed: the PR with T1208 |

Source: T1200 (owner 2026-11-26: "i want you to choose best judgement for users to have the best experience across bc
and make it easy to find a location by zooming in from far out"; "build A ...").

## Work

1. The three-tier list (22 / 53 / 34 places, a reason each; positions the basemap's own points) as a data file in the map
   package, drawn as a GeoJSON source: tier 1 from z4, tier 2 from z6, tier 3 from z8; the basemap's own places from z10.
2. Lake names placed before town names.

## Acceptance

A test: every tier covers every fishing region (1-8, 7A and 7B) and, at each tier's first zoom, the labels of the towns
shown there can all be placed without two colliding. Pictures: coverage per tier.

Depends: none. Size: S. Owner: no (the list is Claude's call, owner 2026-11-26).
