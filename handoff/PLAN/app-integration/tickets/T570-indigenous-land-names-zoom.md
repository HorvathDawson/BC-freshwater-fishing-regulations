# T570 — Indigenous land names show only when zoomed in a lot more

Status: OPEN

Source: owner 2026-11-10: "indigenous land names shouldnt show until zoomed in a lot more."

## Today
- The map shows Indigenous land (reserve) name labels at a zoom where they crowd the water names. Which map was not
  said; taken as the app's map (phone and web), since the review app's map carries no such labels. Not yet read in code:
  the layer, its source and its current minzoom are the first thing to find.

- Owner 2026-11-10: confirmed, it is the app's map (phone and web).

## Work
1. Find the label layer and its current minzoom; raise it so the names appear only at a close zoom (propose the value
   with two shots, before and after, at the zooms either side of it). Web and app the same.
2. The land outline/fill is not part of this unless the owner says so.

## Acceptance
- At the region and river-overview zooms no Indigenous land name shows; zoomed close they do. Owner picks from the shots.

Depends: —. Size: S. Owner: yes (pick the zoom).

## Step 1 findings (2026-11-10, shots ready, owner picks)
- Layer: `indigenous_land__label` (symbol), source `atlas`, source-layer `indigenous_land`; authored in `app/packages/map/style/layers.source.json` (layer `indigenous_land`, key `label.minzoom`), built into `style.json`; one style for web and phone. Its text size ramps 10 (z9) to 13 (z15). The land fill is always on; the edge starts at z11.
- Current minzoom: 9 (so at the owner's z9.6 near Chilliwack: Soowahlie 14, Tzeachten 13, Skowkale 10, Grass 15, Cheam 1 and others all label).
- Proposed: 12 (labels from the zoom where a reserve is a screen-sized shape; the edge is z11).
- The one-line change, NOT committed: in `layers.source.json`, `indigenous_land` -> `label` -> `"minzoom": 9` becomes `"minzoom": 12`, then `pnpm style:build` (rewrites style.json, style.meta.json, style.sha256).
- Shots in `../T570-shots/` (web; "after" is the layer's range set live to 12, the same as the built change): `z9.6-minzoom9-before` / `z9.6-minzoom12-after` (the owner's spot, 49.02 -121.85); `z11.5` and `z12.5` each at minzoom 9 and 12, centred on Soowahlie 14 (49.084 -121.957), as the owner's spot has no reserve in view above z11. After at 11.5: no name; at 12.5: names. Pick 11, 12 or 13 and it is a one-number change.
