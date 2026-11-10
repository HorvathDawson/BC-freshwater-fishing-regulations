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
