# T165 — No public access takes the warning amber; "has its own regulations" gets a new colour

Status: OPEN

Source: owner 2026-10-23 (after T146): "the no access color is still not aggressive enough. it is too benign. should be
more warning maybe it could take the yellow or orange color then has its own regulation gets a different color?"
Supersedes Q137/Q138 (T146's spruce / dash questions).

## Work
1. status.no_access := the amber status.own uses today (Okabe & Ito orange in light, yellow in dark; the cvd theme's
   warning hue) — it must read as a warning at a glance.
2. status.own gets a NEW colour, apart from closed (crimson), the new no_access (amber), base (blue), the selection
   outline (color.highlight, magenta) and the forest/park ground, in light, dark and cvd, measured with the repo's gates
   (status-colours-distinct, water-status-cvd, apartFromSelection; ΔE floors as T146 set them). Search candidates
   (e.g. violet, teal, deep green) and show 3 options as shots (Fraser + Coquitlam, light and dark, phone) — build the
   best by the gates; the owner can swap (Q150).
3. Everything that names these colours follows: tokens.json comments, the legend words (the legend says "no public
   access" in amber and "own regulations" in the new colour), the T147 no-access block (bar and tint), the T54 partial /
   permit notes, depth-contour and selection gates (T158), the colouring engine (one pathway), every theme.
4. Mutation pins: the old spruce/cyan no_access fails "no access reads as a warning"; own in amber fails "own is not the
   warning colour".
Ships: app (style rebuild). Size: S–M.
