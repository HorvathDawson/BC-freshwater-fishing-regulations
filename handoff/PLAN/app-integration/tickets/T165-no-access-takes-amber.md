# T165 — No public access: streams dotted grey (like under-lake streams, a bit stronger); the warning lives in the banner

Status: OPEN

Source: owner 2026-10-23 (after T146): "the no access color is still not aggressive enough. it is too benign. should be
more warning maybe it could take the yellow or orange color then has its own regulation gets a different color?"
Supersedes Q137/Q138 (T146's spruce / dash questions).

## OWNER DECISION 2026-10-23 (supersedes the amber swap below)
"maybe the stream color through no access areas should be dotted grey? like under lakes but a little more visible?" —
chosen over amber lines and dotted amber: streams through no-access land draw as a DOTTED GREY line (the under-lake
stream style, a little stronger); no-access lake fills grey; the no-access LAND fill stays; the regulations page keeps
the T147 "No public access" block in the amber WARNING colour; "own regulations" KEEPS its amber on the map. The dash is a
second channel by the owner's choice (it was Q138; the `stream-nobaseline` layer is the precedent: its own layer because
line-dasharray cannot vary per feature).

## Work (as decided)
1. Map: a `stream-noaccess` dotted layer (grey, a little stronger than `under_lake`), lake fills inside no-access grey;
   status.no_access becomes that grey for lines/fills in every theme; the colour gates updated (no_access grey must stay
   apart from base, water.ungauged and the under-lake grey — say how it reads apart, the dot pattern being the main cue).
2. The page: the T147 block's bar/tint/heading in the warning amber (status.own's hue or a dedicated warning token,
   whichever the gates prefer); the legend says "No public access" with the dotted grey swatch.
3. status.own stays amber; no new colour needed.
4. Shots: Coquitlam / Seymour / Capilano watersheds, light + dark, phone + desktop; the under-lake style beside it.

## Earlier plan (superseded)
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
