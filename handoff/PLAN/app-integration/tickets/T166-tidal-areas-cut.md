# T166 — Tidal areas cut: turn the DFO tidal polygon into clean cuts across each stream

Status: OPEN — after the cleanup band.

Source: owner 2026-10-23: "another ticket to add is doing a tidal areas cut. this will be a little bit tricky because our
dfo tidal layer was cut mostly to the right spot but has a lot of areas where it cuts away only some of the stream
(doesnt follow it very well) and we need to fix the blobs to work around that. this is a good after cleanup task".
Related: T98 (the tidal RULES spec, RULINGS K7/K8/C-2, on hold — owner Q42), T78 (tidal gauges).

## The problem
The DFO tidal boundary (data/source/bc_fisheries_data.gpkg layer `tidal_boundary`, one province-wide polygon; read by
pipeline/regs/dfo_salmon/untangle.py and added_streams/build_dataset.py) is mostly in the right place, but where it
crosses a river it often takes only PART of the channel: the boundary wanders across a stream or river polygon instead
of crossing it cleanly, leaving slivers ("blobs") of the same reach on both sides. Clipping sections by it directly would
split a river lengthwise and give one bank tidal rules and the other freshwater rules.

## Work
1. Census: every stream line and river/lake polygon the tidal boundary crosses; classify each crossing — clean (crosses
   the channel once, near-perpendicular), partial (cuts only part of a channel or polygon), wandering (runs along a
   channel), braid (several channels).
2. One rule turns each crossing into a CUT ACROSS THE CHANNEL: the tidal limit on that blue line = one route measure
   (e.g. the upstream-most point where the polygon covers the whole channel width, or where the boundary meets the main
   flow line), the same cut applied to every braid channel and the river polygon at that measure; blobs on the wrong
   side are reassigned to the side of their reach. Never a lengthwise split.
3. Output curated data, not geometry guesses at build time: one tidal cut point per crossing in the splits store (kind
   `tidal`, with the evidence and a "Verify: proposed (tidal)" stamp so each lands in the review tool's S0 queue for the
   owner), plus a `tidal` flag on the sections below it. A map artifact (like the slough page) to check the proposals,
   showing the DFO polygon, the proposed cut and the reassigned blobs.
4. Atlas rebuild with the cuts → binding diff (expect only section splits at the cuts) → tiles in the same vintage. No
   rule changes until T98's spec is un-held: the flag ships, the page wording stays K8 until then.

## Acceptance
Every crossing classified; no river split lengthwise; every proposed cut reviewed (S0); a test pins a sample of crossings
(Fraser at Mission, Squamish, Skeena, Cowichan) to one cut each.

Depends: cleanup band done; T98 for the rules. Size: M–L (atlas rebuild). Owner: confirm the proposed cuts.
