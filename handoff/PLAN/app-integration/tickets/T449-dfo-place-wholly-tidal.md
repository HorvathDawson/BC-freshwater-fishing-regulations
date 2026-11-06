# T449 — A DFO place that lies wholly in tidal water: say so, bind nothing

Status: OPEN.

Source: owner 2026-11-06 on the two Nanaimo River DFO places (region-1 `1:nanaimo-river:in-subarea-17-14-tidal-portions-of`
and `…~2`: "in Subarea 17-14 (tidal portions of the Nanaimo River) from Cedar Bridge downstream" and "…from the Cedar
Bridge to the white square boundary signs approximately 400 m downstream"). The Nanaimo's tidal limit is already the
Cedar Road bridge (`data/curated/waters/tidal_limits.json`, split `nanaimo_river__cedar_road_bridge`, decided 10-28), so
both places are wholly below it, in tidal water the freshwater map does not carry — the river correctly ends there. They
sit in the DFO queue as unbound (0 extents) only because no verdict says "tidal".

## Work
1. A DFO place state "tidal water" (beside needs_cut / bound / unbound / no_water): proposed automatically when every
   anchor of the place is at or below the water's tidal limit (here: the tidal-limit cut IS Cedar Bridge, and the place
   runs downstream of it), shown in the review app as a proposal the owner accepts (everything added is a proposal).
2. Accepted: the place binds nothing and is not counted as missing; the regulations page for the tidal water still
   names DFO as the authority (unchanged).
3. Sweep every region for the same pattern (other "tidal portions of …" places) and list them as proposals.
Size: S.
