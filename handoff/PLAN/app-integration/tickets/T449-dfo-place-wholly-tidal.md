# T449 — A DFO place that lies wholly in tidal water: say so, bind nothing

Status: IN REVIEW (e83f7811e; owner accepts/rejects the two Nanaimo proposals in the DFO review app).

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

## Done (e83f7811e)
- Rule (`pipeline.regs.review.dfo.tidal_proposal`, proposes only): no extent yet; the words put the place IN tidal water
  (`TIDAL_PLACE_WORDS`: "tidal portions/waters of", never "to the tidal boundary" / "tidal water fishing boundary signs"
  / "flowing into tidal water"); its water has a row in `waters/tidal_limits.json`; for a `split`/`cut` limit the op runs
  downstream (downstream_of / between) and a distinctive word of the cut's label is in the notice ("cedar" of "Cedar
  Road bridge"); a `mouth`/`whole` limit needs no anchor.
- Decision is a DFO mark: call "tidal water" (T R P y, verified) accepts; call "not tidal" (deferred + note) rejects.
  Write-through (`proposals._tidal_water_proposal`, dfo_loc edit) sets the place's `tidal_water: true` (absent when
  false; `EntryLocation` refuses it beside an extent).
- Accepted: review state `tidal` (ranked last, done), no suggestion (`suggest.needs_suggestion`), reach row outcome
  `tidal_water`, and the bundle (`deliver.salmon`) neither paints its water "parts not mapped" nor lists it as "not yet
  mapped" on the card (its rules apply below the limit, off the freshwater map). Authority unchanged.
- Sweep (all regions, live data): exactly the two Nanaimo places
  (`1:nanaimo-river:in-subarea-17-14-tidal-portions-of`, `…~2`). The other "tidal" wordings (Cowichan, Somass ×4,
  Chapman, Seymour, Dean) are bounded BY the limit — freshwater above it — and are not proposed.
- Tests: `test_dfo_tidal_water.py`, screen test (proposed → accepted → rejected), `test_salmon_file` tidal case.

## Owner 2026-11-06
Accepted tidal places are dropped from the freshwater card's "location not yet mapped" list (as built): "if it is
tidal we do not do it so it is dropped." Left for the owner: accept the two Nanaimo proposals in the review app (DFO
mode, search "nanaimo"); the bundle changes with the next release build.

Owner 2026-11-06 (chat): "Yes, accept both" — both marks saved through the review API (reviewer Dawson); the
write-through set `tidal_water: true` (0a3ba775b, 66aa4bebc). Reaches the app with the next release build.
