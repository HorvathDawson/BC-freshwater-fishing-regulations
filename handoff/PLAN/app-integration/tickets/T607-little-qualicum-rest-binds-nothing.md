# T607 — Little Qualicum "all waters unless indicated below" binds nothing: a whole-water sibling empties the rest

Status: OPEN — owner decision (which of the two fixes), then a reach rerun

Source: the audit of the older DFO marks (T562 item 1, 2026-11-10); coordinator follow-up the same day.

## Today
`1:little-qualicum-river:all-waters-unless-indicated-below` (owner mark, verified 2026-11-03) holds the extent "the
rest of the water" (`op: rest`). The mark landed and the file holds exactly that — and it binds 0 sections.

## Cause (read from the data and the code; no variant was run)
A DFO "rest" is the water minus EVERY other placed place on the same water
(`pipeline/regs/dfo_salmon/entries.py`, the `rests` block: owner 2026-11-02, "the remaining parts without any other
extent applying"). The Little Qualicum has four live places:
- "all waters unless indicated below" — rest
- "from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence" — between two cuts
- "from the hatchery fence to signs located approximately 35 meters downstream" — between two cuts
- "all open portions of the Little Qualicum River" — WHOLE water (owner mark, verified 2026-11-03, 22.8 km)
The fourth is a sibling like the others, and it is the whole river, so the rest is the river minus the whole river:
nothing. Not a missing cut, not a water id mismatch (all four are `1:little-qualicum-river` on gnis:10886), and the
rest computation does what it was told. Stamp River's rest binds because none of its five placed siblings is the whole
water.

## Scope
Two DFO places use `rest`; this is the only one with a whole-water sibling, and the only "landed but binds nothing"
place the audit found among the 83 older marks.

## The decision (owner)
"All open portions of the river" is not a carve-out; it reads as "wherever the river is open". Either:
1. THE RULE: a rest does not subtract a sibling whose extent is the whole water (a whole-water place is not a named
   area). In `entries.py` (the DFO salmon lane, not the review app), with a test; then the reach rerun.
2. THE DATA: "all open portions" takes the same extent as the rest (`rest`), by the owner's own save in the review
   app — no code. Then both bind the river minus the two named stretches.
Either way the two named stretches keep their own rules; which rule applies on the remainder when both general rows
bind it is the chain's existing "narrowest first" order.

## Not done
No mark and no curated file was changed. The fix is outside the review-app lane's paths (1) or the owner's save (2),
and both need the deterministic reach step rerun (not an atlas rebuild).

Depends: owner. Size: S. Owner: yes.
