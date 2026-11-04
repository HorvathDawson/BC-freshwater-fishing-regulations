# T338 — DFO part-closure inside a limits cell reads as the whole river closed

Status: DONE (01919949f, 7a4d3faec) 2026-11-03 (chat 5) — in typed.py; lands in the batch 3 release (typed rows re-read).

"No fishing for chinook from a point 100 m above Fort Babine bridge to Nichyeskwa Creek" (archived 2020/2022 R6
pages) closes the WHOLE river in the reader; only that stretch is closed. Row is flagged (no number shown) but its
status is wrong. Fix in the reader: a "from … to …" closure is a sub-extent note, not a water-wide closure. The one
doubtful T332 wording entry. Size: S.

## Done
`typed.decode` reads a "No fishing for X from … to …" sentence as a closure of a STRETCH (`part_closure`), never the
cell's closure: the row keeps its limit (4 per day) and is flagged with the existing named reason "a closure of part of
the water inside the limits cell: '<sentence>'" (the app shows DFO's words, no number). A plain "No fishing for chinook."
still closes. Test `test_a_stretch_closure_in_a_limits_cell_does_not_close_the_water`; the T320 words pin gains the one
Fort Babine row (was closed, now open with its words). The wording archive entry stays `proposed` (owner rule).
