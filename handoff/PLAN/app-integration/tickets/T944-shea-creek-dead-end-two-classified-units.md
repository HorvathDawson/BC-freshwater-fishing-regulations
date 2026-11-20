# T944 — A dead-end piece of Shea Creek carries both the Bulkley's and the Morice's Classified Waters units

Status: OPEN.

Source: lane/t936-t931 (T931 group B, slow tier), 2026-11-20.

## What
Section 773423 (`360242522:0`) is a dead-end side piece of Shea Creek (`gnis:22080`, Burnie-Shea Park): an inflow from
Shea Creek's own main stem, no outflow. It carries BOTH the `bulkley_river` and the `morice_river` designations (through
the tributary walk), while Shea Creek's main-stem sections carry none. One section with two units is what RU-13 forbids.

## Evidence
`pipeline/tests/test_phase3_rulings.py::test_no_section_carries_two_classified_waters_units` (slow) fails on
r-6258ffded676: `(1, [(773423, {'bulkley_river', 'morice_river'})])`. T931 refused to re-pin it (left red).
Likely cause: 93a481b71 (2026-11-15, `tributaries.A_DEAD_END_GOES_WITH_ITS_WATER`, AGENTS 63) attaching the sink to
both walks although neither collected Shea Creek. The same change merged 76 side parts into their main parts (measured
for T931 B's part-mark and multi-part counts), so check it against "a dead end of a water the walk collected WHOLLY".

## Work
Reach / licensing fix (`atlas/graph/tributaries.py`, `reach.licensing`), then a binding diff; the pin stays as is.
Depends: none. Size: M. Data change: needs a reach run + bundle.
