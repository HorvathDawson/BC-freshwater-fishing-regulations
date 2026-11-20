# T944 — A dead-end piece of Shea Creek carries both the Bulkley's and the Morice's Classified Waters units

Status: IN PROGRESS — fixed on lane/lc-reach-leftovers; left (Claude): main chat review + merge; a reach run + bundle (the next release) carries it.

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

## Done (lane LC, 2026-11-20)
**Cause (general).** 93a481b71 hands a walk a water's dead ends (sinks); the walk itself was right — Shea Creek's code
(`…-005657-271238`, flowing into Gosnell Creek) was collected whole by both the Bulkley's and the Morice's walks, and
773423 is that code's sink. The defect is downstream: `licensing.first_classified_downstream` (RU-13/LI-2) decides a
walked section by walking DOWN to the first classified water, and a sink has no way down, so it read "nothing before
the sea — it stays", with EVERY walker. Shea Creek itself flowed into Gosnell (a row, no designation) and was dropped;
its dead end kept both units.
**Fix.** `tributaries.A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS` + `dead_end_outlets(graph)`: a sink of a code that leaves
its code elsewhere goes on where that code leaves it (main flow first); a code with no way out (a river's mouth) keeps
"nothing below". `first_classified_downstream`'s `options` appends a node's outlets (licensing.py: 5 lines — L7's file,
kept minimal). POLICY_VERSION 48; AGENTS 63 one sentence. Tests: `test_licensing_first_classified.py` (Shea and
774476 shapes, a mouth is no dead end, mutation pin switch-off -> two units).
**Binding diff by place** (reach step in memory, switch off vs on, same code otherwise; switch off reproduces the live
run exactly: rules 6ca9a300dad3d95e, licensing 49146702cc14e299): rules changed 0 (digest unchanged); licensing
72f10f9269d699f8, 16 section rows on 9 sections, each now carrying exactly what the rest of its water carries:
- Shea Creek 773423 (`360242522:0`): Bulkley and Morice removed (Shea -> Gosnell, own row, no designation).
- 774476 (`360245427:0`), 766353/766354 (`360225253:0`/`360225254:0`, a sink and its feeder), unnamed, Morice
  tributaries: Bulkley -> Morice.
- Harmer Creek 317343 (`356366119:0`): both Elk designations removed (its water drops at Grave Creek's row).
- Brownie Creek 317475 (`356366568:205`, feeds a dead end): Elk (2 rows) -> Fording River's elk_river.
- Horsefly 60985 (`355995362:0`, unnamed): horsefly_river removed (its water: Little Horsefly's row).
- Stellako 60818 (`355994971:78`, unnamed, feeds a dead end): both Stellako rows removed (its water: the Endako's row).
Sections with two units: 1 -> 0, so `test_phase3_rulings::test_no_section_carries_two_classified_waters_units` (left
unchanged) goes green on the next bundle; no bundle was built here (disk 0.2-9 GB free all morning; the reach tables are
15.6 GB), the count is from the run's own placements against the bundle's `designation` units.
**The 93a481b71 merges.** Every dead end in the live bundle (a sink of a code that leaves itself; 1,175 stream
sections) against the rest of its water: 1,154 carry exactly its rules and licensing (those merges are right); 0 carry a
RULE their water lacks (no rule-side copy of this bug); 4 carried a designation their water lacks (this bug: Shea,
774476, Harmer, Horsefly — all fixed above); 17 lack something their water has — 15 are 2300 connectors (policy: a
barrier is never collected), Powell Lake's second outlet `360228982:0` (its lake not collected) and a 14 m Taku piece
`360886555:30843` (zone rules, not a walk). Those 17 predate 93a481b71 and are not this bug.
