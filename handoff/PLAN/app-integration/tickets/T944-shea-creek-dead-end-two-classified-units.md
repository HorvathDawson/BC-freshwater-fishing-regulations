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

## Why these changed (plain summary)
A dead end is a small side piece of a stream that water flows into but not out of. A section takes the Classified
Waters unit of the first classified water its stream flows into. To find that water, the check walks downstream from the
section. From a dead end there was no way downstream. The check read that as "reaches the sea", so the dead end kept
EVERY unit whose tributary walk had collected it. A dead end now flows wherever the rest of its stream flows. That
changes 16 licensing rows on 9 sections, and no fishing rule:
- **Shea Creek loses both its units (Bulkley and Morice).** Shea Creek flows into Gosnell Creek, which has its own row
  and no Classified Waters unit, so the rest of Shea Creek already carried none. Its dead end now matches. It was the one
  section in BC with two units, which the rules forbid.
- **Three unnamed Morice tributaries go from the Bulkley to the Morice.** Their water reaches the Morice first, so the
  Morice's unit is theirs, as it already was for the rest of those streams.
- **Brownie Creek goes from the Elk to the Fording.** Its water reaches the Fording River first.
- **Harmer Creek and unnamed creeks off the Horsefly and Stellako drop units the rest of their water doesn't carry.**
  - Harmer Creek's water stops at Grave Creek's own row.
  - The Horsefly creek's water stops at Little Horsefly's row.
  - The Stellako creek's water stops at the Endako's row.
  - None of those rows carries a unit.
- **No fishing rule changes.** The rules digest is identical with the switch on and off.
- **The earlier dead-end merge work (93a481b71) was right.** All 1,175 dead-end stream sections were checked against
  the rest of their water. 1,154 match exactly, and none carries a rule its water lacks. The only error was 4 extra
  units, all fixed here: Shea, 774476, Harmer and Horsefly. Brownie and the Stellako creek are not dead ends; they are
  creeks that feed one, so they moved with it. The 17 sections that lack something their water has (mostly 2300
  connectors) predate that work and are a separate issue.

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

## Review leftover (lane LF, 2026-11-20, 454f0fe65)
`dead_end_outlets` gave a river's mouth an outlet back upstream whenever its code left itself anywhere (a distributary,
a braid, a lake beside the river). Now `A_MOUTH_NEVER_FLOWS_BACK_UP`: a sink on its code's MAIN FLOW
(`fwa_edge_types.is_main_flow`) is a mouth, and an edge its code leaves by that is upstream of it (its water flows down
into the sink) is no outlet of it. A side-channel dead end (Shea Creek's 1100 piece) keeps its outlets.
- Measured on the live atlas: 8 of 1,353 sinks with outlets lose them, all of them mouths: the Fraser's
  (`356364114:0`, it went into lake:329178071) and Tofino Creek's, the Columbia and the Taku at the border (the Taku is
  the 14 m piece `360886555:30843` above), Poison Cove Creek, an unnamed creek, and one closed loop of two codes. Shea,
  774476, 766354 and the Horsefly piece are unchanged.
- Proof: the reach step in memory via heavy.sh, guard on and off: rules 6ca9a300dad3d95e and licensing
  72f10f9269d699f8 both times (the T944 numbers). No live output changes, so no POLICY_VERSION bump; the switch is pinned
  in `test_policy_version_is_bumped_when_classify_changes` at 49.
- Test: `test_a_mouth_never_flows_back_up` (a mouth with a distributary above it and a side piece; the mutation is the
  switch off).
- Not changed: a side-channel dead end NEAR a mouth (the Fraser delta's Morey Channel, `355994874:0`) still takes the
  code's exit, which is upstream of it: it is a dead end, not a mouth, so this guard leaves it (a possible follow-up).

## Classified-waters pins re-cut (lane W4, 2026-11-20, release r-fec3fab4c5aa)
The T841 gate stopped the release on exactly this ticket's moves: the Elk rows (`r4:elk_river_s_tributaries_see_exceptions`,
`r4:elk_river_upstream_of_elko_dam`) no longer reach Harmer Creek (gnis:15289) and Brownie Creek (gnis:39245); the
Bulkley and Morice rows no longer reach Shea Creek (gnis:22080). Nothing else moved. Pins re-cut from the candidate
bundle (`python -m pipeline.regs.classified_waters --write-pins`), 74 designations (coordinator OK, owner had seen the moves).
