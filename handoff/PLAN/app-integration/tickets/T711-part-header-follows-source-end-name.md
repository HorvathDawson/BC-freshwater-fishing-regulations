# T711 — Part header text follows the "Source" end name (ruling P55)

Status: IN PROGRESS (code + tests done; left (Claude): the Kanaka shot once the next release carries it).

Source: owner 2026-11-13, boundary-name review: "Source" stands for Kanaka, Mat, Coquitlam and the other tops among the 62 changed by P55. Bug: the part header text still says "up to a lake outlet" while the name says "→ Source".
## Work
1. Find where the part header sentence is built and why it ignores the end name (header derives the end kind separately from the name).
2. Make the header read from the same end name/kind as the part name; check all 62 P55 tops, not just the three.
## Acceptance
For every P55 top, header and name agree (test over all 62, mutation-pinned); Kanaka Creek top part shown in a shot.
Depends: none. Size: S. Owner: no. Default: "Source" wording in both.

## Done (2026-11-13, pipeline lane)

Cause: the part NAME took P55's own-end rule in `builders._names` (an unnamed `lake_outlet` at the water's top km →
`source`), while the HEADER (`part.runs_label`, the export's part `runs`) read the run's raw `from` token and said
"a lake outlet". Fix: one function, `part.own_end` (with `part.water_ends_km`, moved from builders), read by both
`_names` and `runs_label` (and `part_labels`, the picker). The header says a source as every water's header does:
"… up to the top of the river".

Check on the live export (r-adc4e248a163), recomputed with the new code: all 62 P55 tops change, nothing else
(`test_part_names_unnamed_lake.py::test_province_wide_every_source_top_has_a_source_header`, mutation: without the
water's ends the same 62 say "a lake outlet"). Kanaka Creek: "From the Cliff Park Falls up to a lake outlet" →
"From the Cliff Park Falls up to the top of the river"; Buntzen Creek: "From Coquitlam Watershed boundary up to the top
of the river". Rides the next deliver (no atlas or reach change).
