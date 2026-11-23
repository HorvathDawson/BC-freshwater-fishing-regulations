# T711 — Part header text follows the "Source" end name (ruling P55)

Status: DONE — a2727a3b0; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — a2727a3b0; part header follows the Source end name; Kanaka shots approved 11-20; left (Claude): ships with the next data release. (Earlier note: IN REVIEW (code + tests done, a2727a3b0; in the live release r-6258ffded676; the Kanaka Creek shots taken on staging 2026-11-20, `T711-shots/`. owner: OWNER-TODO "Picks taken: T594, T598, T610 closed; T711" ("Source" in both, 11-20); left (Claude): main chat review + merge; ships with the next data release).))

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

## Owner answer 2026-11-20

Owner 2026-11-20 ("Couldn't we just assume most of these are done already?"): the pick (should the part header say "Source" too?) takes the default, "Source" wording in both, which is built (a2727a3b0). Stays IN PROGRESS: the Kanaka Creek shot is Claude's step.

Status line before this answer: IN PROGRESS (code + tests done; left (Claude): the Kanaka shot once the next release carries it).

## The Kanaka Creek shot (lane L2, 2026-11-20)
Staging (release r-6258ffded676, web, 390 px, light): Kanaka Creek's top part reads "Cliff Park Falls → Source" in the
Map view's part name, in the list of its three parts and in the Regulations header — "Source", never "a lake outlet".
`T711-shots/kanaka-top-part-map.png`, `T711-shots/kanaka-all-parts.png`, `T711-shots/kanaka-top-part-regulations.png`.
APPROVED by the owner 2026-11-20 ~06:34 (dialog).
