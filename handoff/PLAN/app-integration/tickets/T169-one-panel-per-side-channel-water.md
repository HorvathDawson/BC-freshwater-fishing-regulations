# T169 — One gauge panel for one water: Nicomen Slough reads the same answer along its whole length

Status: OPEN — default built from owner Q84 option (a) unless the owner picks (b).

Source: owner 2026-10-23: "why did one section of nicomen slough not get conditions even though both ends of the lake
did … why does the middle section have different gauges then the other parts?"

## Why it happens (measured on the live bundle)
Nicomen Slough (gnis:21105) has three panels along its length: 4739 (own 08MH085/086 + 11 Fraser gauges: 59618,
1453542), 280 (Fraser only: 59619, 1453408, 59621, 1265712 — side-channel lines ADOPTED by `mainstem_of`, which gives an
adopted line the river's whole answer) and 3654 (own gauges only: 59620, lake 1265716, 59622). 59620 is a 57 m CONNECTOR
(FWA edge type 1410), not a side-channel line, so `mainstem_of` never adopts it; since T101 the walk does not climb from
a side channel into its river, so it reaches only Nicomen's own gauges — level-only 1960–64, no live feed → GREY. The lake
1265716 and 59622 take that line's answer. So the alternation is FWA line coding meeting two rules that disagree (T101
follow-up "inward vs adoption"; Q84).

## Work
One decision: a side-channel WATER (a stream item whose lines branch from and rejoin a river — Nicomen, Maria, Herrling,
Morey, Gravel …) gets ONE panel for all its sections (lines of every edge type and its polygons): its own gauges + the
river's (Q84 (a)); `mainstem_of` adoption and the inward walk cut both ask that one function. Measure: panels before/after
for every such water (sections whose answer changes, grey → answered), no change outside side-channel waters; whole-value
tests on Nicomen (all 9 sections one panel) + a mutation pin. Ships: deliver (panels). Size: M.
