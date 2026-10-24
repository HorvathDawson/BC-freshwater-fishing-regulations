# T169 — One gauge panel for one water: Nicomen Slough reads the same answer along its whole length

Status: OPEN — owner 2026-10-23 delegated: "do what you think is best just make sure it is sane".

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

## Owner rule (2026-10-23)
"do what you think is best just make sure it is sane and doesn't make our predictions all over the place on non-braids
etc. it would be hard when we have unnamed braids of a slough? … from my point of view a predicted section flows into
unpredicted then back into predicted without reason for middle section being unpredicted?"

So: NO GAP WITHOUT A REASON. Along one flow path, a run of sections between two pieces answered from the same source
takes that answer too, unless the run has a reason of its own (its own LIVE gauge, a real confluence bringing other water,
a closure of the gauge's reach). And the change is NARROW: it applies only to pieces that are part of a side-channel
system (a named slough/side-channel item, or an unnamed braid/connector whose ends both sit on one river's side-channel
system); ordinary streams, tributaries and lakes are untouched.

## Work
1. Connectors and unnamed braid pieces (FWA 1410/1250/1300-coded lines, and their polygons) whose both ends sit on one
   river's adopted side-channel lines take that answer (the same adoption `mainstem_of` gives the coded side channels) —
   this alone fixes Nicomen 59620 → 1265716 → 59622.
2. One decision: a side-channel WATER (a stream item whose lines branch from and rejoin a river — Nicomen, Maria, Herrling,
Morey, Gravel …) gets ONE panel for all its sections (lines of every edge type and its polygons): its own gauges + the
river's (Q84 (a)); `mainstem_of` adoption and the inward walk cut both ask that one function. 3. Measure: panels before/after
for every such water (sections whose answer changes, grey → answered), no change outside side-channel waters; whole-value
tests on Nicomen (all 9 sections one panel) + a mutation pin. Ships: deliver (panels). Size: M.

## Acceptance (sanity)
- Nicomen Slough: all 9 sections answer, from one panel; no grey run between answered pieces.
- A census of every "answered → unanswered → answered" run along a flow path, before and after: after, every remaining
  run has a stated reason (listed).
- Sections whose panel changes are ONLY side-channel pieces (named side-channel items, connectors/braids between them);
  0 changes on ordinary streams, tributaries or lakes — a test fails if any non-side-channel section's panel moves.
