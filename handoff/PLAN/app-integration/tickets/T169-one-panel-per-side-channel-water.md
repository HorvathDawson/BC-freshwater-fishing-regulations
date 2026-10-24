# T169 — One gauge panel for one water: Nicomen Slough reads the same answer along its whole length

Status: DONE (lane-T169, ae99328e) — not yet delivered. Owner 2026-10-23 delegated: "do what you think is best just make
sure it is sane".

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

## As built — 2026-10-23 (ae99328e, pipeline/atlas/gauges/build_panels.py)
ONE MAP, `side_channel_waters(graph, area_of, water_of, kind_of, adopted, continues)` -> `{side-channel water: river
section}`: a STREAM water at least one of whose lines `mainstem_of` adopts onto a section of ANOTHER water (not one
`water_continue` system, not a lake item). Its river section is the LARGEST of the sections its lines were adopted onto
(where the channel gives its water back). Real graph: 30 waters, 83 sections (Nicomen, Maria, Herrling, Seabird N,
Jeperson ×2, Cannery, Annacis, Morey, Strawberry, Blind Slough/Stave, Bell/Nelson, Widgeon/Pitt, Old Kootenay, East Branch
Kootenay, Hotsprings, Baldy, Back, McArthur Island, Rushton, Squamish Powerhouse, Alec Chief/Toozaza, Allard/Dease …).
Computed ONCE in `build`, before the walk, and asked by:
- `water_panel` — every section of the water (lines of every edge type, its polygons) takes ONE panel: the members that
  LED on any of its sections in the walk (best rank along the water; the member as its own section has it), at most
  `OWN_LEADS_PER_QUANTITY` per quantity, then the river section's members with their leads ranked after; target area =
  the river section's. River silent: its own gauges alone; neither: no panel. Runs last of the water passes, so it
  overrides the walk, the per-line adoption and both polygon passes. It replaces the per-line `own_gauges` (deleted: once
  side-channel waters leave the per-line loop, 0 real sections reach it).
- `branch_points(..., channels=)` — which water at a branch point is the side channel is the map's answer (size only where
  the map names neither). No panel moves today (every pair already had the channel as the smaller water); it removes the
  second decision the T101 follow-up "inward vs adoption" named.
- `braid_connectors` (part 1) — a run of UNOWNED, unadopted stream lines on one code whose every same-code neighbour
  answers for ONE river (an adopted line, a side-channel water's section, the river's own section), with a side-channel
  piece among them and ends on both sides, every piece smaller than the river there, takes its side-channel neighbour's
  answer (downstream first). 0 sections on today's graph: Nicomen's 1410 connectors are OWNED by Nicomen (the water pass
  covers them), and every unowned candidate run fails for a reason — 2,252 are the unnamed river ITSELF (the run holds
  the adopted braid's parent; size guard), 2,246 end at a lake or unowned non-run piece, 1,004 are one-sided. Kept as the
  guard for an unnamed braid's connector (fixture).

Tests: `pipeline/tests/test_side_channel_water.py` (21, + 3 needs_bundle): whole value on a 9-section Nicomen (one panel);
the NARROW PIN (every section that moves against pre-T169 is a side-channel-water section; river and creek identical);
braid connector whole value + nothing else moves; two-rivers / lake / size refusals; an own LIVE gauge leading the whole
slough and never the river; silent river; not-a-side-channel (the river's own pieces, a continued system, a lake item,
a Nelson-style tributary code); branch points asking the map. Mutation pins: no water pass, own gauges dropped, smallest
river section, no connectors, any-end connectors, no size guard, branch_points deaf to the map, one call of the map.
`test_side_channel_gauges.py`: the miniature's s_head now equals s_foot (one panel); the own-gauge pin retargeted at
`water_panel`. needs_bundle `test_a_side_channel_water_reads_one_panel_in_the_bundle` (Nicomen 9, Herrling 7, Maria 7)
FAILS ("predates") on the live bundle until deliver — verified.

## Measured — 2026-10-23 (real graph, read-only, scripts/heavy.sh)
Panels rebuilt from the live atlas (`data/generated/atlas/full`) with the live bundle's gauge inputs (match, gauge_stats,
HYDAT regulation, water_continue, registry); the BASE code reproduces the live `section_panel` EXACTLY (246,705 of 246,705
sections, members and areas). Panels do not read the reach run, so the batch-7 run / card-gate block does not touch this.
No full scratch deliver was run — the reconstruction is the panel step itself; deliver is the owner's ship step.
- Sections whose panel changes: 24, ALL in side-channel waters, 0 outside: Nicomen 7, Herrling Island 6 (its own 08MF047
  now leads all 7), Maria 4, Jeperson 4, Bell Slough 2 (area only, 1.4 -> 2.6 km² = Nelson's; members 08MF064
  unchanged), Seabird Island N 1. Sections with a panel 246,705 -> 246,705.
- Flow answered 246,060 -> 246,063 (+3 gained / 0 changed / 0 lost: Nicomen 59620, 59622, 1265716).
  Level answered 127,270 -> 127,270 (0 gained / 22 member sets changed / 0 lost).
- NICOMEN, one by one. "One panel" = own 08MH086*1, 08MH085*2, then the Fraser 08MH044*3, 08MH024*4, 08MH071*5,
  08MH126*6, 08MF035*7, 08MF005*8, + 08KA004, 08KA005, 08KB001, 08ME019, 08MF040; area 63,971.5 km².
  - 59618 stem (1100/1350, where 085/086 stand) — already the one panel; unchanged.
  - 1453542 polygon — took 59618's; unchanged.
  - 59619 (1100), 59621 (1100/1350), polygons 1453408, 1265712 — the Fraser by adoption at another Fraser section
    (08MH071*1, 08MF073*2, 08MF038*3 …, 63,646 km², no own gauge) -> the one panel.
  - 59620 (1410, 57 m), 59622 (1410/1450), polygon 1265716 — own 08MH085/086 alone (level 1960-64; GREY in Flow and,
    with no feed, in Level), 10.9 / 1.4 km² -> the one panel (Flow now answers from the Fraser).
- CENSUS (scratch `census.py`: every answered -> unanswered -> answered run along flow edges, per quantity):
  | | Flow runs | Level runs |
  |---|---|---|
  | live | 2,526 | 2,020 |
  | new | 2,523 | 2,020 |
  Side-channel-water runs 3 -> 0 (Nicomen 59622; 1265716; 1265716 -> 59620). Every remaining run (new), with its reason:
  - LAKE on the path (a true lake or unowned polygon; storage ends the relationship) — Flow 2,112, Level 1,657.
  - RIVER'S OWN PIECE BESIDE ITS ADOPTED SIDE CHANNELS — Flow 321, Level 269: a main-river section (Koksilah 54806, Gold
    58080, Adam 58263, Flathead, Kitsumkalum, Squamish 1234009, Fraser 315470 …) unanswered by its own walk (typically
    just above an unowned double-line polygon the walk counts as a lake), while its own 1100/1350 side pieces answer
    because `mainstem_of` adopts them onto the largest same-code section within 4 hops, past that polygon. Ordinary
    river sections: outside T169's narrow scope (Q169b).
  - ADOPTED ONTO A SILENT RIVER SECTION — Flow 84, Level 89: a river's own side piece (Elk 674, Bonanza 398, Lillooet
    60221, Baker Creek, Nahatlatch, Cranberry …) adopted onto the largest nearby section, which has no answer, between
    answered pieces of the same river (Q169a).
  - AREA RATIO (out of MAX_AREA_RATIO / REGULATED_MAX_RATIO of every gauge at both ends) — 4 each: Quinsam Diversion,
    Otter Creek, Kettle 448188 (no modelled area), Yaza Creek.
  - GRAVEL SLOUGH 235795, 235796 (Flow; in Level the run 235797 … 1359291): its 1425 connector lines between its own
    polygons. Gravel is coded like a tributary (100-072260, as Nelson is) and its 1100/1350 lines adopt onto Gravel
    itself, so it is not a side-channel water; its polygons answer by the polygon fallback (`stream_polygons`, Elk Creek
    08MF018/048 from the line below) but a LINE between polygons has no such pass and no walk reaches it (Q169c).
  - BELGO CREEK 448591 (Level): the ends answer from different gauges — its own 08NM232 above, Mission Creek's below (a
    confluence; not the owner's same-source case).

## Adversarial review — 2026-10-23
- A tributary entering a slough (Norrish/Siddall into Nicomen; the fixture's creek into the foot): another water on a
  nested code, never in the water pass or a connector run — 0 creek sections move, real and fixture.
- A slough with its own LIVE gauge: Cannery Channel 08MH028 (level 2016-24, in the feed) — unchanged (both its sections
  already led with it). In the fixture a live own gauge Q leads every section of its slough and sits on no river section:
  Q44 holds (`across_branch` untouched; own gauges never enter the river's panel).
- An unnamed braid of a big river (Fraser at Agassiz): the river's unowned 1100/1350 pieces keep the per-line adoption
  (unchanged); an unowned connector between them is caught by `braid_connectors` (fixture). On the real graph none
  qualifies, and the 2,252 runs the size guard refuses are the unnamed river itself — no main stem moves.
- Vedder/Chilliwack: the Vedder Canal is the Vedder's own polygon and the Chilliwack a continuation — neither is a
  side-channel water; 0 sections move.
- Two side channels of one river meeting (Strawberry/Nicomen, Jeperson blk/gnis): the map names neither the other's
  channel, so branch_points keeps size; both take the Fraser.
- Bell Slough (river section = Nelson Slough's: Flow none, Level 08MF064) keeps exactly that; T101's Bell/Nelson result is
  intact, and Nelson is not a side-channel water.
- Lead rank: one panel means a side channel's own gauge leads the whole water (T101 follow-up option (b) came with option
  (a)). Of the own gauges involved only Cannery's 08MH028 is live; Herrling's 08MF047 and Nicomen's 085/086 are 1960s.
- Found while measuring, not changed (scope): the 321/269 and 84/89 river-piece runs above; Gravel's connectors.

## Owner questions (defaults applied)
- Q169a — A river's OWN side piece adopted onto a silent section (Elk, Bonanza, Lillooet … 84/89 runs): adopt onto the
  nearest ANSWERED same-code section instead of the largest? DEFAULT: unchanged here (moves ordinary river pieces; needs
  its own before/after).
- Q169b — A main-river section left grey by an unowned double-line polygon while its side pieces answer past it (321/269
  runs): walk a river's own unowned polygon as the river (as T101 did for stream items' polygons)? DEFAULT: unchanged;
  a follow-up ticket.
- Q169c — Gravel Slough's three 1425 connectors between its answered polygons: give a stream water's connector lines its
  polygons' answer (a line version of `stream_polygons`)? DEFAULT: follow-up (3 sections).
- Q169d — One panel for a long side-channel water (Alec Chief Creek, 18 sections, a channel of Toozaza Creek at 0.24)?
  DEFAULT yes — it already read one panel; nothing moved.

## Ship steps
Merge lane-T169 into redesign/stream-sections; `python -m pipeline.deliver` via scripts/heavy.sh (owner); then
`pytest -m needs_bundle pipeline/tests/test_side_channel_water.py pipeline/tests/test_stream_polygon_panels.py
pipeline/tests/test_side_channel_gauges.py`, and diff section_panel against: 246,705 sections with a panel, Flow 246,063,
Level 127,270, 24 sections changed, all in side-channel waters.
