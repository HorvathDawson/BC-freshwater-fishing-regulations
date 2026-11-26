# T1146 — Side channels with no main stem that the river's own rule does not reach become nameless parts

Status: DONE — 9f0856154, 312e728d0 (the mechanical cause; the curation groups C/D/E stay open below and in OWNER-UNFILED.md, for T1140 / the planning chat)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1146 | Side channels with no main stem that the river's own rule does not reach become nameless parts | DONE | 9f0856154, 312e728d0 | — |

Source: the 0.10.0 chat's read-only investigation for T1140, 2026-11-25, on the candidate r-71d09ad82e81. Of 1,476 parts
over the 478 multi-part waters, 26 have no name and show the bare water name; all 26 are side channels with no
main stem. 15 of them exist only because the river's own or regional rule does not reach the channel (Seymour p3,
Skeena p4, Manson and 12 more); 6 because a Strathcona Park rule does not reach the channel (Burman, Bedwell,
Thelwood and 3 more; Thelwood lies inside Strathcona - Westmin Park). If the binding is wrong, the fix makes the
part take its river's rule and merge into its neighbour, and it needs no name. Related: T1140, the memory notes
`receiving-river-not-a-tributary`, `split-fix-changes-trib-walk`, `reach-only-rerun`.

Owner, 2026-11-25 (planning chat dialog), to "name them now and fix the binding next patch (recommended) / fix the
binding in 0.10.0": **"Fix the binding in 0.10.0"**.

Work:
1. FIRST, read-only: is this one mechanical cause in the reach walk (a side channel without a main stem does not
   take its river's water-level rule), or separate curation cases? Are the 6 Strathcona parts the same fault?
2. If mechanical: fix in code with a test that fails without it (mutation), rerun the reach step, and diff every
   rule binding against the candidate. Expected: only those channels gain their river's rule and merge away.
   Anything else stops the release and comes to the planning chat.
3. If it needs the owner's marks case by case: stop; the cases go to the owner. No agent writes a mark.
4. Parts that still have no real-world name afterwards (Rees p0, Comox p0, ...) are T1140's question to the owner.

Acceptance: the count of bare-name parts before and after, by cause; the binding diff attributed line by line;
the tiles and bundle of one vintage (rule: tiles and bundle share a vintage). Severity: high (a part with the
wrong rule set, the Squamish case's family). Model: Opus. Size: S if mechanical, otherwise unknown. Owner: the
marks if step 3.

## Owner

- [x] 2026-11-25 — The 15 rule-gap channels → "Fix the binding in 0.10.0" (2026-11-25).
- [x] 2026-11-25 — The fix's diff is wider than predicted (34 feeder creeks, Manson's two stubs through its lakes, the Old Kootenay River Channel's last piece): accept, they are tributaries / stop at the channel / show me the list first → "Accept: they are tributaries (Recommended)" (2026-11-25)

## Finding and fix (L30, 2026-11-25)

There is not one cause. There are four kinds of cause, and only one of them is mechanical.

| # | cause | where | verdict |
|---|---|---|---|
| A | The tributary walk climbs only UP (`up_adj`). A side channel that LEAVES a collected piece of its own water and never flows back into a collected piece (a distributary, dead end, or a channel into a pond) is never reached. The dead-end net fires only when the WHOLE watershed code was collected, so it fails when any piece of the code is carved out or refused | `pipeline/atlas/graph/tributaries.py` (`tributaries_of_reach`, `_dead_end_seeds`) | MECHANICAL: fixed |
| B | The Strahler guard refuses an equal-code side channel whose FWA order is bigger than the piece of its own river it flows into (`bigger_river`) | `tributaries._refusal` | MECHANICAL: fixed (same fix) |
| C | 2300 connector (canal) pieces: the walk's barrier | `tributaries.may_enter` | CURATION/policy (the guard is kept): OPEN |
| D | Area/region membership is by overlap. A straddling main-stem section that is not cut takes the area's rule along its whole length. The side channel lies wholly outside the area, so it correctly does not take the rule | `pipeline/atlas/splits/border.py` `mark_inside_areas`; parks `cut: false` in `data/curated/waters/area_closures.json` | CURATION (owner: cut the park?): OPEN |
| E | Rules differ for a real reason: inside an area closure, a rule only on the channel, a region straddle (T183), added streams the owner left out, a licensing stretch cut | — | NOT A DEFECT, or curation: OPEN |

**The fix (9f0856154).** `tributaries.A_SIDE_CHANNEL_GOES_WITH_ITS_WATER` (`_side_channel_seeds`; `expand` passes
the registry). A walk that collects a stream item now also collects that item's no-main-flow pieces, grouped by
connection, when EVERY piece of the same item they touch was walked. It never collects a barrier, a carved-out
piece, the entry's own water or the reach, and the walk climbs on from what it adds. Because `expand` serves every
walked binding, this covers rules, licensing designations, DFO places, in-season points and the review app.
POLICY_VERSION is 51. There are 12 tests in `pipeline/tests/test_walk_dead_ends.py`, and every guard is
mutation-proven. Turning the flag off fails 5 tests. Removing the barrier, carved-out, own-water,
all-neighbours-walked, needs-a-neighbour, no-main-flow, this-item-only or never-the-reach guard fails 1 test
each. Not climbing on from an added piece fails 2.

**The real binding diff.** A side reach run on the promoted atlas, against the candidate's
`data/generated/reaches/full`, with `pipeline.atlas.reach.binding_diff`. 12 rules, 1 licensing record and 1 DFO
place gain 56 sections (47.7 km). Nothing is lost or rescoped, and no unresolved, diagnostic, tidal or
outside-B.C. table changed. Every section is listed in `../T1146-files/diff-attribution.txt`, and
`../T1146-files/what-changes.md` says the same in an angler's words.
- As predicted: the 19 channel sections of Forsyth, Koksilah, Halfway, MacMillan, Manson, Omineca and
  Shelagyote. The Babine designation reaches Shelagyote's channel, and the DFO place
  "1:cowichan-river:including-tributaries-and-cowichan" reaches Koksilah's 5 channel pieces.
- Beyond the prediction. The owner answered 2026-11-25 18:06 in the planning chat dialog, to "accept, they are
  tributaries / stop at the channel / show me the list first": **"Accept: they are tributaries (Recommended)"**. So
  all three stand:
  - (a) 34 unnamed feeder creek sections that flow only into those channels. The walk climbs on from the
    channel: Shelagyote 19, Manson/Omineca 8, Koksilah 4, Halfway 3.
  - (b) Manson's two stubs between lakes (359006963:0 and :920), reached through the lakes.
  - (c) Old Kootenay River Channel (gnis:2123). Its 170 m piece 356365662:1210 gains `kootenay_river_lower.r4`
    (burbot catch and release), which its other 3 pieces already carry.
- The Nechako did NOT change, against the prediction. Channel 355995054:0 (735 m) lies wholly in Region 7A, and
  the DFO place `6:f:closure` (Fraser River watershed) is clipped to Region 6. The only Nechako piece it touches
  straddles 6/7A and is the place's direct extent, which is not walked, so the channel stays out. That is right.

**The 26 nameless parts.** Six join the part beside them: Forsyth p1, Koksilah p1, MacMillan p2, Shelagyote p0,
Omineca p0 and Manson p0. Halfway p2 shrinks from 12 sections to 3: the 2300 piece 359004004:502 and two stubs
touching ponds. The other 19 are unchanged. The part merges are predicted from the rule sets; this lane did not
run tiles or deliver.

**Still open (C, D, E).** Copied word for word from the finding's per-part table:

| part | rules the siblings have that it lacks (or what differs) | cause | verdict |
|---|---|---|---|
| Mahood Creek p1 | serpentine r2-r4, bear_mahood_creek.r1 | C: 2300+1450 ditch grid, still in the item (`registry.build.is_ditch` keeps it: name source not side_channel?) | CURATION |
| Stirling Creek p1 | kettle/west kettle tributaries | C: 1100+1150+2300 piece into Acme Lake | CURATION |
| Robertson Creek p1 | stamp_river r2,r3 | drains Boot Lagoon into Great Central Lake, above the top of both Stamp stretches (only whole rules climb head lakes, T1053) | CURATION/policy |
| Cottonwood River p1 | z5 base (main stem straddles 5/7A, channel wholly 7A) | D/E: region straddle by design (T183) | NOT A DEFECT |
| West Road p3 | z5 base (river is the 5/6 line, channel wholly in 6) | D/E: same | NOT A DEFECT |
| Noons Creek p4 | its own and region rules | E: added stream -2200000392, unplaced; owner accepted "leave out" 2026-11-11 (unplaced_piece_calls.json) but it is still in the item | CURATION |
| Seymour River p3 | no rule difference from p2. Separate only because p2 is in no-access land. It lacks seymour_river.r1 | E: added stream -2000000390, the same owner call | CURATION |
| Skeena River p4 | NO rule gap (ruleset 994 = p2). Licensing set 72 vs 0: the Classified Waters stretch `skeena_river_2` cuts the braid at 1017 m, not where it cuts the main stem | E: placement of the licensing stretch on the braid | CURATION |
| Burman River p0 | strathcona_park_waters.r1 | D: channels 1.4-2.9 km outside the park. Main section 354155253:0 is 22.4 km with only 13.0 km inside | CURATION |
| Bedwell River p0 | strathcona_park_waters.r1 | D: channels 1.5-5.0 km outside | CURATION |
| Rees Creek p0 | strathcona_park_waters.r1 | D: channel 2.9 km outside. Main section 354150445:0 is 9.6 km with **31 m** inside | CURATION |
| Comox Creek p0 | strathcona_park_waters.r1 | D: channel 261 m outside. Main 14.2 km with 1.8 km inside | CURATION |
| Thelwood Creek p0 | strathcona_park_waters.r1 | D: channels lie in STRATHCONA-WESTMIN park, 718-751 m outside Strathcona Park | CURATION (owner: is Westmin "Strathcona Park waters"?) |
| Campbell River p6 | z1 summer_stream_closure (within MU group 1-1..1-6) | D: main 354154635:14945 straddles MU 1-6/1-10; the channel is not a member | CURATION |
| Squamish p1, Gordon p0, Okanagan Oxbows p1 | carry extra area-closure rules | E: inside a cut ER or national park | NOT A DEFECT |
| Theodosia p1, Snehumption p1 | carry an extra rule (Powell Lake, Similkameen) | E: the channel flows into that water | NOT A DEFECT |

Strathcona (D), word for word: "The park rule should NOT reach these channels: by the park polygon
(area_catalog.gpkg) they are outside it. The fault runs the other way. Parks are membership-only (`cut: false`),
so 365 Strathcona Park sections that straddle the boundary carry the park rule along ~478 km outside the park (my
measure: FWA lines against the polygon; approximate). Rees Creek carries it on 9.6 km from a 31 m overlap. If the
owner cuts the park (curated, atlas rebuild ~32 min, all 365 sections change), the lower main stems of Burman,
Bedwell and Rees, and probably Comox, lose the rule and the channels merge into them. Thelwood stays apart unless
"Strathcona Park waters" includes Strathcona-Westmin."

**Review round (independent review of #264).** No HIGH or MED findings. One LOW and two test gaps, all fixed in the
second code commit:
- A channel that another water flows into is never seeded. "Another water" means an inflow, stream or lake, whose
  watershed code is neither the channel's nor under it, or is missing. Without this guard, the walk climbing on from
  a channel refused as `bigger_river` could enter a different water feeding it. There are 26 such inflow edges in
  the province (the Fraser's Maria and Annacis sloughs with the Williamson; the Pitt into Widgeon Slough; the
  Columbia's Baldy and Hotsprings channels; the Squamish Powerhouse Channel). None of them fired in the side run.
- New tests cover a channel touching a walked-past piece and a channel whose downstream end rejoins an unwalked
  piece.
- The binding diff is unchanged. I checked every inflow of the 22 seeded channel pieces behind the 56 sections
  (30 edges): each is the channel's own code or a code under it, so the guard skips none of them. MacMillan's channel
  drains INTO Diana Lake, which is in another basin. That is an outflow, not an inflow, so the channel stays seeded.

**Open notes for the owner** (their looks are lines in `OWNER-UNFILED.md`, "T1146 follow-ups").
- The MacMillan Creek channel that drains into Diana Lake (360244113:889, 815 m) now carries the Skeena bait ban,
  because FWA and the registry call it MacMillan Creek. Its water leaves the Skeena basin for Diana Lake.
- In-season notices (`pipeline/regs/inseason/points.py`) walk through the same `expand`, but the binding diff does
  not cover them. Their change is not measured.
