# T460 — Fraser: ▲ from "Williams Lake River confluence → Region 7A boundary" offers two parts, one skips a part

Status: DONE 2026-11-06 — 96f7b5919 (Q1: case (a), the rules are right, 950c2a3; Q2: the step fixed). Owner: phone check on 0.6.8.

Owner: "upstream of Fraser at Williams Lake River confluence → Region 7A boundary has 2 upstream Fraser options?
Shouldn't it only have 1? It gives an option to basically skip one of the sections."

## Found (release r-67b456b5e64b, water-5 `part_mark`, Fraser item ord 9871)
- part 1 "Williams Lake River confluence → Region 7A boundary" (37 sections) has TWO marks above it:
  - mark 1 (−122.6198, 53.1225, no label) → part 15 "Region 7A boundary → Region 5 boundary" (2 sections, "Where
    Region 5 and Region 7 meet");
  - mark 0 (−122.8436, 53.2733, no label) → part 0 "Region 5 boundary → Source" (Region 7).
  - part 15's top is mark 18 (−122.8679, 53.3071) → part 0.
- So part 1 reaches 53.27, ABOVE its own top (the 7A boundary at 53.12), and touches part 0 directly. `part_line`
  shows part 1 holds side-channel blue lines (many `blk`s beside the mainstem's). One of them runs up beside part 15
  and meets part 0's channels. ▲ from part 1 therefore lists 15 AND 0, and taking 0 skips 15.

## Questions to settle (measure first)
1. Is that side channel really Region 5 water? Along part 15 the Fraser is the R5/R7 border. A channel lying wholly on
   the R5 side may rightly be in part 1 (Region 5's rules). If it lies on the border or across it, it is a
   braid-across-a-cut that `splits.braid_sweep` should have cut (RULINGS E17, AGENTS rule 12): then sections above the
   7A boundary carry the wrong part's rules. That is a regulation-placement defect, more serious than the step. Check
   the pieces' geometry against the region polygons, then report which.
2. Navigation, whatever 1 says: a step never offers a part that is reachable by going on from another offered part
   when the two come from DIFFERENT marks (a side channel touching further up is not a fork). A true fork, ONE mark
   with two upper parts (the Zymoetz side channel, `up_parts [0, 5]`), stays as it is. Fix in `core/waterNav.ts` with
   a test on this exact Fraser shape and the Zymoetz shape. Count how many waters/parts in the release change their ▲/▼
   options, and list them.
3. If 1 finds misplaced sections: a pipeline ticket (braid sweep at region-boundary cuts), the reach-only rerun +
   binding diff (memory: split fix changes the trib walk), and a release. Do not promote without the owner.

Related: T443 (step bar), rule 12 (braids), T67/E17. Size: M. Owner: none unless 1 finds misplaced rules.

## Q1 found (2026-11-06): case (a), Region 5 water; no braid across the cut
Measured: release r-67b456b5e64b (water-5, lines, geo) against the `wmu` polygons in `bc_fisheries_data.gpkg`,
dissolved by `REGION_RESPONSIBLE_NAME`, the same polygons the regions and the border cut come from.
- Part 1 holds 5 side channels above the 7A cut (mainstem 356364114 @ 674465 m, 53.1225 N). Each meets the Fraser
  mainstem at BOTH ends INSIDE part 15's span (674465–703745 m), and none attaches below the cut:
  | blk | lat | joins mainstem at (m) | sid · m |
  |---|---|---|---|
  | 355995847 | 53.158–53.162 | 682886 / 683389 | 61179 · 470, 61180 · 340 |
  | 355999316 | 53.241–53.247 | 692946 / 693775 | 62527 · 909 |
  | 355995086 | 53.261–53.265 | 697062 / 698149 | 60853 · 1470 |
  | 355999633 | 53.271–53.273 | 699253 / 699507 | 62637 · 587 |
  | 355992951 | 53.278–53.286 | 700284 / 701232 | 59989 · 1126 |
  So: Fraser side channels from 53.158 N (−122.7186) to 53.286 N (−122.8574), 6 sections, 4,902 m.
- All 5 lie WHOLLY in Cariboo (Region 5). Their length inside Omineca is 0.00 m. They touch the R5/R7 line only
  where they rejoin the mainstem (the line follows the main channel here). They reach up to 164–253 m from the line,
  on the R5 side. The R7-side channels beside part 15 (355996501, 355993311, 355995178, 355995175, 355996805) lie
  wholly in Omineca and sit in part 0.
- So nothing straddles a cut. `braid_sweep` cuts a channel only when it attaches on both sides of a cut, and none
  does here. Region 5 rules on these 6 sections are right by region. Only ▲/▼ is wrong (Q2). Mark 0
  (−122.8436, 53.2733) is where 355999633's top meets 355995175 (part 0, R7 side) at the same mainstem junction, and
  that junction is what makes part 1 "touch" part 0.
- For the record, the rules differ. Part 15 ("Where Region 5 and Region 7 meet") adds the R7A "upstream of the
  Cottonwood River confluence" set (any bait, spring stream closure lifted) on top of part 1's rules. The 6 R5-side
  sections do not get it, which is correct if R7A rules stop at the region line. This is not a placement defect.

## Q2 fixed (2026-11-06, 96f7b5919)
`core/waterNav.ts` `skips`: a way from a part is not offered when another way out of that part reaches it going on, the
two ways leave at different places, and it does not reach back. A fork at one place (the Zymoetz canyon) stays. So do
channels that leave a part and come back to it (the Fraser at Lillooet, never going back through the part) and two ways
that each reach the other (a loop). Tests use the Fraser's real marks (above Williams Lake, and Lillooet). Each half of
the rule was checked by mutation: the test fails without it.
**Release-wide** (r-67b456b5e64b, through the app's own `loadWaterGroup`): 650 waters with marks, **10 change** (one
skipped step each way): Fraser (this one), West Road (Blackwater) (the same shape at the R5/R7A line), Little Qualicum
(the hatchery part), Peace (Clayhurst Ecological Reserve), Skeena (side channel at the reserve), Snehumption Creek (2 side
channels), and the border streams Blanchard, Little Hay, Wap and Sleigh (a step into a later stretch past an earlier one).
Every part stays reachable from the mouth in all 650, except Bulkley River (6 of 12 parts unreachable). That was true
before this change too, so it has its own ticket: T464.
