# T460 — Fraser: ▲ from "Williams Lake River confluence → Region 7A boundary" offers two parts, one skips a part

Status: OPEN (owner 2026-11-06, chat 5).

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
