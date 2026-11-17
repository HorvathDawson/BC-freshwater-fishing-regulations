# T771 — Salmon chain order: size counts side-stream sections; no check when two equal places disagree

Status: OPEN (pipeline, `pipeline/deliver/salmon.py` `chain_order`; no answer is wrong today).

Source: second-look fix lane A (finding 2) and the Skeena cascade review part 1, 2026-11-15 night; owner 22:12 on the
Skeena sockeye pair ("this iss handled by rest of option in extents": that pair is now ordered by T761).

## What is wrong
1. `chain_order` sorts by tier, then "fewer sections first". The count includes side-stream sections, so a 10-section
   whole river (the Stamp) sorts before a 37-section stretch of it (the Ash stretch, 33 of them side streams). No
   answer changes today because their dates never overlap.
2. Two places of the same tier and the same size that say different things for the same fish on the same day are
   ordered by id (name order). Nothing stops the build. The Skeena sockeye pair disagreed on 49 days and the right one
   won by name; the Tatshenshini pair was the same kind of case.

## What to build
- Size for ordering = sections on the place's OWN water line(s), side streams not counted (or: a place that lies
  inside another's own-water stretch always sorts first). Diff the chain of every section before and after.
- A build check: two places in one section's chain with the same tier and the same order key apart from id, that give
  different answers for a fish on a day, STOP the build and name both (accepted pairs listed with a reason, as other
  gates do). Rest-of pairs (T761 `rests_behind`) are ordered by structure and pass.
