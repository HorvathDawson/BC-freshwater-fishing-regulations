# T771 — Salmon chain order: size counts side-stream sections; no check when two equal places disagree

Status: PARTIAL — item 2 built (cf637ab44): two places of one chain, region and order key apart from the id that answer a fish differently where the chain reaches them STOP the salmon step, naming both (`salmon.equal_place_conflicts`, `same_answer`: closed equals closed, an opening's words count; accepted pairs `EQUAL_PLACES_ACCEPTED`, none). left (Claude): item 1 (size by own water) waits on a rule that keeps nested places right — see "Lane L10" below.

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

## Today's list (lane/bugs-080c, 2026-11-17, read-only replay of the bundle's salmon step on the live atlas, reach run
and typed feed; scratchpad `t771_list.py`)
348 chains; 56 pairs of places tie on the order key apart from the id (all tier 3, the same size, region and rides flag;
T761 / T783 rest pairs skipped). **None gives a different verdict for a fish on a day where both speak.** One pair
differs in words only: Morice "from fishing boundary signs ~100 m downstream of Gosnell Creek to Lamprey Creek" (~2) and
"from Gosnell Creek to Lamprey Creek" (CH 244 days, CO/SK/PK/CM 16: same verdict, DFO's text differs). So a
stop-the-build gate would pass today; not added (the main chat decides). Item 1 (size counting side streams) not
touched.

## Lane L10 (2026-11-22, branch fix/T840-rules-smalls)
- **Item 2 built** (cf637ab44, `test_t771_equal_places.py`, mutation: the gate returning [] fails 3 tests). Proof on live
  data: a read-only replay of the bundle's salmon step (live atlas, reach run, typed feed, the live bundle's tables;
  scratchpad `l10/salmon_replay.py`) reproduces the live bundle's salmon payload byte for byte (352 chains) and
  passes the gate. Its first form compared whole verdicts and stopped on the Morice pair (~2 "from fishing boundary
  signs ~100 m downstream of Gosnell Creek" / "from Gosnell Creek to Lamprey Creek": "No fishing for chinook" vs
  "No fishing for salmon", both closed) — so two closures count as one answer; an opening's words still count (T840).
- **Item 1 not built.** "Size = sections on the place's own water" breaks a nested case: a whole-watershed place W ("and
  tributaries", own stem 100 sections) and a tributary T's own place ("T and tributaries", own stem 200) overlap on T's
  sections; today T sorts first (400 < 1,000 sections in all), by own-stem size W would (100 < 200) and the wider
  place would answer on T. A tributaries-only place (own sections 0) would sort before every named tributary's place
  too. The rule needs a containment reading ("a place lying inside another's own-water stretch sorts first") rather
  than a count; no answer is wrong today (the Stamp / Ash dates never overlap). For the main chat to pick.

State 2026-11-22 (stopped for credits): lane L10, branch fix/T840-rules-smalls (head cd66153eb before this line); see the ticket's Status for done / left.
