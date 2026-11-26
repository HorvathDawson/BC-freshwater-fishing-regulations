# T771 — Salmon chain order: size counts side-stream sections; no check when two equal places disagree

Status: PARTIAL — item 2 built (cf637ab44): two places of one chain, region and order key apart from the id that answer a fish differently where the chain reaches them STOP the salmon step, naming both (`salmon.equal_place_conflicts`, `same_answer`: closed equals closed, an opening's words count; accepted pairs `EQUAL_PLACES_ACCEPTED`, none). Review fix a2b7dc068: the check compares every later place with an equal key, not only adjacent ones (a rest moved between two equal places by `rest_after` hid them; test + mutation). Replay 2026-11-23 on the merged branch (a2b7dc068 + origin/main), read-only on the live inputs and bundle (r-741815c6090d): payload byte-identical to the live `salmon_doc` (272152 bytes, sha256 164497bbe52f9564...), salmon_section identical (1949557 rows), no equal-place stop. left (Claude): item 1 (size by own water) waits on a rule that keeps nested places right — see "Lane L10" below.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T771 | Salmon chain order counts side-stream sections as size (the whole Stamp sorts before the Ash stretch), and two equal places that disagree are ordered by name with no build stop (the Skeena sockeye pair before T761; Tatshenshini) | PARTIAL | cf637ab44, a2b7dc068 | item 2 built (equal places that disagree stop the salmon step; replay on live is byte-identical, 11-23); left (Claude): item 1 (size by own water) needs a containment rule, nested places break a count (ticket "Lane L10") |

Source: second-look fix lane A (finding 2) and the Skeena cascade review part 1, 2026-11-15 night; owner 22:12 on the
Skeena sockeye pair ("this iss handled by rest of option in extents": that pair is now ordered by T761).

## What is wrong
## What is wrong
1. `chain_order` sorts by tier, then "fewer sections first". The count includes side-stream sections, so a 10-section
   whole river (the Stamp) sorts before a 37-section stretch of it (the Ash stretch, 33 of them side streams). No
   answer changes today because their dates never overlap.
2. (built) Two places of the same tier and size that disagree for a fish on a day were ordered by id with no stop.

## Done
- Item 2 (lane L10, cf637ab44 + review fix a2b7dc068, `test_t771_equal_places.py`; mutation: the gate returning []
  fails 3 tests): as the Status says. Two closures count as one answer, an opening's words still count (T840): the
  Morice pair (~2 "from fishing boundary signs ~100 m downstream of Gosnell Creek" / "from Gosnell Creek to Lamprey
  Creek": "No fishing for chinook" vs "No fishing for salmon", both closed) passes.

## Left
- **Item 1 not built.** "Size = sections on the place's own water" breaks a nested case: a whole-watershed place W ("and
  tributaries", own stem 100 sections) and a tributary T's own place ("T and tributaries", own stem 200) overlap on T's
  sections; today T sorts first (400 < 1,000 sections in all), by own-stem size W would (100 < 200) and the wider
  place would answer on T. A tributaries-only place (own sections 0) would sort before every named tributary's place
  too. The rule needs a containment reading ("a place lying inside another's own-water stretch sorts first") rather
  than a count; no answer is wrong today (the Stamp / Ash dates never overlap). For the main chat to pick.
- When built: diff the chain of every section before and after.
