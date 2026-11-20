# T804 — One stated rule, applied everywhere: a joining water with a row of its own is not inside its neighbour's stretch rule (Gosnell Creek and the Thautil River come out of the Morice closure; Lamprey Creek stays in)

Status: DONE — fa098f1f4, e59d7146f, da515cfeb. a joining water with its own row is out of its neighbour's stretch rule (Gosnell Creek, Thautil River out of the Morice closure; Lamprey Creek in), province-wide, reach policy 47.

Closed by the 2026-11-20 audit: built fa098f1f4 and merged with the 11-17 mini re-cut (da515cfeb); owner OK'd the changed answers 2026-11-17 17:01 (e59d7146f, OWNER-TODO 17:01); shipped in 0.8.0 onward.

Source: owner 2026-11-16, question 9 (`handoff/PLAN/curation-review/skeena-cascade-review-2026-11-15.md` 2(b)): "yes
they are out but please make sure this rule doesnt break other stuff. we need a trigger for why they are out? why
does this tributary rule not apply? what is the condition just so it is a universal rule."

## What happens today
The rule book's Morice row "No Fishing from Gosnell Creek to Lamprey Creek, Jan 1-Aug 31" binds 1,958 sections: the
stretch and all of Gosnell Creek, the Thautil River and Lamprey Creek with their side streams. Gosnell and the
Thautil have rows of their own ("No Fishing Aug 1-Sept 30; bait ban"); ruling T9 says a joining water with a row of
its own is left out; Lamprey Creek has no row, so it stays in.

## What to build
1. Write the condition down in one sentence (the trigger): which rows it applies to (a stretch rule that takes its
   side streams), what counts as "a row of its own" (a named row in the regional water table for that water; say
   whether a row inherited by "including tributaries" of another water counts), and what stays in.
2. Find why T9 does not fire for the Morice stretch today, and every other stretch where it does not fire.
3. Apply it as the one rule, province-wide; diff every rule binding before and after; the list of waters whose answer
   changes (which days, which fish) goes to the owner before it ships.

2026-11-17 ~17:01, owner: land it (verbatim in OWNER-TODO 17:01); Tatshenshini named-tributary places bind only the named tributary; dormant three follow DFO words; Claude re-saves the 12 changed marks.
