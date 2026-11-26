# T795 — Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year because an ecological reserve covers 0.1 % of the lake

Status: PARTIAL (data fix DONE 46c13b267: `zp:superior_closures.r3` takes six lakes out (Fraser, Kotcho, Cold Fish, 15 Mile Swamp, and Westwick and Priest per the owner's question 34 "Both open with the note"), the reserve held as an undrawn-part note `r3p`; left (Claude): the build check that lists a `within` closure holding a lake less than half inside its area (needs lake-in-area shares stored by the atlas build; no follow-up ticket yet))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T795 | Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year because an ecological reserve covers 0.1 % of the lake | PARTIAL (six lakes fixed 46c13b267; left (Claude): the half-inside build check) | 46c13b267 | general audit 2026-11-16, answers checker B1 (its text: git history, the 2026-11-16 audit folder, a6d3bfcf6 (answers.md)) |

Source: general audit 2026-11-16, answers checker B1 (its text: git history, the 2026-11-16 audit folder, a6d3bfcf6 (answers.md)).

## What is wrong
A `within(area)` rule holds every lake the area's polygon touches (AGENTS rule 13). "Fishing is prohibited in
Ecological Reserves" therefore closes the whole of: Fraser Lake (5,435 ha; 0.1 % inside Ellis Island ER; the book
gives it a lake trout quota, p.59), Kotcho Lake (8,167 ha; 0.1 %), Cold Fish Lake (827 ha; 0.1 %), 15 Mile Swamp
(2,343 ha; 3 %). Westwick Lakes (50 ha; 26 %) and Priest Lake (2.6 ha; 59 %) are partly inside. The other 16 named
lakes the rule closes are 97 to 100 % inside; all 77 lakes closed by the national-park rule are at least 90 % inside.

## What to build
- Take the four barely-touched waters out with `outside_items` (the reserve is carried as a part-closed caveat).
- A build check: a `within` closure that holds a lake less than (say) 50 % inside the area is listed for a decision.
- Westwick and Priest lakes: the owner's call (open with a part-closed caveat, or closed).

## Owner, 2026-11-16 (question dialog; verbatim in `docs/the old owner to-do page`, "Answers by batch")
Go-ahead card: "Go on all (Recommended)" for the four barely-touched waters. Question 34 (Westwick Lakes, Priest Lake): "Both open with the note".

## Lane result (2026-11-16, branch `lane/book-rule-fixes-1116`, commit 07db8a6a8)
- `zp:superior_closures.r3` takes six lakes back out (`outside_items`): Fraser (wbk:329148308), Kotcho (wbk:329203305),
  Cold Fish (wbk:329529449), 15 Mile Swamp (wbk:328945575), Westwick Lakes (wbk:329370953), Priest (wbk:329101439).
  New `superior_closures.r3p` holds the closure on the six as an undrawn part "inside the ecological reserve" (a note,
  never a colour). Each lake now reads by its own rules all year (Fraser Lake: open, lake trout quota 1, none under 60 cm).
- Proof: reach run on the built atlas (read-only): r3 -6 lakes, r3p binds the six, no other rule moved.
  `test_t100_area_caveat.py` worklist +1 (`zp:superior_closures`).
- LEFT OVER: the build check ("a `within` closure holding a lake less than half inside its area is listed for a
  decision") needs lake-in-area shares measured in the atlas build; nothing stores them today. Not small: a ticket.
- Main chat: reach run + bundle + release to see it on the card.
