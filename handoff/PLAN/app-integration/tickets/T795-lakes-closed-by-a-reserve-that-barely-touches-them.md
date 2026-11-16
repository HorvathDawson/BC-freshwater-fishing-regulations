# T795 — Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year because an ecological reserve covers 0.1 % of the lake

Status: OPEN (data: `outside_items` on `zp:superior_closures.r3`; the two partly-inside lakes wait on the owner, question 34).

Source: general audit 2026-11-16, answers checker B1 (`handoff/PLAN/audit-2026-11-16/answers.md`).

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
