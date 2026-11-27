# T1248 — Kinbasket Lake's tributaries row misses 235 km (Marion, Sun, Warspite, Lansdowne creeks)

Status: OPEN — probably not a real gap; trace Sun and Warspite creeks

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1248 | Kinbasket Lake's tributaries row misses 235 km (Marion, Sun, Warspite, Lansdowne creeks) | OPEN — probably not a real gap; trace Sun and Warspite creeks | — | trace Sun and Warspite; if all four lie above Kinbasket's exception, close as no gap |

Source: T1177 lane A's sweep (2026-11-27), every stream flowing into a lake tributaries row's lakes, through every lake
above, against what the row binds. Kinbasket's own row "Bull trout catch and release" (`kinbasket_lake_tributaries.r1`)
binds 14,758 km of a SHOULD 14,946 km, less its printed exception (the Columbia above Kinbasket). 235.0 km are missing:
195.6 unnamed, Marion Creek 15.1, Sun Creek 8.3, Warspite Creek 6.3, Lansdowne Creek 6.0. The same 235 km are missing
from Lake Revelstoke's and Lower Arrow Lake's rows, whose walks climb through Kinbasket since T1177.

## What is known
- Not T1177's walk change: Kinbasket's row binds the same before and after it (only the rows below it moved).
- Not the region limit (no `region_clip` diagnostic on the rule), not the tidal limit; `outside_bc` takes 2 sections.
- A read-only check (one graph, `may_enter` at each gap piece's downstream edge into the rule's binding or the lake)
  found only one such edge, and the walk's own test said yes to it (356507449:514 into 356507449:142, both order 1, one
  code): the missing water is mostly NOT met by the walk at an edge from its binding. It drains in some other way
  (through a lake the walk does not climb, or a line the graph joins elsewhere): cause NOT VERIFIED.
- LEAD (bundle read, 2026-11-27, no graph): Lansdowne Creek (gnis:16339) and Marion Creek (gnis:13511) drain into
  Columbia Lake (wbk:329093897), the head of the Columbia above Kinbasket, i.e. ABOVE Kinbasket's printed exception
  ("Does not include Columbia River upstream of Kinbasket Reservoir"). The walk is blocked at the Columbia, so it never
  reaches them, as the book says. The sweep counted them as a gap because it subtracted only the carve-out's own walk,
  which stops at the Columbia's head lake (RIVER_CONTINUING at a stretch's top). So the 235 km are most likely NOT a
  real gap but a sweep artefact; Sun Creek (gnis:20294) and Warspite Creek (gnis:25200) are not traced yet (single
  sections with no neighbours in the bundle): NOT VERIFIED until they are.
- The check's own SHOULD differed from the sweep's (1,611 km against 235 km) because it did not subtract the other
  lake rows' exceptions; the 235 km figure is the sweep's (T1177 lane A `out-sweep/sweep.md`).

## Finding (T1177 lane A3, 2026-11-27)
Probably not a real gap: Lansdowne and Marion creeks drain to Columbia Lake, above Kinbasket's printed exception, which
the owner ruled stops the rows below too (RULINGS T19). Upper Arrow's new climb (T1177) was checked against the same set:
none of its 24,104.7 km lies in the Columbia above Kinbasket or what drains to it (20,205 km). Sun and Warspite not traced.

## Work
1. Find where these four creeks meet Kinbasket's water in the graph and which `may_enter` refusal (or missing edge)
   keeps the walk out; say whether it is a graph defect (atlas) or a walk rule.
2. Then a fix with an answers diff, or the owner's call.

## Acceptance
The cause is named with the piece and edge; any change has its rule-bindings and answers diff. Severity: medium (a
bull trout release missing on 235 km). Model: Opus. Size: S to find.
