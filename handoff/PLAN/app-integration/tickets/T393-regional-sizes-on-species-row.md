# T393 — Regional DFO size rules live on the species row

Status: DONE 2026-11-05 (bfc758925; owner report 2026-11-05, 0.5.0 build 8, water gnis:14589, Regulations).

The regional DFO block lists size rules as loose lines: 'An "adult chinook" in Region 2 is defined as being over 50 cm
except in the following areas where an…', 'All retained chinook, chum, pink, and sockeye must measure 30 cm or more', 'All
retained coho must measure 25 cm or more'. Owner: should these live on the species row itself?

Proposal (Claude, yes): each size goes to its species' row in the salmon table (chinook: adult over 50 cm here + 30 cm
minimum; coho 25 cm; chum/pink/sockeye 30 cm) and the loose lines go. The adult-chinook size is per water already (T343
item 4). Check the DFO wording registry (T332) carries the size per species, not as free text.

## Done (bfc758925)
Before: only a row that may be fished took its region notes; a CLOSED species' size rules fell through to the loose
"Region rules (DFO)" block, and even an open row's notes were listed under the table. Now every species row (closed
too) carries `sizesBy`, its own line from the parsed sizes (`sizesOf`: "chinook 30 cm or more to keep, adult chinook
over 50 cm", the per-water 62 cm where DFO says so), shown as the table row's size line; the region block leaves out
every size note a row now says. Region 2's five size notes (feed checked: all `fold: "sizes"` with species) leave the
block on gnis:14589. Non-size species notes (an annual limit) keep their T227 placement. Test rewritten (salmon.test.ts,
"a closed chinook's size is on its own row"). Phone check with the next APK.
