# T1246 — Turner Creek is filed under Noons Creek by a code fault; it is its own water

Status: BLOCKED — stopped before any data change; waits for the owner (the fix and its timing); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1246 | Turner Creek is filed under Noons Creek by a code fault; it is its own water | BLOCKED — no data change; owner: see ## Owner | — | the owner picks (a), (b) or (c); then the curated added-streams fix |

Source: the owner, 2026-11-27 (planning chat), verbatim. First "Show me the map first"; after the map: "Make it its own
water (Recommended)". Earlier, on Noons Creek (T1191, Q4): the FWA stream and the city stream of the same name in the
same area both get the Noons Creek regs, for safety. The finding is the data chat's (-c9), 00:45 MST 11-27.

## Cause (corrected)
NOT an alias. The city line for Turner Creek (Port Moody, 2.2 km) carries Noons Creek's own watershed code 900-057239:
the coastal-code estimate gave it that code, and the uniqueness check looks only at other city codes (the known T1150
hole). The registry groups lines by code, so the line joins the Noons item. Turner Creek is a separate creek: its own
mouth on Burrard Inlet about 0.7 km west of Noons Creek, never within about 0.6 km of it. The book's rule: Noons Creek
2-8 Incl. Tribs, "No Fishing upstream of railway bridge".

## Answers today are right (a filing fault only)
Turner Creek and its one tributary carry only Region 2 and provincial rules. The Noons row ("No Fishing upstream of
railway bridge", incl. tributaries) skips the Turner piece through a leave-out row the owner accepted on 2026-11-11
(`unplaced_piece_calls.json`). Nothing opens or closes.

## The fix (not done)
Give Turner its own code in the curated added-streams file and re-code its tributary under it. The tributary's water id
moves (marks and matches resync), and the accepted leave-out row goes stale (it stops the reach run until it is changed
or removed).

## The same fault in two more (T1150's three)
- Tunnel Creek (Burnaby, 0.22 km), filed under the Seymour River; the mouths are 1.07 km apart.
- Capitol Creek (Burnaby, 0.72 km), filed under "Squatters Creek"; the mouths are 0.29 km apart.

## Report only
25 items answer to a city-supplied name that is not their own (Port Moody 4, Burnaby 15, Squamish 6). Only these 3 are
separate creeks; the other 22 are classed by which lines each item holds, not checked for connection one by one.

## Acceptance
No data change until the owner answers. Then: each of the creeks in his pick is its own water with its own rules, no
answer changes, the leave-out row is changed or removed, marks and matches resync.

Does not block the re-cut. Depends: T1150. Owner: yes (below).

## Owner

- [ ] 2026-11-27 — Turner Creek and its two kin (Tunnel Creek, Capitol Creek): (a) fix all three together in T1150, in the hardening release (the data chat's recommendation; the re-cut ships Turner as a part of Noons with its right rules); (b) Turner only now, in the re-cut, removing his 2026-11-11 leave-out row; (c) all three now, in the re-cut. No data change until he answers.
