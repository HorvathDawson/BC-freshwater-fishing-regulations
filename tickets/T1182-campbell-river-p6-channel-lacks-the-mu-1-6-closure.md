# T1182 — Campbell River part 6 (a side channel) lacks the MU 1-6 summer closure the river has

Status: OPEN — a closure risk; fold into the T1177 build

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1182 | Campbell River part 6 (a side channel) lacks the MU 1-6 summer closure the river has | OPEN — a closure risk; fold into the T1177 build | — | bind the MU 1-6 summer closure to the 0.3 km channel (it follows its river, in both units); answers diff |

Source: the planning chat's note `side-channels-step-back` (side-channel table row 17; its NEW TICKET line). The owner's rule: a river on a unit
line is in both units (`docs/OWNER-RULINGS.md`: "campbell river is correct it straddles so is included in mu 1-10 and 1-6 so is in closure").
The Campbell's part 6 (a 0.3 km loop, "Side channel, joining 16.9 km") lacks the MU 1-6 summer closure the river beside it has: an angler
could fish a closed water. Pick 5 of the step back (channels of a river on a region or MU line follow the river, Cottonwood, West Road,
Campbell) is the default; the same fix applies to Cottonwood p1 and West Road p3 (they lack a Region 5 base).

## Work
Make the channel take the river's closure by the river-on-the-line rule (an extent, not a special case); check Cottonwood and West Road
the same way; answers diff.

## Acceptance
Campbell part 6 answers the summer closure on its days; the other two channels carry the region base; nothing else moves.
Severity: medium (closure risk). Model: Opus. Size: S. Depends: T1146, T1149.
