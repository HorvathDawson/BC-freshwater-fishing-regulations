# T834 — a water row's own printed quota takes that fish out of the region's stream share, automatically

Status: OPEN (main chat; rules engine, owner-asked 2026-11-17).

Source: owner 2026-11-17 ~08:58 (verbatim, main chat): "It would just make the same limit tho. That is fine I guess but
the kasko and keen should be the same. Also lardeau and duncan should also do that for rainbow right? I'm confused why
it is like this seems like it would auto be handled". Earlier the same morning: "Dollys and bulls always have same quota
per the book."

Today the lift out of a region's stream share ("2 from streams", Region 4 `trout_char_quota.r3`) is a hand-written
`exempts` on each parsed rule. The Lardeau (r4 rainbow 5, r5 bull trout 2) and the Duncan (r4, r5) carry it (RULINGS
T3a); Kaslo River r1 and Keen Creek r1 print the same "Bull trout daily quota = 2 (any size)" and carry none, so the
engine keeps 2 in total there while the shipped card says "Not part of the 2 from streams".

Fix: one rule in Python, not per-row data: a water row that prints its own daily quota for a fish the region's stream
share counts takes that fish out of the share on that water (and walks with a ✱ row per T3a), and still counts toward
the region's total. Policy flag + mutation pin. Inventory first: every row in every region that prints such a quota,
whether it carries the lift today, and what changes. The changed answers go to the owner before anything ships.
