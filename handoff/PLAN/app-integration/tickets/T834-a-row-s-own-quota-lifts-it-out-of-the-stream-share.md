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

2026-11-17 ~10:38, owner on the side build (lane/t834-share-lift 916965dc7, verbatim): changed waters "I think this is
right but you didn't explain well"; smaller rows: "How the fuck would it read that it is just 2 of the 4 and nothing
should be different then that unless it lists a value bigger then regional quota in which case it will fill it up then
add a couple additional slots after being filled up regional". This is an overflow reading (the row's fish fills the
share first; a bigger number adds only the difference as its own slots), not a lift. Equal numbers (Kaslo/Keen bull
trout 2 vs "2 from streams") would change nothing, and the hand-written Lardeau/Duncan bull-trout lifts would go.
Follow-up asked with numbers; NOT merged.

2026-11-17 ~10:50, owner follow-ups (verbatim in OWNER-TODO.md): Kaslo "Yes: 2 in all"; Lardeau "5 rainbows and 2
bull ... or 2 other trout like cuttroat then ... the 5+2 with any 2 removed"; day total "i would say 7 because we have
other ones like one of the kootenay lakes having a 10 a day quota for rainbows". REWORK: not a lift out of the share.
Day pool = max(the region's cap that applies there, the sum of the water's own daily quotas); own-quota fish under their
own number, other fish under the region's cap, all inside the pool, which replaces the region's total on that water.
Lane 916965dc7 is NOT merged.

2026-11-17 ~11:10, owner: "everything should use same reading..." then "Strict everywhere (Recommended)". FINAL MODEL
(replaces the ~10:50 day-pool reading): for each regional cap C that counts fish X (stream share, region total) and the
water's own quota N for X: N <= C, X sits inside C (no change); N > C, that cap becomes N for X only (other kinds stay
<= C, all inside N). Lardeau/Duncan 5 (bull + others <= 2: the T3a bull-trout lifts go), Kootenay Lake main body 10,
Kaslo/Keen 2, Adams 4, Buckley 1. GOTCHAS entry (Questions for BC Fish and Wildlife) carries every case.
