# T834 — a water row's own printed quota takes that fish out of the region's stream share, automatically

Status: DONE — 596a677c7, 6cbfd9bd6, 3bbd782e1, df555d0e2. a water's own daily quota above a region cap raises that cap for its fish (strict everywhere, RULINGS T17); the card leads with the region's number.

Closed by the 2026-11-20 audit: owner's final model 11:10, changed waters OK 12:29, card wording 13:15 all recorded in the ticket; merged 596a677c7 + re-cut 6cbfd9bd6, regression fix 3bbd782e1; shipped from 0.8.0.

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

2026-11-17 12:29, owner: the changed-waters page OK'd ("Yes, ship all"). Card, verbatim: "the total is still the
regional number so we should only use that but then the exemption overrides it only for that species but will still
fill up the trout total quota ... the total is 5 not 20 or 25 ... it should say something like 5 with exception to 20
brook trout which also uses up your 5 quota." Card leads with the region's cap; the own number shows as an exception
for that fish that counts toward the cap. Catch confirmation pending (see OWNER-TODO 12:29).

2026-11-17 12:32, owner: "For now say legal ( need to ask about this with co tho)" (15 brook + 5 cutthroat). Engine unchanged; card leads with the region cap and shows the own number as an exception counting toward it.

2026-11-17 13:15, owner: box head "Lead with 5 (Recommended)" ("Keep 5, brook trout up to 20 ..." not "Keep 20 in all"); Kootenay, Peace and Lardeau cards "Yes, they read right". Then merge with the Nahwitti vector.

## 2026-11-17 13:30: built, merge-ready on lane/t834-share-lift (worker; not merged, not released)
- RULE ("Strict everywhere", owner 11:06; RULINGS T17, T3a revised): a water row's own daily quota N above a region cap
  C (the trout/char total; on a stream also the "from streams" share) lifts its fish out of C and C is raised to N over
  every fish it holds; N <= C changes nothing. Bundle: `bundle.rules.lifts._cap_lifts` / `row_caps` (`basis:
  own_quota`, flag `OWN_QUOTA_ABOVE_A_CAP_RAISES_IT`); answers: `rows.conds.pools` (the F17 raise, in force only while
  the lifting quota is), read by the keeping engine. Corpus: the share lifts on `lardeau_river.r5` / `duncan_river.r5`
  dropped (bull trout 2 = the share's 2).
- CARD (owner 12:29 / 13:15): one box per raised cap, leading with the region's number — top line "Up to 5 today, or 20
  if they’re brook trout", box head "Keep 5, brook trout up to 20", card "they count toward the 5; past 5, brook trout
  only" (`rows.groups.A_RAISED_TOTAL_IS_ONE_BOX`, `trout.table.A_HEAD_LEADS_WITH_THE_REGION`, `A_RAISED_SHARE_IS_A_BAND`,
  `A_TOP_SAYS_WHAT_THE_OTHER_FISH_COME_TO`, `A_WILD_QUOTA_HOLDS_EVERY_FISH_OF_ITS_RULE`); Kaslo / Keen's bull trout inside
  the 2 from streams (`tree.AN_EQUAL_OWN_TIER_SITS_IN_THE_SHARE`); a lone kind inside its share is its row
  (`rows.groups.A_ONE_KIND_INSIDE_ITS_SHARE_IS_ITS_ROW`: the Duncan on Jan 15). No worked keep-N / kept-elsewhere lines on
  a raised total (the other-waters question is the owner's to BC Fish and Wildlife); the trout check proves the head's
  first number against what the other fish come to, the card gate a raised total as the water's number.
- PROOF (side build over the live atlas, a reach-only rerun for the corpus edit): card gate 0; `card_check --all` with the
  trout table 5,947 / 5,947, 0 problems; `test_t834_share_lift.py`, `test_t834_strict_baskets.py` (mutation-pinned).
- Also: `test_trout_check.py` FOUND no longer pins `A_SHARE_HOLDS_ITS_UNLIFTED_FISH` on Kaslo (no live case left) and
  builds one model per water.
