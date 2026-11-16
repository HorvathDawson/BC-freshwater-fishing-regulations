# T793 — Wild-trout limits are missing from the keep card on at least 15 lakes and on the Kitimat and 23 streams under it (the app allows what the book forbids)

Status: OPEN (pipeline display model: the stored `group.buckets` the card draws; first to fix).

Source: general audit 2026-11-16, answers checker A1 and A2 (`handoff/PLAN/audit-2026-11-16/answers.md`); the same fault as T782 item 1.

## What is wrong
The book: Comox Lake "No wild rainbow trout over 50 cm" (p.15); Sproat, Great Central, Makii; Cultus, Harrison (p.23),
Pitt (p.25) "No wild trout over 50 cm"; Chehalis, Chilliwack, Lillooet, Lois, Ruby, Windsor; Dodd Lake "Wild
trout/char daily quota = 2 (no wild trout over 40 cm)". The card says "Up to 4 … any size; up to 1 over 50 cm":
"wild" is nowhere in the keep section. Kitimat River and 23 streams, Jul 1 to Oct 31: "Up to 5 rainbow trout" without
"hatchery" (p.51: hatchery rainbow 5; wild rainbow are 1 from streams, none under 30 cm); on Jan 15 the same card
says "hatchery".
The parse and the stored row are right (`comox_lake.r1`: RB, `lengths [{min_cm 50, take 0}]`, `origin: wild`; the
row's `conds` and `caps` hold it); `group.buckets`, which the card draws, holds only "any size" and "only 1 over
50 cm". It shows only under "See every regulation line".

## What to build
- The origin condition (wild / hatchery) reaches the card's tree wherever a stored row has one; a build check: a row
  with an origin condition whose card tree says neither "wild" nor "hatchery" stops the answers build.
- The sweep found 14 lakes plus Dodd by hand and the Kitimat set; it is a floor (it misses cards that say
  "hatchery" for another fish). List every answer key the fix changes; that list is the review.
