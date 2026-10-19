# Round 20g data: the origin rule where its scope is, fewer steelhead words, the possession line (2026-10-19)

Owner items 1–3 (data side); rows 23 → 24, display 15 → 16, glossary 2 → 3. ANSWERS-SPEC ROUND20g. Scratchpad:
/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20g-data/
(before/, after/ PNGs; reader-before.md, reader-after1.md, reader-after2.md; tests-before.txt; build/gate/suite logs).

## 1 The origin rule: the fact, verified
The owner is right. Printed p.21 (PDF p.23), Region 2 Daily Quotas: "Trout/char: 4, but not more than • 1 over 50 cm (2 hatchery
steelhead over 50 cm allowed) • 2 from streams (must be hatchery) • 1 char …, none under 60 cm. And you must release: • Wild
trout/char from streams • All wild steelhead • Hatchery trout/char under 30 cm from streams. NOTE: There is no general minimum size
limit for trout in lakes". The rule row agrees: `z2:trout_char_quota.r6` "Trout and char — release all, from streams, wild only",
`daily@origin=wild&water=stream`, `water` stream. It is the winner of every Vedder kind's wild verdict. The total `.r1` has no water.
So wild trout and char may be kept from Region 2 lakes.

Change (general): `card.origin_place` checks the rules that send the other origin back for every kind. If they all hold on the share's
kind of water only, and the total counts every water, the share tier carries `origin` and `group.origin` is null. The place word
comes from `tree.share_place`: "streams" for a region's or the province's rule, "this stream" for a water's own rule.
`TreeTier.origin` is allowed on a share tier only. The group keeps its origin where the rule holds wherever its total does: a
water's own total, such as the Bella Coola's own 1 a day or a creek's own 2. In the live answers file the share wording occurs 17 times and the group
wording 8 times ("… wild trout go back" 6 of those). These are occurrences of interned strings, not group counts.

| where | before | after |
|---|---|---|
| `group.origin` (Vedder, Region 2 streams) | "Keep hatchery fish only — wild trout and char go back." | null |
| share tier `origin` (new field) | — | "From streams, keep hatchery fish only — wild trout and char go back." |

## 2 The steelhead row
| field | before | after |
|---|---|---|
| kind `size_note` | "it counts toward the 1 over 50 cm (20 in), but once you keep one, a second fish over 50 cm (20 in) may be kept" | removed (field gone from the model). The band head "… — 2 if one is a hatchery steelhead" says it once |
| kind `sentence` (ST) | "Of the 2 a day: … rainbow trout over 50 cm (20 in) — it counts toward the 1 …" | "Of the 2 a day: up to 2 may be hatchery steelhead, rainbow trout over 50 cm (20 in)" |
| duty | "after you keep 2, stop fishing for the day" | "stop fishing here after 2 a day" |
| duty | "write each one on your licence right away" | "record each on your licence right away" |
| presence (known, big rainbow is a steelhead) | "Steelhead are known to be in this water, so a rainbow trout over 50 cm (20 in) counts as a steelhead." | "A rainbow trout over 50 cm (20 in) here counts as a steelhead." |

The words are the same on every steelhead water. Fixture cards checked: Vedder, Vedder May, the plain Region 2 stream, two-lifts,
count creek, Fraser. Live: the Chilliwack and Bella Coola cards were shot. "here" in the stop duty is the book's "stop fishing that
water". The first cut, "stop fishing after 2 a day", made a reader ask whether it meant all fishing (confidence 5 → 4). "here" brought
it back to 5. "right away" stays because the book says to record immediately.

## 3 Possession
| field | before | after |
|---|---|---|
| `display.rules[].plain` (per_daily) | "Possession: twice the daily quota (fish at home don’t count)." | "Possession: 2 days’ quota. Fish at home don’t count." (1 → "the daily quota"; "… for kokanee." where the rule names fish) |
| possession limits with a number | "Only 1 lake trout over 60 cm (24 in) in possession (fish at home don’t count)." | "… in possession. Fish at home don’t count." |
| glossary `possession_quota.says` | "How many fish you may have at any one time — in your camp, cooler, vehicle or on the trip home: two days' quota, unless a table says otherwise. Fish at the place you normally live (your ordinary residence) don't count toward it." | "The fish you have with you at any time — in camp, a cooler or a vehicle: 2 days' quota. Fish at home don't count." |
| glossary `example` | "With a daily quota of 5 trout you may hold 10: two days' fish at camp is fine, a third day's is not." | "With a daily quota of 5 trout you may have 10: 2 days' fish, not 3." |
| glossary `quote` | the p.80 definition, verbatim | unchanged, verbatim. The page puts it in a collapsible "In the rule book's words" fold (the reference page does this now) |

## For the UI agent (regs-v11.html / check.mjs pins, re-snap)
- `group.origin` is null on Region 2 stream cards. Draw the share tier's `origin` under the share's line, with "How to tell ›" and
  Rule book p.21 (the share's `by`). Where `group.origin` is still set, it stays at the head.
- The possession foot is now two sentences. The posrow regex
  `^…(Possession)\s*:\s*([^(.]+?)\s*(?:\((.*?)\))?\.?\s*Source$` will no longer match. Value "2 days’ quota"; the exception is
  "Fish at home don’t count."
- Possession sheet: `o.says` is the plain line. Draw the term's `says` and `example`, and put `quote` in a `<details>` fold.
- The steelhead kind has no `size_note`. Duties and presence words are as above.

## Comprehension (reference page v36 at 390 px, PNGs only, fresh opus readers)
The reference page now draws `group.tree`: tiers, the share's `origin`, kinds, duties and presence (`treeBlock` in page_v36.js).
Before = that page on the rows-23 data; after = the same page on rows 24.

| Q | before | after (final) |
|---|---|---|
| wild cutthroat from a Region 2 lake | "probably no", WRONG, conf 2 ("Keep hatchery only" under "lakes included") | "probably yes", right, conf 2 (inferred from "From streams, …") |
| wild cutthroat from the Vedder | no, 5 | no, 5 |
| hatchery steelhead toward the 1 over 50; another allowed? | yes / yes, 4 ("counts toward… but…" read as a contradiction) | yes / yes, 4 (from the band head + worked line alone) |
| steelhead a year | 10, 5 | 10, 5 |
| cooler after two days | 8, 3 | 8, 3 |
| after 2 steelhead | stop for the day + record, 5 | stop fishing here + record right away, 5 |

Still confusing, logged and not fixed here:
- Possession: the readers can't tell which daily number is doubled, the "2 a day from streams" headline or the 4.
- Nothing says in so many words that wild fish may be kept from lakes. It is inferred from "From streams".

## Checks
- `pipeline/tests/test_answers_r20g.py`: 7 tests. 4 failed before the change (tests-before.txt) and pass after.
- Pins moved in test_answers_{23,28_live,display,r13,r14,r15,r18,r19,rows_fixes,tidal,ur1,ur2,v22}.py and test_moments.py.
- `model --check` rc 0. Live build rc 0 twice. Card gate: 15,862 groups / 7,144 cards, 0 problems. Page v36 regenerated.
- Fast suite, plain pytest via heavy.sh: 3434 passed, 2 failed, 6 skipped. The 2 failures were live-data pins (the possession
  sentence check, display version 15). Both were moved and pass; the full suite was not re-run after that.
