# Round 20h data: a bait allowance names its ban, possession said once, each card's own possession example (2026-10-19)

gear 10 → 11, rows 24 → 25, glossary 3 → 4. ANSWERS-SPEC ROUND20h. Scratchpad (before/after PNGs, reader notes, logs):
/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h-data/

## 1 Bait: the fact differs from the brief
- The Vedder's Oct 10 roe allowance is **not** about the May 1–31 bait ban.
- That ban is `chilliwack_vedder_rivers.r5`, `ban: any_bait`. It bans roe too: on May 15 every bait element is banned (`bait_ban`).
- So the suggested "May 1–31: bait ban — roe still OK" would be **false**.
- The roe "still OK" is the exception in the province's year-round fin-fish ban, which is in force on Oct 10: `zp:bait.r1`
  "The use of fin fish (dead or alive) or parts of fin fish other than roe is prohibited throughout the province" (p.8;
  `ban: [fin_fish], except: [roe]`). `z7b:bait.r2` is the same rule for Region 7B.
- No dated bait rule in the export carries an exception (scanned all rules). The live frames have 464 + 19 `banned_roe_ok`
  rows, all from those two rules.
- Fix: the allowed element's row carries `excepted_from` {element, by}, the banned element and the clause whose ban excepts it.
  It is set only on the days that ban is in force; on a bait-ban day roe is banned and has no `excepted_from`.
- The page should name the ban, e.g. "not part of the fin-fish ban", never a bare "the ban".

## 2 and 3 Possession: the book cite
- p.21 (Region 2): "Possession Quotas — Possession quotas = 2 daily quotas".
- p.80: "possession quota: … In most instances, the possession quota is two times the daily quota".
- p.80: "daily quota: the maximum number of fish of a given species, group of species, or size class that you may keep in one
  calendar day".
- Region 2's table reads "Trout/char: 4, but not more than … 2 from streams". The daily quota is the 4; the 2 limits where
  some of them may come from.
- So possession is 2 × 4 = 8, never 2 × 2.
- `group.possession` {per_daily, of, n, by, example, doubles} takes the ONE possession rule holding every kind of the group
  and applies it to the group's total. The gate proves n = of × per_daily, that `of` is the total, and that the multiplier is
  the rule's own (mutation-tested).

## Strings changed (old → new), for Fable
| field | old | new |
|---|---|---|
| gear `bait[]` roe row (Vedder Oct 10) | — | new `excepted_from` {element: "fin_fish", by} |
| glossary `possession_quota.says` | "The fish you have with you at any time — in camp, a cooler or a vehicle: 2 days' quota. Fish at home don't count." | "The fish you have with you at any time — in camp, a cooler or a vehicle." |
| glossary `possession_quota.example` (fallback) | "With a daily quota of 5 trout you may have 10: 2 days' fish, not 3." | "With a daily quota of 5 trout you may have 10: 2 days' fish." |
| rows `group.possession.example` (new) | — | Vedder "With 4 trout and char a day you may have 8: 2 days’ fish." (each card from its own total) |
| rows `group.possession.doubles` (new) | — | Vedder "Possession doubles the 4 a day, not the 2 from streams." (null with no share under the total) |

The possession sheet should show the card's `group.possession` example and doubles in place of the glossary's generic
example. The plain line "Possession: 2 days’ quota. Fish at home don’t count." is unchanged.

The 20h before-reader found "not 3" in the example puzzling, so it was dropped from both examples.

## Comprehension (reference page v36, 390 px, fresh opus readers, PNGs only)
The reference page now draws `group.possession` under the card, and the roe tile says "the fish or fish parts ban excepts it".

| Q | before | after |
|---|---|---|
| roe today? | yes, 5 | yes, 5 |
| a bait ban today? | no (inferred), 4 | no (inferred), 4 |
| roe is an exception to which ban? | not said, 2 | the fish-or-fish-parts ban, 5 |
| cooler after 2 days | probably 8, 3 | 8, 5 |
| which number is doubled | not said, 2 | the 4, 5 |
| sheet says it twice? | yes, 4 | no, 4 |

Left for the page: the sheet's generic 5 → 10 next to the card's 4 → 8; show the card's example instead.

## Checks
- `pipeline/tests/test_answers_r20h.py`: 5 tests. 4 failed before the change; the gate-mutation test was added after.
- Pins moved in test_answers_{r20g,tidal,v22,28_live}.
- `model --check` rc 0. Live build rc 0 (twice). Card gate 15,862 groups / 7,144 cards, 0 problems. Page v36 regenerated.
- Fast suite, plain pytest via heavy.sh, run to the end after all pin moves: **3441 passed, 6 skipped, 0 failed**.
