# T139 — A third headless one-kind pool appeared: Liard River watershed (r7, 7-53)

Status: DONE — d69e1407 (needs `deliver` to ship).

Source: `pipeline/tests/test_answers_one_kind_group.py::test_every_shipped_headless_water_is_drawn_but_the_two_open_shapes`
(needs_bundle) fails on the current bundle: besides the two known open shapes (Duncan DV, z8 r5) a third headless pool is
shipped — `r7:liard_river_watershed_see_map_on_page_63@7-53::liard_river_watershed.r2`.

## Work
Find which change made the Liard pool headless (T80 one-kind groups, A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW, T104/T128
wording, or a data change), decide whether it is a data/ruling defect or a third legitimate open shape, fix at the cause
(or add it to the known shapes with the reason), and keep the card gate at 0 problems.

## Acceptance
The needs_bundle test passes on a fresh deliver; the Liard card reads correctly (name its lines before/after).

Depends: none. Size: S. Owner: no.

## As built (2026-10-23, d69e1407)

**Where:** one water, Beattie Lake (wbk:329665706, r7 7-57, inside the Liard watershed), on 3 card frames: the bull trout
keep days (May–Aug 14 and Oct 16–31; the lake is closed Nov 1–Apr 30, and bull trout are catch-and-release Aug 15–Oct 15).

**Cause: T127, not T80 or the wording.** The reader lets the Liard's "bull trout quota = 1 (none under 30 cm or over
50 cm)" (liard r2) displace the lake's own "trout quota = 1 (none over 50 cm)" (beattie_lake.r2) for bull trout (step 4,
`ladder`). Both count toward Zone B's 5 (z7b trout_char_quota.r1, `contains`). That total's group sits on the lake's
trout row and already says the bull trout in a bucket for the Liard rule. Before T127, the bull trout row was held
(`parent`) by the lake row through a cross-reference of the **displaced** lake pool. T127 rightly removed that
cross-reference, which left the row pooled, with no group and no parent, so the app drew it nowhere. Proven by mutation:
`A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE = False` gives the row `parent 0` again.

**Ruling: it is not a third open shape.** The Duncan shape is a one-kind row whose total has no other row to carry its
group. Here another row does carry it. Fixed at the cause by a second hold in `rows.parent_of`, the one function that
decides holds: a pooled row whose total (`total_of`) is another rule's is held by the row that carries that total's
group (`produce`'s `home`, passed in the stubs as `total` / `draws`). The cross-reference hold is still checked first,
so every existing parent is unchanged. The flag is `rows.A_ROW_HELD_BY_ITS_TOTALS_GROUP`. The two open shapes are
unchanged: Duncan DV on Crazy Creek when the rest is released, where the DV row is the home of its own total, and the
z8 r5 total that is limited to streams.

**Whole set (every water rebuilt with this code, rows + ladder, against the live bundle and export):** headless
trout/char pools are only the two open shapes: z8 r5 (982 waters / 2,726 frames) and Duncan DV (14 / 28). The new hold
fires on exactly 1 water and 3 frames (the Liard DV row, holder group z7b r1 with a headline and DV among its kinds).
Card gate: 7,180 cards, 16,044 groups, **0 problems**.

**The Beattie card, Jun 15 (Oct 25 is the same without lake trout):**
- Rows, before: row 0 is the lake trout row (EB GB CT LT RB), group Zone B 5, headline "2 a day per angler". Row 1 is
  the Liard bull trout row (DV), `parent None`, no group, so it is headless and core `keep.ts` skips it.
- Rows, after: same, except row 1 has `parent 0`. It is held, and drawn inside row 0's article.
- Lines (identical before and after; the bull trout was always said inside the group):
  - Header "5 a day per angler across Zone B, streams included"; headline "2 a day per angler".
  - "This lake's rule: up to 1 a day (part of the 5)" covers brook, brown, cutthroat and rainbow trout 50 cm or less,
    and lake trout 30–50 cm, "Up to 1 a day between them".
  - "LIARD RIVER WATERSHED (see map on page 63)'s rule: up to 1 Dolly Varden/bull trout a day (part of the 5)", with
    "Of the 1 a day: up to 1 may be Dolly Varden/bull trout, 30–50 cm (12–20 in)".
- The app's draw does not change: before, the held row had no article, and only the stored `parent` was missing. What
  changes is that the store is consistent: no pooled row is groupless and unheld. On the app side (`presOf`), only a
  row with a steelhead `presence` reads `drawn`, and this row has none.

**Tests (mutation-pinned):**
- `test_answers_keep_answers.py::test_3_t139_a_row_whose_total_is_another_rules_is_held_by_the_row_carrying_that_totals_group`
  (no data, whole values). The mutation `A_ROW_HELD_BY_ITS_TOTALS_GROUP = False` fails its first assert. Also
  `test_3_t139_mutation_without_the_second_hold_the_bull_trout_row_is_unheld`.
- `test_answers_one_kind_group.py::test_beattie_lake_bull_trout_row_is_held_by_the_zone_b_group_that_says_it`
  (needs_bundle) asserts the whole row, holder group and Liard bucket on Jun 15 and Oct 25, no DV keep row on Sep 1, and
  a gate of []. The same mutation fails it (holder None), and
  `test_mutation_without_the_totals_group_hold_beattie_bull_trout_is_headless` pins that state.
- The source test `test_every_shipped_headless_water_is_drawn_but_the_two_open_shapes` passes now, rebuilding from the
  shipped file. After a deliver, the shipped scan no longer contains Beattie.

**Needs a deliver to ship** (`python -m pipeline.deliver`: answers only change; no reach, atlas or tiles). Expected diff
in ui-rules-answers.json: `parent: 0` on the Liard DV row of Beattie Lake's 3 frames, nothing else. card-check.json:
0 problems.

Left / follow-ups:
- On the same group, the stored `caps` list says "No Dolly Varden/bull trout may be kept." and "…in Zone B." for the
  under-30 cm and over-50 cm bands, with no size in the words. Whether any surface shows these words unqualified was not
  checked here; worth a look in the card-words lane.

Owner questions:
- **Q-T139a** Beattie Lake: does the Liard watershed's "bull trout 1 (30–50 cm)" sit **beside** the lake's own "trout
  quota = 1", allowing 2 a day (1 trout + 1 bull trout), or **inside** it (1 trout or char in all)? Default built: beside
  (the reader's step-4 `ladder` displacement; headline "2 a day per angler"). If the answer is "inside", that is a reader
  (bundle) ruling change, not a card change.
