# F9: the 72 unexplained rows in test_answers_reference (a ruling for the owner/lead)

Run on 2026-10-20 against the main checkout's live data, read-only: bundle + verdicts
`data/generated/bundle/`, export `data/generated/regs/`. The golden was built fresh into this
worktree's `data/generated/scratch/answers_reference/4a98db20e5ed3618` (JOBS=2, about 10 min).
The run reproduced the reviewer's result exactly: `test_port_on_the_pages_own_ladder` FAILED
with `{'row unexplained': 72}`, 11 other tests passed.

The port runs `rows.py` on the page's OWN ladder, and every decided answer on these waters
matches the page. So all 72 are card-arrangement differences in `real_daily` only, with nothing
coming from the ladder. They have **two causes**, and both are **(b): a documented page bug the
port has no detector for**. None is a defect in our answers layer. None is a data/vintage
difference (c): the golden and the port read the same export and bundle.

## Cause 1: Kitimat River (gnis:3225), 24 rows. Page F1 ("rest" item), with a gap in the port's F1 detector

- **Rows:** pool `z6:trout_char_quota::trout_char_quota.r1`, parts 1005, 907 and 1201. The
  sample dates are 07-01 to 10-15 (8 dates x 3 parts), which is exactly the window of Region 6's
  "1 trout from streams Jul 1-Oct 31" (`trout_char_quota.r4`, the row's `narrow`).
- **Page:** `real_daily [n 5, all false, sum null, cappedSum 5, rb false]`.
  **Ours:** `[5, false, null, 6, false]`. Our capped list is ST 2 (xref, Kitimat r5) + LT 3
  (sub-limit r3) + **GB/CT 1** = 6. EB and RB are open.
- **Root cause:** page_v35.js `speciesItems` builds items only from `row.groups` and the
  conditions that name fish. Every other keepable fish goes into one trailing item,
  `rest = [EB, GB, CT]`. `quotaLines` then applies the stream share only if
  `it.members.every(S => covers(nw, S))`. Brook trout (EB) is a char, and r4 has
  `species_except: [CHAR]`, so r4 does not cover it. The whole rest item therefore keeps 5 and
  counts as open, even though cutthroat and brown trout (CT, GB) can be kept only 1 at a time here (their
  decided answers say `daily 1`, `narrow r4`, the same on the page and ours). This is exactly
  rows.DECISIONS **F1**: "an item showed one range for fish with different ones". Ours
  (`rows.species_items` / `fish_bands`, one item per kind) splits GB+CT (bands `[30, inf, 1]`)
  from EB (`[30,50,5],[50+,1]`). That split is correct.
- **Why the port calls it unexplained:** `port.classify_row` turns F1 on only when the page's
  items overlap, or when a condition has `except == ["RB"]`. Its `page_items` list leaves out the
  page's `rest` item, so this form of F1 has no feature.
- **Ruling suggested:** (b), the F1 class that already exists. The detector should model the
  page's `rest` item (row `allMembers` not named in any group or condition with fish, keepish),
  and flag F1 when our items give its fish different bands. Mutation: the same Kitimat parts
  outside Jul 1-Oct 31 (no stream share, so the rest item is uniform) must not be flagged.
- **Not the cause:** the implementer suspected the general cap's `except: ["ST"]` (F12, against
  Region 6's "quota includes hatchery steelhead"). It does not change `real_daily`.
  `_shared_caps` drops ST on a steelhead water anyway (the page's `isSteelRiver()` does the
  same), and the "1 over 50" cap binds nothing here (every capped `lo` is 30). The except itself
  comes from `_not_counted`: in the page's own ladder, ST hatchery shows r2 as
  `lifted by kitimat_river.r5`. Whether that lift is right is a ladder question, outside this
  test. It is noted for the owner, not ruled here.
- **Side note, not a cause:** our `capped_sum` (6) is above `n` (5) because open kinds exist
  (`all: false`, `sum: null`). The page allows the same shape (`eff_cap` and `effCap` return
  null only when nothing is open). The app does not read `real_daily` (only the generated type
  in `answers.generated.ts`), so this has no display effect today.

## Cause 2: Gotchen Lake (wbk:329184482) and Phillipps Lake (wbk:329123638), 24 rows each. Page ignores an orphan count clause (rows.DECISIONS F16, RULINGS Q2), and there is no port class

- **Rows:** Gotchen, pool `r5:gotchen_lake@5-15::gotchen_lake.r1` ("Trout/char daily quota =
  2"), part 1596, all 24 sample dates. Phillipps, pool `r4:phillipps_lake@4-23::phillipps_lake.r1`
  ("Trout daily quota = 2"), part 1538, all 24 dates.
- **Page:** `real_daily null`. **Ours:** `[2, false, null, 1, false]`, with `capped: [DV]` and
  every other kind open.
- **Root cause:** both rows carry an `orphan` fact for DV: Region 5's
  `trout_char_quota.r4` "1 Dolly Varden/bull trout", or Region 4's `trout_char_quota.r4`
  "1 bull trout (Dolly Varden) of any size". It is a count clause of the zone table, and it
  still speaks for the fish under the lake's own pool. Ours (`rows.fish_bands`, the `orphan`
  branch) caps DV at 1, as DECISIONS **F16** says ("An ORPHAN count clause … caps the fish's
  bands like a sub-cap", RULINGS **Q2**: the clause naming the fish beats the lake's group
  number). `test_answers_28_gate_classes.py::test_c_an_orphan_count_clause_caps_its_fish_under_a_larger_water_pool`
  pins this on Polley Lake. page_v35.js `quotaLines` caps the base only from
  `['outercap','subcap']` and never reads `orphan`. To the page, DV keeps 2 like every other
  kind, so nothing is capped and `effCap` returns null.
- **Ruling suggested:** (b). Add a documented PAGE_BUGS class (name it **F16**): "an orphan count
  clause (another table's, still speaking) not read as a cap on its fish". Its feature: a capped
  fish of the row has an `orphan` fact with a take. Mutation: the same row with the orphan fact
  removed must stay unexplained.

## Would the test break Tier 2?

Yes. `scripts/test-full.sh` step 3 runs `pytest -q -m slow`.
`test_port_on_the_pages_own_ladder` is marked `slow` + `needs_bundle`, so that step selects it,
and as committed it fails on these 72 rows, so Tier 2 ends `FAILED: pytest slow`. A first run on
the main checkout also builds the golden into `data/generated/scratch/answers_reference/` (6-10
min, 2-4 node workers, about 45 MB) before it fails. Step 2 (`pytest -q`, default
`-m "not slow"`) deselects it, and Tier 1 is unaffected.
