# T1020 — answers_diff: 28,714 licence frames differ on `alien_closures[].rule` across the 0.8.4 → production release pair

Status: DONE (59666231b): a tool gap, not a real change.

Source: T809's cutover diff run, 2026-11-22 (lane L13): r-fec3fab4c5aa → r-741815c6090d, same atlas. One "other" group
holds 28,714 sections (314 waters, 26,921 unnamed stretches): path `alien_closures[0].rule` (and `[1].rule`), before and
after both reading as no licence line. Example: Ace Creek (`gnis:36488`), section 360804586:0.
## Work
1. Decide which it is: the tool does not resolve an `alien_closures` rule ref the way it resolves gear and licence refs
   (a tool gap: fix `answers_diff` so rule ids compare), or a real change in what the stored licence frame holds (then
   say which release change did it, T789 / T973 / the packed formats T686).
2. If real: say what a visitor sees (nothing, if both print no line) and whether it needs the owner.
## Acceptance
- A rerun of the same pair shows the group explained (resolved to ids and equal, or named as a real change), with a test
  that plants the difference and sees it reported once.
Depends: —. Size: S. Owner: no.

## 2026-11-24: done (lane L9, 0.9.1 batch)
- A TOOL GAP. `alien_closures[].rule` is a rule's export index (T789 G8) and `answers_diff` resolved no rule ref in the
  licence frame. r-741815c6090d has 3,405 rules to r-fec3fab4c5aa's 3,404: one more before index 2145, so each of the 28
  interned closure rows names its rule one index later (Bulkley `bulkley_river.r1` 2145 -> 2146, Zymoetz `r10` 2428 ->
  2429, ...). Resolved to rule ids, the 28 rows are the same set on both sides. Nothing a visitor sees changed; no owner
  decision.
- Fix: `RULE_KEYS["licence"] = {"rule"}`, so the licence net compares rule ids.
- Rerun of the same pair (heavy.sh lane 3, 175 s): 10 groups, 1 of 1,957,010 sections (Lost Lagoon only). The 28,714
  group is gone.
- Test `test_t1020_a_renumbered_alien_closure_rule_is_no_change_and_a_new_rule_at_the_old_index_is_one`: A gets rules P, Q
  appended and B gets Q, P. The same closure at another index is empty. Another rule at the SAME index is one "other"
  group on `net:licence`, which reading the index alone would miss. Mutation (the licence key removed): the first case
  is reported and the second goes silent, so the test fails both ways.
