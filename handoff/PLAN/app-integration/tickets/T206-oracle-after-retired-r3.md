# T206 — App golden parity after the retired overlapping rules (z1/z2 r3, Khartoum/Lois r3)

Status: OPEN

Source: overlapping-rules.md, accepted 2026-10-26 and applied (decisions.md). The release r-3d6399afd7e2 drops
`z1:trout_quota.r3`, `z2:trout_char_quota.r3`, `khartoum_lake.r3` and `lois_lake.r3`. The binding diff shows only those 4
rules changed. card_check found 0 problems, test_q44 passes, and the full pytest suite is green apart from the
pre-existing test_review_vintage (aff6039d).

## Problem
`pnpm -C app check` has 62 failures in 13 conformance files (plus the known release-budget red, T203). They come from
**page v36's frozen oracle** (`app/fixtures/golden/hydrate.json`, `golden.json`, `page_v36.html` #data.rid), which still
prints the retired rules:
- settle / card / describe parity: `oracleRixOver` throws on the retired key ("a content change — re-cut").
- sheets "Overruled (4)" -> "(3)" on the Region 1/2 trout source sheets: vedder_515/1010, chw1/2, cow1-3, wilkes, iron_720/1010,
  x15p2/p3, x16p0-p8, x23p0, x37p0-p2.
- Lois (x25p_1010): the "hatchery rainbow = 6" row folds into the RB/ST aggregate row. The head now reads "Rainbow trout and
  steelhead" instead of "Steelhead"; affects keep/grouphead/grouplines/others/tail/plainrow/pins.
- T04 rule_ids_sha256 against the oracle.

The answers did not change (card_check passed). Only the oracle's wording and rule numbers predate the change.

## Options
1. An `ORACLE_RETIRED` amendment, the inverse of settle's `ORACLE_PREDATES`. Drop the 4 keys from the oracle's rix lists
   centrally (oracles.ts), and add keyed exclusions (exclusions.ts) for the sheet fold counts and Lois's rows.
2. Re-cut the oracle (`page_data --html` + `port/hydrate.mjs`). The 2026-10-22 attempt broke 275 tests and was reverted (T63),
   so this needs its own lane.

Already re-pinned: `inputs.release.test.ts` ape_501 is now looked up by key (rix 1644 -> 1642), and the pytest Lois year strip
uses `OVERLAP_RETIRED`.
