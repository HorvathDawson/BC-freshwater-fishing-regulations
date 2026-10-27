# T206 — App golden parity after the retired overlapping rules (z1/z2 r3, Khartoum/Lois r3)

Status: DONE (66a0aa70) — re-cut on r-f6918815ca78

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

## 2026-10-26 — after the zone daily-quota merge (release r-daf44e264890)
Done: `oracles.ts ORACLE_ENTRY_RENAMED` renames page v36's zone-quota entry ids at load (golden.json, hydrate.json,
#data.rid) — the merge kept every rule_id and the binding diff showed the same sections under the new keys. Taps,
regs-fixture and harness re-cut on r-daf44e264890.

BLOCKED (owner call): the merge also moved Region 4's Other fish rows. Bass, pike, walleye, yellow perch closures tie on
rank between `z4:invasive_species_notice.r1` and the quota box's own "0, CLOSED" lines; the tie breaks by key order
(`P.cands`, rix order), so `z4:daily_quotas` (d) now wins where `z4:species_quotas` (s) lost to `invasive_…` (i). The
answer is the same (closed) but the one folded row "Largemouth bass, northern pike, smallmouth bass, walleye and yellow
perch" becomes 5 rows citing the box lines (glenn, klwa fixtures; sheets/others/keep reds on Region 4 states). Regions 3
and 5 (`bass_perch_illegal`, `bass_illegal`) still sort first and are unchanged. Choose: accept it (re-pin as a reasoned
exclusion) or make the tie-break name-independent (code, then deliver + re-cut). The retired-r3 amendment
(`ORACLE_RETIRED`) waits on that call so both land as one set of exclusions.

## 2026-10-26 (late) — tie-break fixed; the rename is the bigger part (still OPEN)
- Done (3797095c, built in r-d32f814feac0): `rows/evaluate.py` `closure_order` breaks a closure tie without entry
  names. One entry's rules keep rule order. Between entries, a standing closure comes before a dated one, then the
  closure naming more fish. Region 4's folded Other-fish row is back (glenn/klwa fixtures). Fraser and z3 are
  unchanged (`test_t206_closure_tie_break.py`, year-strip live green).
- Found: the zone daily-quota rename (bcaa5c79) moved the STORE's rix order (`zN:daily_quotas` now sorts ahead of its zone
  siblings). Page v36 prints lists, species and rows in rix order within ties, so `pnpm -C app check` has about 157
  conformance reds: settle 112, card 50, sheets 34, methods 7 and a few others. Most are order-only, e.g. x5p0 broad
  `3122,3137` vs `3137,3122`, or x19p0 spp `ST,RB` vs `RB,ST`. Also: Lois/z1/z2 r3 throw in `oracleRixOver`;
  Cowichan's `inpart` → `speaks` (drop_on_parts); the atlas rebuild renumbered handles pinned by
  `waterGroup.fixture`, `WaterMapView`, `unnamed.release` (H_ATLAS 15745aa46e2f0c0d) and `SheetHost`.
- Tried and reverted: re-sorting each oracle run by store rix fails, because `broad` is most-fish-first, not rix
  order. Striking the retired rules from hydrate.json also breaks describe's 726-rule pin. Both need their own lane.
  The choice is either an ORDER-ONLY amendment that names the renamed keys (compare the renamed-entry rules as a
  set, everything else in order) or the oracle re-cut (option 2).
- The handle re-pins need the OLD fixture's handles mapped to the new ones by section id. `atlas/full.prev` is not
  the pinned atlas (the 380887xxx added streams differ).

## 2026-10-27 — DONE (66a0aa70)
Amended the oracle at load (oracles.ts), with no re-cut. `REGS_STORE_REQUIRED=1 pnpm -C app check` is green apart from
release-budget (T203).
- ORDER: `flipsOver` and `renamedOrder`. A pair may differ in order only where the rename swapped the two rules' store
  numbering. This covers the settle lists, card broad, card spp (by the rules naming the fish), and the Region 4
  Other-fish sheet links and zone lines.
- `ORACLE_RETIRED`: the retired rules are dropped from the oracle lists, and describe keeps its 726 pin. `retiredRuled`
  rewrites the r3 lines in the golden (23 trout source sheets, x37p2's where-not and its r3 sheet). `LOIS_FOLD`: the card
  rows fold, and the golden/pins group r2 "Steelhead" becomes r1 "Rainbow trout and steelhead".
- `ORACLE_ON_PARTS_DROPPED` (c16b1c5f): inpart → speaks, part null. The six new Boats blocks are pinned.
- Re-pins: unnamed H_ATLAS 15745aa46e2f0c0d, the waterGroup/WaterMapView handles (each checked by section id in
  atlas/full), and the ticket literals. The taps, harness and regs-fixture were re-cut. SheetHost reads a slice of the
  ruled golden.
