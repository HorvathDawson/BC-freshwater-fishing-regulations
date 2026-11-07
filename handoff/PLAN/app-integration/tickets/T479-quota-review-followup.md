# T479 — T475 follow-up: "How to tell hatchery", "Exception to regional quota", examples fold (owner 2026-11-07)

Status: IN REVIEW — 31dd63547. Owner (review of T475): "The new quota is better but now how to tell (for hatchery) should say it is how
to tell hatchery instead. Also if a quota is bigger then the regional quota it should say exception to regional quota. Also
the how it adds up section I think should be turning into a collapsed section and change it to examples of how to
interpret regulations." (Filed as T479: T478 was already taken by the 0.6.8 walk.)

1. `WORDS.HOW_TELL` = "How to tell hatchery ›" — the one word both places draw (GroupCard's origin `HowTell`, tree.ts's
   steelhead kind link). It always opens the hatch sheet, so it always means hatchery.
2. Pipeline (`answers/tree.py over_region`, cascade stays in Python): a water's own tier, or a water's own total, carries
   `over_region: true` where one of its kinds may be kept above the take of a regional / B.C.-wide QUOTA (pool or subcap,
   not a size cap) that a rule of the same entry LIFTS or DISPLACES for that fish (the ladder state). Fixtures: Vedder 4
   hatchery rainbow (over the 2 from streams), two-lifts row, Polley Lake 8 (over Region 5's 5); not a lake's 2 rainbow
   "part of the 4", not equal (4 = 4), not the Bella Coola's 1. App: `TierVM.exception` / `GroupHeadVM.exception` =
   "Exception to regional quota", drawn as a kicker over the line (`kicker:exception`). Schema additive (answers/2).
3. `WORDS.INTERPRET_EXAMPLES` = "Examples of how to interpret regulations" (was HOW_IT_ADDS_UP); ui-native `Worked` is a
   fold like the Methods Reference fold, closed by default (`worked-summary`, aria-expanded).

Tests: `pipeline/tests/test_answers_t479_over_region.py`, `core/regs/t479.test.ts`, `ui-native/regs/T479Wording.test.tsx`;
VM fixtures re-cut (harness). Proof pending: owner's look at the shots, a scratch rebuild (from the T468 reaches) for
the flag, then a release.
Size: S-M.
