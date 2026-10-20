# Round UR5 data: the panel's one data word (2026-10-20)

rows 26 → 27. ANSWERS-SPEC "ROUND UR5". Source: ~/.claude/projects/…/handoff/PLAN/USER-ROUNDS.md "Round UR5", item D1.

| where | old | new |
|---|---|---|
| worked kept-elsewhere question, over the size (`rows.over_elsewhere`) — Kootenay Lake main body | "Kept rainbow trout or cutthroat trout over 50 cm (20 in) elsewhere today?" | "Kept cutthroat trout over 50 cm (20 in) elsewhere today?" |

Why: Region 4's clause "Only 1 rainbow trout or cutthroat trout over 50 cm a day" reaches the main body's trout and char group only
for cutthroat (the group's subcap `of: ["CT"]`); rainbow has its own row there (10 a day any size, 20 a year over 50 cm, the
lake's rule replaces the clause for them — the card's own "Rules that don't apply here"). The question named the rule's fish, so
Linh (ESL) and Doug (retiree) read a daily cap of 1 big rainbow (conf 2–3, both before and after the page fixes). Where the clause
holds every fish its rule names, or the rule names a group, the rule's own words stay.

Checks: pipeline/tests/test_answers_ur5.py (1 of 2 fails before: scratchpad ur5-data/tests-before.txt; the second pins the
unchanged case); page check ur5-elsewhere-names-here (fails on the rows-26 snapshot). Chain: ur5-data/chain.sh (answers tests,
build, card gate, page v36, snap, oracle); fast suite unfiltered after.

Results: answers tests 387 passed; model --check rc 0 (generated files unchanged); build rc 0; card gate 15,862 groups / 7,144 cards,
0 problems; page v36 regenerated; re-snapped (oracle 18 groups / 3,100 baskets); check.mjs 122/122; fast suite unfiltered 3449 passed,
6 skipped. Re-test (fresh opus, PNGs only, Kootenay main body after screens): Linh L1 conf 3 → 4 and no "1 big rainbow a day" claim;
Doug D1 conf 4 with the second-big-rainbow doubt gone. Both still want the KL rainbow stamp on the Keep tab (UR5 D2, owner).
