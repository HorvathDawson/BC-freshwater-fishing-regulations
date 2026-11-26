# T790 — Records to put right: answers shown as open, stale ledger rows, stalled tickets, readings missing from the rulings file, lost follow-ups

Status: PARTIAL (docs and small tickets; main chat; items 1 and 7 DONE 2b4e0d826; item 4's ledger half DONE c0f8da79c (47 DONE/DROPPED rows to DONE.md); left (Claude): items 2, 3, 4 (stale statuses, missing ticket numbers), 5, 6 (the lost follow-ups still to ticket), 8, 9)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T790 | Records to put right: answers shown as open, stale ledger rows, stalled tickets, readings missing from the rulings file, lost follow-ups | PARTIAL (items 1, 7 and item 4's ledger half done: 2b4e0d826, c0f8da79c; left (Claude): 2, 3, 4 rest, 5, 6, 8, 9) | c0f8da79c, 2b4e0d826 | general audit 2026-11-16, decisions checker (`handoff/PLAN/audit-2026-11-16/decisions.md`) |

Source: general audit 2026-11-16, decisions checker (`handoff/PLAN/audit-2026-11-16/decisions.md`).

Done: item 1 (T754 says the owner's answer "Build it this way (Recommended)", 2026-11-15 round 4) and item 7 (the two
out-of-date production command blocks r-f749475d0962, r-a178299f6005 removed; `release/PRODUCTION.md`: the feeds worker
goes first when the release carries closed areas) in 2b4e0d826; item 4's ledger half (47 DONE / DROPPED rows moved
out of README.md) in c0f8da79c.

Left (Claude), items as the audit listed them:
2. T759: the shared ticket describes the rejected design; the rewritten ticket is on `data/t759-missing-place-check`.
3. 14 owner readings of 2026-11-13 to 11-15 are not in `pipeline/docs/RULINGS.md` (rest-of with "areas and times";
   the tributary readings; lake loops; the T759 rule; one-bank; "Whole river is right"; "4 km as the book says"; the
   Shuswap "use as printed"; Pallant; the Alexandra Bridge; rest-of only where a place overlaps what it lists; the
   T708 dialog answers). Ruling G9 (Pend d'Oreille) holds by hand-typed data only: pin it.
4. Stale ledger rows (statuses): T690, T664, T753, T752, T750, T747, T734 (shipped or landed), T760, T738; ticket numbers
   T744, T745, T748, T766 to T769 have no file.
5. Stalled, waiting for "the next APK / release" that has happened: T261, T292, T310, T390, T461, T535, T562, T585,
   T633, T674, T603, T606, T670, T671, T672, T676, T687, T689. Give each an owner and a step, or close it.
6. Lost follow-ups to ticket: the suggester proposes a new cut where one exists (match by position); the review app
   names the unticked row when Confirm is greyed; the Hope gauge (66 m reads 08MF076) and three gauge cuts never
   placed (08HD024, 08ME002, 08MH032); the About-page "not affiliated with the government" sentence (said built, on
   no branch); the approved push privacy line (written nowhere); leftovers of T549, T554, T673, T600.
8. T778: the "test on built data" was not written (unit tests on hand-made graphs only).
9. 5 of the owner's 17 reviewed continuation links are stale and stopped shipping; 4 new ones wait with no owner row
   (`data/generated/bundle/continuations_held.json`).

Notes: item 3 needs each reading's source words; item 6 is not numbered by lanes (lanes mint numbers that collide).
