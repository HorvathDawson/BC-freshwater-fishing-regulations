# T790 — Records to put right: answers shown as open, stale ledger rows, stalled tickets, readings missing from the rulings file, lost follow-ups

Status: PARTIAL (docs and small tickets; main chat; items 1 and 7 DONE 2b4e0d826; item 4's ledger half DONE c0f8da79c (47 DONE/DROPPED rows to DONE.md); left (Claude): items 2, 3, 4 (stale statuses, missing ticket numbers), 5, 6 (the lost follow-ups still to ticket), 8, 9)

Source: general audit 2026-11-16, decisions checker (`handoff/PLAN/audit-2026-11-16/decisions.md`).

1. T754 one-bank places: the owner answered "Build it this way (Recommended)" (2026-11-15, round 4); the ticket, the
   ledger and OWNER-TODO still say "options for the owner / six questions".
2. T759: the shared ticket describes the rejected design; the rewritten ticket is on `data/t759-missing-place-check`.
3. 14 owner readings of 2026-11-13 to 11-15 are not in `pipeline/docs/RULINGS.md` (rest-of with "areas and times";
   the tributary readings; lake loops; the T759 rule; one-bank; "Whole river is right"; "4 km as the book says"; the
   Shuswap "use as printed"; Pallant; the Alexandra Bridge; rest-of only where a place overlaps what it lists; the
   T708 dialog answers). Ruling G9 (Pend d'Oreille) holds by hand-typed data only: pin it.
4. Stale ledger rows: T690, T664, T753, T752, T750, T747, T734 (shipped or landed), T760, T738; 28 DONE rows still in
   README.md; ticket numbers T744, T745, T748, T766 to T769 have no file.
5. Stalled, waiting for "the next APK / release" that has happened: T261, T292, T310, T390, T461, T535, T562, T585,
   T633, T674, T603, T606, T670, T671, T672, T676, T687, T689. Give each an owner and a step, or close it.
6. Lost follow-ups to ticket: the suggester proposes a new cut where one exists (match by position); the review app
   names the unticked row when Confirm is greyed; the Hope gauge (66 m reads 08MF076) and three gauge cuts never
   placed (08HD024, 08ME002, 08MH032); the About-page "not affiliated with the government" sentence (said built, on
   no branch); the approved push privacy line (written nowhere); leftovers of T549, T554, T673, T600.
7. Fresh production commands in OWNER-TODO; delete the two out-of-date blocks (r-f749475d0962, r-a178299f6005).
   `release/PRODUCTION.md`: for a release with closed areas the feeds worker goes BEFORE or WITH the data.
8. T778: the "test on built data" was not written (unit tests on hand-made graphs only).
9. 5 of the owner's 17 reviewed continuation links are stale and stopped shipping; 4 new ones wait with no owner row
   (`data/generated/bundle/continuations_held.json`).

## Lane result (lane/bugs-080, 2026-11-17)
Item 4, the ledger half: the 47 DONE / DROPPED rows still in README.md moved to DONE.md (`check-tickets` passes).
Not done here: the stale rows' statuses (T690 … T738), the missing ticket numbers, items 1-3 and 5-9 (OWNER-TODO and
RULINGS are the main chat's files today).

## Lane result, second round (lane/bugs-080b, 2026-11-17)
- Item 1 DONE: T754's ticket, ledger row and OWNER-TODO row say the owner's answer ("Build it this way (Recommended)",
  2026-11-15 round 4) and "to build".
- Item 7 DONE: OWNER-TODO's two out-of-date production command blocks (r-f749475d0962, r-a178299f6005) removed; the
  fresh commands come with 0.8.0's data release. `release/PRODUCTION.md` "Every later release": the feeds worker goes
  first when the release carries closed areas (before or with the data).
- Item 6, the lost follow-ups, still to ticket (not numbered here: lanes mint numbers that collide). The suggester
  proposing a new cut where one exists (match by position); the review app naming the unticked row when Confirm is
  greyed; the Hope gauge (66 m reads 08MF076) and the three gauge cuts never placed (08HD024, 08ME002, 08MH032); the
  About page's "not affiliated with the government" sentence; the approved push privacy line; leftovers of T549, T554,
  T673, T600.
- Not done: items 2, 3 (the 14 readings for RULINGS need each reading's source words), 4 (stale statuses), 5, 8, 9.
