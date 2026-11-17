# T786 — A place whose last save is "wrong" with no fix still ships its older drawing (the owner: "I want you to for it to be redrawn… Just be safe")

Status: DONE 2026-11-17 (126ac0488): release gate `pipeline/regs/review/wrong_saves.py`; see the last section.

Source: owner 2026-11-15 evening, round 1; general audit 2026-11-16, decisions checker item 1 (`handoff/PLAN/audit-2026-11-16/decisions.md`).

## What is wrong
The owner's words on a place marked wrong with no fix: "I want you to for it to be redrawn. We will finish these tonight
so not a big deal... Just be safe". The main chat's reading, written twice and never built: such a place does not ship
its disputed extent. `pipeline/regs/review/audit.py` line 98 states the opposite ("changes nothing that ships"); only a
flag exists (`OLD_EXTENT_FLAG`, audit section B, a chip in the review app). On staging today: the dormant Region 3
Fraser mainstem ships against a "wrong" mark; Sproat and Stamp Falls end on "wrong" saves though their fixes are in.

## What to build (after the owner's yes on question 18)
- The release lists every place whose last save is "wrong" (or "unsure" with no extent fix) and whose older extent
  still binds; a live one ships DFO's words undrawn (carried on the water) until it is re-saved; a dormant one is
  listed only.
- The morning list names those places each release.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 18, typed: "can we sort this out now i want to review it and make it correct for sure". He reviews the saves now, in chat with pictures (T806); the "ship DFO's words undrawn" build stays unanswered.

## 2026-11-17 ~15:18: closed (owner, OWNER-TODO 15:18)
All 34 re-check places answered from pictures 2026-11-16 and re-saved (marks 2795-2839). Owner: "I feel like we have done these like 3 times already so it should be good???". No safety net built. Owed later: pictures for 2739 (Morice Gosnell-Lamprey, after its fix) and 2783 (Sheldens, after the map rebuild).

## 2026-11-17 ~15:19: REOPENED as a release gate (owner)
"The safety net should be a build warning I think then we fix it and do not ship \"wrong\" places..". Build: the release preflight (and fast-fail) lists every place whose latest save is "wrong" with no extent fix, live or dormant, and REFUSES to ship while any is listed (a dated, reasoned allowance like NOT_LANDED_OK is the only way past). We fix and re-save, then ship. Pinned with a test and a mutation. Today the list should be empty (all re-saved 11-16).

## 2026-11-17: DONE (126ac0488), the gate
`python -m pipeline.regs.review.wrong_saves --check` lists every dfo / entry / rule / closed_area target whose LATEST save
is "wrong" or "unsure" with no extent fix (unsure too: the marks audit treats them as one, `audit.old_extent_in_force`;
a closed area never has a fix, it lands only from a verified mark). Live and dormant alike (it reads the store, not the
places). fast-fail (a `step`) and the release preflight (`check_wrong_saves`, after `marks`; not skipped by
--no-fast-fail) refuse while one is listed, each line naming the place, its mark line, its verdict and "fix and re-save
it". The only way past: a `WRONG_SAVE_OK` row, dated, with its reason and the store line it excuses (empty). audit.py's
`SHIP_RULE` no longer says such a save "changes nothing that ships". Tests: `pipeline/tests/test_review_wrong_saves.py`
(+ two in `test_release_command.py`, one a mutation that drops the gate from PREFLIGHT).

NOT EMPTY on 2026-11-17 (marks.jsonl 2839 lines), no allowance added — the main chat decides:
- entry r2:stave_river@2-8, line 30, wrong (2026-10-28)
- entry r2:pitt_river@2-8, line 1265, wrong (2026-10-29)
- dfo 3:thompson-river:from-kamloops-lake-outlet-downstre (dormant), line 2521, wrong (T806: answered by the closed areas)
- dfo 6:morice-river:from-fishing-boundary-signs-locate (dormant), line 2596, wrong (T806: retired, fine)
Until each is re-saved (or allowed), fast-fail and the release preflight are red.
