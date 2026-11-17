# T786 — A place whose last save is "wrong" with no fix still ships its older drawing (the owner: "I want you to for it to be redrawn… Just be safe")

Status: DROPPED (owner 2026-11-17 15:18: not needed; the saves were re-checked 11-16).

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
