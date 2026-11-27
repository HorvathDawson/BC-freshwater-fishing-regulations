# T1243 — An annual quota beside its own row's dated release (RU-3 covers the daily quota only)

Status: IN PROGRESS — approved (owner 2026-11-27: pick A); built on branch fix/T1243-annual-quota-under-release (a3438dfbb, b6220d82b), left (Claude): review + PR, then the record-line knock-on confirmed on the re-cut's candidate

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1243 | An annual quota beside its own row's dated release (RU-3 covers the daily quota only) | IN PROGRESS — approved (pick A), built on fix/T1243-annual-quota-under-release | a3438dfbb, b6220d82b | review + PR; on the re-cut's candidate, confirm the Little River card (release only, and the two record lines gone) |

Source: T1177 lane C (spring closures, O1), 2026-11-26 night. Found on the Little River (Shuswap Lake row, R3, p.32):
since `shuswap_lake.r2b` ("rainbow trout and char catch and release, May 15-Mar 14") binds the river, the reader's RU-3
(a same-row dated release or closure silences the row's undated quota on its dates, `read.effective_rules`) displaces
the daily `r7` / `r9` (reason `same_row_release`), but the annual `shuswap_lake.r8` ("Rainbow trout 5 per licence year
over 50 cm") and `r10` ("Lake trout and Dolly Varden/bull trout 5 per licence year, over 60 cm") still speak. So a card
may read "release all" beside "5 per year". Nothing can be kept there, so no angler keeps a fish wrongly; the words
contradict.

Count (T1177's after bundle, every rule set: an annual quota and a same-row dated take-0 release co-bound for a shared
fish): 4 pairs on 1 row, the Shuswap row only (r8 and r10 with r2b on the Little River, and with r2 on the undrawn map A
part of the lakes). No other water shows it today. Undated releases beside an annual quota were not counted.

## Work
1. On his pick A: RU-3 also silences the row's annual quota for the fish and dates of its own dated release (a named
   constant in `read`, mutation-pinned, `trace=True` names the loser); `POLICY_VERSION` unchanged (the reader, not
   `classify.py`).
2. On his pick B: no change; a GOTCHAS line says the book prints both.

## Built (pick A, 2026-11-27)
`read.ladder.SAME_ROW_RELEASE_SILENCES_ANNUAL` (RULINGS Q32): a row's dated daily release or closure reaches the same
row's undated annual quota, in RU-3's own scope (same row, release dated, quota undated); the guide's
`ladder.same_row_release` says so. Measured with the reference reader over every rule set binding an annual quota, all
365 days: the live bundle (main before #339) changes nothing; the spring-closures bundle changes the Little River only
(1 section, 3.6 km), rainbow trout, lake trout and Dolly Varden, May 15-Mar 14: "release all" now speaks alone, without
"5 per licence year". The ticket's 4 pairs hold: r8 and r10 with r2b change; with r2 (map A, not yet mapped) nothing.

KNOCK-ON, NOT YET SEEN BY THE OWNER: after this no keeping quota speaks on the Little River for those fish on any day
(closed Mar 15-May 14, release all the rest). `answers/display/builders.record_kept` drops a record duty where no
keeping quota speaks, so the two provincial "record your retention on your licence immediately" lines (rainbow trout
over 50 cm, char over 60 cm) should also leave the Little River card. Read from the code, not from a built answers
file: to be confirmed on the re-cut's candidate.

## Acceptance
The Little River's card on Jul 15 (rainbow trout) reads release only (A), or both lines as printed (B); a pin in
`test_competition.py` or `test_rulings_pinned.py`. Severity: low. Model: Opus. Size: S.

## Owner

- [x] 2026-11-26 — Little River (Shuswap Lake row): the annual "5 per licence year" quotas still show beside the river's
  own catch and release. A: silence the annual quota on its own release's dates (recommended). B: leave the book's words.
  → "Hide the annual quota while release applies (Recommended)" (2026-11-27)
