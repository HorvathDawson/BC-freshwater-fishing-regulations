# T1243 — An annual quota beside its own row's dated release (RU-3 covers the daily quota only)

Status: OPEN — waiting on the owner's pick, not to be built before his answer (it changes card words); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1243 | An annual quota beside its own row's dated release (RU-3 covers the daily quota only) | OPEN — waiting on the owner's pick; owner: see ## Owner | — | the owner's pick (silence the annual quota on its own release's dates, or keep the book's words); then a reader change with a mutation pin |

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

## Acceptance
The Little River's card on Jul 15 (rainbow trout) reads release only (A), or both lines as printed (B); a pin in
`test_competition.py` or `test_rulings_pinned.py`. Severity: low. Model: Opus. Size: S.

## Owner

- [ ] 2026-11-26 — Little River (Shuswap Lake row): the annual "5 per licence year" quotas still show beside the river's
  own catch and release. A: silence the annual quota on its own release's dates (recommended). B: leave the book's words.
  Default: nothing is built until he answers.
