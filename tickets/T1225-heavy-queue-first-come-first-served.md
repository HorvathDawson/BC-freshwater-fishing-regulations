# T1225 — heavy.sh serves its waiters first come, first served
Status: IN PROGRESS — lane: fix/T1225-heavy-queue-first-come (part of T1193, the scripts clean-up).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1225 | heavy.sh serves its waiters first come, first served | IN PROGRESS | — | the PR (fix/T1225-heavy-queue-first-come) to merge; then the cherry-pick onto the release branch |
Source: 2026-11-26 21:45, default lane: seven waiters (oldest since 18:07); five handovers 20:58-21:45, a NEWER waiter
won each time, free memory 89 %. Cause: the queue entry `<lock>.queue/<pid>` was written but never read by the wait
loop; every waiter polled `mkdir` on its own 20 s nap, and a new arrival tried at once, so it beat all who waited.
Built: a waiter tries the lock only when FIRST IN LINE, the oldest LIVE entry of its lane's queue (order: the entry's
new line 3 `since: <epoch>`, an older heavy.sh's entry falls back to its line-1 date, a tie to the lower pid). Live =
the pid runs, is not stopped or a zombie, and started no later than its entry. The head polls every `HEAVY_HEAD_POLL`
s (2), the rest `HEAVY_WAIT_POLL` (20). The memory gate stays strict and is taken with the lock: a head waiting for
memory holds the lane (nothing overtakes, so nothing starves). A run that cannot see its own entry (ps unreadable)
says so once on stderr and tries in no order, as before.
`--status` lists each lane's line in served order with its wait, marks an older heavy.sh's entry, and marks a holder
past `HEAVY_LONG_HOLD` s (3600) `LONG HOLDER` (a server that never ends does not belong on a heavy lock; nothing is
killed). A holder still waiting for memory shows `NOT STARTED`: the owner file gains a keyed line `gate:`; older
readers and `pipeline/release.py` read line 1 and keyed lines by name, so they do not see it. Lanes, lock dir, owner
file, `--again`, the duplicate refusal, the pause file, nesting and exit codes are unchanged. Old and new copies share
the lock dirs and entries (each worktree runs its own copy for days); `scripts/tests/heavy.test.sh` 16a-d covers
new/old as holder and waiter. Toward an old waiter the script degrades to today's behaviour (it may jump the line; no
deadlock, no overlap).
Old and new copies side by side (`scripts/tests/heavy.test.sh`):

| case | what happens | test |
|---|---|---|
| new holder, old waiters | they take it on their own polls, one at a time; old `--status` reads the holder | 16a |
| old holder, new waiters | served in their order; old `--status` lists them by the date line | 16b |
| old waiter first, new behind | the new one waits its turn until the old one's own poll takes it (up to 20 s) | 16c |
| new waiter first, old behind | the old one may jump the line, as today; no deadlock, no overlap | 16d |

Acceptance: `bash scripts/tests/heavy.test.sh` passes (`fast-fail.sh` runs it); its sections 13-17 (order,
dead/stopped/reused-pid entries, memory-gated head, mixed versions, long holder) fail on the old script 118d8e4f8
(`bash scripts/tests/heavy.test.sh <old heavy.sh>`): 7 checks every time, 10-11 in two runs. Release branch: take
`scripts/heavy.sh` alone (`git checkout <merge-commit> -- scripts/heavy.sh` on `release/0.10.0`), as the test and
README conflict or are absent there; it also carries T1161's caffeinate block. Depends: —. Size: S. Owner: no. Model:
Opus.
