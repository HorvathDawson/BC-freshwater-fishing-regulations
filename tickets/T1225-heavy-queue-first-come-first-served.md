# T1225 — heavy.sh serves its waiters first come, first served

Status: IN PROGRESS — lane: fix/T1225-heavy-queue-first-come (part of T1193, the scripts clean-up).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1225 | heavy.sh serves its waiters first come, first served | IN PROGRESS | — | the PR (fix/T1225-heavy-queue-first-come) to merge; then the cherry-pick onto the release branch, and the old copies age out as worktrees update |

Source: measured 2026-11-26 21:45 on the default lane: seven waiters, the oldest since 18:07, the others since 20:09,
20:21, 20:31, 20:46 (two) and 21:35. The lock changed hands five times between 20:58 and 21:45 and a NEWER waiter won
each time; free memory was 89 %, so the memory gate was not the cause. Separately, a dev server (`expo start`) was run
through heavy.sh and held the lock for 20 minutes with 11 jobs behind it. Part of T1193.

## Why a newer waiter won (scripts/heavy.sh at 118d8e4f8, the copy on `main` before this ticket)

- Line 226 writes the waiter's queue entry `<lock>.queue/<pid>`, but the wait loop never reads it: only `--status`
  (132-135) and the duplicate check (188-192) do. The line had no order at all.
- Lines 229-254: every waiter loops `mkdir "$LOCK"`, then `nap "$WAIT_POLL"` (253; 20 s, line 46). The holder's
  `release` (208, on EXIT) removes the lock; the first waiter whose own 20 s nap happens to end next takes it. Each
  waiter's phase is set by when it arrived, not by how long it has waited, so with seven waiters the oldest had about a
  one-in-seven chance at each handover (it losing all five: about (6/7)^5, 46 %).
- A NEW arrival tries `mkdir` at once, on its first pass of line 229, with no nap. A job that starts within the 20 s
  after a release (the next step of a chain such as `before_pr.sh` or `fast-fail` -> build, a re-run) beats every job
  already waiting.
- The memory gate (264-270) is taken AFTER the lock: a holder short of memory waits holding the lane. That is
  unchanged (below).

## The rule built

1. A waiter tries the lock only when it is FIRST IN LINE: the oldest LIVE entry of its lane's queue. The order is the
   entry's new third line `since: <epoch>`; an entry from an older heavy.sh has none, and its line-1 date (to the
   second) is used. A tie goes to the lower pid (deterministic).
2. Live = the pid runs, is not stopped (`T`) or a zombie (`Z`), and started no later than its entry was written
   (`ps -o etime`; a reused pid started after it). Anything else is skipped, never waited for. One `ps` call per poll.
3. The first in line polls every `HEAVY_HEAD_POLL` s (2; never slower than `HEAVY_WAIT_POLL`); the others keep
   `HEAVY_WAIT_POLL` (20). A stale lock is still cleared by any waiter under the takeover mutex, then taken by the first
   in line.
4. Memory gate, decided: unchanged and strict. It is taken with the lock, as before, so a first-in-line job waiting
   for free memory holds the lane and the jobs behind it wait, smaller ones too. Why: it is today's behaviour, it is
   the simplest honest rule (nothing overtakes, so nothing starves), and letting small jobs past a memory-starved head
   would starve exactly the big jobs the gate is for.
5. A run that cannot see its own entry in the line it reads (ps unreadable) says so once, on stderr, and tries the lock
   in no order, as before: never stuck, never silent.
6. `--status` lists each lane's line in the order it is served: `queued <n>: <pid> <label>  waiting <h>h<mm>m (since
   <date>)`, an older heavy.sh's entry marked `[older heavy.sh: takes the lock out of turn]`; a holder past
   `HEAVY_LONG_HOLD` s (3600; the script's `<h>h<mm>m` elapsed vocabulary) gets `LONG HOLDER: …; a server that never ends
   does not belong on a heavy lock`. No killing, no timeouts. `scripts/README.md` says the same in one line.
7. Unchanged: the three lanes, the lock dir and owner file, `--again` and the duplicate refusal (it reads line 2 of an
   entry, which stays the job), the pause file, the re-entrant nested call, exit codes.

## Old and new copies side by side (each worktree runs its own copy for days)

The new script uses the same lock dirs, owner file and queue entries; it only appends line 3 to its own entries and
writes them whole (a dot file, then `mv`; the old globs never see the dot file).

| case | what happens | test (scripts/tests/heavy.test.sh) |
|---|---|---|
| new holder, old waiters | they take it on their own polls, one at a time; old `--status` reads the holder | 16a: pass |
| old holder, new waiters | served in their order; old `--status` lists them by the date line; old duplicate check refuses a new waiter's twin | 16b: pass |
| old waiter first in line, new behind | the new one waits its turn on a free lock until the old one's own poll takes it (up to its 20 s) | 16c: pass |
| new waiter first, old behind | the old one may jump the line, as it does today; no deadlock, no overlap | 16d: pass |

Toward old waiters the new script degrades to today's behaviour: it cannot make them polite. It never waits on a dead
old entry, and an old waiter faces fewer racers than today (new waiters behind it no longer compete).

## Acceptance

`bash scripts/tests/heavy.test.sh` passes (run by `scripts/fast-fail.sh` when heavy.sh or the test changes) and its
new sections 13-17 fail on the 118d8e4f8 script (`bash scripts/tests/heavy.test.sh <old heavy.sh>`: 10 failures):
13 five waiters in order and `--status` order with labels; 14 dead, stopped and reused-pid entries never block; 15 the
memory-gated head holds the lane; 16 the mixed-version cases; 17 the long holder.

Depends: —. Size: S. Owner: no. Model: Opus.
