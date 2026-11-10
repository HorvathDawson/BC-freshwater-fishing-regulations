# T536 — heavy.sh: a stopped wrapper stops its job, a bad memory reading is safe, and `--status` says what is running

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code; signal behaviour read, not fired). Rank 4.

## Today (`scripts/heavy.sh`)
- **Stop after start leaves the job running unlocked.** `:40-41` traps EXIT (remove lock) and INT/TERM (exit 143); the
  job runs in its own process group (`:47-48`) and is only signalled in the memory branch (`:54`). Stopping the wrapper
  frees the lock while the build carries on: the outcome the comment on `:41` says must not happen. A SIGKILL of the
  wrapper does the same and the lock then reads as stale (`:34-36`).
- **Unreadable memory reading fails the wrong way.** `free_pct()` (`:29`) prints nothing if `memory_pressure -Q` fails or
  changes wording. Start gate (`:44`) becomes a syntax error and the job starts ungated; the monitor (`:51-55`) reads
  empty as 0 and kills the running job with "free memory < 6 %".
- **Nothing to ask.** The owner file (`:39`) holds pid, label, date: no command, no log path. No heartbeat while healthy,
  no status command, and a second identical call queues and runs the whole job again. Chat 9 had two duplicate runs of a
  quiet background job (restart page, "silence is not its death").
- Lower: EXIT trap removes the lock without checking it is still this run's; stale takeover is read-then-`rm` (not
  atomic); any `HEAVY_LOCK` path is accepted, so "max 2 lanes" is not enforced.

## Work
1. INT/TERM: signal the job's group, wait, then KILL, then exit. Record the job's group id in the owner file; a stale
   check tests the job, not only the wrapper.
2. `free_pct` validates 0–100. Unreadable at the start gate = wait and say so; in the monitor = skip and warn, stop only
   after N unreadable samples with its own message and exit code.
3. Owner file carries the command and a log path; heartbeat line every few minutes; `heavy.sh --status` prints holder,
   elapsed, free memory, last log lines; an identical label + command already holding or queued is refused unless `--again`.
4. Lock removed only by its owner; atomic stale takeover (`mv` then delete); only the two known lock names accepted.
5. A shell test with a fake job and a stubbed `memory_pressure` (no real build), run by fast-fail when heavy.sh changes.
6. AGENTS 18 and the restart-page rule point at `--status`.

## Acceptance
- Shell test covers: stop mid-job (job gone, lock gone), killed wrapper (lock not taken while the job lives), empty
  memory reading at start and mid-run, duplicate refused, stale lock taken once by two waiters.

Depends: —. Size: M. Owner: no.
