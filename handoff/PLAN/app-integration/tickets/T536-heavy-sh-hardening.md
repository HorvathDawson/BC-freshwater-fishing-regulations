# T536 — heavy.sh: a stopped wrapper stops its job, a bad memory reading is safe, and `--status` says what is running

Status: DONE — fad79daf0 (shell test 37/37; see Built).

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

## Built (2026-11-10, fad79daf0)
`scripts/heavy.sh` rewritten (CLI unchanged: `heavy.sh <label> <pct> -- <cmd>`, HEAVY_LOCK, HEAVY_HELD, the precheck):
1. INT/TERM/HUP: TERM to the job's process group, `HEAVY_STOP_GRACE` s (10), KILL, then the lock goes (exit 143). The
   owner file records `pgid:`; a waiter takes a lock only when the wrapper AND that group are gone (a SIGKILLed wrapper's
   job keeps the lane, and the waiter says so).
2. `free_pct` accepts only an integer 0–100. Unreadable at the start gate: waits, says so. While running: warns, stops
   after `HEAVY_UNREADABLE_MAX` (12) unreadable samples in a row with its own message, exit 74.
3. Owner file: line 1 `<pid> <label> <date>` (as before; release.py prints it) then `pgid/cmd/cwd/log/started`. `log` =
   `HEAVY_LOG`, else the file stdout goes to (lsof). Heartbeat every `HEAVY_HEARTBEAT` s (300), also in `<lock>/beat`.
   `heavy.sh --status`: both lanes, holder, elapsed, wrapper/job alive, command, log tail, queued waiters, free memory.
   Same label + command holding or waiting on either lane: refused, exit 3, unless `--again`.
4. Unlock only by the owner (line-1 pid = this wrapper). Stale takeover under a `<lock>.takeover` mutex, re-check, `mv`
   then delete. Only `heavy-job.lock` / `heavy-job-2.lock` in `HEAVY_LANES_DIR` (default `~/.claude`) are accepted (exit 2).
5. `scripts/tests/heavy.test.sh` (fake jobs, stubbed `memory_pressure` + `footprint`, scratch lanes; 8 s), run by
   fast-fail when heavy.sh or the test changes.
6. AGENTS 18 and the restart page point at `--status` (the AGENTS hunk landed inside 7323ce649, swept by T539's commit).

Proof: `bash scripts/tests/heavy.test.sh` → 37 checks, 0 failed in 8 s: stop mid-job (job + child gone, lock gone, 143),
TERM-ignoring job KILLed after grace, SIGKILLed wrapper (lock kept while the job lives, taken after it ends), empty / 150
reading at start (waits, then starts), empty mid-run (74 after 3, own message, job + lock gone), low free 137, footprint
137, duplicate holding / other lane / queued refused (3), `--again` queues, stale lock taken over once by two waiters with
no overlap, unknown lane 2, nested HEAVY_HELD, `--status` content, lock not removed by a non-owner. Mutation: the same test
against the previous heavy.sh fails 25 checks. `--status` read the live lanes (pre-T536 owner file shown as such).
`test_release_command.py::test_heavy_runs_a_scripts_precheck_before_it_takes_the_lock`: the heavy.sh half passes (lane
renamed `heavy-job.lock` + `HEAVY_LANES_DIR`); its second half fails since T534 (session.sh now wants one AAR in
`MLN_LOCAL_MAVEN`) — not this ticket's.

Not verified: a real long job stopped or killed through the new wrapper (only fake jobs); INT from a terminal (bash ignores
INT in a backgrounded non-interactive script, so the test uses TERM). Running wrappers started before fad79daf0 keep the
old code until they exit.
