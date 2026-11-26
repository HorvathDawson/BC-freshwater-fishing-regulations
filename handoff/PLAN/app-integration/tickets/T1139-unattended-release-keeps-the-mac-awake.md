# T1139 — An unattended release keeps the Mac awake

Status: OPEN — 0.10.1, tooling (first, it is one line)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1139 | An unattended release keeps the Mac awake | OPEN — 0.10.1, tooling (first, it is one line) | | 0.10.0 chat 2026-11-25; the 0.10.0 night release lost eight hours |

Source: the 0.10.0 chat and the planning chat, 2026-11-25. The Mac slept from about 03:50 to 12:06 with the lid
closed, so the overnight 0.10.0 release stopped right after the cut and nothing reached staging until the
afternoon. Related: T1063 (`scripts/heavy.sh`), T993 (the runbook), T1133 (builds off the Mac).

Work: `scripts/heavy.sh` runs its job under `caffeinate -i -s` (idle and system sleep held while the job lives;
released when it ends, also on failure); the runbook's overnight section says what a closed lid does on battery and
on power (with the lid closed a Mac sleeps unless on power with a display attached: say so, and say "lid open, on
power"); the release command prints a warning at the start when it is not on power. A test that the wrapper is in
the command line.

Acceptance: a job started through `heavy.sh` shows a caffeinate assertion (`pmset -g assertions`) while it runs
and none after. Severity: medium (it cost a night). Model: Sonnet. Size: S. Owner: no.
