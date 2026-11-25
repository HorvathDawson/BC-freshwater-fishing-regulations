# T1063 — `scripts/heavy.sh`: a fair queue (longest wait first), a release build ahead of check tiers, and test runs inside the lock

Status: OPEN.

Source: the 0.9.0 chat, 2026-11-24: the 0.9.1 lanes' `before_pr` tiers made the production fast-fail take 30 minutes and queued the production build behind them.

Added 2026-11-24 (the 0.9.1 chat): there is no fair queue: the job that waited longest (the 0.9.0 iOS build, 70
minutes) lost the lock to later arrivals; and several lanes' vitest runs outside the lock pushed the load to 85–145,
timing out render tests in pre-push hooks (see T1070). T1058's lane was asked to confirm.

Evidence 2026-11-24 (0.9.1 chat, T1058 / T1059 lane): waiters retry `mkdir` every 20 s and the queue file only
feeds `--status` (`scripts/heavy.sh` lines 226–253), so the lock goes to whoever retries first: a waiter queued at
13:58 lost the lock twice to later arrivals (14:23 and 14:47).

## Work
0. Fair queue: the lock goes to the longest waiter; vitest and other test runs from hooks take the lock (or a light
   lane of it) so they cannot pile up outside it.
1. A priority (or a reserved lane) for release builds in `heavy.sh`: a waiting release job takes the next free slot.
2. Say in AGENTS how a lane asks for it, and that only a release run may.

## Acceptance
a test of the queue order; the runbook names the flag.

Severity: medium (a release build waited 70 minutes; false-red hooks). Release: 0.9.2. Model: Sonnet. Size: S. Owner: no.
