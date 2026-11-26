# T1063 — `scripts/heavy.sh`: a fair queue (longest wait first), a release build ahead of check tiers, and test runs inside the lock

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1063 | `scripts/heavy.sh`: a fair queue (longest wait first), a release build ahead of check tiers, and test runs inside the lock | OPEN — 0.10.1 (medium since 2026-11-24) | | planning chat 2026-11-24 |

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

## Deep look 2026-11-25 night: the phone builds, measured from logs
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `phone-builds-deep-look.md` (the replacement text for this ticket is there).
- The builds are short: Android Gradle 56 s to 6 m 3 s (45 builds in Gradle's logs); iPhone archive 81-131 s (7
  builds), export plus upload 82 s. MapLibre is not rebuilt each time (once per pin; T1170).
- The hours are four causes: (1) Gradle hung after it said done (T1058 removes the trigger and caps THAT hang at
  about 2 min; hangs in other steps are not watched, and Xcode has no watchdog at all); (2) the "fast" pre-check
  reruns the whole pipeline test tier on a tree CI and the follows already tested, outside the lock
  (`fast-fail.sh:399-409`): the 30 min; (3) the machine lock is first-come-first-grabbed (the 70-min iPhone
  wait), and the iPhone build holds it up to 45 min while only polling Apple; (4) T1059 reads only the newest
  1,000 runs per workflow and app-ci already has 1,330, so the CI "cannot tell" can come back.
- "Build overnight, upload in the morning" has no command: `--publish-only` does not upload the Android bundle to
  Play, and an iPhone `--no-upload` build is uploaded by hand.
- Estimate (from logs and guesses, not timed): the phones' part of a release morning from about 2.5-3 h to 20-30
  min after the lock, pre-check and watchdog fixes, and to 5-10 min of the owner's time with overnight builds.

## Owner

- [ ] 2026-11-25 — Picks (defaults first): the lock: a priority lane for release builds, no holding it while idle, a lighter pre-check for store builds; phones built on the release candidate overnight once "upload what was built" exists; stay on the Mac, Android to GitHub after T1170; a bundle is kept after a stopped Gradle client if its signature, MapLibre and R8 checks pass; the external TestFlight submit is run by Claude on your word.
