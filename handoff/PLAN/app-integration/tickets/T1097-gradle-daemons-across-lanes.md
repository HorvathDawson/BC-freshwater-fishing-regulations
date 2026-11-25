# T1097 — Gradle daemons across lanes: `gradlew --stop` ends other lanes' builds; idle daemons hold memory outside heavy.sh

Status: OPEN.

Source: 0.9.1 chat 2026-11-24 22:34, finds of the T1058 / T1059 lane (PRs #154, #155).

## What was found
- `gradlew --stop` in `app/tools/maplibre-android/build.sh` (line 69) and `app/tools/testbed/session.sh` (lines 98,
  158) stops every Gradle daemon of that Gradle version on the machine, so a lane that cleans up ends another
  lane's running build. Same family as the "never pattern-kill" rule.
- After a `build-apk.sh` run an idle Gradle daemon and three Kotlin daemons (about 2.4 GB) stay up, outside
  `scripts/heavy.sh`'s memory accounting. #155 makes build-apk's own Gradle runs `--no-daemon`; other callers
  (the MapLibre build, the test bed, any lane calling gradlew directly) are not covered.

## Work
1. Replace each `gradlew --stop` with stopping only the daemon this script started (its own PID, or run with
   `--no-daemon` so there is nothing to stop). No machine-wide stop anywhere in the repo; a guard test greps for it.
2. Every Gradle call in repo scripts runs `--no-daemon` (or one shared wrapper does), Kotlin daemon included
   (`kotlin.daemon` settings), so nothing outlives the job that heavy.sh counted.
3. AGENTS: one line beside the pattern-kill rule.

## Acceptance
The guard test fails on a `gradlew --stop`; after each script ends, no Gradle or Kotlin daemon it started is left
(checked by PID in a test or the script's own exit check). Two builds side by side: one ending does not end the other.

Depends: T1058 (#155). Severity: medium (one lane can kill another's release build; 2.4 GB uncounted). Release:
0.9.2, with T1063. Model: Sonnet. Size: S. Owner: no.
