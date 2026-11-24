# T1058 — A production Android build hangs for an hour after BUILD SUCCESSFUL (Gradle daemon out of Metaspace)

Status: DONE — 551cb1ce8, e01373329. What happened: the daemon log shows `stop()` (Metaspace expiry, 81 % of 512 MiB) BEFORE
"dispatching the build result", then "Timed out waiting for finished message from client". In 13 other expiring builds
(11-19 to 11-24) the result went out first and nothing hung, so this is a race. It is not a crash: the build itself
succeeded. The fix: `plugins/with-gradle-memory.js` writes `-Xmx2048m -XX:MaxMetaspaceSize=1024m` at prebuild (test:
`app/tools/gradle-memory.test.ts`, through `expo config --type introspect`). `deploy/lib/gradle-watch.sh` runs both
build-apk Gradle builds `--no-daemon`, so the single-use daemon exits with the build. It stops a client still running
120 s after its BUILD line, or past 1 h, with a message, and the build fails with code 124 (test:
`pipeline/tests/test_gradle_watch.py`, a fake gradlew that hangs after BUILD SUCCESSFUL; 4 mutations caught). Proof:
the staging sideload build's steps (R8 on, debug-signed, no upload) through heavy.sh in a lane worktree took 2 m 30 s
and printed BUILD SUCCESSFUL. Gradle's own Metaspace peaked at 416 MiB (41 % of 1 GiB; it would have been 81 % of
512). The single-use daemon exited with the build. Peak memory was about 7.5 GB RSS: about 4.2-5.2 GB for the build's
tree (daemon, workers, Hermes, node) plus about 3.2 GB for the three Kotlin compile daemons. The hang itself was not
reproduced (a race). The owner may delete the machine-wide workaround in `~/.gradle/gradle.properties` once this is on
main; until then build-apk says that it overrides the project's value.

Source: the 0.9.0 chat, 2026-11-24 10:14: prod `build-apk` printed BUILD SUCCESSFUL, the daemon expired at `MaxMetaspaceSize=512m`, and the `gradlew` client hung on a closed socket for over an hour. Evidence: `~/.gradle/daemon/9.4.1/daemon-53034.out.log`. Workaround in place: 1024m in `~/.gradle/gradle.properties` (machine-wide, not in the repo).

## Work
1. Set the Metaspace limit in the prebuild-generated `gradle.properties` (so CI and a fresh machine get it).
2. `build-apk.sh` stops a Gradle client whose daemon is gone (a timeout with a clear message), instead of waiting forever.

## Acceptance
the generated gradle.properties carries the limit (test on the config plugin); a dead-daemon run ends with an error, not a hang.

Severity: medium (silently blocks a Play build). Release: 0.9.1 (before its Play build). Model: Sonnet. Size: S. Owner: no.
