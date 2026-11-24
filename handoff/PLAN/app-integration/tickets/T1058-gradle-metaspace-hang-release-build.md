# T1058 — A production Android build hangs for an hour after BUILD SUCCESSFUL (Gradle daemon out of Metaspace)

Status: OPEN.

Source: the 0.9.0 chat, 2026-11-24 10:14: prod `build-apk` printed BUILD SUCCESSFUL, the daemon expired at `MaxMetaspaceSize=512m`, and the `gradlew` client hung on a closed socket for over an hour. Evidence: `~/.gradle/daemon/9.4.1/daemon-53034.out.log`. Workaround in place: 1024m in `~/.gradle/gradle.properties` (machine-wide, not in the repo).

## Work
1. Set the Metaspace limit in the prebuild-generated `gradle.properties` (so CI and a fresh machine get it).
2. `build-apk.sh` stops a Gradle client whose daemon is gone (a timeout with a clear message), instead of waiting forever.

## Acceptance
the generated gradle.properties carries the limit (test on the config plugin); a dead-daemon run ends with an error, not a hang.

Severity: medium (silently blocks a Play build). Release: 0.9.1 (before its Play build). Model: Sonnet. Size: S. Owner: no.
