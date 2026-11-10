# T533 — Release and APK preflight read CI's result and check everything since the last ship

Status: OPEN.

Source: workflow review 2026-11-09 (owner-led; each finding re-read against the code by a second reviewer). Rank 1.

## Today
- No release or APK script asks GitHub whether CI is green on HEAD. `pipeline/release.py:123-130` (`git_pushed`) only
  checks HEAD is on origin; `deploy/staging/build-apk.sh:98-102` runs fast-fail only. `gh` is already used on the release
  path (`deploy/staging/upload.sh:54-60,99`) but never to read `app-ci` / `pipeline-ci`.
  Recorded: STATUS "CI was red since 10-29 and nobody saw it" (29 pipeline tests + two app checks); two more CI-only reds
  on 2026-11-09 (`scripts/fast-fail.sh:69,84`).
- fast-fail's four changed-file checks (`silent_skips`, `as_ci`, `py_compiles`, `vitest_changed`,
  `scripts/fast-fail.sh:62-66,74-81,95-110,142-154`) take "changed" from the merge-base with origin (`:39-42`). Release
  preflight requires HEAD pushed (`release.py:332-341`), so in the usual push-per-ticket flow they check nothing.
  `FAST_FAIL_BASE` exists (`fast-fail.sh:12,34-35`) but no caller sets it.

## Work
1. One cheap helper (used by release preflight, `--dry-run`, `build-apk.sh` before the heavy lock, and `upload.sh`): read
   the newest `app-ci` and `pipeline-ci` runs for the branch. Red on HEAD (or on the newest finished run) = stop, naming
   the run. Still running or no run for HEAD (path filter) = say so, do not block. Escape with a printed reason, same
   pattern as `--no-fast-fail`.
2. Release preflight and `build-apk.sh` pass `FAST_FAIL_BASE` = the commit the last shipped thing was built from (the
   release ledger's commit for LATEST; the last Play build's commit for the APK). Record that commit where it is missing.
3. `as_ci`: when any `pipeline/**.py` source changed since the base, run the whole no-data tier, not only changed test files.

## Acceptance
- A test with a faked `gh` answer: red stops, running warns, none-for-HEAD warns, escape prints its reason.
- With HEAD pushed and a base two tickets back, fast-fail lists and runs the changed files (shown in its output).
- fast-fail total stays ≤ 2 min in the no-change case (time printed).

Depends: —. Size: M. Owner: no.
