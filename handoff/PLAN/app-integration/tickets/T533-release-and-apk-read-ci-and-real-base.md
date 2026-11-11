# T533 — Release and APK preflight read CI's result and check everything since the last ship

Status: DONE — 98b0ccea1 (see Built; CI reads red today: pipeline-ci run 42734778050).

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

## Built (2026-11-10, 98b0ccea1)
1. `scripts/ci_status.py`: the newest `app-ci.yml` / `pipeline-ci.yml` runs for the branch (`gh run list`). Run for the
   sha finished red (failure, timed out, startup failure) = STOP naming the run; running or cancelled = WARN; no run for
   the sha = WARN, and then the newest finished run decides (red = STOP). `gh` failing = STOP (exit 2), never a pass.
   Escape `--skip REASON` / `CI_CHECK_SKIP=REASON`, printed; an empty reason refused. Used by release preflight (after
   the pushed check; `--no-ci-check REASON`, passed on to the upload as CI_CHECK_SKIP) and its `--dry-run`,
   `deploy/staging/build-apk.sh` before the heavy lock (`--no-ci-check=<reason>`; a dry run shows it and goes on) and
   `deploy/staging/upload.sh` before anything is sent.
2. FAST_FAIL_BASE: release preflight passes the ledger `commit` of LATEST (said; when none or not in the clone it says
   fast-fail falls back to its default, not since the last ship). build-apk.sh passes `app/dist/apk/last-play-sha` for
   `--play` (missing until the next Play upload writes it: then the commit of the `app: versionCode <last> for Google
   Play` bump, said), else `app/dist/apk/last-sha`.
3. `as_ci`: with a FAST_FAIL_BASE and a changed `pipeline/**.py` source, the whole no-data tier runs as CI's Tests step,
   chunked (`scripts/pytest-chunked.sh`). On the default base (every lane's everyday run) only the changed test files
   run and it says so — a deliberate narrowing: running ~7,000 tests in every lane's fast-fail (and before every heavy
   job, T535) was not "cheap". fast-fail prints the changed paths (first 12) and how many Python files it compiles.

Proof: `pipeline/tests/test_ci_status.py` (6: green passes; red on HEAD stops naming the run; running warns; no run warns
unless the newest finished run is red; gh failing stops; escape prints its reason, empty refused).
`test_release_command.py` (+3: red CI stops preflight before fast-fail and the escape reaches the upload; fast-fail gets
LATEST's commit as base; an unrecorded base is said). Real reads: `python scripts/ci_status.py` → app-ci WARN (no run for
HEAD), pipeline-ci STOP (newest finished run 45a1427c014a red, run 42734778050) — CI is red right now;
`release --staging --dry-run` and `build-apk.sh --dry-run` / `--play --no-upload --dry-run` print the CI lines and the
base (e7eb12a8f7c7 last sideload; 1aba2b60b58a = versionCode 26 bump). `FAST_FAIL_BASE=51c1b45e9` (two tickets back):
32 changed paths listed, 6 Python files compiled, the silent-skip guard run on the changed tests.

Not verified: a real `release --staging`, `build-apk.sh` or `upload.sh` run through the new check; the whole no-data
tier under a ship base (not run here: it is the suite, minutes); fast-fail no-change total is ~35 s by its steps but a
whole run is red today on the T637 override check, so no single "all passed" time was printed.
