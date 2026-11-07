# T483 — Fast-fail: cheap checks before (and while) long jobs

Owner 2026-11-07: builds sometimes reveal easy failures only at the end. Run the cheap checks up front, or while a
build waits, so they fail in a minute or two. Rule: AGENTS.md 20a — every late failure a cheap check could have
caught becomes a new fast-fail item, added with its fix.

## Do
- `scripts/fast-fail.sh`: one command, unfiltered output, exits non-zero on the first failure, ≤ 2 min target.
  Starting items (each with a one-line "caught:" comment):
  - app typecheck;
  - vitest on files changed vs `HEAD` (and their tests);
  - the colours check (caught 11-07: pin-study.mjs flagged after the build);
  - answers-stale (caught 11-07: uncommitted Python made the generated answers stale);
  - ticket ledger check (README row ↔ ticket file);
  - `pipeline/tests/smoke.txt` entries exist for new pipeline test files;
  - no conflict markers in tracked files.
- Call it first from `python -m pipeline release`, `scripts/heavy.sh` callers that build, and the APK build script
  (a `--no-fast-fail` escape is allowed only with a printed reason).
- Mine STATUS.md / the ticket ledger / memory for past late failures that fit; add those too.

## Done when
- A deliberately broken colour, stale answers file, or type error each stops the release/APK path in < 2 min.
- AGENTS.md 20a points at the script and the list is commented.
