# T483 — Fast-fail: cheap checks before (and while) long jobs

Status: DONE — proved 2026-11-08: the release started with scripts/fast-fail.sh (all passed in 8 s). Was: IN REVIEW — 50c2260d9 (script + release/APK wiring; rule AGENTS 20a cc54044cc). Proof pending: the owner's next release/APK run starts with it.

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

## Done (50c2260d9)
- `scripts/fast-fail.sh`, 8 items each with a "caught:" line: conflict markers, ticket ledger, smoke coverage
  (`test_smoke_coverage`; 3 files merged on 11-07 had no smoke id — fixed), changed Python compiles, colours,
  answers-stale, app typecheck, vitest on changed test files + the sibling tests of changed sources (`vitest related`
  pulled most of the suite in for one core file: > 2 min, dropped). Changed = since the merge-base with
  origin/redesign/stream-sections, working tree included.
- First step of `pipeline release` preflight (`--no-fast-fail REASON`, printed; empty refused) and of `build-apk.sh`
  before the heavy lock (`--no-fast-fail=REASON`). Release preflight also checks HEAD is pushed (T484).
- `pipeline.deliver all --out-dir` refuses, before any step, a release root missing the released basemap (and tiles,
  with `--tiles`), naming the `ln -s` (the T481/T482 scratch-build failure).
- Measured: 2–3 s warm, ~15 s with a cold `tsc -b`. Each break stopped it in ≤ 3 s: a type error (also stopped
  `python -m pipeline release --staging` at preflight, 2.4 s), a hex colour in app/tools (also stopped `build-apk.sh`,
  1.2 s), a stale answers schema (pydantic field change), a conflict marker, an unlisted test file.
