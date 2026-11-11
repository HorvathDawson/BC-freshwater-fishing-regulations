# T666 — One release path: everything cut from the live release is refreshed by the release itself

Status: OPEN

Source: owner 2026-11-11, after build 27's release took six runs: "this feels hard to do a release, maybe we should
have a single path to refresh all these items?"

## What went wrong on build 27 (r-303b4007a70a)
Each stop was a thing that depends on the live release but is refreshed by hand, found one at a time:
1. Two tests failed only inside fast-fail (its no-build plugin moves cached paths): a8a1fbc, 9f81817.
2. In-season notice matches had to be re-resolved on the new release (d2dc3e5).
3. `--baseline` / `--accept-diff` from the first run were wrong for the rerun once LATEST had moved.
4. The app's golden fixtures (`node app/tools/regs-recut/sync.mjs`) are cut from LATEST, so they can only be re-cut
   AFTER promote; fast-fail then fails on the next run, and the re-cut is a new commit, which must be pushed and
   makes fast-fail (8 min) run again.
5. 13 pipeline tests compare with LATEST and are red from the build until promote.
6. Every fix is a new HEAD: push, CI, fast-fail again.
7. GitHub failed to start the run-timing workflow; the only way to retry is the whole command.

## Work
1. List every artifact and test that is cut from or compares with the live release (goldens, dev regs release,
   in-season matches, section-lines, the 13 tests, baselines). One table in the release module is the source.
2. The release refreshes them itself, against the CANDIDATE, before promote: one step "refresh what follows the
   release" that re-cuts fixtures and re-resolves matches, shows the diff, and commits them with the release id (or
   stops with the one command to run). Tests that compare with LATEST compare with the candidate during a release.
3. Preflight runs ALL of its checks and reports every failure in one list, not the first one.
4. The command remembers its own flags (baseline, accepted diff) in the release ledger; a rerun needs none.
5. Fast-fail's result is keyed on the tree content it reads, not on HEAD, so a docs or fixtures-only commit does not
   rerun it in full.
6. The staging step can retry only the workflow start (bounded, said out loud), and `--dry-run` goes through every
   step's checks so a release can be rehearsed before the real one.

## Acceptance
- From a finished build, one command reaches staging with no hand step in between, or stops once with the full list.
- A rehearsal (`--dry-run`) on the current tree predicts every stop the real run would make.

Depends: T535, T601. Size: L. Part of the closed-testing run (high impact: Fable allowed).

## Owner decision 2026-11-11
- The fresh-copy (clean checkout) test run happens once in the RELEASE pre-check, not in every quick check (T535 offer).
