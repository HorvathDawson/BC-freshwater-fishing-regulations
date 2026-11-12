# T666 — One release path: everything cut from the live release is refreshed by the release itself

Status: IN PROGRESS (all six items and the fresh-copy run built and unit-tested, 24c74a64e; left (Claude): the first real `release --staging` run is the proof)

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

## Built 2026-11-11 (24c74a64e) — nothing here ran a real release; the first one is the proof
1. **The table**: `pipeline/release_follows.py` `FOLLOWS` (`python -m pipeline.release_follows` prints it): goldens,
   the fixture release + section lines, the golden pages, the in-season matches, the reach baseline. The tests that
   compare with the release are FOUND BY RULE (they ask the one helper: `under_test.release` / `liveRelease` /
   `namedRelease` / `*.release.test.ts`), not listed.
2. **Stage 5 `follows`** (between checks and promote), on the CANDIDATE: in-season resolve (commit of the one file) →
   `sync.mjs --release <candidate>` → commit with the release id when the change only renumbers; a changed golden page
   (`app/packages/ui-native/src/regs/fixtures`) stops uncommitted until `--accept-fixtures <token>` (T667 D5) → the
   rule-found tests of both suites with `RELEASE_DIR` = the candidate and `REGS_STORE_REQUIRED=1` → ONE push gate
   (the one command, or `--push` for the release's own commits only). `sync.mjs` and `check-goldens-paired.py` read
   `--release` / `RELEASE_DIR`; fast-fail is given `RELEASE_DIR` once the fixtures are cut for the candidate.
3. **Preflight** is one table (`release.PREFLIGHT`); every check runs, one stop lists every failure with its next
   step. `scripts/fast-fail.sh` `FAST_FAIL_ALL=1` lists every failed step (`FAST_FAIL_REPORT`).
4. **Flags remembered** in the ledger (`run`): `--baseline` (also copied under baseline/), `--accept-diff`,
   `--accept-fixtures`, `--min-app-version`, `--push`; a `--baseline` left over from before the promote is ignored out
   loud; `--forget-flags`. Escapes are never remembered.
5. **Fast-fail's pass is keyed on the tree's content** (`release.tree_key`); after a pass its base is the commit it
   passed on, so a fixtures or docs commit reruns no test tier.
6. **Workflow start** retried by `scripts/start-workflow.sh` (3 tries, 120 s apart, each said; exit 75, which the
   staging stage names); the upload's first refusals are asked up front (`staging_prechecks`). **`--dry-run` is a
   rehearsal**: the same preflight table, then (once a CANDIDATE exists) manifest, app gate, binding diff, in-season
   `--check`, taps.py into a temp file, the upload's refusals; it ends with the list of stops.
- **Fresh-copy run** (owner decision below): `scripts/fresh-copy-tests.sh` (HEAD in a temp clone, CI's no-data tier,
  chunked), preflight's last check, once per release (`--no-fresh-copy REASON`).
- Tests: `pipeline/tests/test_release_command.py` (65), `test_release_follows.py` (15),
  `scripts/tests/start-workflow.test.sh` (13 checks); ten mutations of release.py each caught.

## Left
- The first real release run: the follows stage end to end (the re-cut on a candidate, the tests on it — 5 pipeline and
  22 app files today, never run together with `RELEASE_DIR` — the commit, the push gate), fast-fail with
  `FAST_FAIL_ALL` over the real step list, the whole fresh-copy tier, the rehearsal on a real CANDIDATE, a real
  workflow-start refusal.
- The rehearsal does not predict: the build itself, whether a golden page changes (known only after the full re-cut),
  the tests on the candidate (they need the re-cut fixtures), the upload and its smoke test.
- Defaults taken, for the owner's batch: the release does not push unless `--push`; escapes are not remembered;
  "an answer changed" = a golden page file changed; the fresh-copy run reruns after any commit that touches more than
  the release's own fixtures / matches.
