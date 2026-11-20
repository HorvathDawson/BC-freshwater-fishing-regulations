# T791 — A smaller check plan: rehearse the release once, run the tests once, run the data tests on the new release

Status: PARTIAL (owner 2026-11-16, question 14: "Yes, as proposed (Recommended)"; item 3's code part and item 6 DONE df6f6d22e (fast-fail leaves the no-data tier to the fresh copy; app check timings); item 1 is practice; left (Claude): item 2 (one before-push command; a pre-push hook is the owner's call) and item 4 (the data tier on the candidate, in chunks))

Source: owner 2026-11-16 01:45 "Everything takes so long maybe we should have less tests all the time and do it together at end or something", "Feels wasteful"; general audit, guards checker (`handoff/PLAN/audit-2026-11-16/guards.md`).

## Measured
The staging release of r-46e4355a7277 ran 105 minutes over nine attempts; about 60 minutes repeated work that told
nothing new. The same 6,490 no-data tests ran 16 times on this Mac that night (96 minutes). 1,208 data tests in 115
files and 276 slow tests have no automatic runner; the release runs 7 of those files.

## The plan (after the owner's yes)
1. While working: the one test file touched plus fast-fail on its normal base (about a minute).
2. Before a push: one command that runs what GitHub runs (every `pipeline-ci.yml` step read from the file), automatic
   at push time (a pre-push hook is the owner's call).
3. In a release: build; `python -m pipeline release --staging --dry-run --rehearse-follows` on the candidate and fix
   everything it lists; then one real run. Drop the in-place whole tier and its two subset steps when the fresh copy
   runs in the same preflight. Rerun the fresh copy only when a commit since its pass can reach a pipeline test.
4. Added: the data tier once on the candidate, in chunks (time not measured; about 7 minutes for 700 tests by hand).
5. Not cut: the reach gate, the binding-diff stop, the changed-page stop, the fresh-copy run once per release, the
   whole app check on the candidate, the scanners, GitHub's runs.
6. Update AGENTS' timings (the app check takes 3 to 4 minutes, not 17 seconds).
Expected: about 10 minutes saved on a clean release; about 60 of last night's 105.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 14: "Yes, as proposed (Recommended)" (section H's proposal; staging releases still wait for GitHub's run: he did not pick the skip).

## Lane result (lane/bugs-080b, 2026-11-17)
- Item 3, the part that is code: when the release's fresh-copy run follows in the same preflight, fast-fail leaves it
  the whole no-data tier and its two subsets (source scanners, review tests): `release.FRESH_COPY_NEXT` /
  `fast-fail.sh fresh_copy_next`. `--no-fresh-copy` keeps them in place. Tests + two mutations.
- Item 6: the app check's time corrected (3 to 4 min) in AGENTS 18, `scripts/test-smoke.sh`, `scripts/test-full.sh`.
- Item 1 is practice (no code). Not built here: item 2 (one before-push command / hook: the owner's call on the hook),
  item 4 (the data tier on the candidate, in chunks, as a follows step).
