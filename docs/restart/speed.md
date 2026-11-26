# Restart: the speed build chat (T1144, T1161, T1135, T1171, T1170, T1120; then T1165, T1159, T1133)

Started 2026-11-25 night by the owner: make development and releases faster. Orchestrator on Fable; at most 3
lanes (Opus for CI and pipeline changes); one independent review per PR; merge queue with auto-merge; PR titles
93 characters or less; each change shows the failure it still catches. Each lane has its own scratchpad worktree
of `origin/main` (a real `pnpm -C app install` and a `.venv` link first); never the main checkout or a release
branch. No heavy jobs: `scripts/before_pr.sh` is not run, each PR body says so. The planning chat numbers tickets;
measured plans: `tickets/archive/night-notes-2026-11-25.tar.gz`. The folder rename
is the owner's morning item: never run `scripts/rename_handoff.py` unasked.

## On main (state written 2026-11-26 03:45; minutes are measured)

- #279 pipeline tests in parallel on CI: Tests step 15.3 min (median of 21) to 7.5 / 9.0; check job 17.6 to 9.8 /
  11.3; 1.7 to 1.9x (the runner is 2 cores x 2 threads). Queue time: 7.7 to 15.4 min per PR (median before: 19.7).
- #280 release and heavy.sh keep the Mac awake; unattended on battery is refused (T1161 item 1; T1139 = same).
- #276 the PR title checked with its " (#N)" suffix (`before_pr.sh --title`, and on push to an open PR).
- #290 push hook: tests only the PR's own paths (a merge of main: 388 paths not re-tested), one push at a time per
  machine, vitest workers from free cores. Every branch must merge main to get it. A push still takes about 250 s.
- #283 MapLibre Android library built on a runner by hand (23.4 min), fetched and verified by `build.sh`; the lock
  says `"published": false`. #301 restart pages are outside the docs reading total (the owner confirms). #275, #299.

## Open

- #282 release pins files (T1135 pins part): reviewed, in the merge queue. Left: the T1140 fixture and the T1053 /
  T666 pins, two property conversions, the vintage guard, T1135 items 1, 3, 4.
- #293 fresh machine, `inseason resolve --first-build` (T1120): reviewed, auto-merge on. Clean-clone run: the stage
  works (2m27s); deliver then stopped on the part-name check (gnis:10607 "km 8 → km 8" twice). Ruled out: the
  first-build resolution (38 matches identical to the release-based one). OPEN: a from-nothing RELEASE is not
  proved; split it by rerunning the clean clone's deliver on current main (about 50 min).
- #286 build-key holes (T1171): NOT mergeable. Second review, in a PR comment: the package trace stops every real
  build on pandas / scipy Cython modules (`pipeline/_trace.py` `_sweep`: use the spec name; test with real
  pandas); an empty package or config record is accepted; the code backfill covers 333 modules. Owner picks
  (relayed 02:10): one-time backfill; keys hold only imported packages and read config. Then a third review.

## Owner

- Typed yes in the speed chat, not given yet (the planning chat relayed a yes at 02:10): the web-only release path
  and the data-contract floor check (T1165), the pre-cut gate (T1159), T1133's first steps, the public GitHub
  Release file for the MapLibre library (`publish=true` on `maplibre-android.yml`, from main).
- Until #286 lands: a `.venv` reinstalled from requirements.txt (pytest-xdist, from #279) reruns reach and deliver
  once. Every shipped `libmaplibre.so` since T978 holds his account name and a worktree path (a runner build has
  neither).

## Not started, in order

1. No third test run on push to main; a cache job stays; `tickets/archive/**` reaches its pipeline test (T1144).
2. Measure only: a 3-way split of the pipeline tier, `-n 2`, one BLAS / OpenMP thread per worker.
3. The night queue: T1112 + T1102; T1135's rest; T1063; T1147, T1137, T1138, T1130, T1079; T1161's rest.
