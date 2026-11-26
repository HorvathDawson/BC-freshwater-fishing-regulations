# Restart: the speed build chat (T1144, T1161, T1135, T1171, T1170, T1120; then T1165, T1159, T1133)

Started 2026-11-25 night by the owner: make development and releases faster. Orchestrator on Fable; at most 3
lanes (Opus for CI and pipeline changes); one independent review per PR; merge queue with auto-merge; PR titles
93 characters or less; each change shows the failure it still catches. Each lane has its own scratchpad worktree
of `origin/main`; never the main checkout or a release branch. No heavy jobs: `scripts/before_pr.sh` is not run,
each PR body says so. The planning chat numbers tickets; measured plans are in
`handoff/PLAN/app-integration/tickets/archive/night-notes-2026-11-25.tar.gz`. The folder rename is OFF (a morning
item for the owner): never run `scripts/rename_handoff.py` unasked.

## On main (written 2026-11-26 02:30; check `gh pr view <n>` first)

- #279 pipeline tests in parallel on CI: Tests step 15.3 min (median, 21 runs) to 7.5 / 9.0; check job 17.6 to
  9.8 / 11.3; gain 1.7 to 1.9x (the runner is 2 cores x 2 threads); queue 7.7 min (median before: 19.7).
- #280 release and heavy.sh keep the Mac awake, unattended run refused on battery (T1161 item 1; T1139 is its
  duplicate). #275 this page. #276 the PR title checked with its " (#N)" suffix (`before_pr.sh --title`).

## Open PRs, in the order to finish them

1. #290 push hook: tests only the PR's own paths; one push at a time per machine; vitest workers from free cores.
   Review "merge", follow-ups applied. Auto-merge once its checks pass. Every chat must then merge main.
2. #282 release pins files (T1135 pins part). Review clean, the planning chat's word given. Its push is refused
   by main's order-dependent `test_t1140_inside_area.py` (the hardening chat is fixing it): after #290 is on main,
   merge main, push, add "T1140 fixture not re-cut: left" to the body, auto-merge.
3. #293 fresh machine: `inseason resolve --first-build` and an `inseason` build stage (T1120). In review. No
   auto-merge until the repo-move chat's clean-clone run reports (it cherry-picks 0872a455d onto 7dc238a22).
4. #286 build-key holes (T1171 step 1). Review said fix first; the lane has the fixes and the owner's two picks
   relayed 02:10 (one-time code backfill for the live atlas; a stage's key holds only the packages it imports and
   the config values it reads). Then a second review. Merge only after #293's proof AND the planning chat's word.
5. #283 MapLibre Android library built on a runner (T1170). Trial: 23.4 min; 89 of 91 AAR entries identical to the
   Mac's, the two `libmaplibre.so` differ (build paths, compiler build). In review. The lock says
   `"published": false`: NO GitHub Release until the owner types yes in the speed chat.

## Waiting for the owner's typed yes in the speed chat (a relay is not enough)

The web-only release path and the data-contract floor check (T1165), the pre-cut gate (T1159), the ship
workflow's first steps (T1133), the public Release file for #283. The planning chat says he said yes to all four
in its dialog at 02:10; the speed chat asked him for one line.

## Not started, in order

1. No third test run on push to main; a small cache job stays; `tickets/archive/**` reaches the pipeline test
   that reads it (T1144). Worktree `wt-nopush`, branch `ci/T1144-no-push-run-on-main`, nothing committed.
2. Measure only: a 3-way split of the pipeline tier, `-n 2`, one BLAS / OpenMP thread per worker (owner 01:10).
3. The planning chat's night queue: T1112 + T1102; the rest of T1135 (failing test's name, rehearsal goes on,
   resume); T1063; T1147, T1137, T1138, T1130, T1079; the rest of T1161.

## Finds sent to the planning chat (T1144 lines)

A fresh worktree needs a real `pnpm -C app install` and a `.venv` link before any commit or push; a busy Mac
fails correct pushes (three load-sensitive app tests); every shipped `libmaplibre.so` since T978 carries the
owner's account name and a worktree path (a runner build removes them).
