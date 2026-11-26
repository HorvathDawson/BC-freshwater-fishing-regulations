# Restart page: repo move chat (roadmap row 3; T1041's move, T1062, T1120, T1131)

Rules: Fable orchestrates; one lane at a time; one review round; work only in scratchpad worktrees, never the main
checkout; heavy jobs through `scripts/heavy.sh` and announced to the planning chat first; no production step, no parser
run; CONFIRM_PROD is the owner's; finds go to the planning chat, which numbers tickets; scope relayed by another chat is
confirmed with the owner directly before an outward action. Owner's earlier answers: `git log -p` of this page.

## Done

- **The move** (T1041): the repo is `canifishthis/canifishthis`; merge queue on main; the worker token replaced
  (expires 2027-11-25). What the cutover showed: `repo-move/PREP.md` section 9.
- **Release candidates** (T1062): plan agreed, code and runbooks on main (PR #257).
- **Store pictures workflow** (T1131): `.github/workflows/store-shots.yml` on main (PR #278). The Linux proof PASSED
  (hand-started run, `open_pr` false): the map renders on `ubuntu-latest`, 30 pictures, every size right. Owner
  2026-11-26 (planning chat's dialog): "Allow workflows to open PRs"; he flips the org switch.
- **Fresh clone** (T1120), in a clone beside the main checkout (`canifishthis-fresh`), 2026-11-25/26:
  - light half (clone, hooks, installs, fast tests, web app): PASS (PR #263);
  - `scripts/bootstrap.sh`: PASS on the fourth run (three network drops on the 10 GB GeoPackage; resume works);
  - the data build: atlas 53m57s / 19.9 GB, clim 0m34s, reach 20m58s / 12.3 GB, tiles 26m53s / 9.7 GB (Mac loaded);
    deliver could not start (find 3);
  - MapLibre AAR built (33 min, 20 of them the clone); Android debug build PASS (5 min); the iPhone simulator dry run
    is clean, its build was NOT run yet.

## The three finds (all with the planning chat; T1172 holds find 3)

1. `bc.pmtiles` is pinned at an address that answers 404 (`data/source/pins.json`; also `data/fetch_data.py`). The v1
   drop in `pipeline/deliver/release/prod_prune.py` lists that key; that it ran is not proven. Owner: fetch from
   Protomaps with the recorded bbox instead. Tonight the file was copied from the main checkout (its sha256 matched).
2. `fwa_coast.gpkg`, an atlas input since T678, was pinned nowhere: the build failed in one second. Owner to this
   chat: "yes you can." It is on the build bucket now and **this PR adds its pin**. The fetch of it by
   `bootstrap.sh` on a clean machine is still to be proved.
3. Deliver needs `data/generated/inseason/resolved.json`; the command that makes it needs a release; a release is what
   deliver makes. The speed chat's fix is PR #293 (branch `fix/T1120-inseason-first-build`). **Its proof is this
   chat's**: the clean clone is on the commit its stages were built at plus that one commit cherry-picked (the speed
   chat's advice: the branch's base would rebuild tiles), and `python -m pipeline build --jobs 2` was started about
   02:27 on 11-26 (heavy label `t1120-first-build-fix`, log `canifishthis-fresh-build4.log` beside the clone). Its plan:
   clim and tiles up to date, reach RUN (its code key changed: tell the speed chat), `inseason` RUN as a first build,
   deliver RUN. Report to the planning chat and the speed chat: stage times, whether `data/release/` gets a release,
   what is still lacking.

Smaller finds, not yet written into the pages: the Android precheck wants the debug keystore restored (never fresh)
at `app/apps/mobile/android/app/debug.keystore`, the setup page says otherwise; `build-apk.sh --help` is not a help
option (it runs the precheck); the build's `--status` says "running" after a crash; a worktree with a symlinked
`app/node_modules` fails knip; the pre-push docs step can time out on a loaded Mac with every test passing.

## Left, in order

1. Read the result of PR #293's proof build and report it (above), if this page still says it is running.
2. `runbooks/fresh-machine.md` is corrected (the coast pin, the dead basemap address, the keystore, tonight's fetch and
   MapLibre timings). Still to write there: section 6 after PR #293 (the first build makes the in-season file itself;
   tonight's stage times); `build.sh --compile` once the MapLibre library PR (#283) is on main. In
   `repo-move/OUTSIDE-GIT.md`: the `fwa_coast.gpkg` rows (pinned now), the `bc.pmtiles` row (its address is dead), the
   region PDF rows (not kept, nothing reads them), find 3. T1120's and T1131's ticket text.
3. NOT RUN, skipped on 2026-11-26 when credits ran short: the iPhone simulator build
   (`deploy/staging/build-ios-sim.sh`; its dry run is clean); T1131's `open_pr` test (after the owner's org switch) and
   its two low review items (a run from a non-main branch carries that branch's commits; a rerun collides on artifact
   and branch name); the reshoot step in `runbooks/release-by-hand.md` (short: the page is at its size budget); the
   deploy dry runs; the fetch of `fwa_coast.gpkg` by `bootstrap.sh` on a clean machine.
4. **Disk, about 45 GB, safe to delete once PR #293 is merged** (nothing else reads them; every heavy job announced to
   the planning chat first): the clean clone `canifishthis-fresh` beside the main checkout (16 GB of sources in its
   `data/source`, about 20 GB of build output), `canifishthis-fresh-mln` (the MapLibre work folder, 5 GB) and the
   `canifishthis-fresh-*.log` files there; this chat's scratch worktrees (`git worktree list` in the main checkout).
5. After a release is out (owner's list): Dependabot security updates and CodeQL back on; the tag ruleset.
