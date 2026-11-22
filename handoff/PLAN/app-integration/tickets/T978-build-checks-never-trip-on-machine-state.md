# T978 — The release build's checks never fail on this machine's state, only on code

Status: IN PROGRESS (main chat 2026-11-21, with T993).

Source: owner 2026-11-20: "Add a ticket to clean up all of these test to make these sort of failure impossible". The 0.8.4
production build stopped four times in fast-fail, never on a code defect:
1. A worktree without `cache/` (the patched MapLibre AAR), so the AAR lookup came back empty and the build exited with no message.
2. No feeds run token in a worktree. The email precheck failed; it was built with `--no-email`.
3. `data/generated/inseason/resolved.json` exists only in the main checkout, so `test_mini_release` raised.
4. The committed mini release was stale after merges. It needed `python -m pipeline.tools.mini_release --write`.
5. "review marks have landed" passes only where the owner's marks are (the main checkout).
6. A stale live export (no `grp_runs`) failed `test_answers_27` and `test_t801_mainstem_tag`. The same two tests fail on
   clean dev until the export is rebuilt.

## Work
- Split the tests into code tests and live-data tests.
  - Code tests use fixtures only (the mini release or invented data) and are the build's gate.
  - Live-data tests run in the data release step against the release they check, never in the app build.
- A test that needs generated or live data builds its own (as the mini chain does), or is a live-data test.
- Merges that change mini-release inputs re-cut it, or CI fails at merge, not at build time.
- build-apk / build-ios-tf in a worktree: one precheck lists every missing link (cache, .venv, resolved.json, run token)
  with the fix. No silent exits.
- The marks-landed check moves to the data release (it is about data, not the app).
- MapLibre (owner 2026-11-20): the owner's PR merged upstream (https://github.com/maplibre/maplibre-native/pull/4919).
  Build MapLibre from upstream `main` (pinned to a commit) instead of the local lean T420 patch in `cache/maplibre`.
  Then no worktree needs the patched AAR link. Keep that until a MapLibre release includes the change, then use the
  release. Check the AAR build-id step and the "MapLibre patched always" rule (T893) against it.

## Acceptance
- A fresh worktree of a green dev runs the build's fast-fail clean with no data rebuilt.
- A stale export or marks state fails the data release step, not the app build.

Depends: —. Size: M. Owner: no.

## Lane W4, 2026-11-20 (lane/w4-ci): pipeline-ci + the release's fresh-copy run green again
Eleven no-data tests read this machine's state, so CI (and the fresh clone) failed where this checkout passed:
- 9 (test_part_touches 7, test_part_runs 1, test_mini_release 1): `export.read` reads the in-season notices' end
  tokens, whose resolution is generated since T679 #11 (`data/generated/inseason/resolved.json`, absent on CI). The
  hand-made-bundle tests now point the export at a notice file with no matches (`bundle_fixture.no_notices`); the mini
  test reads the mini tree's notices, as its chain does. The export's refusal of a missing resolution is unchanged.
- 2 (test_answers_27 t517, test_t801_mainstem_tag): hand-made waters lacked the export's `grp_runs` (T679 #17 moved the
  join into the export); each test now gives its water `export.read.group_runs(parts)`, as the export writes it.

## Owner 2026-11-21 12:40
Asked for the 0.8.5 batch: "No, next chat". Waits for the chat after 0.8.5.

## Owner 2026-11-21 (new chat after 0.8.5)
Picked for this chat together with T993 (build/audit streamlining; no release in this chat). See T993's note of the
same date for the verbatim answer.

## Owner 2026-11-21, dialog: MapLibre 0002
Asked: post 0002 upstream / build main + 0002 / leave it. Owner, verbatim: "Can we measure the different of patch 2 I
think we thought it wasn't worth it". So: measure 0001-only (upstream main) against the lean patch (0001 + 0002) on the
stressed emulator, as T420 measured lean vs full; the owner decides on the numbers. (T420's "lean vs full" run compared
the lean patch with the full one, not 0001 alone, so this is not measured yet.)

## Built (lane/t978-build-checks, 2026-11-21)
Commit: a323ef2af (code + tests). Not merged; the main chat reviews and merges.
1. One precheck lists every missing link with its fix (`deploy/lib/build-links.sh`, sourced by both builds). From a worktree
   the fix is a link or copy of the main checkout's (`ln -s <main>/.venv .venv`, `mkdir -p cache/maplibre && ln -s
   <main>/cache/maplibre/fslean-m2 …`, `cp <main>/app/workers/feeds/.run-token …`, the debug keystore); in the main
   checkout it is the command that makes it. build-apk.sh: .venv, app/node_modules, JDK, Android SDK, the review app's
   node_modules (only when fast-fail will typecheck it), keystore, run token, the MapLibre repo — the silent exit is gone
   (the `find` no longer runs on a missing dir under `set -e`; `mln_check` refuses "no AAR" instead of reading it as
   stock). build-ios-testflight.sh: a new precheck before the lock (xcodebuild, pod, node, team id, API key + its .p8,
   node_modules + wrangler login when uploading; `--precheck` runs only it; it no longer stops at the first missing
   secret after the lock wait). session.sh's AAR message names the same fix. `.gitignore`: `/.venv`, `/cache` (a link is a
   file; `.venv/` did not match it, so a linked .venv showed as a changed path).
2. Marks-landed (and wrong saves, override ids on the live atlas, goldens vs LATEST, /changes budget) are `data: …` steps
   in fast-fail: run only with `FAST_FAIL_DATA=1`, which `release.run_fast_fail` sets; elsewhere each says "left to the data
   release" and the pass line counts them. The release preflight still runs marks / wrong saves / classified itself.
3. Gate tests on machine state: W4 (above) fixed the six tests; fast-fail on a fresh worktree (no data/generated,
   no data/release, only .venv linked and `pnpm install --offline`) passed with the default base (172 s) and with the build's ship base (FAST_FAIL_BASE = the last Play build 558c527c3: the whole no-data tier, 35 chunks, 923 s, through heavy.sh).
   No T777 work; overlap: none of the moved steps is a test file, so T777's curated-root sandbox is still the open part.
4. Mini release: CI already failed on it (`mini_release --check`, prints `--write`); it now also puts the command in a
   `::error` annotation. fast-fail's trigger was four folders (grown one per miss); it is now every input of the mini
   chain (any pipeline source outside tests, its fixtures, data/curated, config, pins), so the lane's run and the run after
   a merge (default base = every unpushed commit) re-check it before any build.
Tests: test_release_follows.py (data steps left without the variable, run with it; every machine-data command is a data
step; the release sets it), test_staging_scripts_fail_safe.py (missing MapLibre repo listed, never silent; the link fixes
in a scratch repo + worktree; TestFlight precheck lists both missing secrets before the lock). AGENTS 20a has one line.

## MapLibre item: STOPPED (gap)
Upstream PR 4919 (merged 2026-11-20, merge commit 9cf8446d) is our 0001 only (source_state, render_tile, tile.hpp,
geometry_tile + tests). The lean patch is 0001 + 0002 (`upstream/0002-…`: `updateVertexVectors` returns whether a vertex
was rewritten; a bucket is marked modified / re-uploaded only then; 5 bucket files + paint_property_binder.hpp). Upstream
main at 3a6b9557 (2026-11-21) still has `void updateVertexVectors` and `updateModified(true)` unconditionally. So a build
from upstream main drops half of what the shipped AAR carries; per the brief, not built. Options for the owner: post 0002
as its own upstream PR (it was drafted for that) and build from main once it lands; or build main + 0002 as a small local
patch (no longer the whole lean patch). iOS: the patch was never in the iOS build (T893); unchanged.

Left (Claude): the MapLibre switch once the owner picks; the TestFlight build still runs no fast-fail of its own (AGENTS
20a says it should; not added here); `pnpm check`'s vitest tests that use `liveRelease()` still read LATEST when a checkout
has one (a fresh worktree has none; T777's ground).

## MapLibre 0002 measured (lane/t978-maplibre-measure, e69df5274, 2026-11-22)
Owner, verbatim: "Can we measure the different of patch 2 I think we thought it wasn't worth it". Render-thread CPU,
seconds, mean [range], 3 blocks per build alternated in one boot (Android emulator; 1 core at the 60 fps cap: the Mac
was at load 18-40 from six orphaned headless Chrome trees, so T420's E-core stress was not usable):

| place | lean (shipped: 0001 + 0002 on the release) | upstream main, 0001 only | main + 0002 |
|---|---|---|---|
| Coquitlam z13, held | 25.5 [25.3-25.7] | 20.5 [19.7-22.1] | 20.1 [18.6-21.6] |
| Coquitlam z13, 160 flings | 96.7 | 67.1 [66.8-67.3] | 67.5 [59.3-79.4] |
| Fraser Valley z11, held | 22.6 | 15.4 [14.1-16.2] | 14.6 [12.9-15.5] |

0002 is within noise (-2% / -5%, ranges overlap; upload share 19.5-21.7% vs 21.4-23.0%). Upstream main is 20-32%
cheaper than the shipped lean AAR from upstream's own changes. On main: colours after Map -> Conditions -> Map correct,
0 ANRs in tap-glide, build-id proof PASS. Build: full AAR 703 s, rebuild 264 s, APK 67-93 s; `build.sh` needs only the
tag -> a pinned upstream commit, no patch. Owner decides (OWNER-TODO): build from a pinned upstream main commit with no
patch (recommended), or keep the lean AAR.

## Owner 2026-11-22, dialog: MapLibre for 0.8.6
"Pinned upstream main (Recommended)": build from one pinned upstream main commit, no patch of ours; move to a release
tag once upstream releases one containing 0001. The switch: branch `build/T978-maplibre-upstream-main` (in progress).
