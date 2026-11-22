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
