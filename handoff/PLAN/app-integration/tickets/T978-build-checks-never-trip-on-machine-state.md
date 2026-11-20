# T978 — The release build's checks never fail on this machine's state, only on code

Status: OPEN

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

## Acceptance
- A fresh worktree of a green dev runs the build's fast-fail clean with no data rebuilt.
- A stale export or marks state fails the data release step, not the app build.

Depends: —. Size: M. Owner: no.
