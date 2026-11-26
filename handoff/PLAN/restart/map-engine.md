# Restart: the map engine chat (T1154)

The chat that builds T1154, the seam that makes a later map engine switch easy. Nothing the user sees may change.
State of the work is the T1154 ticket and its ledger row; this page says how the chat works and what is owed.

## Scope (frozen, owner 2026-11-26)
- T1154 only. The Mapbox trial and the local tile server are T1155, later. MapLibre stays on `pmtiles://file://`.
- Refactors stay within noise: 5% cold start, 2 fps, 10 MB. Offline imagery is the interface only, no downloads.
- Not to be touched: the MapLibre patch's behaviour, `.github/workflows`, the release tooling, the data build.
- The design is the scoping notes `T1154-scoping.md` (the ticket names the archive that holds them); they overrule
  the owner's pasted handoff wherever the two differ.

## Order
0. The two missing tests (the still tap, the per-tile event): merged, PR #284.
1. The style dialect and tile addresses: PR #288.
2. The imagery provider and the offline-imagery interface: built on branch `refactor/T1154-imagery-provider`
   (two commits, golden unchanged); its independent review and PR are next. Beside it:
3. the native split into a shell and `native/maplibre/`, with the boundary rule: in progress on branch
   `refactor/T1154-native-split`, small commits, each green on the render contract tests. Not reviewed.
   Both branches start from main plus #287 and #288: merge main into them once those two have landed.
4. The build-time switch (`EXPO_PUBLIC_MAP_ENGINE`, one engine per build): not started; it follows 3.
Before it: T1148's wording, PR #287 (owner 2026-11-26: the "Shortest" sentence; both app lines fixed). T1148
stays PARTIAL: the look inside the cache on an Android phone and an iPhone is owed.

## How this chat works
- A scratchpad worktree of `origin/main` per branch; never the main checkout. A fresh worktree needs
  `pnpm -C app install --frozen-lockfile --prefer-offline` and a `.venv` link to the main checkout's (T1112).
- Orchestrated on Fable; at most two Opus lanes; one independent review per PR; every PR through the merge queue
  with auto-merge; Conventional Commits; PR titles 93 characters or less.
- Each lane proves its change neutral: the golden style file (added by #288) stays
  byte-identical, the render contract tests keep their assertions, and each new test is shown to fail by a mutation.
- The planning chat numbers tickets and is asked before any PR that edits a docs file is queued. New finds go to it.
- Questions to the owner: multiple choice, in the chat, with pictures.

## Owed before T1154 can close (needs the Mac and phones; wait for the planning chat's word)
- The device bench on one test-bed APK and one iPhone build, before and after: `perf`, `pan`, `glide`,
  `zoom-pinch`, `switch`, `regs-open`, `offline`, `satellite`, `back-flash`, and the iOS `regs-open`.
- Pictures before and after at the same places, one set per theme and basemap.
- After the build switch: the APK carries the same `libmaplibre.so` build id (`check-loaded.sh`).
- The open "## Owner" lines of T1154, answered or moved.
- `scripts/before_pr.sh` was skipped for the PRs of 2026-11-26 on the owner's word (the Mac was in use); CI was
  the whole-suite run. From the next session it runs again before each PR.

## For the planning chat
- The golden style file is re-recorded on any intended look change (`STYLE_GOLDEN=write`); retire it after step 4.
- T1122's store-form table: the satellite row follows PR #287's privacy sentence.
