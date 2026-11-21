# Next session (written 2026-11-21 10:40): 0.8.4 is out; start the post-release list

## State
- **Release IDs and builds:** in the STATUS.md top entry (2026-11-21). 0.8.4 is out: production data in the old
  formats, Android on Play Internal, iPhone on TestFlight internal with external waiting for Apple's beta review, and the
  production website. Never Play Closed; an App Store submission is the owner's.
- **dev** `redesign/stream-sections` is pushed. Every release lane is merged; T940 stays unmerged on purpose (below).
- **The one-time agent override** of `release --prod` was used once and reverted. Production data is human-only again
  (OWNER-TODO 2026-11-20).

## Do next, in this order
1. **Check the stores:**
   - TestFlight external review: `node deploy/staging/testflight-distribute.mjs --version 0.8.4 --build <the iPhone build in STATUS>` prints the
     state.
   - New tester feedback: `node deploy/staging/testflight-feedback.mjs --days 7 --shots <dir>`. Put new items on a ticket
     like T982.
   - In-app reports: `GET https://feeds.canifishthis.ca/reports` with the prod run token
     (app/workers/feeds/.run-token.prod, passed by `$(cat …)`, never printed).
2. **T980:** three follow tests were skipped to ship.
   - Two T286 page-read budgets are over by 1 to 4 calls (Norrish Creek, Cultus Lake). Fold the reads; don't raise the
     budgets.
   - The unnamed tributary-closed fixture sample is missing.
3. **T978:** release-build checks must fail only on code, never on machine state. Also build MapLibre from upstream
   main: the owner's PR maplibre-native#4919 is merged.
4. **T982** (TestFlight feedback, 6 items) and **T979** (bigger map on the water Map tab).
5. **0.8.4 changelog:** the store notes went out with one line. The fuller draft is committed (7ba37616d) and shows from
   the next build.
6. **T940:** show the owner its side-by-sides (worktree `.claude/worktrees/agent-a902ae6ef7e370db1`,
   `T940-shots/sbs-*.png`) and ask the light-card swatch rim question. Merge only after.
7. **After 0.8.4 is live in BOTH stores' production:** T686 (switch to the packed formats, `SHIP_PACKED_FORMATS`), then
   T975 (remove the old-format readers).
8. **Postponed by the owner:** T709 and T715.

## Waiting on the owner (also in OWNER-TODO and the To-Do artifact)
- Ask a Conservation Officer the T973 question (pipeline/docs/GOTCHAS.md, "Classified Waters licence"). Its answer
  drives T974.
- Q182: keep the iPhone keyboard Save bar (T723)?
- T725: the oldest supported iPhone.
- T760: optional "Wait for Wi-Fi" phone check.
- T981: accept the municipal / park-bylaw closed-waters inventory rows when they are drafted.

## Notes
- Leftover worktree:
  `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/cf82a6c9-701e-4893-8e78-27e57c90d8fd/scratchpad/wt-w4`
  (lane/w4-ci, merged). It was kept because it had working-tree edits; check them, then remove it.
- The owner is low on credits. Keep briefs short, run long jobs as background scripts, and use one combined
  independent review before a merge.
