# Next session (written 2026-11-21 10:40): 0.8.4 is out; start the post-release list

## State
- **Release IDs and builds:** in the STATUS.md top entry (2026-11-21). 0.8.4 is out: production data in the old
  formats, Android on Play Internal, iPhone on TestFlight internal with external waiting for Apple's beta review, and the
  production website. Never Play Closed; an App Store submission is the owner's.
- **dev** `redesign/stream-sections` is pushed. Every release lane is merged; T940 stays unmerged on purpose (below).
- **The one-time agent override** of `release --prod` was used once and reverted. Production data is human-only again
  (OWNER-TODO 2026-11-20).

## IN FLIGHT (chat of 2026-11-21 afternoon; read this first after a /compact)
- 14:02 SCOPE LOCK (owner): 0.8.5 takes nothing new except bugs found during the release work; every new mention goes to the next release (new ticket, POSTPONED/next).
- 14:01 STATE: ALL lanes merged into redesign/stream-sections (t984, t983, ui, picks, t988, t989, t981, mouth; mini release
  re-cut). Unpushed. Running in background: fast-fail + smoke + full vitest (post-merge), and ONE combined review agent
  (diff 97a9abcd4..HEAD). NOT yet merged: lane/085-t990 (all-sources button at the END of the regs page, 2 commits,
  tests pass) — merge after the checks/review. T991 sources audit = POSTPONED, next release.
  Next: fix review findings -> merge t990 -> push -> emulator walk (T984, T985 #1/#3, T988 Done on src:trout, T989 adb
  VIEW intent) -> bump 0.8.5 + changelog -> T686 flip -> bundle rebuild needed (mouth columns: `python -m pipeline.deliver`;
  T981 needs the reach step too) -> staging -> T975 lane -> owner prod commands (+ PROD FEEDS WORKER redeploy) -> Play
  Internal/Closed, TestFlight, prod website. Owner wants a PushNotification with every question.
- 13:31 merged t989 + fixed the full check's 3 reds (a0ec66149). RELEASE NOTE: the PRODUCTION feeds worker must be redeployed
  with 0.8.5 (owner command) so gauge cards say the new band words; until then the site reads the old ones (PRIOR_BAND_WORDS).
  T989 is the APP link path (website re-tested: lands); confirm in the emulator walk (adb VIEW intent, close).
- Step 0 done (answers in OWNER-TODO "0.8.5 batch scope"): IN = T686 flip, T975, T981 (path + Lost Lagoon only);
  next chat = T978, T940, T977, T987 (docs/clean-up, postponed); T709/T715 postponed; no Google Group.
- Store checks done; new tickets T985 (in-app reports), T986 (species switch), T988 (trout-table Sources Done, Android).
- Owner design picks all recorded in the tickets (T982 #2/#3/#4/#6, T985 #2/#4, T986, T979, T983).
- Lane branches (worktrees under .claude/worktrees/agent-*), none merged yet:
  - lane/085-t984: T984 built + testbed scenarios rewritten (emulator not run). 2 regs colour-count vitest failures to check vs base.
  - lane/085-t983: T983 fix + T980 un-skipped (fixture re-cut); finishing the inbox own-date pick. Ledger rows → IN PROGRESS at merge.
  - lane/085-ui: T982 #1/#5, T985 #4 (chart handle + hold-drag), T979 (phone map fills); T985 #1/#3 not reproduced on web (native-only: emulator).
  - lane/085-picks: T986, T982 #2/#3/#4/#6 built; no after-shots; delete untracked node_modules/data symlinks before merge.
  - lane/085-t988: table test of every close (61 pass, no source change); cause NOT found. Suspect: Sheet.tsx phone path, the
    RNGH pull GestureDetector round the handle row swallows Done on the tall Sources sheet (SHEET_BODY_CAP). Repro in the
    post-merge emulator walk (adb tap Done on src:trout vs a short sheet), then move Done out of the detector.
  - lane/085-t981: building the curated local-closures path + Lost Lagoon.
  - lane/085-mouth: T985 #2 mouth label (pipeline export + app).
  - lane/085-t989: T989 deep link then back lands on the water (owner 11-21).
- Then: merge all into redesign/stream-sections (conflicts: T979/T982 ticket files, core words), ONE combined review
  (independent agent), after-shots from the main checkout, Android emulator walk (T984 testbed, T985 #1/#3, T988),
  version 0.8.5 + changelog (app/packages/core/src/changelog.ts), T686 flip (`packed.py`), data to staging, T975
  lane, then the release steps in "This chat's job" step 3.

## This chat's job: the EASY BATCH, released all the way to production, then a NEW CHAT (owner 2026-11-21)
Owner: "we want all feedback items targetted in it … do a release all the way to prod then it will ask to start a new
chat for rest". Credits are tight, so stay lean. Lanes on Sonnet for UI work; ONE combined independent review before
the merge; long jobs as background scripts; pictures only where the owner picks.

0. **Ask the owner first, ONE ticket at a time, whether to add it to this batch** (owner 2026-11-21). Use one short
   question each, with a one-line plain summary and your recommendation:
   - T978 (build checks; MapLibre from upstream main);
   - T940 (send its side-by-sides first);
   - T686, then T975 (packed formats; only possible once 0.8.4 is live in both stores' production);
   - T981 (municipal closed waters);
   - T977 (push carry race);
   - T709 and T715 (postponed by the owner).
   Record each answer in the ticket and in OWNER-TODO in the same turn. A "yes" joins the batch; the rest wait for the
   next chat.
1. **Store checks** (cheap):
   - TestFlight review state: `node deploy/staging/testflight-distribute.mjs --version 0.8.4 --build <the iPhone build in STATUS>`.
   - New tester feedback: `node deploy/staging/testflight-feedback.mjs --days 7 --shots <dir>`.
   - In-app reports: GET feeds.canifishthis.ca/reports with the prod run token, passed by `$(cat …)`, never printed.
   - Any new item joins the batch.
2. **The batch:**
   - **T984 (ASAP):** the walkthrough only walks through and changes no state.
   - **T983 (urgent):** old in-season notices show as New after an update. Measure first.
   - **T982, all 6 tester items.** Each is a PROPOSAL; the owner decides. Fix the clear ones (#1 "% of this estimate",
     #5 the no-gauge sentence). For #2, #3, #4 and #6, build the before/after pictures and ask the owner in ONE batched
     question turn (AskUserQuestion, pictures in the same turn), then build what he picks. #6: reproduce first.
   - **T979:** a bigger map on the water Map tab.
   - **T980:** the 3 skipped follow tests. Fold the reads; don't raise the budgets.
   - **The website "join the Android test" button:** only if the owner has sent the Google Group address by then (see
     the 2026-11-21 chat; options: a Google Group, open testing, or email).
3. **Release all the way:**
   - Bump to 0.8.5.
   - Data: `python -m pipeline release --staging --push`, then production. Production is HUMAN-ONLY: give the owner the
     two commands, or ask him for a one-time override as on 11-20.
   - Android: Play Internal, then Closed (`play-upload.py --promote <versionCode> --track alpha`, owner OK'd Closed on
     11-21).
   - iPhone: TestFlight internal and external.
   - The production website.
   - Write the 0.8.5 changelog BEFORE the builds; the store notes are copied from it.
4. **Then tell the owner to start a new chat** for whatever step 0 left out:
   - T978 (build checks; MapLibre from upstream main, PR #4919 merged);
   - T940 (show the side-by-sides, ask the swatch rim question);
   - T686 then T975 (packed formats, only after 0.8.4 is live in both stores' production);
   - T981 (municipal closed waters);
   - T977;
   - T709 and T715 (postponed by the owner).
   Write the next restart page first.

## Chat length (owner 2026-11-21: "make sure future agents recommend when to start new chats or compact")
- Recommend a new chat at a natural break: after a release ships, or when a batch is done.
- Recommend /compact mid-batch when the context is long, before a heavy step (a review, a release), not in the middle of
  one.
- Before either, update this page or write the next restart page, so nothing lives only in the chat.

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
