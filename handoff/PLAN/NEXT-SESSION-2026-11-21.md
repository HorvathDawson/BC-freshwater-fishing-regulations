# Next session (written 2026-11-21 10:40): 0.8.4 is out; start the post-release list

## State
- **Release IDs and builds:** in the STATUS.md top entry (2026-11-21). 0.8.4 is out: production data in the old
  formats, Android on Play Internal, iPhone on TestFlight internal with external waiting for Apple's beta review, and the
  production website. Never Play Closed; an App Store submission is the owner's.
- **dev** `redesign/stream-sections` is pushed. Every release lane is merged; T940 stays unmerged on purpose (below).
- **The one-time agent override** of `release --prod` was used once and reverted. Production data is human-only again
  (OWNER-TODO 2026-11-20).

## This chat's job: the EASY BATCH, released all the way to production, then a NEW CHAT (owner 2026-11-21)
Owner: "we want all feedback items targetted in it … do a release all the way to prod then it will ask to start a new
chat for rest". Credits are tight, so stay lean. Lanes on Sonnet for UI work; ONE combined independent review before
the merge; long jobs as background scripts; pictures only where the owner picks.

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
4. **Then tell the owner to start a new chat** for the rest:
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
