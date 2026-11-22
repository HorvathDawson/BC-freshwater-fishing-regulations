# Next session (written 2026-11-21 19:45, after 0.8.5 shipped)

## State
0.8.5 is out everywhere Claude may put it:
- **Data:** production and staging, packed formats only. The minimum app is 0.8.4.
- **Production website and production feeds worker** (the worker writes the new band words).
- **Android:** Play Internal and Closed testing.
- **iPhone:** TestFlight internal. External is submitted and waiting for Apple's beta review.

Release ids and build numbers are in the top entry of STATUS.md.

The 0.8.5 batch is DONE in the ledger: T686, T975, T979, T980, T982 to T986, T988 to T990.
- T988 was never reproduced: on the emulator walk, Done closed every sheet. Reopen it if a tester reports it again.
- T981 shipped Lost Lagoon only. The owner keeps the StanleyParkVan.com source: no official source closes it.

## Start off well (the first ten minutes)
1. Read AGENTS.md, then this page, then the top of STATUS.md.
2. `git status` should be clean on `redesign/stream-sections`, in step with origin. `main` is never touched.
3. Run `scripts/heavy.sh --status`: every lane should be free, with no leftover job from the last chat.
4. Ask the owner which ticket to take first, as one dialog with a PushNotification. Recommend **T993**: it makes every
   release after it cheaper.
5. Credits are tight:
   - Sonnet lanes for UI and mechanical work, Opus for data and pipeline;
   - one combined independent review before a merge;
   - long jobs as background scripts;
   - short replies.

## First
1. Check the TestFlight external review state:
   `node deploy/staging/testflight-distribute.mjs --version 0.8.5 --build <the iPhone build in STATUS> --dry-run`.
2. Read new tester feedback:
   - iPhone: `node deploy/staging/testflight-feedback.mjs --days 7 --shots <dir>`;
   - in-app reports and Play.

## The work list (all next release; ask the owner which first)
- **T993:** document and clean up every release, CI and CD process so a HUMAN can release from one runbook. Checks
  should do the remembering. T992 (explain the golden pages and the release gates) folds into it.
- **T991:** sources display audit, so sources are shown one consistent way across the app.
- **T981:** the other municipal closed-water rows (owner review).
- **T978:** build checks; MapLibre from upstream main (PR #4919 merged).
- **T940:** show the side-by-sides and ask the swatch rim question.
- **T977. T987.**
- **T709, T715:** postponed by the owner.
- **Market:** the owner's list of similar apps is `handoff/PLAN/COMPETITORS.md` (BetterFish is the closest).

## Rules learned this chat
- **Scope lock** (owner 2026-11-21): once a release is cut, only bugs found during the release work go in. Every new
  mention becomes a next-release ticket.
- **Production feeds worker:** Claude deploys it as a release step (owner 2026-11-21), after the production data and
  after the matching production website when its words change.
- **heavy.sh** caps the start gate at 30% free. Use 30 or less. Never nest heavy.sh on the same lock.
- **Worker tests:** fast-fail now runs the feeds worker suite, which before only GitHub's app-ci ran.
- **Questions:** every question for the owner also goes out as a PushNotification.

## Waiting on the owner (also in OWNER-TODO)
- Ask a Conservation Officer the T973 question, which drives T974.
- **Q182:** keep the iPhone keyboard Save bar? (T723)
- **T725:** the oldest supported iPhone.
- **T760:** the optional "Wait for Wi-Fi" phone check.

## Chat length
Recommend a new chat at natural breaks (a release shipped, a batch done). Recommend /compact before a heavy step when
the context is long. Update this page first.

## Leftover worktrees (checked 2026-11-21; 47 merged, clean ones removed)
Each of these 29 holds commits not in dev, or uncommitted edits. Check each one (`git -C <dir> log --oneline
redesign/stream-sections..` and `status`). Keep its work if it is wanted (T940's is `lane/le-t940`; T778's is its delta
side-channel branch), else remove it.
- Under `.claude/worktrees/`:
  - `agent-a902ae6ef7e370db1` (lane/le-t940);
  - `agent-a210bb30fde58edec` (the T778 delta side-channel branch);
  - `agent-aaab7188849edc942` (lane/book-rule-fixes-1116);
  - five `worktree-agent-*` (4 locked).
- Under `~/`:
  - `bcfish-except-closed`, `bcfish-marks-audit`, `bcfish-review-fixes`, `bcfish-review-followup`;
  - `bcfish-t573-build`, `bcfish-t737u`, `bcfish-t752`, `bcfish-t753`, `bcfish-tatshenshini`.
- Old chats' scratchpads under `/private/tmp/claude-502/…`: twelve `wt-*` and `c2-*` (app/t319, t871, t884, t903
  and others).
