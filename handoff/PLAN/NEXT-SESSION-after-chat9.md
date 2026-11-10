# After chat 9: start here

Written 2026-11-09. It replaces `NEXT-SESSION-after-chat8.md`.

## Paste this to start the chat
> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## State
- **LATEST = r-a178299f6005, on staging** (min app 0.7.0; bindings same as r-d14). Staging site serves 0.7.0, smoke 18/18.
- **Build 22 (0.7.0) on Play internal.** The next APK carries T530 (share a gauge from Conditions); every non-production
  APK now builds with the patched MapLibre by default.
- **Ready for the workflow review (Fable 5.2):** ledger whole (every IN REVIEW ticket names its owner step, mostly the one
  "Phone check with build 22" item in OWNER-TODO); docs checked against the code (0d16ad24e); tree clean, no stray
  worktrees; fast-fail covers chat 8–9's late failures (/changes budget, CI-as-run tests, CI path gate, format pairs, …);
  CI green; emulator green (install, offline, feeds, big-update).
- **Review app:** T532 checklist (DFO + regulation extents, one water at a time) is the review screen now.

## The list, in order
1. The workflow review with Fable 5.2 (owner-led).
2. Owner reviews in the checklist; act on "Something's wrong" marks as they arrive (rebuild only when a new cut is saved).
3. T531 (species under their calendar): waits on the owner's OK, then a mockup.
4. Next APK to Play internal with T530 (owner phone look: the share sheet + the link's preview).
5. Later: T528 River Forecast Centre advisories (mockup first); prod release after DFO labelling
   (`CONFIRM_PROD=r-a178299f6005 python -m pipeline release --prod r-a178299f6005`, owner-run).

## Rules learned in chat 9
- **A key that names parts is not a key for rules.** /changes compares rule content + metres along the stream (T529).
- **A background job's silence is not its death:** read its log before restarting it (two duplicate runs this chat).
- **Emulator test steps written before a feature landed** read the wrong thing; when a step fails, look at the shot first.
