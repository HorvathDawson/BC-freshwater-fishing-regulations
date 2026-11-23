# Next session (written 2026-11-22 20:15, the 0.9.0 batch chat; stopped early: owner low on credits)

## First, every chat
`handoff/PLAN/START-HERE.md` step 0 (reports). This chat's check is OWNER-TODO's "Store checks 2026-11-22 18:15" line.

## Where the 0.9.0 batch stands
Nothing is cut yet. The batch was worked as parallel lanes in scratchpad worktrees; every lane was told to commit, push
and write a "State 2026-11-22 (stopped for credits)" line into each ticket it touched — read those lines first.
Owner scope (2026-11-22): every SMALL open ticket goes into 0.9.0; no bathymetry, stocking or other big builds.
The full gap list with a plan per ticket: `handoff/PLAN/batch-090/gap-audit.md`; the lane brief every agent followed:
`handoff/PLAN/batch-090/lane-rules.md` (worktree paths in it point at the old chat's scratchpad: make new ones).

### Merged to main
#32 (step 0: T1012, T1013), #33 (T991 inventory + pictures), #34 (T981 proposals), #35 (T1011 pages for 156 border
waters + former names + home links, T1010 `?fit=` region link, T1008 source maps served, T1007 = Cloudflare's injected
script), #36 (T1005 crash triage + fixes, T1013 Runs prewarm, new T1014–T1017), #42 (T605 phone reads `?fit=`).

### Open PRs at 20:45 (`gh pr list --state open` for now; lanes wrote "State 2026-11-22 (stopped for credits)" in tickets)
| PR | Tickets | State |
|---|---|---|
| #37 | T997 label merge | reviewed + fixed, conflict resolved, auto-merge on |
| #51 | T465 zoomed-out labels (contains #37's commits) | NOT reviewed; merge after #37; owner looks at T465-shots |
| #39 | T996 APNs on staging, T999 backup, T977, T1005 worker | reviewed + fixed, auto-merge on (staging worker runs an older head of it) |
| #41 | T552 forecast error body | reviewed, auto-merge on |
| #43 | T970/T972/T976/T851/T858 | reviewed + all 6 findings fixed, auto-merge on; device shots owed |
| #44 | T1003/T1009 R8 | reviewed + fixed; auto-merge on but CONFLICTING with main: merge main in |
| #45 | T1000 out-of-B.C. + T1001/T1002/T1019 | review STOPPED before reporting: run it again |
| #40 | T1012 Serpentine ditches | review STOPPED; CONFLICTING; needs the 0.9.0 atlas rebuild |
| #38 | T539 ledger cleanup + L13's work | NOT reviewed; CONFLICTING; L13 was still finishing at 20:45 |
| #47 | T611/T771/T803/T164 (draft) | **3 agent-saved marks in marks.jsonl: owner confirms first**; fast-fail not passed |
| #48 | T776/T779/T775/T590 (draft) | NOT reviewed; 19 wider review/dfo tests failed/errored — compare with main |
| #49 | T919 (c) packed map assets, (d) resume (draft) | NOT reviewed; the 0.9.0 data release must carry the pack; device check owed |
| #50 | T812 Babine circles (draft) | NOT reviewed; 2 pill-theme tests fail (fake map lacks addLayer); owner picks T812 look |

Stopped / not started (plans in `handoff/PLAN/batch-090/gap-audit.md`): T678 rest, T677, T560, T631, T561, T461, T679 #18,
T740 (native code), T914 (Lighthouse script was in the old scratchpad), T862 alias in slices, T504, T867, T931, T558,
T609, T905, T880, T773, T782, T795, T797 item 5, T883, T840, T771 item 1, T885 rest, T754 (owner question 2), T589,
T562 items 1–3, T590 item 3, T1002 items 3–5, T721/T726 (simulator walks). L12 proposes dropping T774 (owner confirms).

## Next (in order)
1. Merge what is reviewed; review the rest (#45 + #40, #38, the drafts), one independent review each (owner rule).
2. Prune this chat's worktrees once their PRs merged (`git worktree list`; they sit in the old chat's scratchpad).
3. One question round with the owner, pictures first (OWNER-TODO lists them): T1010 look (A recommended), T1004 tag
   (B recommended), T940 satellite colours, T991 sources pattern (A recommended), T997 after shots, T467/T802/T504
   pictures (if lane L3 made them), T867, R8 at ~72% enough? (recommended yes), T981's 5 proposals, the 0.9.0
   "What's new" bullets, any Play reviews.
4. Build the picks; then the 0.9.0 data needs a NEW ATLAS (T1012, T1000, T679…): side build from main, parity + per-rule
   binding diff, promote (T1012 needs `--accept-boundary-loss` for 2 automatic lake boundaries, attributed).
5. Cut 0.9.0 with `handoff/PLAN/runbooks/release-by-hand.md`; staging; one device test pass (the list in gap-audit.md,
   plus R8's unwalked modules: locate, bg-update task, local notification, in-app update on Play Internal).
6. Production commands are the owner's (CONFIRM_PROD): the production Android build to Play Internal (then Claude
   promotes it to Closed), the TestFlight production build, prod data / site / feeds worker; T996's APNs secrets on
   the production worker; T1014's version file copy to production R2.
