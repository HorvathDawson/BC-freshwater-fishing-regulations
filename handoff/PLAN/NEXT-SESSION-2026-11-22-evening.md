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

### Open PRs — reviewed and set to auto-merge (check they merged)
- #37 T997 one label box for near boundaries — was CONFLICTING with main; lane L3 was merging main into it.
- #39 T996 APNs proven on staging, T999 incremental backup, T977 accepted, T1005 worker classes.
- #41 T552 forecast error body.

### Open PRs — need review or review fixes before merge (no auto-merge yet)
- #43 T970/T972/T976/T851/T858 (L8): review found 6 items (place-row collisions, screen-reader label, per-region
  "Checked with DFO", 3 tests that cannot fail); L8 was fixing them — read its ticket State lines.
- #44 T1003/T1009 R8 (L1): reviewed, no blocker; L1 was adding retrace-or-comment, the `check --aab` tests and the
  T1003 step-6 docs.
- #45 T1000 out-of-B.C. not drawn + T1001/T1002/T1019 (L6) and #40 T1012 Serpentine ditches (L6): the independent
  review was STOPPED before it reported — run it again before merging.
- #38 T539 ledger cleanup (L13): not reviewed; L13 was still working (T994/T995/T1006/the "Next" item-5 ticket).
- #47 (L10, draft, pushed with `--no-verify`, fast-fail NOT passed): T611, T771 part, T803 items 7/8, T164. **It adds 3
  lines to `data/curated/review/marks.jsonl` — Campbell places re-saved "right" by an AGENT through the review app. Marks
  are the owner's: do not merge until the owner confirms those three (or drop them from the PR).** T803 item 7 edits
  `data/curated/waters/tidal_split_authority.json`, an atlas input: the 0.9.0 atlas rebuild must include it. Left: T771
  item 1 (needs a rule), T840 (spans Python, worker, vectors, app); stopped: T880, T773, T782, T795, T797 item 5, T883.
- Lanes L9 (T919c/d packed assets, T740, T914, T862), L10 (rules/salmon smalls), L11 (review-app leftovers), L12
  (DFO places drawing) were told to push and open DRAFT PRs: `gh pr list --state open` shows them.
- Not started (lane stopped at once): T678 rest, T677, T560, T631, T561, T461, T679 #18 — plans in gap-audit.md.
- Stopped as too big for 0.9.0: T867 (needs an owner answer), T931, T558, T609, T905 (changes the salmon feed format).

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
