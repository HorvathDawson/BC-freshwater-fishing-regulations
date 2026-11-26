# Restart: the clean-up chat (T1081, chat D)

Orchestrator on Fable; Sonnet lanes, at most 3, by folder; Opus only where a rule or contract is judged; lanes spawn
no sub-agents; one review round; every PR through the merge queue, one docs PR at a time (ask the planning chat
before queuing). No code is cut; code candidates and new finds go to the planning chat, which alone numbers tickets.
Work in a scratchpad worktree of `origin/main`, never the main checkout.

## Done

- The first cut (PR #262): closed tickets are `tickets/closed.tsv` + one tar.gz; owner asks are `## Owner` lines.
- The second cut (branch `docs/T1081-second-cut`): markdown outside tickets 190 files / 27,442 lines to 150 /
  14,556; the owner's picks and the counts are in T1081. The guard `app/tools/check-size.mjs` holds the page
  standards (150 lines a page, the reading total, no dated pages, no ticket working pages outside tickets).

## Left, in order

1. **Land the second cut** if its PR is not on main yet (review findings fixed, `scripts/before_pr.sh`, queue on the
   planning chat's word).
2. **The rename, now.** Fresh worktree off main; `node app/tools/check-size.mjs --measure`;
   `python3 scripts/rename_handoff.py --dry-run`, then `--commit` on the planning chat's word (it waits for the open
   0.10.0 PRs to be on main); guards; one PR. Agreed with the 0.10.0 chat (2026-11-25): after the rename only
   code-only picks go onto `release/0.10.0`; its merge back first runs the same script on that branch, which must
   also rewrite the Step 8 record files written there at old paths (`play-production-access/`). Each other branch
   owner runs the script on their branch, then merges main. The legacy restart pages move into this folder and
   `LEGACY_RESTART` goes. The owner gets one command for the memory notes that spell old paths (his git-ignored
   `CLAUDE.md` spells none).
3. **The review tool gets one home** (suggested `tools/curation-review/`; the server side may stay in
   `pipeline/regs/review`), CI on its own files only, a README. A saved-mark write-through test is proven before
   and after. Never the owner's marks, `data/curated/review` or `data/generated/review`.
4. **The pages held on 2026-11-25**, one small PR after 0.10.0 is merged back: `runbooks/release.md`,
   `release-by-hand.md`, `release-gates-explained.md` and `release/PRODUCTION.md` become one `release.md` plus a
   short by-hand page; the T1140 page and the T1053 head-lake list go when their tickets no longer need them. Take
   each off `PAGE_LONG` / `WORKING_PAGES` in the same PR and lower `readingLines`.
5. Close T1081 with the final counts.

## Never touch

`data/**` beyond README text, `docs/sloughs/marks/`,
`docs/curation-review/tidal/owner-decisions.json`, `docs/play-production-access/`,
`docs/repo-move/`, `docs/app-integration/store/`, the roadmap (path rewrites only), another live
chat's restart page. A lane that closes a ticket first answers or moves its open `## Owner` lines.
