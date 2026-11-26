# Restart: the clean-up chat (T1081, chat D)

The first chat (2026-11-25) landed the cut as PR #262. A NEW chat finishes the rest; the planning chat writes its
prompt. Orchestrator on Fable; Sonnet lanes, at most 3, by folder; Opus only where a rule or contract is judged;
lanes spawn no sub-agents; one review round; every PR through the merge queue.

## Done (on main)

Closed tickets are `tickets/closed.tsv` + one tar.gz; owner asks are `## Owner` lines in tickets (`views/OWNER.md`);
restart pages live here; AGENTS 438 lines; decisions by subject; a README per package and stage; the size guard
`app/tools/check-size.mjs` (inside `check-tickets`); `scripts/rename_handoff.py` rehearsed, not run.
Counts: 11,153 to 6,101 tracked files; markdown 143,408 to about 39,300 lines; 1,143 to 340 MB.

## The owner's word, 2026-11-25 evening (verbatim)

"Why can't we do that stuff now before 0.10 all of it seems fine to do. Also it feels like we could remove more
markdown. The ticket mark down is okay but we want to simplify the other files. It should be simple enough to be
human resdble in not a super long period if time. Without an agent summarizing it."

So: (a) the rename and the review tool move no longer wait for the release to be on production; (b) a second cut of
the NON-ticket markdown; tickets are fine as they are.

## Left, in order

1. **Second markdown cut.** 189 non-ticket files, 27,416 lines today. Target: a person reads everything outside
   tickets in one sitting. Largest groups: `app-integration/` loose pages 5,900 (T678-progress 962, contracts 630,
   tokens 347), `research/` 2,588, `pipeline/docs` 2,347 (GOTCHAS 514, RULINGS 452, 18 and 06), `design/` 2,203
   (four trout pages), `data/curated` READMEs 2,120 (README text only, never the data), runbooks 1,594, `ios/`
   1,328, `pipeline/regs` 1,174, AGENTS 438. Ask the owner the target as multiple choice first (lines, or minutes
   to read), then cut; lower the budgets in `check-size.mjs` in the same PR. Code and tests pin some pages
   (06, 15, 18, REACH-BUILDER, ANSWERS-SPEC section names, the trout contract): move the citers first.
2. **The rename, now.** Fresh worktree off main; `node app/tools/check-size.mjs --measure`;
   `python3 scripts/rename_handoff.py --dry-run`, then `--commit`; guards; one PR. It breaks every open branch:
   tell the planning chat first, and each branch owner runs the same script on their branch, then merges main. The
   release branch is the risk: a fix cherry-picked onto it after the rename must not touch a ticket or docs path
   (code-only picks are safe); agree that with the release chat before the commit. The four legacy restart pages
   move into this folder and `LEGACY_RESTART` goes. The git-ignored `CLAUDE.md` in the main checkout and the memory
   notes spell the old paths: hand the owner the lines.
3. **The review tool gets one home** (suggested `tools/curation-review/`; the server side may stay in
   `pipeline/regs/review`), CI on its own files only, a README. A saved-mark write-through test is proven before and
   after. Never the owner's marks, `data/curated/review` or `data/generated/review`.
4. Close T1081 with the final counts. T1120's doc trips are this chat's to fix.

## Never touch

`data/**` beyond README text, `handoff/PLAN/sloughs/marks/`,
`handoff/PLAN/curation-review/tidal/owner-decisions.json`, `handoff/PLAN/play-production-access/`,
`handoff/PLAN/repo-move/`, `handoff/PLAN/app-integration/store/`, the roadmap (path rewrites only), another live
chat's restart page. A lane that closes a ticket first answers or moves its open `## Owner` lines.
