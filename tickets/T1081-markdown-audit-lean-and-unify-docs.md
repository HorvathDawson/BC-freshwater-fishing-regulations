# T1081 — Audit every markdown file: delete the old, cut the bloated, unify the docs

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1081 | Audit every markdown file: delete the old, cut the bloated, unify the docs (1,326 files, about 134,000 lines) | OPEN — chat D starts as soon as the repo move is checked and 0.10.0 is running again (owner 2026-11-25); inventory and layout first, the folder rename at a quiet moment | | owner 2026-11-24 |

Source: owner 2026-11-24 (planning chat), verbatim: "we need to add a ticket to audit all the md files and try to
lean everything out a lot... so much md files a human couldnt read if they wanted rn some old and many bloated etc...
should unify docs etc...". Measured the same day on main: 1,326 tracked `.md` files, about 134,000 lines; 1,237 of
them under `docs/`. Largest: OWNER-TODO (3,654), `app-integration/decisions.md` (1,729), STATUS-archive (1,679),
ANSWERS-SPEC (1,412), implement_progress (1,158), `ios/START-HERE-ios.md` (1,156), APP-PREP (1,078), AGENTS (1,010).
Related: T987 (repo docs and cleanup), T539/T540 (ledger and docs guard), PR #114, T1080 (the owner page).

## Widened by the owner 2026-11-25 14:45 (planning chat), verbatim
"the doc cleanup afte rrepo move really needs to reduce our content in the app. we have so many things and we can
distill tickets a bit and make it more wieldy etc etc so much unneeded content I want. ahuman to be able to go
through code and be able to not get super lost. handoff folder is a weird name too..."

Measured the same day: 6,434 tracked files under `docs/` (1,386 markdown), about 1,060 ticket files (about 835
closed), 180 code, test, script and workflow files spelling the old path. The planning chat read five aims into his
words (each agreed on the layout page before any cut): a repo a person can walk through (a one-screen map, a short
README per package and stage); closed tickets reduced to one index line each; the folder renamed; less in the repo
overall (old restart pages, audits, superseded plans, pictures cited by nothing; never the owner's review files and
marks); code a person can follow, listed as candidates for the planning chat (this chat cuts no code).

## Work
1. Inventory first, no edits: every `.md` with size, last change, who cites it (code, guard, another doc), and a
   verdict: keep / merge into X / cut down / archive / delete. Closed tickets are counted as one group, not file by
   file. The inventory is one table the owner can skim, with the proposed target layout on top.
2. Target layout, proposed for the owner to agree: one short entry page; one rules page (AGENTS, cut to rules that
   still hold, each stated once); one current-state page; one work list (the ledger); one owner page (T1080);
   runbooks; per-feature design pages only for features still being built; everything else deleted or in one
   history folder nothing links to. A human reads the first four in under half an hour.
3. Then cut, in lanes by folder: delete freely (owner rule 2026-10-24: git is the safety net), merge duplicates,
   move closed tickets and finished plans out of the working tree's main paths, fix every citation the guard and
   the code make (`check-tickets.mjs` checkDocs, START-HERE, runbooks).
4. A guard so it stays lean: a size budget for the pages a new chat must read, and a check that a new restart or
   in-flight page replaces the old one.

## Acceptance
The inventory with the owner's yes on the layout; after the cut: the file and line counts before and after are in
the ticket; a new chat's required reading is stated in lines; every guard and test passes; no doc cites a deleted
path.

Depends: T1080 and PR #114 first (they change the two biggest working pages). Severity: process (every chat pays for
reading it; a human cannot). Release: none; its own docs chat after 0.9.1 (proposed chat D), about 3 lanes. Model:
Sonnet for the inventory and cuts, Opus for AGENTS (deciding which rules still hold). Size: L. Owner: yes (agrees the
layout; says which history he wants kept).

## 2026-11-25 afternoon: the owner's layout picks, the conditions, two additions (clean-up chat is session -77)

**Owner's picks (in the clean-up chat's dialogs, relayed):** (1) closed tickets (836 files, 2,012 pictures, 528
MB): "move to archive but keep it compressed; images and assets can be dropped" (one index line per closed
ticket, `tickets/closed.tsv`, and one `tickets/archive/closed-2026.tar.gz`); (2) `docs/` becomes `docs/`
and tickets get a top-level `tickets/`; (3) `app-data/` deleted, its two scanning tests retired (kept: the
satellite and stream-imagery research, under `docs/research/`, in use by T1108); (4) no owner-todo
folder: an "## Owner" block in the ticket it belongs to, one built OWNER page ("it should live in tickets folder
in some way and be easier to manage and control / keep info ... decisions too maybe"); (5) `decisions.md` one
page by subject, only what binds now; (6) restart pages `restart/<chat>.md`, 60 lines or fewer, deleted when the
chat ends. He said "yes start" at about 16:10; the lanes work on local branches.

**The planning chat's conditions on the cut:** `pipeline reports` finds the same count of NEW items before and
after (report ids and crash hashes cited in closed tickets stay findable); ticket numbers are never reused,
claims of unmerged PRs included; `play-production-access/` moves whole; open tickets keep their pictures and
files; the owner's review marks and `data/` are untouched; the planning chat's docs land before the cut PR; the
RENAME waits for 0.10.0 on production and merged back, with no other PR open, on the planning chat's word.

**Added by the owner 2026-11-25:**
- Workflows (relayed by the repo-move chat, 15:44): "clean up workflows after too. can delete bloat as part of
  cleanup" (the work is T1144).
- The curation review tool STAYS in this repo. He asked "should curation review be moved to a different repo?";
  the planning chat's answer was no (it writes through pipeline code, the `test_review_*` tests guard both
  halves, it is about 60 files); his reply: "okay thatr works". Wanted: one obvious home with a short README (how
  to start it, what it writes), its tests in CI only when its files change, the old plans under
  `docs/curation-review/` cut down. On the path move and the CI filter: "do it when it is best timing" =
  with the rename, last, with a saved-mark write-through test proven before and after. A separate repo only if
  outside curators or another province is ever planned.

**After the cut merges:** numbering is `node app/tools/ledger.mjs --next` then `--claim`; owner asks are lines
under "## Owner" (`- [ ] date — ask`), unfiled ones in `tickets/OWNER-UNFILED.md`; the planning chat rewrites
prompt P and closes T1080 (delivered by `views/OWNER.md`).

## Result of the cut (branch `docs/T1081-lean-out`, 2026-11-25 evening; the rename and the review-tool move are still to come)

| | before (`03be2862e`) | after |
|---|---|---|
| tracked files | 11,153 | 6,062 |
| markdown files | 1,483 | 438 |
| markdown lines | 143,408 | 38,648 |
| tracked size | 1,143 MB | 349 MB |

A new chat's required reading, in lines: README 64 + START-HERE 27 + AGENTS 438 + STATUS 45 + its restart page (this
chat's: 37) + `views/OWNER.md` (about 208, built) = about 819. Review (independent, 1 blocker, 9 should-fix, 14 nits): the
blocker and every should-fix item are done; nits applied where they were plain corrections.

## 2026-11-25 evening: the owner moves the rest forward and asks for a second cut
Owner, to the clean-up chat (relayed to the planning chat 19:27), verbatim: "Why can't we do that stuff now before
0.10 all of it seems fine to do. Also it feels like we could remove more markdown. The ticket mark down is okay
but we want to simplify the other files. It should be simple enough to be human resdble in not a super long period
if time. Without an agent summarizing it." So: the rename and the review tool's move no longer wait for 0.10.0 on
production (the planning chat's condition, overruled by him); a second cut of the non-ticket markdown, target asked
of him as multiple choice. A new chat finishes it: roadmap prompt D2, restart page `docs/restart/cleanup.md`.
The rename lands on the planning chat's word, when that evening's 0.10.0 PRs (#264, #265) are on main; after it,
only code-only picks go onto `release/0.10.0`, and its merge back runs the rename script on that branch first.

## The second cut (branch `docs/T1081-second-cut`, 2026-11-25 night)

The owner's picks (dialog): (1) target "6,000 lines", offered as about 3,700 lines of reading plus look-up files; the
chat's look-up figure was wrong (about 4,200 lines cannot shrink: the book's reference text and its fixture copy, the
parser prompts, the generated route table, the changelog), so the reading part was cut as far as picks 2 to 4 take
it; (2) working pages of open tickets outside the tickets folder: "Delete them", what is still open as a short list
in the ticket; (3) research, design, iOS, SEO, verbatim: "we should just simplify to what is currently built and
explaining it we could add auxillary research from other tabs but unifying all content about the same thing for
example is good"; (4) reference pages: "One page per subject, 150 lines"; RULINGS and the data READMEs excepted.
Later that night: "we should also add ci/cd that enforces documentation standards and prevents bloat."

| markdown outside tickets | before | after |
|---|---|---|
| files | 190 | 150 |
| lines | 27,442 | 14,556 |
| lines without the look-up files (the reading) | about 23,200 | 10,350 |

Held as they are, for a later small PR: the three release runbooks and `release/PRODUCTION.md` (1,163 lines; 0.10.0
ships from them), the T1140 page (763; PR #265), the T1053 head-lake list (272; his open line asks him to read it).
The guard (`check-size.mjs`) now also holds: a page outside tickets is 150 lines at most unless listed (`PAGE_LONG`,
only shrinks); one total for the reading (`readingLines`); no page named for a day; no ticket working page outside
the tickets folder unless listed with its reason (`WORKING_PAGES`).

## 2026-11-26: the rename landed (#310); what is left
Tickets are top-level `tickets/`, the rest is `docs/` (#310, 11:31 MST). Each open branch runs
`python3 scripts/rename_handoff.py --commit`, then merges main. #301 (the restart pages outside the reading total):
the owner confirmed it ("Confirm", 2026-11-26). Left: the review tool's one home (`docs/restart/cleanup.md` item 3);
the four release runbook pages cut to one (item 4); close this ticket with the final counts.
