# T1081 — Audit every markdown file: delete the old, cut the bloated, unify the docs

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1081 | Audit every markdown file: delete the old, cut the bloated, unify the docs (1,326 files, about 134,000 lines) | OPEN — chat D starts as soon as the repo move is checked and 0.10.0 is running again (owner 2026-11-25); inventory and layout first, the folder rename at a quiet moment | | owner 2026-11-24 |

Source: owner 2026-11-24 (planning chat), verbatim: "we need to add a ticket to audit all the md files and try to
lean everything out a lot... so much md files a human couldnt read if they wanted rn some old and many bloated etc...
should unify docs etc...". Measured the same day on main: 1,326 tracked `.md` files, about 134,000 lines; 1,237 of
them under `handoff/`. Largest: OWNER-TODO (3,654), `app-integration/decisions.md` (1,729), STATUS-archive (1,679),
ANSWERS-SPEC (1,412), implement_progress (1,158), `ios/START-HERE-ios.md` (1,156), APP-PREP (1,078), AGENTS (1,010).
Related: T987 (repo docs and cleanup), T539/T540 (ledger and docs guard), PR #114, T1080 (the owner page).

## Widened by the owner 2026-11-25 14:45 (planning chat), verbatim
"the doc cleanup afte rrepo move really needs to reduce our content in the app. we have so many things and we can
distill tickets a bit and make it more wieldy etc etc so much unneeded content I want. ahuman to be able to go
through code and be able to not get super lost. handoff folder is a weird name too..."

Measured the same day on main: 6,434 tracked files under `handoff/` (1,386 of them markdown; the rest mostly
pictures in the `Tnnn-shots` folders), about 1,060 ticket files of which about 835 are closed, and 180 code, test,
script and workflow files that spell the path `handoff/`.

What this adds to the ticket (the planning chat's reading of his words; each is a question on the layout page he
agrees before anything is cut):
1. **The goal is a repo a person can walk through**, not only shorter markdown. The layout page starts with a map:
   the top-level folders in one screen, what each is, where to start reading the code, and for each package and
   pipeline stage a short README (what it does, what it reads and writes, where its entry point is). Anything the
   map cannot explain in a line is a candidate to merge, move or delete.
2. **Distill the tickets.** Proposal: a closed ticket's file is deleted and its one line (id, title, the commit or
   PR, the release) stays in a built index; what a closed ticket decided that still binds the code moves to one
   place per subject (a rule in AGENTS, a decision page, a comment at the code), not left in 835 files. Its
   pictures folder goes with it unless a doc still shows a picture from it. Git keeps everything. Open tickets are
   cut to the template: source, what, work, acceptance, the last line.
3. **Rename `handoff/`.** Proposal for him to pick from: `docs/` (plan, runbooks, tickets, store under it) or
   `project/`. One commit, by a script, with every one of the 180 citing files changed in the same commit and the
   guards run; it lands at a moment with no open PRs, which is why this chat sits right after the repo move and
   before the 0.10.1 lanes open.
4. **Less in the repo overall**: old restart pages, audits, status archives, superseded plans, one-off research,
   design prototypes nobody reads, pictures cited by nothing. The inventory gives each a verdict; `cleanup-delete-
   freely` applies (git is the safety net), except the owner's review files and marks, which are never touched.
5. **Code a person can follow** is partly not a docs job: dead code, files that are too long, names that mislead.
   This chat lists what it finds as candidates and hands them to the planning chat for tickets; it cuts no code.

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
ticket, `tickets/closed.tsv`, and one `tickets/archive/closed-2026.tar.gz`); (2) `handoff/PLAN/` becomes `docs/`
and tickets get a top-level `tickets/`; (3) `app-data/` deleted, its two scanning tests retired (kept: the
satellite and stream-imagery research, under `handoff/PLAN/research/`, in use by T1108); (4) no owner-todo
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
  `handoff/PLAN/curation-review/` cut down. On the path move and the CI filter: "do it when it is best timing" =
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
of him as multiple choice. A new chat finishes it: roadmap prompt D2, restart page `handoff/PLAN/restart/cleanup.md`.
The rename lands on the planning chat's word, when that evening's 0.10.0 PRs (#264, #265) are on main; after it,
only code-only picks go onto `release/0.10.0`, and its merge back runs the rename script on that branch first.
