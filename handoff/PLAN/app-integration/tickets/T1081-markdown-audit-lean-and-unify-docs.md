# T1081 — Audit every markdown file: delete the old, cut the bloated, unify the docs

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1081 | Audit every markdown file: delete the old, cut the bloated, unify the docs (1,326 files, about 134,000 lines) | OPEN — its own docs chat after 0.10.0 (proposed chat D); inventory first | | owner 2026-11-24 |

Source: owner 2026-11-24 (planning chat), verbatim: "we need to add a ticket to audit all the md files and try to
lean everything out a lot... so much md files a human couldnt read if they wanted rn some old and many bloated etc...
should unify docs etc...". Measured the same day on main: 1,326 tracked `.md` files, about 134,000 lines; 1,237 of
them under `handoff/`. Largest: OWNER-TODO (3,654), `app-integration/decisions.md` (1,729), STATUS-archive (1,679),
ANSWERS-SPEC (1,412), implement_progress (1,158), `ios/START-HERE-ios.md` (1,156), APP-PREP (1,078), AGENTS (1,010).
Related: T987 (repo docs and cleanup), T539/T540 (ledger and docs guard), PR #114, T1080 (the owner page).

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
