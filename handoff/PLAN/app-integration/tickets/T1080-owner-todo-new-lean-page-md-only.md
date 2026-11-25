# T1080 — A new owner to-do page: markdown only, short, open items only (the artifact page is retired)

Status: OPEN.

Source: owner 2026-11-24 (planning chat), verbatim: "we will make a new owner todo page (no artifact only md files
that you ask)". The Owner To-Do artifact could not be opened by any chat on 2026-11-24, and `OWNER-TODO.md` is 3,654
lines, most of it history. Related: PR #114 (T1041: one file per owner item, the page built from them), held until
`release/0.9.0` is merged back.

## Work
1. A new page the owner can read in a few minutes: only what is open, newest first, in three groups: Do (clicks and
   commands, with the steps), Decide (questions, each with its default), Look (pictures and checks). One or two lines
   per item, a link to its ticket for the detail.
2. Everything answered or done is deleted from the working tree (owner 2026-11-24, picked "Delete it": git keeps
   every line); no history file.
3. No artifact: remove the artifact link and rule from the page, AGENTS, the roadmap prompt and the restart pages.
   Chats ask the owner in chat, in batches, and write each answer into the ticket and the page in the same turn.
4. Fit it to PR #114's one-file-per-item layout: build after #114 merges (or fold the two together), so the page is
   generated once, not restructured twice.

## Acceptance
The page is under about 150 lines and lists every open owner item and nothing closed; the ticket guard still finds
every text it checks; no file names the artifact as a place to write.

Depends: PR #114. Severity: process (the owner cannot use the current page). Release: none; the planning chat does it
(docs only) right after #114 merges. Model: Sonnet for the sort, the planning chat reviews. Size: S–M. Owner: yes
(reads the first version and says what is missing).
