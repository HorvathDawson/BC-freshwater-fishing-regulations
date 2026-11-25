# Owner to-do: where it lives now (T1041)

The owner ticks items off in the Owner To-Do page: https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f (owner
2026-11-11). Every owner item is a row there AND a file in `handoff/PLAN/owner-todo/`, one file per item (it used to
be a section of this page; a dozen lanes editing this one page conflicted on nearly every PR).

- **Read it:** `node app/tools/ledger.mjs` builds the whole page, newest first, into `handoff/PLAN/views/OWNER-TODO.md`
  (git-ignored; the git hooks rebuild it, but it MAY BE STALE: `node app/tools/ledger.mjs --check` says so).
- **Add an item:** a new file `handoff/PLAN/owner-todo/<date>-<what>.md`: first line `order: <number>` (higher is nearer
  the top; one more than the current top puts it first), a blank line, then the item as a `## ` heading and its text.
  A ticket's `owner: OWNER-TODO "<text>"` must quote text one of these files holds (the ticket guard checks it).
- **Change or close one:** edit its file; it keeps its history text. A Store checks line from `pipeline reports` is a new
  file of its own (`<date>-store-checks.md`), never an edit of an older one.
- The page's own head (title, how the page works) is `handoff/PLAN/owner-todo/_head.md`, kept word for word from before
  T1041: where it says "this file keeps the long text" read the item files, and where it says README.md is the ledger
  read `handoff/PLAN/views/LEDGER.md` (each ticket file holds its row).

Nothing is written on this page: the ticket guard fails on any text here but this (an item added here would never
reach the view).
