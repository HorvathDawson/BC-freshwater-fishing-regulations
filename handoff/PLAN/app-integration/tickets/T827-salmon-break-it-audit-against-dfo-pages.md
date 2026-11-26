# T827 — A fresh agent tries to break every salmon rule against DFO's own pages

Status: OPEN (future; owner 2026-11-16; after the salmon landing).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T827 | A fresh agent tries to break every salmon rule against DFO's own pages | OPEN | — | owner 2026-11-16 (future) |

Source: owner 2026-11-16 (verbatim): "We want to create a future ticket to do a similar check of all salmon regulations
to the webpages with a fresh agent that tries to break it and look for issues in the regulations".

## What
Like the T796 break-it review of the 71 book closures (`handoff/PLAN/research/t796-book-closures-hard-cases/HARD-CASES.md`),
but for EVERY salmon answer: a fresh Opus agent (no prior context) reads DFO's live regional pages (cached pages
first; no credits; never the parser), and checks every place, row, species, date range, limit, gear line and closure
against what the app shows (the app's own core over the current release, offline and under the feed): window edges,
Feb 29, wrapped years, side streams vs the exact heading, rows the reader could not read, places DFO dropped, map
colour vs card. Findings become a regression list (HARD-CASES style) both chats re-run; real faults go to the main
chat with evidence; questions go to the owner in one batch.
