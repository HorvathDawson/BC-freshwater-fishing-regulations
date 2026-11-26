# T1179 — Adopt the August 2026 book by hand: a sub-agent applies the changes, the planning chat audits

Status: OPEN — after the re-cut (owner 2026-11-26); extends T1168 items 3-4; no parser run

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1179 | Adopt the August 2026 book by hand: a sub-agent applies the changes, the planning chat audits | OPEN — after the re-cut (owner 2026-11-26); extends T1168 items 3-4; no parser run | — | a sub-agent applies the new book's changes to the curated files by hand (no parser); the planning chat audits them; `drop` boolean + note on Little Qualicum and Lizard Lake; Cowichan, Heber |

Source: the owner, 2026-11-26 (P11): "adopt it eexcept we dont need a parser run you can run a subagent to produce the changes then youi can
audit it and make sure it parsed it right. drop the dropped rows by adding a drop boolean to the entries witrh a note why. also can we
add gotchas for them being dropped with info." Replaces T1168 items 3-4 (the stale rules and the adoption path); T1168 keeps the licence
wording. The comparison is T1157's (`T1157-changed-book`): the gov.bc.ca copy edited 2026-08-25 differs on 37 of 88 pages.

## Work
1. **No parser run** (human-only; CLAUDE.md). A sub-agent (Opus) reads the changed lines of the new book and applies each to
   the curated corpus by hand: the Cowichan River "No Fishing July 4-14" (a 2025-only notice stored as every-year), the Heber
   River's lower-canyon closure now ending Aug 31 (data: Sep 14), the rest of T1157's rule changes. Wording-only changes go
   in or are listed as skipped.
2. **Dropped rows:** LITTLE QUALICUM RIVER (1-6) and LIZARD LAKE (1-3) get `drop: true` and a `drop_note` (last printed in the
   2025-11-18 copy; absent from the 2026-08-25 copy, no notice; the Province asked: `BOOK-ERRORS-FOR-THE-PROVINCE.md` entry 16).
   Readers skip a dropped entry; nothing deletes the entry; a second Puntledge is never minted (it is printed twice in the new book).
3. The planning chat audits every change against the new PDF page by page (a table: row, before, after, page) before anything merges.
4. Adopt the new copy as `data/source/fishing_synopsis.pdf` through `runbooks/annual-synopsis.md`; the pins of T1166 move with it.
5. GOTCHAS entry (written: "Two rows the province's newer book lost").

## Acceptance
Every changed row is in the audit table and matches the page; the two dropped entries are flagged, not removed; the answers
diff lists the Cowichan and Heber changes; no `claude` call was made. Severity: medium (the Cowichan and Heber closures are
wrong next summer). Model: Opus (apply), planning chat (audit). Size: M. Depends: the re-cut.
