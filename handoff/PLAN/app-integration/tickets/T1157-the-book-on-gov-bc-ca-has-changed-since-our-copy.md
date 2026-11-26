# T1157 — The synopsis on gov.bc.ca has changed since the repo's copy: 37 of 88 pages differ in text; find what the data is missing

Status: OPEN — HIGH until read: a row-by-row comparison first (read-only), then the annual runbook's steps for what changed

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1157 | The synopsis on gov.bc.ca has changed since the repo's copy: 37 of 88 pages differ in text; find what the data is missing | OPEN — HIGH until read: a row-by-row comparison first (read-only), then the annual runbook's steps for what changed | — | planning chat 2026-11-25: a read-only pass classifies every changed line (wording / layout / a rule); rule changes go to the owner; parser runs are his |

Source: the planning chat, 2026-11-25 21:44, while fixing T1156. Today's
`https://www2.gov.bc.ca/…/freshwater-fishing/fishing_synopsis.pdf` (25,149,573 bytes, sha256 a83fc5995b29…) was
compared with the repo's `data/source/fishing_synopsis.pdf` (25,041,728 bytes, sha256 08f58537261f…, file date
2026-04-24), page by page, on extracted text: both have 88 pages; the text differs on 37 of them (PDF pp. 2-5,
7-12, 14, 17-19, 22, 23, 26, 29, 34-36, 47, 48, 54, 55, 63, 69, 70, 73, 75-77, 80-83, 85). The raw difference is
kept at `handoff/PLAN/app-integration/T1157-files/synopsis-text-diff-2026-11-25.txt` (893 lines; "-" is the repo
copy, "+" is gov.bc.ca that day). The memory note `synopsis-pdf-not-byte-identical` already says the served file's
bytes move from day to day, so a byte difference alone means nothing; this is a TEXT difference.

What a first look shows (not a full read; text extraction of this PDF mixes rows, so each must be confirmed on the
rendered page):
- Licensing pages (PDF pp. 4, 8-10): new text "Effective April 1, 2026 … anglers can state their Fish and Wildlife
  ID (FWID) number …", "Fishing Licensing in WILD … as of the 2026-27 licence year which starts April 1, 2026".
  The app's licensing words and fee table come from the book (T1020 family): probably behind.
- At least one water row: PDF p. 34 (Region 3), gov.bc.ca prints "SALMON RIVER 3-26 No Fishing July 30-Sept 15
  including tributaries" before the row the repo copy has. The in-season feed may already carry it (a Salmon
  River R3 extent exists there); to confirm.
- PDF p. 17 (Region 1): the repo copy's PUNTLEDGE RIVER rows do not appear in gov.bc.ca's text of that page (moved
  to another page, or changed: to confirm).
- Many pages differ only in a web address or a spelling ("www.gov.bc.ca/WildlifeManagementUnits", "licence").
- 44 changed lines mention "No Fishing", "closed", "quota" or "bait ban".

## Work
1. Read-only: classify every changed line of the 37 pages as layout / wording only / a rule or licensing change,
   reading the rendered pages of both copies side by side (not the text layer alone). For each rule change: the
   water, the region, the book's old and new words, what the app's data holds today (curated row, in-season
   notice), and whether an angler is shown something wrong now.
2. Report to the owner with the list; he decides how each comes in. The path for a changed book is the runbook
   `handoff/PLAN/runbooks/annual-synopsis.md` (keep the old copy, fetch the new, record the checksum, extract,
   parse, review): the parser runs are HUMAN-ONLY (the owner runs them), and every changed row is a proposal he
   reviews; no agent writes a mark.
3. A standing check so this is not found by accident again: the weekly feed (or `pipeline reports`) compares the
   extracted text of the served book with the repo copy's and raises an item when a page's text changes (text,
   not bytes).

## Acceptance
Every changed line is classified with its page; the owner has the rule changes as a list with pictures of the
rows; the data matches the current book or each difference is a recorded decision; the standing check exists.
Severity: HIGH until step 1 says otherwise (the app may be showing last spring's book). Release: step 1 now; the
rule changes in the first patch they fit. Model: Opus. Size: M (step 1), then by what it finds. Owner: the rows.

## Owner

- [ ] 2026-11-25 — After the read-only comparison: look at the list of rule and licensing changes in the current book and say how each comes in (your parser run and review for changed rows). Default: nothing changes in the data until you have seen the list.
