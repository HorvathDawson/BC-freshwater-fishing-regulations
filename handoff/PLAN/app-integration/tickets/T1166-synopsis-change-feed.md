# T1166 — A synopsis feed: its whole job is to notice when the province changes the book (text, not bytes) and raise an item

Status: OPEN — the first patch after 0.10.0 (owner 2026-11-25)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1166 | A synopsis feed: its whole job is to notice when the province changes the book (text, not bytes) and raise an item | OPEN — the first patch after 0.10.0 (owner 2026-11-25) | — | a build lane: fetch the served PDF, compare page text + the PDF edit date with the pinned repo copy, raise a `pipeline reports` item naming the pages; the feeds worker starts it (no GitHub cron); S |

Source: owner 2026-11-25 night (planning chat), verbatim: "we could have a synopsis feed that entire job is
fetch synopsis hash and making sure the pdf hasnt changed. because that triggers us updating stuff." It came from
T1157: the book on gov.bc.ca was republished (made 2026-07-29, edited 2026-08-25) and nobody noticed for three
months. Related: T1157, T1156, T412 (the slow-feed pattern), the memory notes `synopsis-pdf-not-byte-identical`,
`no-github-schedules`.

## Work
1. NOT a byte hash: the served file's bytes move from day to day with no change in the book (measured
   2026-09-16 / 17 and again 2026-11-25). Compare (a) the PDF's own creation and edit dates and (b) a hash of each
   page's extracted text, against a small tracked pin (page, text hash, dates, date pinned) made from
   `data/source/fishing_synopsis.pdf`. The nine region PDFs are checked the same way (T1156's pins).
2. A change raises one item in `pipeline reports` ("the synopsis changed: PDF pp. …") and a line for the owner;
   it changes no data by itself. The text layer of this PDF mixes rows (a hidden overlaid layer), so the item
   says "changed pages", and the reading is done on rendered pages (T1157's method).
3. Runs with the weekly feed through the feeds worker's button-run workflow (never a GitHub schedule). A fetch
   failure is a shown failure, not "unchanged".

## Acceptance
Fed the 2026-11-25 gov.bc.ca copy it reports the 37 changed pages; fed the repo copy it reports none; a fetch
error is reported as an error (tests for the three). Severity: medium. Model: Sonnet. Size: S. Owner: no.
