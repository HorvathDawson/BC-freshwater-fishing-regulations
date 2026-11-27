# T1190 — Synopsis PDF text from what is visibly rendered

Status: PARTIAL — steps 1-4 built (PR open, a second agent reviews); left (Claude): step 5, painting rules for lines and fills, later

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1190 | Synopsis PDF text from what is visibly rendered | PARTIAL — steps 1-4 built (PR open, a second agent reviews); left (Claude): step 5, painting rules for lines and fills, later | 2cc9bd697, 461744993, e4cc0525d, 37fdbccdd, 7fcb1f6e8, f8c24e0c5, 8dd735901, 5f36b8ea5 | step 5: the same painter's model for lines and fills (`## Step 5`); the whole-book gate and the August book are done (0 unexplained, 0 differences) |

Source: the owner, 2026-11-26 ~17:50: "we want to use what is actually visually there based on rendering rules of pdf"; study `night-notes-2026-11-26/pdf-extraction-study.md`.
Owner ~19:20 (`planning-chat-2026-11-26-day.md`): PyMuPDF accepted; struck text kept, marked, one warning line, only a NEW strike flagged; the August book added.

## Method (built)
`pipeline/regs/extraction/visible.py` (the only PyMuPDF import: AGPL-3.0 or commercial, pinned 2.2, pipeline only, noted in `requirements.txt`) says which of
pdfplumber's characters the PDF's painting rules show, on MuPDF's display list. Hidden only for a stated reason: render mode 3/7, opacity 0, a layer off, outside
the crop box, clipped, covered by a later opaque fill or image, drawn again at the same origin, the same colour as an opaque fill under it. No pixel or colour
threshold. Pairing with pdfplumber is by content order; a text disagreement refuses the page by name, an origin one (p.61's "8") only if the page holds rows.
UNSURE IS KEPT (reviews of #323): the glyph stays, one note per hit (`[visible] PDF page N: kept, <kind>: '<text>'`): a fill under a non-rectangle clip, an image
whose clip is a guess (`image-clip-unknown`), a translucent or blended "background", a line painted before the text (`possible-strike`); any of the four on a page
with rows fails the gate unless the owner lists it FOR THAT BOOK (pdf_sha256) in `known_unsure.json` (empty). DELETED: the 400 dpi cleaning render, palette, ink test, 4-30 pt filter, DBSCAN, the "BROKEN
CODE" overlap cut, debug plots, scikit-learn/joblib/threadpoolctl (scipy listed itself for one picture tool).
STRIKES: carried as `raw_regs_marked` ("Sept 1-Oct ~~15~~"), only on a struck row; `raw_regs` unchanged, since the baseline sha, `rows_digest`, carry, ingest
and reuse key on it. One `[struck]` line per run; a strike not in `known_strikes.json` is NEW and fails the T426 gate. Recorded: Nanaimo River p.19 "Oct ~~15~~".

## Results
- Tests: `test_visible_text.py` (46 hand-written PDFs, both reviews' probes), `test_visible_text_book.py` (the hard pages, Nanaimo, p.61), dump/cut-reading.
- After the re-review (2026-11-27 03:50, one heavy.sh job): gate on the current book 1,393 of 1,393, 0 unexplained (Heber accepted; Nanaimo known). August book, this
  code vs the pre-T1190 extractor's reading (`baseline/2026-08/reading.json`, cut from 48fc85d7d): 1,393 of 1,393 identical on the row sha (the TEXT), 0 pages
  refused; 48fc85d7d's own code vs that reading: 0 unexplained. Reports beside it. Notes: 0 `image-clip-unknown`; 26 `possible-strike` hits, all banner headings on 16 pages without rows.
  Reproduce: `baseline check --manifest …/2026-08/reading.json --accepted …/2026-08/accepted_diffs.json --pdf data/source/fishing_synopsis.2026-08.pdf`.
  Found: the extractor command could not start since 0399ce248 (`import get_config` in `main()`); fixed in 37fdbccdd, and by main's T1192.

## Step 5 (follow-up, not built)
`find_tables` and the row bands (a 150 dpi pixel column) still read every painted edge, so a stale frame could steer them: row bands from the display list's fills, `find_tables` given only visible edges, a raster check as a report only.

## A GitHub workflow (owner may want the feeds worker to start one; not built)
`python -m pipeline.regs.extraction.extract_synopsis <pdf> <out.json>` (`--check-against` = the gate) on ubuntu + Python 3.13 with requirements + constraints, the PDF
and its sha, ~2 GB, an artifact upload (a rows-only flag would skip the 400 dpi row images); `workflow_dispatch` from the worker, no cron. Gate measured on the Mac only.

## Acceptance
Gate 0 unexplained; the August book compared row by row, each difference explained; no colour or pixel threshold, a test per rule. Opus. M. Not for the 0.10.0 re-cut.
