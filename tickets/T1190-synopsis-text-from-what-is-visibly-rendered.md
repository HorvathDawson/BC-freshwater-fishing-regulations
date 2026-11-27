# T1190 — Synopsis PDF text from what is visibly rendered

Status: PARTIAL — steps 1-4 built (PR open, a second agent reviews); left (Claude): the whole-book gate + August run before merge; step 5 later

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1190 | Synopsis PDF text from what is visibly rendered | PARTIAL — steps 1-4 built (PR open, a second agent reviews); left (Claude): the whole-book gate + August run before merge; step 5 later | 2cc9bd697, 461744993, e4cc0525d | before merge: the whole-book gate and the August book's new-method run (heavy, not yet run); then step 5, painting rules for lines and fills (`## Step 5`) |

Source: the owner, 2026-11-26 ~17:50 (planning chat), verbatim: "also we should do a quick review of the pdf extraction code we have because it does wildd
stuff to remove bad text in the background of the pdf. i think now that claude is better we could look at the pdf extraction again and do it better (to get the
sane result) because we know the extractiuon is well tested and good. then you can use that to make sure the new method works good. we want to use what is
actually visually there based on rendering rules of pdf of something. make sense? this is just to make everything more robust for the future."
The study: projects-folder note `night-notes-2026-11-26/pdf-extraction-study.md` (prototype in scratch). Owner decisions ~19:20 (memory note
`planning-chat-2026-11-26-day.md`): PyMuPDF accepted; struck text kept, marked, one warning line, only a NEW strike flagged; the August book added.

## Method (built)
`pipeline/regs/extraction/visible.py` decides which of pdfplumber's characters are on the paper by the PDF's own painting rules, on MuPDF's display list
(one paint-order number shared by text spans and paths, plus blend modes, opacity, layers and clips). A glyph is hidden only for a stated reason: render mode
3/7, opacity 0, a layer off (the page is traced with every layer on), outside the crop box, clipped (MuPDF's TEXT_CLIP), covered by a LATER opaque fill
(Normal blend, opacity 1 up its groups, inside its clip; two fills meeting under one glyph cover it together) or image, the same glyph drawn again later at
the same origin (spaces too), or the same colour as the fill provably under it. No pixel is sampled and no colour threshold is used. pdfplumber stays the
layout engine; the two read the same content stream and are paired in content order: a text disagreement refuses the page by name (`PageNotJudged`); an
origin disagreement (PDF p.61's scale-bar "8", placed 47 pt off by pdfminer, a map page) is said on stderr and refuses the page only if it holds rows.
`get_cleaned_page` calls it; nothing else in the extractor changed. A page that fails now fails the whole extraction run, naming every page.

LIBRARY: PyMuPDF 2.2 (MuPDF 1.28.6), AGPL-3.0 or Artifex commercial; pinned in `constraints.txt`, the licence noted in `requirements.txt`. Pipeline only,
never shipped in the app. Only `visible.py` imports it: a swap re-implements its five MuPDF readers; the rule tests write their PDFs by hand and stand.

DELETED: the 400 dpi cleaning render, the two-colour background palette, the per-character ink test (`COLOR_DIFF_THRESHOLD`, `LUMINANCE_THRESHOLD`,
`MIN_INK_PIXELS_STANDARD`, `SYMBOLS_TO_RELAX`), the 4-30 pt font-size filter, DBSCAN over (line, paint order) with `StandardScaler` and the 80 %/20 %
passes, the "BROKEN CODE" first-line overlap cut, the cluster debug plots (matplotlib), and scikit-learn with joblib and threadpoolctl (nothing else used
them; scipy is now listed itself for `app/tools/fish-sources/make-signal-crayfish.py`, which had it only through scikit-learn).

STRIKE-THROUGH: a thin horizontal line painted after a visible glyph, through its middle band, lying mostly on text. The row carries it as a SEPARATE field,
`raw_regs_marked` (`raw_regs` with each struck run in `~~`, e.g. "Sept 1-Oct ~~15~~"), present only on a row with a strike; the extractor refuses a marked
text that is not `raw_regs` plus the marks. `raw_regs` is unchanged because it is what every consumer keys on: the T426 baseline row sha, the batch
`rows_digest` (water + raw_regs) and entry ids, `synopsis_book carry` (raw_regs == regs_verbatim), ingest (`regs_verbatim`), the review tool's reuse index.
A row with no strike serialises byte for byte as before. A run prints ONE `[struck]` line listing every struck item; one not in
`pipeline/regs/extraction/known_strikes.json` is marked NEW, and the T426 gate fails on a NEW strike or a recorded one it no longer reads. Baseline recorded:
Nanaimo River, PDF p.19 (printed 17), "Oct ~~15~~" (BOOK-ERRORS entry 15), the only strike in the current book.

## Results
- Tests: `test_visible_text.py` 26 rule tests on PDFs written by hand; `test_visible_text_book.py` 4 pins of the real book (hidden pdfplumber chars:
  p.17 2,581, pp.24/25 27 each, pp.65-67 25 each; the Nanaimo mark; p.61's one misplaced glyph). Glyph-level, the module equals the study's prototype on
  all 88 pages (0 verdict differences). Hard pages 17-19, 24, 25, 61, 65-67 re-read against the baseline: 0 unexplained (Heber, the accepted T415 diff).
- August 2026 book, OLD method (before the swap, a pristine checkout of main): 1,393 rows, 0 page errors (65 s, 2.3 GB).
- NOT RUN (the heavy lane stayed taken for 3 h by other lanes' chained jobs): the whole-book gate on the current book and the NEW method on the
  August book. Before merge, through `scripts/heavy.sh`: `extract_synopsis --check-against pipeline/regs/extraction/baseline/2025-27.json`
  (must say 0 unexplained and one known `[struck]` item), then the August book read by both methods and diffed row by row.

## Step 5 (follow-up, not built)
Apply the same painter's model to lines and fills: `find_tables` (lines strategy) and the row bands (`get_color_sections`, a pixel column at x0+2 of a 150 dpi
render) still read every painted edge and pixel, so a stale table's frame could steer them. Read the row bands from the display list's fills, and give
`find_tables` only the edges that are visible. Keep a raster cross-check as a report (the study's `raster_check`), never a gate.

## A GitHub workflow (owner may want the feeds worker to start one; not built)
The entry point is a plain command: `python -m pipeline.regs.extraction.extract_synopsis <pdf> <out.json> --workers 4` (and `--check-against` for the
gate). A workflow needs: ubuntu + Python 3.13, `pip install -r requirements.txt -c constraints.txt` (PyMuPDF has manylinux wheels), the PDF (the tracked
copy, or a fetch from gov.bc.ca with its sha recorded), about 2 GB of memory for 4 workers, and an artifact upload of the rows file (row images at 400 dpi
are large: a rows-only flag would help). `workflow_dispatch` only, started by the worker (no GitHub cron, owner 11-05). Not yet proved on Linux:
the gate's 0 unexplained was measured on the Mac.

## Acceptance
1. Current book (`data/source/fishing_synopsis.pdf`, sha 08f58537…): the whole-book gate shows 0 unexplained rows. 2. August 2026 book: old and new
methods compared row by row, each difference explained. 3. No colour or pixel threshold; a test per rule (`pipeline/tests/test_visible_text.py`, PDFs
written by hand) and a pin of the hard pages (`test_visible_text_book.py`). Model: Opus. Size: M. Not for the 0.10.0 re-cut.
