# T1190 — Synopsis PDF text from what is visibly rendered

Status: OPEN — the frame; the method waits on the study

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1190 | Synopsis PDF text from what is visibly rendered | OPEN — the frame; the method waits on the study | — | the study (prototype in scratch, no PR) brings a method; then a lane builds it beside today's extractor and proves it on both books against the baseline |

Source: the owner, 2026-11-26 ~17:50 (planning chat), verbatim: "also we should do a quick review of the pdf extraction code we have because it does wildd
stuff to remove bad text in the background of the pdf. i think now that claude is better we could look at the pdf extraction again and do it better (to get the
sane result) because we know the extractiuon is well tested and good. then you can use that to make sure the new method works good. we want to use what is
actually visually there based on rendering rules of pdf of something. make sense? this is just to make everything more robust for the future."
A study agent started ~17:55 (prototype in scratch, no PR); its findings fill "Method".

## Today
`pipeline/regs/extraction/extract_synopsis.py` decides which characters are real by looking at pixels: `get_cleaned_page` (L163) renders each page at 400 dpi,
`_get_bg_palette` (L109-126) takes the two commonest light colours as "background", and `_check_char_sanity` (L128-153) keeps a character only if its box holds
enough dark pixels unlike both (`COLOR_DIFF_THRESHOLD` 30, `LUMINANCE_THRESHOLD` 180, `MIN_INK_PIXELS_STANDARD` 4, font size 4-30 pt; L90-104), then groups and
clusters what is left (DBSCAN, `DBSCAN_EPS` 0.15). It works, and it is well tested, but every number is tuned by hand to this book's colours.

## Goal
Keep a character because the PDF's own rules make it visible: its fill colour and text render mode, the clip path it is drawn in, what is painted over it
later in the page's paint order, its size. No pixel sampling, no colour thresholds. The same rules also see marks the text layer does not carry, such as the
drawn line through Nanaimo River's "15" (p.17, `BOOK-ERRORS-FOR-THE-PROVINCE.md` entry 15), which the method should report, not drop silently.

## The oracle
Today's extraction is the answer key: the committed baseline `pipeline/regs/extraction/baseline/2025-27.json` (every row's water, MU, raw regs, symbols,
page, image key) and its gate (`python -m pipeline.regs.extraction.baseline check`, T426). Single pages run in seconds; the whole book is a heavy job
(`scripts/heavy.sh`). No parser run and no `claude` call is part of this (CLAUDE.md).

## Method
Open: the study's findings go here (the PDF operators read, the library, what is kept and why).

## Acceptance
1. On the current book (`data/source/fishing_synopsis.pdf`, sha 08f58537…): the baseline check passes with no accepted diff, or every differing row is listed
   with its page and the reason, one by one, and the owner agrees each is the new method being right.
2. On the August 2026 book (the gov.bc.ca copy edited 2026-08-25, T1157, T1179): today's extractor and the new method give identical row text, or each
   difference is explained the same way.
3. The new method has no colour or pixel threshold; a test per visibility rule (white text, text under a filled box, clipped text, a struck character).
Model: Opus. Size: M (after the study). Depends: the study's findings; not for the 0.10.0 re-cut.
