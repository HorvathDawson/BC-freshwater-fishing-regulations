# T1032 — Highlight boxes for region and province rule text

Status: DONE — 2c7f8cab2. Region / province / area rules carry located highlight boxes (329/355); the 26 not located get none and are listed below. Ships with the 0.9.0 data release; not seen in the app.

Source: owner 2026-11-23, answering whether the 0.9.0 release adds highlights for the pages that hold region-wide or
province-wide rules, verbatim: "Yes, in 0.9.0 (Recommended)".

Background: T991 round 3 (PR #63) gives each water's Regulations tab a Source row with one link per synopsis page the
card cites, each opening the PDF with every highlight box that page's rules carry. Only a water's own table row had a
box (`row_boxes.json`, T215), so the region and province pages (Skeena: p.8, 9, 10, 49) opened with nothing marked.

## Work
1. Locate each zone / province / area rule's printed words in the release's synopsis PDF, deterministically (no LLM):
   the text, reduced to letters and digits, must occur on the page exactly once, on word bounds. Anything else gets no
   box and is listed with why (absent / ambiguous / short). A box is never guessed.
2. Ship them as the rule's `book_box`, the shape a row box has, so the app (`bookLine`, `regsSourcesOf` in #63) takes
   them unchanged.
3. Report coverage per cited page.

## Built
- `pipeline/regs/extraction/passage_boxes.py`: the locator (pdfplumber text flow; the rule's own `verbatim` on its
  entry's pages, else the entry's whole `printed` text), one box per run of lines in one column, 2 pt margin, and the
  coverage report (`python -m pipeline.regs.extraction.passage_boxes [--export-dir DIR] [--json OUT]`).
- `display.decided.book_boxes`: a water keeps its row box (unchanged); a `zone` / `province` / `area` rule is
  located in the PDF `book_files()` names (the release's claims copy, else the repo copy). No new sidecar: the PDF
  is already tracked and shipped, so nothing new to pin or bootstrap.
- App: no change. #63's `bookLine` already passes `facts.book_box` for every rule, and `citedPages` gathers every
  run's boxes per page.
- Tests: `pipeline/tests/test_t1032_passage_boxes.py`, exact boxes on printed p.8 / 9 / 10 / 49 (PDF 10 / 11 / 12 / 55)
  and every not-located reason; proved by mutation (word bounds, uniqueness, minimum length, text flow, block split,
  margin, rule-before-entry order, the entry fallback, the kinds `book_boxes` boxes, the report's per-page count).

## Coverage (live export 2026-11-23, rules a water's rule set holds)
329 of 355 zone / province / area rules located (325 by their own words, 4 by their entry's); water rows unchanged
(3,031 with a row box). Per printed page, rules citing it that carry a box ON that page / rules citing it (a rule whose
entry spans pages 8 and 10 is boxed on the page its words are on):

| p. | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 13 | 21 | 28 | 34 | 41 | 42 | 49 | 58 | 64 | 68 | 80 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| boxed | 1/1 | 1/1 | 7/8 | 8/8 | 49/52 | 13/18 | 11/20 | 26/29 | 25/25 | 23/23 | 22/26 | 2/2 | 23/23 | 34/40 | 25/29 | 37/41 | 21/22 | 1/4 |

Every water-row page (14–19, 22–26, 29–32, 35–40, 43–47, 50–54, 59–61, 65–66, 69–71) is fully boxed, as before.

Not located (26), so no box:
- 3 ambiguous: z1 daily quota fragments printed twice on p.13 (R1 and Haida Gwaii boxes): "none from streams",
  "1 over 50 cm", "All wild steelhead".
- 23 absent: the curated text is not the book's words (on no page of the PDF): z4 Kootenay Lake boundaries r1/r3/r4,
  z4 tributaries of lakes; z6 Atlantic salmon alert, set lining r1–r3, Skeena Quality Waters, steelhead closure r2
  ("Skeena, Nass, Iskut…" vs the book's "Skeena River, Nass River…"); z7a set lining r1–r3, stocked lakes; z7b
  possession exceptions r0–r3; z8 Syilx territory; zp measuring and possession, no-fishing scope, quota defaults r3
  ("size limit" vs the book's "size limits"), spear fishing r4. A curator can bring these to the book's words; this
  ticket writes no curated file.

## Not done / not verified
- Not seen in the app on a device or the web: the release that carries the boxes is the 0.9.0 data release.
- The answers build was run for one water (Skeena, `--items gnis:2936`) against the live bundle: rules citing p.49 carry
  their boxes (e.g. "Single barbless hook…" → PDF 55 [37.08, 451.91, 188.89, 476.41]).

Depends: T991 (#63). Size: S. Owner: no.
