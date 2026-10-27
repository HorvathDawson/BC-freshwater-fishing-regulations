# T215 — "open the PDF" lands on the page WITH the entry's section highlighted

Status: PARTIAL

Source: owner 2026-10-27. The app's "open PDF to page" goes to the printed page; it should show the row/passage the
rule came from highlighted (the review app already makes a marked page image + row crop: `page N with it marked`).
Options to weigh: a served page image with the highlight drawn (simple, works everywhere), or the PDF with a highlight
annotation / a viewer that draws the bbox. Uses the source bbox the parse already stores; the synopsis PDF is not
byte-identical to gov.bc.ca (memory) — cite printed pages, link our copy.

## Owner pick (2026-10-27)
The PDF itself with a highlight (not a page image): open our synopsis PDF at the printed page with the entry's row/passage
highlighted using a PDF HIGHLIGHT ANNOTATION on that page (owner 2026-10-27), working on web and native.

## Built (2026-10-27) and what is left
- DONE: `app/packages/core/src/regs/highlight.ts` (+ test) — a source row box (extractor points, top-left origin, with page size)
  -> PDF `/Rect` + `/QuadPoints` (flipped, clamped; null = open the page unhighlighted).
- BLOCKED on data (an export, not run here): the app has only `rules[].book` = [[printed, pdf]] — no box. Needed: the deliver
  export adds each rule's source box(es) from `data/generated/regs/extraction/row_boxes.json` (`{pdf, box:[x0,top,x1,bottom],
  size:[w,h]}`, keyed through the entry's row image keys), shipped as `rules[].book` third element or `rules[].book_box`;
  then `RuleFacts.book`, `Run{t:"book"}` and `bookLine` carry it (answers.generated.ts is generated — regen with the export).
- THEN (app, no code yet): the open step needs OUR copy of the PDF served (bookUrl points at gov.bc.ca, not byte-identical and
  CORS-blocked) and `pdf-lib` (not installed): load, add a Highlight annotation from `highlightGeometry`, web -> blob URL
  `#page=<pdf>`; native -> write to cache (expo-file-system, already a dep) and open it. 24 MB PDF: consider extracting just
  the page. Falls back to today's `openBook` when there is no box.
