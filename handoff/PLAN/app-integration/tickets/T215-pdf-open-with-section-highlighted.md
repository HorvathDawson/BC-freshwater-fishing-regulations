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

## Built (2026-10-27), owner option B: the FULL PDF, web and phone the same
- DONE (code + tests): `rules[].book_box` [{pdf, box, size}] from `row_boxes.json` by (printed page, water name) — only when the
  sidecar's `pdf_sha256` is THE synopsis PDF (`display/decided.py book_boxes`; 1393/1393 water rows match; zone and province
  tables have no row, so they open unhighlighted); `RuleFacts.book_box` -> `Run{t:"book", box}` -> `onBook(pdf, box)`.
- DONE: the release ships our PDF: manifest `synopsis {id,url,bytes,sha256}` (raw `synopsis-<sha12>.pdf` beside the data files;
  `release build --synopsis`, default the repo copy; publish uploads it, content-type application/pdf).
- DONE: `pdf-lib` (ui-native); `highlightPdf` writes one /Highlight per box (`core highlight.ts` geometry); web fetches the
  release copy (HTTP cache), opens a blob URL `#page=<pdf>` in a window opened inside the press; native downloads it once per
  release into the cache dir, writes the annotated copy and opens the file (Linking, then the share sheet). No box, no copy,
  or any failure: the plain `openBook` link as before. NOT VERIFIED ON A DEVICE.
- LEFT: the deliver export + release cut (waits for the T211 merge + heavy lock), then the web screenshot; owner R2 upload
  (OWNER-TODO "R2 upload": the release now includes the PDF; bucket CORS for GET). `SourceLine` ("Source ›" sheet) pages carry
  no box yet.
