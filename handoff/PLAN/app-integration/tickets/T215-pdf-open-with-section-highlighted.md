# T215 — "open the PDF" lands on the page WITH the entry's section highlighted

Status: OPEN

Source: owner 2026-10-27. The app's "open PDF to page" goes to the printed page; it should show the row/passage the
rule came from highlighted (the review app already makes a marked page image + row crop: `page N with it marked`).
Options to weigh: a served page image with the highlight drawn (simple, works everywhere), or the PDF with a highlight
annotation / a viewer that draws the bbox. Uses the source bbox the parse already stores; the synopsis PDF is not
byte-identical to gov.bc.ca (memory) — cite printed pages, link our copy.

## Owner pick (2026-10-27)
The PDF itself with a highlight (not a page image): open our synopsis PDF at the printed page with the entry's row/passage
highlighted — a highlight annotation on that page (or a viewer drawing the stored bbox), working on web and native.
