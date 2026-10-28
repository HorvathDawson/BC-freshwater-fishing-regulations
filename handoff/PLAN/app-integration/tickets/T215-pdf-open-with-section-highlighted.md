# T215 — "open the PDF" lands on the page WITH the entry's section highlighted

Status: DONE 3e5ef46f 15d19068 7c513588

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
- VERIFIED 2026-10-27 on web (release r-09789c3d9411, 3043 rules carry a box): Cowichan River's "Rule book p.15" opens a blob PDF at
  PDF page 17 with the river's row highlighted (T215-shots/web-cowichan-p15.png). Re-cut: taps/tokens/inventory, regs-fixture
  r-4c045b454820, frozen VM fixtures (+boxes).
- LEFT (owner): R2 upload (OWNER-TODO "R2 upload": the release now includes the 24 MB PDF; bucket CORS GET). Phone path unverified
  on a device. The "Source ›" sheet pages carry no box yet; zone/province tables have no row box (plain link).

## Loading feedback + our copy everywhere (owner 2026-10-28)
- DONE: `regs/synopsisLoad.ts` is the one load state (one download at a time, a second press is ignored; cancel aborts the
  fetch and closes the blank tab); web streams the body for "Loading the synopsis… N%" (Content-Length, else manifest bytes);
  phone's fetch has no stream, so it is indeterminate there (same card, Cancel works). `SynopsisLoading` card floats in the
  Shell and sits atop the sheet (a sheet's Modal covers the Shell); the pressed page's links read "Loading the synopsis… N%".
  Nothing draws in the first 150 ms (a cached copy opens with no flash). Failure: card with Retry + "Open on gov.bc.ca ›".
- DONE: a link with NO box (zone/province tables) now opens OUR copy at its page, unmarked (web `#page=`, phone OpenAction /
  page-first on Android); the About page's synopsis link uses our copy too. gov.bc.ca remains only as the error fallback and
  when a release names no synopsis (fixture).
- Verified on :39218 desktop + phone width with CDP throttle (T215-shots/loading-*.png, error-*.png); staging
  data-staging.canifishthis.ca serves r-46be3c135e68/synopsis-08f58537261f.pdf as application/pdf, CORS `*`, immutable.

Commits: 3e5ef46f 15d19068 7c513588 f0bce5ad
