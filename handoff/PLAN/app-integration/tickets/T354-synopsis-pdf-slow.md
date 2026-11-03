# T354 — Opening the synopsis PDF takes a very long time

Status: OPEN (owner 2026-11-02, 0.4.0 mobile).

Opening the rule book (synopsis PDF) from a water page takes a very long time. Find out where the time goes:
- downloading the whole PDF on first open (size?);
- copying or decoding before display;
- rendering every page instead of jumping to the marked page.

Fix options:
- ship or pre-download the PDF with the release and open it from local storage;
- open straight to the cited page, then render lazily;
- show progress immediately.

Time it with the test bed pdf scenario, before and after (Android + iOS).
Size: S–M.

## Owner 2026-11-03: none of (a)/(b)/(c); keep the whole book, cited page first, and make it cheap
"isnt there a way to do a mix of small extract then add it back into the correct spot in pdf? ... maybe we could
compress pdf a bit?" Plan (proposal, measure before shipping):
1. **Incremental update, no parse in the app.** A PDF may be extended by APPENDING objects to the end of the file. The
   app copies the book natively (no JS heap), then appends a few KB: a new page tree with the cited page first, the
   highlight annotations on it, and a new xref with /Prev. The phone opens the whole book at the cited page,
   highlighted. The pipeline pre-computes what the append needs (root, page object numbers, last xref offset, size)
   into the release, so the app reads nothing from the 24 MB file. The book uses xref + object streams (312) and is
   linearized; the append writes an xref STREAM and drops linearization (valid; viewers read it).
2. **Compress the shipped copy.** 18.4 of 23.9 MB is images (59 JPEGs); fonts 0.1 MB. Re-encode images to ~150 dpi at
   the pipeline step (text and page layout untouched; printed page numbers unchanged). Expect roughly 6-9 MB.
   Render gate: page shots old vs new, owner looks at a few photo pages.
Both cut download time, copy time and memory; (1) alone removes the 100-200 MB spike.
