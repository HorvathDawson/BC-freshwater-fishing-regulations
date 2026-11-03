# T354 — Opening the synopsis PDF takes a very long time

Status: IN REVIEW (code done 2026-11-03; left: emulator timing before/after, owner looks at the photo pages).

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

## Done 2026-11-03 (code)
- `pipeline/deliver/release/synopsis.py`: the shipped copy. JPEGs past 1600 px long side downsampled (9 of 59), page
  Resources/Group made indirect, deterministic, render-gated in pdfium (every page: same text, worst mean diff 1.74/255,
  p88). **25.0 MB -> 11.7 MB.** Cached at data/generated/synopsis/. The release CLI ships it; `ship()` writes the page
  index into the manifest (`synopsis.append`, 4 KB).
- `app/packages/ui-native/src/regs/appendPdf.ts`: the incremental update (front copy of the cited page + highlights, root
  /Pages overridden, xref stream with /Prev), ~0.9 KB. On the real book: qpdf syntax check clean, pdfium 89 pages, the
  front page renders identical to its book page without annots, the rest unchanged.
- Native open: `cached.copy(f)` + `f.write(tail, {append: true})`, no JS read of the book; falls back to pdf-lib when the
  release has no index. Web keeps pdf-lib (`#page=` works there).
- pikepdf added to requirements.txt (pipeline only).

Left: (1) emulator time + memory of an open, before/after (the T360 sitting); (2) owner: look at photo pages 1, 43, 46,
87, 88 (`python -m pipeline.deliver.release.synopsis` prints the gate; the copy is in data/generated/synopsis/).
