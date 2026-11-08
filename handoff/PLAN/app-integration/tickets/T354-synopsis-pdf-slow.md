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

Owner 2026-11-03: the compressed photos look good (approved).

## Changed 2026-11-03 (owner): the book opens IN THE APP, highlight in place, no fallbacks
- In-app viewer: `react-native-pdf` 7.0.5 (+ `react-native-blob-util` 0.25.1), `ui-native/src/regs/PdfViewer.native.tsx`
  (full-screen Modal, one back, page N of M), mounted once by the Shell; store `pdfView.ts` (data only). Opens at the
  cited PDF page. The web opens the browser tab at `#page=` with the SAME append (no pdf-lib on either side).
- Append v2: every page's /Annots is an indirect array in the shipped copy; the append overrides that array with the
  highlights added (page never rewritten, nothing moves). Index 5 KB. Front copy removed.
- No fallbacks: no index / wrong size / bad version / failed write THROW (the card says so + reported); a viewer that
  cannot draw says so on its face + reported (`reportFailure`, crashReport.ts). Removed: pdf-lib in the app (dev-only now,
  tests), `highlightPdf.ts`, `expo-intent-launcher` (outside-app hand-off), the "no PDF app" words.
- App-wide version of this rule: T333 (audit phase).

Left: (1) DEV BUILD on the emulator (native packages: rebuild; check react-native-pdf on Expo SDK 57 / RN 0.86 new arch,
the highlight drawn, page jump, back closes); time + memory before/after (T360 sitting); (2) the test bed's pdf/offline
scenarios still look for an OUTSIDE viewer (tools/testbed/scenarios/offline.mjs, run-ios.mjs "No app on this device"):
switch them to `pdf-viewer` / `pdf-viewer-failed` testIDs in that sitting; (3) release r-88c4fc7bbf1f carries the v1 index:
the label refresh's deliver re-run (or the next build) ships v2.

- 2026-11-08 (test bed `memory`, emulator): "Page 25 of 88" seen 10.1 s after the tap, but that clock starts before a
  Maestro `tap-text` run (a JVM start) and polls with uiautomator dumps, so it is mostly the harness. The app's own
  marks: the highlighted copy 28 ms (7 ms held), the page drawn 300 ms after the viewer mounts. Not a measure of the open;
  the owner's phone check (item 11) is the judge. A clean in-app timing (press → page drawn, logged) would settle it.
