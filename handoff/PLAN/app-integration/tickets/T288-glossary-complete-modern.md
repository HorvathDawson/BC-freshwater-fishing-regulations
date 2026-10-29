# T288 — Glossary: every Synopsis definition, A–Z scrubber, modern layout, fuzzy search

Status: DONE — the 34 new terms reach the app with the next release built from 732f100f1 or later (no release was built
here). (owner 2026-10-29: "the glossary doesnt have everything in the synopsis terms and definitions page. also we
should have the scroll bar with alphabet. and be formatted to be more modern in style. make sure this is fuzzy search too").

## Work
1. **Complete:** diff the app's glossary against the Synopsis "Terms and definitions" page (printed p.80; the repo PDF is not
   byte-identical to gov.bc.ca — cite printed pages) and any other definition boxes it points to. Add every missing term
   with its exact wording + printed page, through the pipeline's glossary source (no LLM parser runs; the bundle stays the only
   runtime source). A test pins completeness against the page's term list.
2. **A–Z scrubber:** a vertical letter rail (tap/drag jumps to the letter, letters with no terms dimmed), sticky letter
   headers; works on phone + desktop, accessible (each letter a button, 44 px targets via the strip).
3. **Modern layout:** term cards or clean rows (term bold, definition body text, page cite small), generous spacing, existing
   tokens/components (T260), light/dark/colour-blind.
4. **Fuzzy search:** typo-tolerant (e.g. "barbles" → barbless, "steelhed" → steelhead), matches term and definition, ranks
   term hits first, highlights the match.

## Acceptance
Completeness test green; screenshots phone + desktop (list, scrubber in use, search with a typo); a11y gate covers it.

Depends: none. Size: M. Owner: no.

## Done (2026-10-29)
- **Source:** p.80 read off the repo PDF (PDF p.86) deterministically — `pipeline/common/synopsis_definitions.py` (pdfplumber,
  3 columns, bold run ending ":" at a column's edge is a term; no LLM). 45 terms. The curated transcription
  (`data/curated/regulations/reference/definitions.md`, which the bundle's `glossary_source` copies) had dropped words; now the
  page's: Management Unit (+ "For detailed maps … many bookstores."), possession quota (+ "(see above)"), hatchery trout
  (+ "(see centre colour section)"), wild trout (+ "(see illustration on page 13)"), and `max`/`min` split into two terms.
  No existing term dropped or renamed.
- **34 terms added** (`glossary.t_definitions`, ids `def_*`, words verbatim as `says` and `quote`, pages p.80 + any page the
  definition points to): Above, Adipose fin, Adult chinook salmon, Anadromous, Angling, Annual, Bait, Barbless hook, Below,
  Chumming, Confluence, Creek, Day, Fish, Fishing, Hatchery trout, Kokanee, Landed immigrant, Max, Min, Non-resident,
  Non-resident alien, Ordinary residence, Resident, River, Single hook, Slough, Snagging (foul hooking), Spear fishing, Sport
  fishing, Stream mouth, Trout/char, Watershed, Wild trout. The other 11 p.80 terms already had a written term under their
  name (`glossary.COVERED`). Same term shape, so glossary VERSION stays 5 (the app reads old and new releases alike).
- **Completeness test:** `pipeline/tests/test_glossary_complete.py` — the PDF's 45 terms; the transcription says the page's
  words; every printed term is a glossary term; mutation (drop kokanee → missing).
- **App:** core `menu/glossary.ts` — `glossarySections` (A–Z + rail, empty letters dimmed), `glossaryRows` typo-tolerant
  (substring, else edit distance with swaps: name 1 typo from 4 letters / 2 from 7; definition 1 from 5 letters, first letter
  fixed), name hits first, `termMarks`/`saysMarks` for highlighting, excerpt when the match is deep in a definition.
  ui-native `GlossaryPage` — sticky letter heads, a 44 px A–Z strip (tap/drag anywhere on it; each letter a button for
  keyboard/screen reader, disabled when empty; a letter bubble while scrubbing), rows with bold term, page cite, three lines
  of words, marks in tint + accent underline; search box shows focus on its border, clear button. `glossBlocks` skips the
  "book's words" fold when it would repeat the words.
- **Shots** (headless Playwright, 390 / 1280): `T288-shots/after-*` (list, scrubber, search "steelhed" + "barbles", a term) —
  against the live release with ONLY its water file swapped (Playwright route, nothing on disk) for one whose glossary holds
  the 34 new terms, as the next release will; `live-release-phone-list.png` is the live release as is (29 terms).
  A11y gate: `a11y.shell.test.tsx` already walks the Glossary page (names, 44 px boxes).
