# T288 — Glossary: every Synopsis definition, A–Z scrubber, modern layout, fuzzy search

Status: OPEN (owner 2026-10-29: "the glossary doesnt have everything in the synopsis terms and definitions page. also we
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
