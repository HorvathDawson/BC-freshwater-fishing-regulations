# T474 — "This water in the rule book p.N ↗" on every water that has an entry

Status: DONE 2026-11-07 0f4b0063a (owner: "we should always have a link somewhere that will open the regulation page for the specific waterbody if there is one for it").

The bundle DOES carry it: a rule's `facts.book` is its entry's printed pages with the PDF page (`pipeline/deliver/answers/display/decided.py
book_pages`, from the entry's `pages` = catalogue `source_pages`). The rules at `baseRank` 0 are the water's own entries (rank 3 is
the region's general pages, e.g. p.23). `core/regs/waterbook.ts waterBookOf` unions their pages; `RegsScreen` draws one link in a
fixed spot, the first line of the page body (`regs-water-book`), web and native. A water with no entry of its own: no line, no
guessed page. Fraser River shows p.59.
Tests: `core/regs/waterbook.test.ts`, `regs/WaterBook.test.tsx`.
Size: S.

Fix 2026-11-07 (T60 clash: "Rule book p.22" offered twice, line + a rule's own link): dropping the rule-side duplicate would touch ~11 core call sites
(`ruleBook`/`ruleBookLine` in standing, decided, caveats, sheets, tree, ...), so instead `waterBookOf(inp, page)` leaves out any page the
built RegsPage already links (a deep walk for `{t:"book"}` runs, in core). The water's page is still linked on the screen, by that rule's link; when
all its pages are linked there, the top line is empty. `WaterScreen` builds the page first and passes it. Tests: `waterbook.test.ts` (dedupe), surfaces/WaterBook/regs-props pass.
