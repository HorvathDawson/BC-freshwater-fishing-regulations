# The consumer page v36, kept as the app port's oracle (never shipped)

Page v36 ("Quota Display Studies") renders the answers file (answers/2) and decides nothing. It is the page the regs
screens were ported from. It is FROZEN: the tooling that re-cut its data blocks (`page_data.py`, `convert.py`) is archived
in `archive/pipeline/answers-reference-v36/`. Moved here from `app/design/regs-v11/reference/` in T87 (2026-10-28), when
the design prototype was deleted.

| file | what | read by |
|---|---|---|
| `page_v36.html` | the page with its `#data` / `#answers` / `#cases` blocks inlined | `app/conformance/src/regs/{ruleSources,oracles}.ts` (the T06/T07 parity tests), `app/tools/regs-recut/{hydrate.mjs,taps.py}`, `app/tools/regs-fixtures.test.ts` |
| `page_v36.js` | the page's script | the same, plus `app/tools/emit-fish.mjs` (the FISHART tips) |
| `calendar.generated.js` | the calendar tables, generated (`python -m pipeline.tools.emit.calendar`; `--check` in CI) | `ruleSources.ts` `pageDecode`, `app/tools/regs-recut/hydrate.mjs` |
