# The consumer page v36, kept as the app port's oracle (never shipped)

Page v36 ("Quota Display Studies") renders the answers file (answers/2) and decides nothing. It is the page the regs
screens were ported from. It is FROZEN: the tooling that re-cut its data blocks (`page_data.py`, `convert.py`) is archived
in `archive/pipeline/answers-reference-v36/`. Moved here from `app/design/regs-v11/reference/` in T87 (2026-10-28), when
the design prototype was deleted.

How it is used (T667 slice 5b, 2026-11-13): page v36's CODE is the oracle, run over the RELEASE under test, never its
frozen data. `app/tools/regs-recut/page-oracle.mjs` runs `page_v36.js` (up to `function segBar`) over page blocks that
`page_blocks.py` cuts from the release through the Python reference decoder, writing `app/fixtures/golden/page-oracle.json`
(rules by durable key; re-cut by `sync.mjs`, a change stops the release); `conformance/src/regs/ruleSources.ts`
`pageDecode` runs the same decode layer in the test over every rule of the release. So an approved ruling needs no edit
here: the per-ruling table (`v36-rulings.json`) and the frozen output (`hydrate.json`) are gone.

| file | what | read by |
|---|---|---|
| `page_v36.html` | the page with its `#data` / `#answers` / `#cases` blocks inlined (frozen inputs) | `app/conformance/src/regs/{ruleSources,oracles}.ts` (describe.parity's page-input run), `app/tools/regs-recut/taps.py` (golden water names), `app/tools/regs-fixtures.test.ts` |
| `page_v36.js` | the page's script: THE ORACLE'S CODE | `app/tools/regs-recut/page-oracle.mjs`, `ruleSources.ts` `pageDecode`, `app/tools/emit-fish.mjs` (the FISHART tips) |
| `calendar.generated.js` | the calendar tables, generated (`python -m pipeline.tools.emit.calendar`; `--check` in CI) | `ruleSources.ts` `pageDecode`, `app/tools/regs-recut/page-oracle.mjs` |
