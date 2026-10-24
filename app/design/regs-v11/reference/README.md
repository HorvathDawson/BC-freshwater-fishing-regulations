# The consumer page v36, kept as the app port's oracle (never shipped)

Page v36 ("Quota Display Studies") renders the answers file (answers/2) and decides nothing. It is the PROTOTYPE the
regs screens were ported from, so it lives here beside the port tooling (`../port/`), not in `pipeline/` (moved
2026-10-23, T61 lane C; owner call 2). It is FROZEN: the tooling that re-cut its data blocks (`page_data.py`,
`convert.py`) is archived in `archive/pipeline/answers-reference-v36/`.

| file | what | read by |
|---|---|---|
| `page_v36.html` | the page with its `#data` / `#answers` / `#cases` blocks inlined | `app/conformance/src/regs/{ruleSources,oracles}.ts` (the T06/T07 parity tests), `../port/{hydrate,taps}`, `../tools/{check,snap}.mjs`, `app/tools/regs-fixtures.test.ts` |
| `page_v36.js` | the page's script | the same, plus `../port/{stage,inventory}.mjs`, `app/tools/emit-fish.mjs` (the FISHART tips) |
| `calendar.generated.js` | the calendar tables, generated (`python -m pipeline.tools.emit_calendar`; `--check` in CI) | `ruleSources.ts` `pageDecode`, `../port/hydrate.mjs` |
