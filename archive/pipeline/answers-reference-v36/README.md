# The consumer page v36's refresh tooling (archived)

> **ARCHIVED 2026-10-23** (T61 lane C, owner call 2: archive the prototype answers reference) from
> `pipeline/deliver/answers/reference/`. Kept as a record; nothing runs or imports it. The page itself
> (`page_v36.html`, `page_v36.js`, `calendar.generated.js`) is still read by live checks, so it moved beside the port
> tooling that reads it: `app/design/regs-v11/reference/` (see the README there). The page is now FROZEN: nothing
> re-cuts its `#data` / `#answers` / `#cases` blocks, so the conformance suites' `ORACLE_PREDATES` entries
> (settle.parity.test.ts) stay until the oracle is retired rather than refreshed (T63's page-v36 note).

| file | what it did |
|---|---|
| `page_data.py` | cut the regs files down to the page's 28 waters and rewrote the page's three JSON blocks and inlined script (`python -m pipeline.deliver.answers.reference.page_data --html …/page_v36.html`) |
| `convert.py` | the export → page converter `page_data.py` used (rule / licence slimming, the 8 own cases' remap); its one test, `test_reference_convert.py`, was deleted with it |
| `page_v35_inputs.json` | the 28 display waters and the 8 own cases (from page v35's blocks), read by `convert.py` |

To run them again, copy these three files back to `pipeline/deliver/answers/reference/` beside the page files
(they import `pipeline.deliver.answers.reference.convert` and read `page_v36.js` / `calendar.generated.js` from their
own directory). The page v35 harness that came before is in `archive/pipeline/answers-reference-v35/`.
