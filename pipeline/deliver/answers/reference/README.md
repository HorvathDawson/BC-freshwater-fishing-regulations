# The consumer page v36, kept as the app's oracle (never shipped)

Page v36 ("Quota Display Studies") renders the answers file (`ui-rules-answers.json`, answers/2) and
decides nothing. Nothing in the pipeline imports or ships it. It stays here because live checks read it:

| file | what | read by |
|---|---|---|
| `page_v36.html` | the page with its `#data` / `#answers` / `#cases` blocks inlined | `app/conformance/src/regs/ruleSources.ts` (the T06/T07 parity tests), `app/design/regs-v11/port/{hydrate,stage}.mjs`, `taps.py`, `tools/check.mjs` |
| `page_v36.js` | the page's script | the same, plus `pipeline/tests/test_answers_{25,26,27,28_card,v22}.py` |
| `calendar.generated.js` | the calendar tables, generated (`python -m pipeline.tools.emit_calendar`; CI checks it) | `page_v36.html`, `test_feb29.py` |
| `page_data.py` | cuts the three regs files down to the page's waters and rewrites the page's blocks | `test_feb29.py`; run by hand to refresh the page |
| `convert.py` | the export -> page converter `page_data.py` uses (rule / licence slimming, the 8 own cases' remap) | `page_data.py`, `test_reference_convert.py` |
| `page_v35_inputs.json` | the 28 display waters and the 8 own cases (from page v35's blocks) | `convert.py`, `page_data.py` |

```sh
# refresh the page's blocks from the live regs files (no credits)
python -m pipeline.deliver.answers.reference.page_data --html pipeline/deliver/answers/reference/page_v36.html
```

## Archived: the page v35 harness (audit F12, owner 2026-10-22)

`page_v35.js` and the harness that ran it in node (`harness.js`, `golden.js`, `checks.js`, `manifest.js`,
`sample.py`, `regenerate.sh`, `compare.py`, `port.py`) are in `archive/pipeline/answers-reference-v35/`, with
their README. The check they pinned — `rows.py` on the page's own ladder reproduces the page's card except where
a documented page bug shows — lives on as a golden fixture with no harness:
`pipeline/tests/answers_v35_port.py` and `pipeline/tests/test_answers_v35_port.py`.

Moving the v36 files out of `pipeline/` needs the app's readers above repointed in the same change (the
plan's wave 2.3 / 2.17), so they stay until then.
