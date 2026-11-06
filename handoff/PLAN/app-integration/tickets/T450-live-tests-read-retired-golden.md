# T450 — Two pipeline "live" tests still read the retired golden.json

Status: OPEN.

Source: 2026-11-06 (chat 4): `pytest pipeline/tests -k …` stops at collection on
`test_answers_keep_answers_live.py` and `test_answers_pipeline_needs_live.py` — both open
`app/fixtures/golden/golden.json`, which T439 retired (owner: golden.json is gone). A collection error hides every other
test in the run.

## Work
Re-point both at the release-backed oracle (`app/fixtures/golden/taps.json`, re-cut by `regs-recut/sync.mjs`) or delete
them if they only compared against golden.json (as T439 did on the app side). Collection must be clean.
Size: S.
