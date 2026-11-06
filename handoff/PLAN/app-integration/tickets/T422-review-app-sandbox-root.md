# T422 — The review app's sandbox: one temp root instead of four path overrides

Status: OPEN (leftover of T331 item 2, split out 2026-11-05). Needs a design call before code.

`CURATION_MARKS`, `CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR` (curation-review/backend `reuse.py`,
`dfo.py`) still repoint curated files one by one. They are the review app's sandbox: ~15 test files start the app with
them, and `regs/review/parity.py` / `gate.py` set `CURATION_ENTRIES_DIR` / `CURATION_BUILD` for side builds. A path
override per file is how a test or side build can read a mix of real and sandbox files without noticing.

## Work
One mechanism: the backend takes a root (e.g. `--config <file>` or `CURATION_ROOT`) and every curated path resolves
under it (`pipeline.common.curated`); tests and side builds point at a temp root; the four overrides are removed and
refused.

## Acceptance
No `CURATION_*` path override left (a grep test); the ~15 review-app tests and the parity/gate side builds run on a
temp root.

Size: M. Owner: no (the design call is Claude's, unless it changes how the owner starts the app).
