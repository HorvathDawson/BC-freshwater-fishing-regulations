# T776 — Marks audit tool and review words: three small faults found while reading the audit

Status: OPEN (pipeline `pipeline/regs/review/audit.py`, `handoff/PLAN/curation-review/backend/dfo.py`).

Source: the marks audit and second look, 2026-11-15 night.

## What is wrong
1. `audit.load` keeps the LAST row of `splits.resolved.json` per cut id. 32 cut ids have more than one row (a cut seen
   from a side line); the audit then reads the side line's measure and reports a wrong order or length.
2. Audit section F ("Fraser only" and channels) does not model `pipeline/atlas/reach/channels.py`, so it lists
   channels as missing that the build adds (Maria Slough on the six Mission-area places).
3. `dfo.cannot_edit_words` says "above its point" for a `downstream_of` watershed extent.

## What to build
The three fixes, each with a test on built data; rerun the audit and attach the changed rows.
