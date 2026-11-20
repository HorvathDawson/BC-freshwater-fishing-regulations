# T776 — Marks audit tool and review words: three small faults found while reading the audit

Status: OPEN (pipeline `pipeline/regs/review/audit.py`, `handoff/PLAN/curation-review/backend/dfo.py`).

Source: the marks audit and second look, 2026-11-15 night.

## What is wrong
1. `audit.load` keeps the LAST row of `splits.resolved.json` per cut id. 32 cut ids have more than one row (a cut seen
   from a side line); the audit then reads the side line's measure and reports a wrong order or length.
2. Audit section F ("Fraser only" and channels) does not model `pipeline/atlas/reach/channels.py`, so it lists
   channels as missing that the build adds (Maria Slough on the six Mission-area places).
3. `dfo.cannot_edit_words` says "above its point" for a `downstream_of` watershed extent.

4. (second audit run, 2026-11-16 00:46, after the build) Section A `not_last_save` flags the five T761 rest-of saves
   (marks 2790 to 2794): it renders the file's extent as "rest" without its bounds and so says the file is not what
   the last save says. The files DO hold the bounds and listed places (checked: the Skeena place). The comparison is
   not T761-aware.
5. Section A `orphan_cut` lists the six closed-area cuts (Shuswap station point, Thompson Bonaparte mouth, Deadman
   upper signs, Hwy 97C bridge, Juniper lower and upper signs) as named by nothing: it does not read
   `data/curated/dfo_salmon/closed_areas.json`.

## What to build
The three fixes, each with a test on built data; rerun the audit and attach the changed rows.
