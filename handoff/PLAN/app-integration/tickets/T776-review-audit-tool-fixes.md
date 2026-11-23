# T776 — Marks audit tool and review words: three small faults found while reading the audit

Status: DONE — c77eeb601 (2026-11-22): items 1-5. Audit rerun on the served atlas, before vs after: 19 findings resolved, 0 new (rows below).

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

## Done 2026-11-22 (L11, c77eeb601)
1. `audit.primary_split_pos`: the cut's own row of `splits.resolved.json` (no `concern`, no `picked_up`; the first), a side line's
   row only when the cut has no row of its own. 190 cut ids have more than one row.
2. Section F models `pipeline/atlas/reach/channels.py` (`own_channels`, the build's own function over a view of the registry): a
   channel the build adds to a named water is not listed as dropped.
3. `dfo.cannot_edit_words` says "below its point" for a `downstream_of` watershed extent.
4. `fix_matches` reads a rest-of's bounds and listed places (T761), `fix_extents` / `words` render them.
5. Section A `orphan_cut` reads `data/curated/dfo_salmon/closed_areas.json` (`Inputs.closed_areas_text`).
Tests: `test_review_audit.py` (4 new), `test_review_followup_1115.py`; each pinned by a mutation.
Audit rerun on the served atlas (`--no-landed`), old code vs new: 291 -> 272 findings, 19 resolved, 0 new, no text changed:
- A `not_last_save` + B `unsure`: `1:puntledge-river:downstream-from-a-fishing-boundary`, `6:skeena-river:upstream-of-cnr-bridge-at-terrace`
  (the T761 rest-of saves, now read with their bounds and listed places).
- F `dropped_channel` x15: the three Fraser mainstem-waters places x five channels (blk:355991777, blk:355992187, blk:355994562,
  gnis:11481, gnis:13499) — channels the build adds.

State 2026-11-23: review fixes dd942ce76, ff7cece62, 4916c3cf0 on branch fix/T775-review-app-leftovers (PR #48); origin/main merged.

Review 2026-11-23: `primary_split_pos` and `reuse.host_rows` keep different rows (157 of 190 multi-row cuts), and the build
reads neither (its position is per extent, `extent._cut_at`): left as it is, ticketed as T1037.
