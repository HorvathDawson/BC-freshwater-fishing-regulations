# pipeline/regs/review: the review app's library

The server side of `tools/curation-review/`, kept in the pipeline so the review and the build share one implementation (AGENTS 16):
the owner's MARKS and their fingerprints, the apply paths that write a mark's decision into its final curated file, the review
queue order, pre-checks, and the vintage a mark is held against.

- Entry: `python -m pipeline.regs.review.apply [--dry-run]` (every open domain: `marks.jsonl` to its curated file),
  `.export` (print the decisions in force), `.precheck`, `.gate stale` (false-stale count must be 0), `.parity` (the app's binding
  against the reach run), `.tickets --out <dir>` (wrong marks as ticket text), `.commit_marks` (commit the marks file alone).
- Reads `data/curated/review/marks.jsonl` and the curated corpus; writes the domain's curated file (sloughs, names, tidal,
  DFO places, entries, splits) through `apply.py` / `apply_proposals.py`; backs up first. No build stage reads `marks.jsonl`.
- Modules: `marks.py`, `fingerprint.py`, `tiers.py` (the one queue order), `precheck.py`, `vintage.py`, `dfo.py`, `tidal.py`,
  `proposals.py`, `export.py`.
- Rules: `marks.jsonl` is the owner's, never edited by hand (AGENTS A8); a reviewer signs off one VINTAGE (65); the app writes one
  thing, a mark (66); one queue order, one reader of the run (67); apply refuses a shrink of more than 10 rows (W46).
- Tests: `pipeline/tests/test_review_*.py` and `test_curation_review_*.py`.
