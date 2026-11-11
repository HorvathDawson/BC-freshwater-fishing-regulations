# T638 — Pipeline suite: leftovers from the kill trace

Status: OPEN

Source: suite trace report 2026-11-11 (commits 304feae, 8822df2, 68fca13, 553fe40).

Measured: the full suite in one process grows to about 111 GB (mostly compressed) and macOS kills it
(`memorystatus: killing largest compressed process`). `heavy.sh` reads `memory_pressure` free %, which compressed
pages do not lower, so its guard read 83–86 % free up to the kill. Run as 6 processes: 7,067 passed, 7 failed,
1 skipped; 5 fixed, 2 left (item 1 here, and T639).

## Work
1. Regenerate `data/generated/regs/ui-rules-export.json` and `ui-rules-guide.json` (`python -m pipeline.deliver export`
   through heavy.sh): the guide on disk predates T607 and lacks the `other_places` extent op
   (`test_export_codec::test_the_files_on_disk_are_the_bundles_pair`).
2. `heavy.sh` also watches the job's physical footprint (`vmmap --summary <pid>`), or a `scripts/` runner always
   runs the full suite in chunks, one process each. The curation-review tests are the largest growth (60 GB in one chunk).
3. Stale bytecode: `matcher.py` edited twice in one second at the same size kept its old `.pyc`. Gating runs use a
   fresh `PYTHONPYCACHEPREFIX`.
4. One fresh chunked full run after items 1–3 and T639; expected 0 failed.

## Acceptance
- A full suite run finishes without a kernel kill and with 0 failed, from one command.

Size: M.
