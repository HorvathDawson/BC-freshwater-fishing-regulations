# T638 — Pipeline suite: leftovers from the kill trace

Status: PARTIAL — items 2–3 fad79daf0, c85f497c0 (item 1 done earlier); left (Claude): item 4, one chunked full run `scripts/heavy.sh test-full 40 -- ./scripts/test-full.sh` after T639, expected 0 failed.

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

## Built (2026-11-10, items 2–3: fad79daf0, c85f497c0)
- Item 2, both halves. `scripts/heavy.sh` samples the job's process-group PHYSICAL FOOTPRINT (`footprint`, compressed
  pages included; ~20 ms, every `HEAVY_FOOTPRINT_EVERY` s = 30) and stops the job over `HEAVY_MAX_FOOTPRINT_GB` (default:
  the machine's RAM, 48 GB), exit 137, its own message; the footprint is in the heartbeat. `scripts/pytest-chunked.sh`
  runs the suite one process per chunk: 40 files a chunk, every `test_curation_review*` file alone (420 files → 31
  chunks); `--only REGEX`, `--dry-run`, pytest args after `--`; exit 5 is not a failure; names failed chunks.
  `scripts/test-full.sh` steps 2–3 use it (`-- -m slow` for the slow tier).
- Item 3: `scripts/fast-fail.sh`, `test-smoke.sh`, `test-full.sh` and `pytest-chunked.sh` export a fresh
  `PYTHONPYCACHEPREFIX` (mktemp, one per run, removed at exit). fast-fail step times unchanged (the Python steps 0–3 s).

Proof: `scripts/tests/heavy.test.sh` case 7 (footprint 3 GB > 1 GB limit while free reads 80 % → 137, job gone);
`pytest-chunked.sh --dry-run` (31 chunks); `--files 1 --only 'test_smoke_coverage|added_streams/tests/test_clean|test_curation_review_safety'`
→ 3 chunks, 9 + 33 + 3 passed; `-- -k no_such_test` → exit 5 counted ok; `-- --no-such-flag` → rc 1.
Not verified: the whole chunked suite (item 4) and its peak footprint per chunk; the 48 GB default against a real atlas
build's footprint (an atlas build is ~10 GB, so it should never trip).
