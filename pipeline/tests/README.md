# pipeline/tests: the pytest suite

About 559 test files for the whole pipeline, review library and curation-review backend, plus a few that guard the repo itself (`test_workflow_pins.py`,
`test_before_push.py`, `test_ci_paths.py`). Config: root `pytest.ini` (`--strict-markers`) and `conftest.py`.

- Run: `.venv/bin/python -m pytest -q <file or -k>`: with no `-m`, CI's no-data tier (T1193), its NOT RUN line counting the data tests left out; on a
  machine with builds add `--with-data` (`-m "not slow"`); rtk's pytest filter hides that line: `rtk proxy` shows it. `-m slow`: stage agreement and
  determinism. Tiers (AGENTS 18): `scripts/test-smoke.sh` per commit, `scripts/test-full.sh` (via `scripts/heavy.sh`) before a merge; gating runs unfiltered.
- A test that reads generated or fetched data carries a marker (`needs_bundle`, `needs_atlas`, `needs_reach_run`, `needs_tiles`, `needs_source`,
  `needs_cache`) and FAILS when the data is missing, printing the command that makes it (`conftest.py` `need`; AGENTS 62). CI runs the no-data tier by
  deselecting those markers; a no-data run hides this machine's builds (`ci_no_build.py`, `tiers.py`) so a test that secretly reads one fails here too.
- Helpers: `under_test.py` (the release / build a test runs against; tests never type a release id or section number), `anchors.py` (the pipeline
  twin of `pnpm anchors`), `machine_state.py`, `mini_chain.py` (the real chain on the invented world), `bundle_fixture.py`.
- Fixtures: `fixtures/` (`mini/` the invented world, `dfo_salmon/`, `gauge_feed/`, pre-cutover releases; `fixtures/mini/README.md`) and the big
  `answers*.json.gz` files beside the tests.
- Rules: assert the whole composed string, never a fragment you interpolated (AGENTS 33); a check is proved against an output, never a value seeded from its
  own input; no silent skips, and a missing-data failure names the command that makes the data (AGENTS 62).
