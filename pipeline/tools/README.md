# pipeline/tools: audits, generators and one-off writers

Utilities that are run on purpose and never by a build stage (apart from the checks in `pnpm check` / CI). Three kinds:

| folder / file | what | entry |
|---|---|---|
| `emit/` | the GENERATORS (AGENTS 40): every value that exists on both sides of the Python / TypeScript boundary is written from its one Python source into `app/` | `python -m pipeline.tools.emit [--check]` (`--check` in `pnpm check`, CI, `scripts/test-full.sh`); the table of files is in its `__init__.py` |
| `audit/` | read-only reports on a build, the corpus or the curated tree (`check_curated.py`, `build_parity.py`, `answers_diff.py`, `tile_sizes.py`, `base_deltas.py`, the tidal and trout audits) | `python -m pipeline.tools.audit.<name>` |
| `water_map/` | a labelled PNG of any water's rules from a release on disk | `python -m pipeline.tools.water_map "<water>" [--mode salmon\|freshwater\|both] [--date D] [--release R]` |
| `mini_release.py` | a whole release built by the real pipeline from an INVENTED world (no live data), for the app's logic tests | `--check` (CI) / `--write` (keeps `app/packages/data/dev/mini-release/`) |
| `slough_calls.py`, `mined_names.py`, `tidal_limits.py` | the write-through writers: owner marks to `data/curated/waters/{slough_calls,found_names,tidal_limits}.json` (AGENTS 66, via `regs/review/apply.py`) | called by the review app |
| `synopsis_book.py`, `synopsis_diff.py` | the annual synopsis update: carry unchanged rows, diff two books (runbook `annual-synopsis.md`) | `python -m pipeline.tools.synopsis_diff OLD NEW` |
| `source_pins.py` | the pinned `data/source/` files mirrored to the build-assets bucket (`pins.json`) | `python -m pipeline.tools.source_pins pin PATH` (the owner's step) |
| `one_book.py` | the guard (T1156): fails when a second full synopsis PDF is under `data/source/` besides `fishing_synopsis.pdf` and the dated archive copies (the book is `fishing_synopsis.pdf`; validate against nothing else; the region PDFs are not kept; rules in its docstring; CI checks the tracked files only); run by `pipeline/tests/test_one_book.py` (CI) | `python -m pipeline.tools.one_book` |
| `electric_kw.py` | a one-off pass (T209) that stored the kW number of electric-motor rules | done; unclear whether it should stay |

## `audit/`: every report (`python -m pipeline.tools.audit.<name>`)
| module | what |
|---|---|
| `check_curated` | is any frozen match artifact out of date with its roster; are the hand-edited waters files sound (a build runs it) |
| `build_parity` | compare two build outputs: is the registry the same after a change |
| `answers_diff` | every answer that differs between two built releases, in plain words (T809) |
| `base_deltas` | a section's answer is its region base plus the named rows that reach it (RULINGS Z1) |
| `tile_sizes` | bytes per zoom and per layer in a PMTiles archive, read only |
| `audit_splits` | how far each curated split's resolved cut landed from its hand-verified coordinate |
| `audit_split_binding` | which curated splits a rule can bind to, and which can never be referenced |
| `reparse_candidates` | which entries to delete so `run_parse.sh parse` re-parses them |
| `unnamed_trib_sweep` | does every "and tributaries" rule reach the unnamed streams it should (T57) |
| `datum_remarks` | every datum, relocation or gauge-zero remark HYDAT holds for the B.C. roster, as a proposal file (T1047) |
| `trout_size`, `trout_table` | the trout table's size and three counts; the table builder over a built release (T810) |
| `water_map_export` | one chosen water drawn part by part, coloured by the status index |
| `slough_fold_preview`, `slough_fold_gate` | the confirmed-slough fold's dry run, and its mechanical promote gate (T322) |
| `tidal_candidates`, `tidal_pass2`, `tidal_preverify`, `tidal_preview`, `tidal_triage`, `tidal_words` | the tidal-boundary proposals and their checks (T166, T212): candidates, DFO's own words, a second method, what the owner's boundaries would take, where he edited the polygon, landmarks named in the words |

- Rules: an audit prints or writes a report under `data/generated/`; it never writes curated data (the writers above are the exception and only
  from marks). Every module here must be named somewhere (`pipeline/tests/test_no_orphan_modules.py`), or be listed there with why.
- Tests: `pipeline/tests/test_mini_release.py`, `test_water_map*.py`, `test_synopsis_diff.py`, `test_source_pins.py`, `test_no_orphan_modules.py`; the generators are held by their `--check` runs.
