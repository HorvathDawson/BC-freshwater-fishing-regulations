# T94 — Gauge promote: one reviewed station at a time; demotions shown before writing

Status: DONE — 1406d435, a2b70ccd (review). No deliver needed (tooling only; no curated or shipped data changed).

Source: memory `gauge-regenerate-drifts`: on 2026-10-10 a full match + promote demoted 25 stations and rewrote 1,042
rows. `pipeline/gauges/generate/promote.py` takes only `--dry-run` / `--out`; `pipeline/tools/check_curated.py` still
prints the full `match … && promote` as the fix.

## Work
`promote --only <station…>`; a full promote refuses to demote without `--accept-demotions` and lists each demotion
first; check_curated suggests the safe command.

## Acceptance
A test: a full promote with a would-be demotion exits non-zero and writes nothing; `--only` changes exactly one row.

Depends: none (not T68, which is the feed). Size: S. Owner: no.

## As built (2026-10-23, 1406d435)
- `pipeline/gauges/generate/promote.py`: reads the raw match from `--matches` (default `GENERATED.gauges.match`,
  new config key → `data/generated/gauges/match.json`); the curated `matches.json` + `candidates.json` (`--out`,
  `--candidates`) are both the output and the BASELINE.
  - `--only S …` (`merge`): the baseline with exactly the named stations replaced by their fresh row, in whichever
    file it now belongs; every other row byte-for-byte unchanged (`dump` round-trips the committed pair exactly —
    checked on the real files). A named station the fresh match lacks is an error.
  - `demotions(before, promoted, candidates)`: THE one definition (ORDER rank; a station in neither file ranks below
    candidate; baseline candidates have nothing to lose). Full promote and `--only` both obey it: each demotion
    printed `S  was -> now`, exit 2 and nothing written unless `--accept-demotions`. `--dry-run` lists them and
    writes nothing. Also prints how many promoted rows would change.
  - A baseline that is absent or a bare match list (what the old matcher wrote over the curated file) is refused:
    a full promote needs `--accept-demotions`, `--only` refuses outright.
- `pipeline/gauges/generate/match.py`: default `--out` is the generated file; `--out` = the curated file is refused
  before anything loads. `main(argv)`.
- `pipeline/tools/check_curated.py`: the gauge fix is `match --build … && promote --only {stations}`; `fix_command`
  fills in the stale ids from `undecided` (the stale-id decision `check` also uses — one function).
- Docs: AGENTS.md ㊳, `pipeline/docs/15-live-data-flow.md`.
- Tests `pipeline/tests/test_gauge_promote.py` (temp copies only; disk inputs swapped for a fixture): a full promote
  with a would-be demotion exits 2 and both files stay byte-identical; `--only C` makes the matches file exactly
  baseline + C's whole row and leaves candidates byte-identical; whole-value `demotions`; bare-list baseline refused;
  `fix_command` whole string; matcher refuses the curated path.
- Mutations run (each restored): M1 drop the refusal → full-promote-refuses + only-obeys-demotion; M2 `merge` ignores
  `only` → only-changes-exactly-that-row (+2); M3 `>=` in `demotions` → demotions whole-value + one-definition (+2);
  M4 bare list read as `{}` → bare-list-refused; M5 `fix_command` unfilled → surgical-command test.
- Real-data smoke on scratch copies (never the curated file): `--only 07EA001` changed exactly that row of 2,074;
  candidates byte-identical.

## Adversarial review (2026-10-23)
Mutations M1–M5 re-run and confirmed (each fails the test named); M6 (`--dry-run` falls through to the
write) fails the dry-run test; M7 (`--only` skips `demotions`) fails the only-obeys test. Defects found and fixed:
- **The raw writer still defaulted to the curated file.** `pipeline.gauges.matches.write_match(matches,
  path=None)` wrote `MATCH_FILE` when no path was given; only the matcher's CLI refused it. Now `path` is
  required and `write_match` itself asks `refuse_curated` — THE one check, also asked by `match.main` before it
  loads a build (the CLI's own copy is gone). It covers BOTH curated files (`CURATED_FILES`: the raw list
  over `candidates.json` wiped the queue just as surely) and compares resolved paths plus `samefile`
  (a case-variant spelling on macOS's case-insensitive disk passed the old string compare). The raw
  file's `_about` no longer claims the atlas and bundle read it; promote does.
- **Error messages still named the old one-step regeneration.** `read_match` (missing file), the atlas
  build (no matches) and the bundle (empty match) told a person to run `generate.match --build …` — which now
  writes the generated file, not the one they read. All three now say restore the committed file
  (`git checkout`) or run `REGENERATE` (`match … && promote --only <station ...>`, one constant in
  `matches.py`); `read_match` on a missing RAW file (promote's `--matches`) says run the matcher instead
  (`test_a_missing_match_names_the_two_step_regeneration`, whole messages). `consume/cuts.py`'s flow diagram shows the promote step.
- **A top-level JSON list as baseline crashed** (`AttributeError` on `.get`) instead of being refused like a
  bare-list `stations`. `_stations` returns None for it.
- New tests (`test_gauge_promote.py`): `test_write_match_refuses_both_curated_files` (temp stand-ins via
  `CURATED_FILES`, so a mutation that drops the refusal cannot write the real files — the first draft of this
  test did, and was rewritten), `test_the_curated_refusal_has_one_home` (no second copy of the refusal),
  `test_a_top_level_list_baseline_is_refused`. Mutations: M8 drop `refuse_curated` from `write_match` and
  M9 drop `candidates.json` from `CURATED_FILES` → `test_write_match_refuses_both_curated_files`; M10 `.get` on
  the top-level value → `test_a_top_level_list_baseline_is_refused`.
- Note on the builder's real-data smoke: no `data/generated/gauges/match.json` exists yet, so `--only 07EA001`
  promoted from a copy of the curated file (read_match reads both shapes). It proves the splice is surgical, not
  what a fresh match would demote; that first real `match` + `promote --dry-run` is the owner's.
- Checks: targeted gauge/check_curated/match/missing-file suites green (15 in test_gauge_promote.py); full
  `pipeline/tests` via heavy.sh with read-only generated symlinks: 4,741 passed, 13 failed + 24 errors, none in
  gauges/promote/check_curated — missing `data/source/bc_fisheries_data.gpkg` in the worktree (the errors),
  bundle-vintage answers/fixture/tidal assertions, and other lanes' guards (entry_id in
  regs/review/fingerprint.py, an app test in the dataflow gate).
- No deliver needed (tooling and messages only; no curated or shipped data changed).

## Left / follow-ups
- None in scope.
