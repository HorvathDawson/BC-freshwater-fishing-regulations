# T94 — Gauge promote: one reviewed station at a time; demotions shown before writing

Status: DONE — 1406d435. No deliver needed (tooling only; no curated or shipped data changed).

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

## Left / follow-ups
- None in scope.
