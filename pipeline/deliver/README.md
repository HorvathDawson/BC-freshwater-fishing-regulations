# pipeline/deliver: from the atlas to a release

Takes the atlas, the reach run, the tiles and the curated corpus and produces the one release the app reads
(`data/release/r-<id>/`, moved to `LATEST` only after every check passes). Every answer is decided here, ahead of time: the app
decides nothing (AGENTS 56).

- Entry: `python -m pipeline.deliver all --build data/generated/atlas/full` (normally the `deliver` stage of `python -m pipeline build`;
  a release is `python -m pipeline release --staging`, `pipeline/release.py`). Each step also runs alone
  (`python -m pipeline.deliver bundle|verdicts|status_index|export|answers|card_check|store`), and `all` reuses a step whose inputs did not
  move (`pipeline.common.stepcache`, `--no-step-cache` runs everything).
- The steps, each reading only the previous one's output: `bundle/` (`bundle.sqlite`; the reference reader `bundle/read/ladder.py`),
  `verdicts/` (the reader run once per rule key, reading, fish and origin), `status_index.py`, `export/` (`ui-rules-export.json` + guide),
  `answers/` (the answers file; contract `answers/ANSWERS-SPEC.md`; the trout table in `answers/trout/`), `answers/card_check.py`, `store/`
  (`regs.sqlite`, staging only), `release/` (the shipped files: paint, core, near, water, `regs-<region>` x9, `regs-sid`, salmon).
- `tiles/` is its own stage: `python -m pipeline.deliver.tiles --build ...` writes `atlas.pmtiles`, `atlas.meta.json` and `layers/` into
  `data/generated/tiles/`, never the basemap (README there).
- `salmon.py` (the DFO salmon chains per day, from the reach run's `dfo_salmon.jsonl.gz`), `calendar.py`, `lake_facts.py` (the one landing
  point for lake values), `sync_inputs.py`.
- Reads: `data/generated/atlas/`, `data/generated/reaches/`, `data/generated/tiles/`, `data/curated/`, `clim.json`.
  Writes: `data/release/r-<id>/` (`CANDIDATE` with `--hold-latest`) and `data/generated/` staging.

## Rules

Reviewed artifacts are never regenerated to check something (AGENTS 38). A value on both sides of the Python/TypeScript boundary is generated
from here (AGENTS 40: `pipeline.tools.emit`, the answers model). The UI export ships encoded; the model is `export.model.build()` (58). One
data flow: every derived fact is computed once and read everywhere (56). The release is checked before it is promoted (`release/app_gate.py`,
`atlas/reach/binding_diff.py`); production upload is the owner's (AGENTS A7).

## Tests

`pipeline/tests/test_answers_*.py`, `test_bundle*.py`, `test_export*.py`, `test_store*.py`, `test_release*.py`, `test_verdicts*.py`,
`test_salmon_*.py`. Those that read a built bundle carry `needs_bundle` and fail without one.
