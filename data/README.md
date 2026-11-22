# `data/` — the pipeline's inputs and outputs

Three kinds of data, kept in sibling folders because confusing them is the recurring bug in this repo (`AGENTS.md` 35):

| folder | holds | losing it costs |
|---|---|---|
| `source/` | fetched inputs: the FWA GeoPackage, HYDAT, the synopsis PDF, stations, municipal layers, … | a download |
| `curated/` | the owner's decisions and hand-made data (authored and reviewed): [`curated/README.md`](curated/README.md) | human hours |
| `generated/` | everything a program writes: [`generated/README.md`](generated/README.md) | CPU |

Code names these paths through `pipeline.common.curated` (`CURATED` / `GENERATED` / `SOURCE`, keyed in the root
`config.yaml`), never as literals (AGENTS 36, 41).

## Getting the source data

On a fresh clone, `bash scripts/bootstrap.sh` fetches the exact bytes the live release was built from, checked against
`source/pins.json` (runbook: `handoff/PLAN/runbooks/fresh-machine.md`). Refreshing from upstream is a separate choice:

```bash
python data/fetch_data.py --help     # the BC Freshwater Atlas layers and the other upstream sources
```

then re-pin what changed with `python -m pipeline.tools.source_pins pin PATH` (it uploads the bytes; the owner's step).

## The scripts here

- `fetch_data.py` — the upstream fetcher (above).
- `build_tidal_boundary.py`, `process_tidal_boundary.py` — the DFO tidal polygon, processed once and stored on R2 (each
  file's docstring has its steps).

Licences: third-party material and its terms are listed in the root `NOTICE`; the OpenStreetMap-derived data is ODbL,
`LICENSE-ODbL.md`.
