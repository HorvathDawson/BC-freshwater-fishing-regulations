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

## Notes on `source/`

The book is `source/fishing_synopsis.pdf`; validate against nothing else (`python -m pipeline.tools.one_book` fails on a second full book anywhere under `source/`, even renamed: by name, or by size and pages; CI holds the tracked files only, so there it checks those, and a machine with `source/official/` filled is checked on the whole folder). The nine region PDFs in `source/official/` are pinned reference copies that nothing reads: `python -m data.fetch_region_pdfs` fetches them and checks `source/region_pdfs.pins.json` (T1156).


`source/dfo_tidal_areas/` is 380 saved DFO web pages in dated folders (snapshots of the tidal-area descriptions the tidal boundary was read
from, read by `pipeline/tools/audit/tidal_pass2.py`): keep them, they are the evidence. `source/pins.json` pins the big files that are not in git.

## The scripts here

- `fetch_data.py` — the upstream fetcher (above).
- `build_tidal_boundary.py`, `process_tidal_boundary.py` — the DFO tidal polygon, processed once and stored on R2 (each
  file's docstring has its steps).

Licences: third-party material and its terms are listed in the root `NOTICE`; the OpenStreetMap-derived data is ODbL,
`LICENSE-ODbL.md`.
