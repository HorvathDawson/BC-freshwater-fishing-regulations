# Can I Fish This?

**[canifishthis.ca](https://canifishthis.ca)** — a map of British Columbia's freshwater fishing
regulations: tap a stream or lake and see what applies there, today.

BC's rules are spread across the synopsis PDF, its zone and water tables, DFO in-season notices and
licensing schedules. This project reads them once, binds every rule to the stretches of water it
covers, and decides every answer ahead of time, so the app only looks answers up.

> This branch (`redesign/stream-sections`) is **v2**. `main` still runs v1, which is archived here
> under `archive/`. Nothing is merged to `main` until the feed crons are reworked
> (see `pipeline/docs/` and the P3 note).

## How it works

```
source data ─┐
             ├─ atlas ──► reach ──► deliver ──────────────────────────────► tiles
curated data ┘  graph,     every rule   bundle → verdicts → status index    PMTiles
                sections,  bound to     → UI export → answers (answers/2)   (+ sidecar)
                registry   sections
```

- **Atlas** (`pipeline/atlas/`): the stream network and lakes from the BC Freshwater Atlas, cut into
  *sections* at the curated splits; a registry of named waters. Every artifact downstream keys
  sections by the atlas's handle table, `section_handles.txt`.
- **Reach** (`pipeline/atlas/reach/`): binds each rule of each catalogue entry to the sections it
  covers (extents, tributary walks, areas). Deterministic.
- **Deliver** (`pipeline/deliver/`): the SQLite bundle, then the reader's every answer
  (`verdicts.sqlite`), the status index, the UI export and the answers file the app reads. The
  app decides nothing.
- **Tiles** (`pipeline/deliver/tiles/`): the map, with a sidecar recording which atlas and registry
  it was cut from.

Each stage records digests of what it read (atlas handles, registry, corpus, reach run) and the
next stage refuses a mismatched pair. The rulings on how the book is read are in
[`pipeline/docs/RULINGS.md`](pipeline/docs/RULINGS.md); the rules for working in this repo are in
[`AGENTS.md`](AGENTS.md).

## Layout

```
pipeline/            Python pipeline
  atlas/             graph, sections, registry, splits, reach builder
  regs/              synopsis extraction, LLM parsing (human-run), catalogue model, DFO salmon
  deliver/           bundle, verdicts, status index, export, answers, tiles
  gauges/ runtiming/ stocking/ bathymetry/   live and supporting data
  common/            paths (config.yaml → pipeline.common.curated), models, digests
  build.py           the one rebuild command
  tests/             pytest (markers: slow, needs_bundle, needs_atlas, …)
app/                 pnpm workspace: core/ui/map packages, apps/web and apps/mobile (Expo)
curation-review/     the curation tool (FastAPI backend + frontend) for reviewing entries
data/
  source/            fetched inputs (FWA GeoPackage, gauges, synopsis PDF, …)
  curated/           human-owned decisions — never regenerable
  generated/         everything the pipeline writes
archive/             v1 (pipeline, webapp, worker, crons, deploy docs)
```

## Getting started

Python 3.13 (a `.venv`), and [tippecanoe](https://github.com/felt/tippecanoe) for tiles.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
python data/fetch_data.py                 # source data (~30 min the first time)
```

The synopsis PDF goes at `data/source/fishing_synopsis.pdf`.

### Regulations (only when the synopsis changes)

```bash
python -m pipeline.regs.extraction.extract_synopsis   # PDF → synopsis rows
bash pipeline/regs/parsing/run_parse.sh               # rows → data/curated/regulations/entries/catalogue/
```

The parse runs Claude and spends credits: **a person runs it**, never an agent.

### Rebuild

```bash
python -m pipeline build --dry-run        # the plan: each stage, its key, run or up to date
python -m pipeline build                  # reach → deliver → tiles, whatever is out of date
python -m pipeline build --atlas          # also build a side atlas (<build>_next) + parity report
python -m pipeline build --promote        # promote <build>_next, then rebuild against it
python -m pipeline build --force reach    # rerun a stage and everything after it
```

A stage reruns when anything it reads changed (curated data, source stamps, the corpus, its own
code) or its outputs are missing. Each run is recorded in `data/generated/build-manifest.json`, and
the run ends with a strict check that the tiles and the bundle come from one atlas and one
registry. Peak memory is about 12 GB.

### Tests

```bash
.venv/bin/python -m pytest -q             # the fast suite (slow tests deselected)
.venv/bin/python -m pytest -q -m slow     # stage agreement and determinism
```

A test that reads generated or fetched data carries a `needs_*` marker and **fails** when that
data is missing, printing the command that makes it. CI runs the no-data tier by deselecting those
markers (`.github/workflows/pipeline-ci.yml`).

### The app and the curation tool

```bash
cd app && pnpm install && pnpm check      # what app CI runs
pnpm tiles                                # serve the local tiles, bundle and feeds
pnpm dev:web                              # the phone UI in a browser
./scripts/dev-build.sh                    # refresh the gauge feeds (~1 min)
curation-review/run.sh                    # the curation tool (see curation-review/README.md)
```

## License

Not affiliated with or endorsed by the Government of British Columbia. Regulation data is from
publicly available provincial and federal documents. Always check the official sources before you
fish.
