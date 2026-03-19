# Can I Fish This?

**[canifishthis.ca](https://canifishthis.ca)** — A map-based tool for looking up freshwater fishing regulations in British Columbia.

BC's fishing regulations are scattered across PDFs, synopsis tables, and in-season notices. This project pulls all of that together into a searchable, clickable map so you can find what applies to the water you're actually standing next to.

## How it works

A Python pipeline processes BC's Freshwater Atlas (GeoBC) and provincial regulation data through several stages:

1. **Atlas** — Builds a graph of BC's stream network and waterbodies from the FWA GeoPackage
2. **Tiles** — Exports the atlas geometry to PMTiles for efficient map rendering
3. **Enrichment** — Matches regulation entries (extracted from synopsis PDFs via LLM) to atlas features, producing a searchable JSON index

The output gets uploaded to Cloudflare R2 and served through a small worker. The frontend is a React + MapLibre app that renders the tiles and lets you search/click any stream or lake to see its regulations.

## Project layout

```
pipeline/           Python pipeline (atlas → tiles → enrichment)
  atlas/            Stream network graph + waterbody geometry
  tiles/            PMTiles export via tippecanoe
  enrichment/       Regulation matching and index building
  extraction/       Synopsis PDF extraction
  parsing/          LLM-based regulation parsing
  matching/         Feature name resolution + overrides
  graph/            FWA network graph builder
  deploy/           R2 upload sharding
  tests/            pytest suite

webapp/             React + TypeScript frontend (MapLibre, Vite)
r2-worker/          Cloudflare Worker serving data from R2
data/               Source GeoPackage + tidal boundary data
scripts/            Dev server, R2 seeding, rclone setup
```

## Getting started

Requires Python 3.11+, a conda env or venv, and [tippecanoe](https://github.com/felt/tippecanoe) installed.

```bash
pip install -r requirements.txt
```

### 1. Fetch source data

After cloning, pull down BC's Freshwater Atlas and supporting GIS layers:

```bash
python data/fetch_data.py              # downloads everything (~30 min first time)
python data/fetch_data.py --skip-ftp   # just the WFS layers, skip heavy FTP downloads
```

You also need the BC fishing synopsis PDF — drop it at `data/fishing_synopsis.pdf`.

### 2. Extract and parse regulations

Extract regulation rows from the synopsis PDF, then parse them into structured data with Gemini:

```bash
python -m pipeline.extraction.extract_synopsis    # PDF → synopsis_raw_data.json
python -m pipeline.parsing.parser                  # raw rows → parsed regulation entries
```

Parsing requires Gemini API keys. Create a `.env` file in the project root with at least one key:

```
GOOGLE_API_KEY=your-key-here
```

The keys referenced in `config.yaml` are loaded from env vars at runtime. You can configure as many as you want for key rotation.

### 3. Run the pipeline

```bash
python -m pipeline --step all        # full run: atlas → tiles → enrich
python -m pipeline --step tiles enrich  # skip atlas if only regs changed
pytest pipeline/tests/ -q
```

Pipeline output lands in `output/pipeline/deploy/`.

## Running the webapp locally

```bash
cd webapp && npm install
node scripts/dev.mjs    # seeds local R2, starts worker + Vite dev server
```

Site runs at `http://localhost:5173`, data API at `http://localhost:8787`.

## Deploying

Push to `staging` or `main` — Cloudflare's git integration deploys both workers automatically. Data uploads go through `scripts/seed-r2.sh`. See [DEPLOY.md](DEPLOY.md) for the full rundown.

## License

This project is not affiliated with or endorsed by the BC government. Regulation data is sourced from publicly available provincial documents. Always verify regulations with official sources before fishing.
