# pipeline — main commands

Run everything with the venv: `.venv/bin/python`. The whole rebuild is one command; the steps below
are what it runs, for when you need one alone.

## The rebuild

```bash
python -m pipeline build --dry-run        # each stage, its key, run or up to date
python -m pipeline build                  # reach → tiles → deliver, whatever is out of date
python -m pipeline build --atlas          # + a side atlas data/generated/atlas/<build>_next and its parity
python -m pipeline build --promote        # adopt <build>_next, then rebuild against it
```

See `pipeline/build.py` and AGENTS rule 62. Heavy (peak ~12 GB): one heavy job at a time.

## The stages alone

```bash
# Atlas: graph, sections, registry (whole province ~15–20 min). Never into a promoted build.
python -m pipeline.atlas.build --full --out data/generated/atlas/full_next
python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run   # parity report only
python -m pipeline.atlas.promote data/generated/atlas/full_next             # full -> full.prev

# A small area, for checking a change fast
python -m pipeline.atlas.build --gnis "Campbell River" --out data/generated/atlas/validate

# Reach: bind every rule to its sections (deterministic)
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out data/generated/reaches/full

# Deliver: bundle → verdicts → status index → UI export → answers
python -m pipeline.deliver all --build data/generated/atlas/full

# Tiles (~40 min; writes atlas.pmtiles, atlas.meta.json and layers/ — never the basemap)
python -m pipeline.deliver.tiles --build data/generated/atlas/full
```

## Regulations (⛔ HUMAN-ONLY — the parse spends credits)

```bash
python -m pipeline.regs.extraction.extract_synopsis       # PDF → synopsis rows
bash pipeline/regs/parsing/run_parse.sh status            # where you left off (no credits)
bash pipeline/regs/parsing/run_parse.sh parse-dry         # export batches only (no credits)
bash pipeline/regs/parsing/run_parse.sh all               # parse, then review (spends credits)
```

Entries land in `data/curated/regulations/entries/catalogue/region-*.json`. An agent never runs
`parse`, `all`, `review` or `repass`: hand the command to a person.

## Curation

```bash
bash curation-review/run.sh               # review entries, edit splits (validated, backed up)
python -m pipeline.tools.check_curated    # are the committed match artifacts fresh?
```

## Notes

- `data/source/bc_fisheries_data.gpkg` (~9.6 GB FWA source) must be present for the atlas and tiles.
- How the book is read: `pipeline/docs/RULINGS.md`. Rules for this repo: `AGENTS.md`.
- After editing code, `graphify update .` keeps the knowledge graph current.
