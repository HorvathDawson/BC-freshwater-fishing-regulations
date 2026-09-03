# pipeline — main commands

Run everything with the venv: `.venv/bin/python`. Order below is the usual flow.

## 1. Splits (curated stream cuts)

```bash
# Rebuild splits.json from the curated source (deterministic, no API cost)
.venv/bin/python -m pipeline.oneoff.build_splits            # writes data/curated/waters/splits.json
.venv/bin/python -m pipeline.oneoff.build_splits --dry-run  # preview stats only
```

## 2. Build graph + registry

```bash
# Whole province (heavy, ~15–20 min). Applies splits, writes registry.json.
.venv/bin/python -m pipeline.atlas.build --full \
  --splits data/curated/waters/splits.json --out output/v2/full

# Small area (fast) — scope by GNIS name or bbox
.venv/bin/python -m pipeline.atlas.build --gnis "Campbell River" --out output/v2/validate
.venv/bin/python -m pipeline.atlas.build --bbox MINX MINY MAXX MAXY --out output/v2/validate
```

Output: `output/v2/<name>/registry.json` (+ graph artifacts / `graph.gpkg`).

## 3. Parse regulations (⛔ HUMAN-ONLY — spends credits)

```bash
REGISTRY=output/v2/full/registry.json bash pipeline/regs/parsing/run_parse.sh
```

Resumable, row-granular (`--skip-existing`): only entries missing from
`pipeline/regs/parsing/entries/` are re-parsed. Finish a run before re-exporting.
Do NOT run this from an agent — hand it to a human to run in their terminal.

## 4. Curation review app (edit/confirm parsed entries)

```bash
bash curation-review/run.sh        # backend + frontend
```

## Notes

- `data/source/bc_fisheries_data.gpkg` (~9.6 GB FWA source) must be present for build/splits.
- Merge/pickup radius for splits is 5 m route-measure (distinct cuts stay distinct).
- After editing code, `graphify update .` keeps the knowledge graph current.
