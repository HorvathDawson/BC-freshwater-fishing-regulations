# pipeline: main commands

Run everything with the venv (`.venv/bin/python`) from the repo root. The stages and modules are mapped in
`pipeline/docs/MAP.md`; the rules for this repo are in `AGENTS.md`; how the book is read is
`pipeline/docs/RULINGS.md`.

## A release: build once, promote the same artifact

```bash
python -m pipeline release --staging --dry-run   # the plan: preflight, baseline, build plan, checks, promote, upload
python -m pipeline release --staging             # build, checks, promote LATEST, deploy/staging/upload.sh
python -m pipeline release --staging --accept-diff data/generated/release/checks/<r-id>/binding_diff.json
python -m pipeline release --prod <r-id>         # HUMAN-ONLY: the same release to production (refuses an agent)
python -m pipeline release --cut [X.Y.Z]         # version and branch (release_version.py; --data-only for data-<date>)
```

`pipeline/release.py`; the runbook is `docs/runbooks/release.md`. The build runs with `--hold-latest`:
the new release waits in `data/release/CANDIDATE` and LATEST moves only after the checks pass.

## The rebuild

```bash
python -m pipeline build --graph          # the stage graph: what each stage reads, runs, writes
python -m pipeline build --dry-run        # each stage, run or up to date, and why
python -m pipeline build                  # every stage out of date (--jobs 2 by default; --jobs 1 for one at a time)
python -m pipeline build --status         # the last run: per-stage time, peak memory, status, logs
python -m pipeline build --atlas          # plus a side atlas <build>_next and its parity; --promote adopts it
python -m pipeline build --side DIR       # every stage into DIR; nothing live (a proof run)
```

`pipeline/build.py`, AGENTS 62. It is heavy: run it through `scripts/heavy.sh` (one heavy job at a time).
A stage is skipped while its inputs' digests (atlas, corpus, curated tree, source stamps, its own code) match its
last successful run (`data/generated/build-manifest.json`). Two caches, both keyed on content
(`pipeline/common/stepcache.py`, recorder `pipeline/_trace.py`): every stage keys on the modules it really imported,
the code files it read (`schema.sql`, `tile-contract.json`), the versions of the packages it imported (not every
installed one), the config.yaml values it read (not the whole file), Python and SQLite, and the tiles also on
tippecanoe's version and tile-join's binary (T1171); imports or config reads that cannot be traced stop the build. The
atlas records the same code stamp in its meta, and `pipeline release` refuses an atlas whose code moved (an atlas built
before T1171 is proven once by `python -m pipeline.atlas.inputs --backfill-code`). And each `pipeline.deliver all` step is reused when its arguments, inputs and reads are unchanged
(`data/generated/deliver-cache/`). So a rule edit reruns reach and deliver; an answers or wording edit reruns
answers onward, not the bundle. `build --force deliver` runs every delivery step afresh.

## The stages alone

```bash
python -m pipeline.atlas.build --full --out data/generated/atlas/full_next    # never into a promoted build
python -m pipeline.atlas.promote data/generated/atlas/full_next [--dry-run]
python -m pipeline.atlas.build --gnis "Campbell River" --out data/generated/atlas/validate   # a small area
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out data/generated/reaches/full
python -m pipeline.deliver all --build data/generated/atlas/full
python -m pipeline.deliver.tiles --build data/generated/atlas/full            # never the basemap
```

## Regulations (HUMAN-ONLY: the parse spends credits)

```bash
python -m pipeline.regs.extraction.extract_synopsis       # PDF to synopsis rows
bash pipeline/regs/parsing/run_parse.sh status            # where you left off (no credits)
bash pipeline/regs/parsing/run_parse.sh parse-dry         # export batches only (no credits)
```

`run_parse.sh all` spends credits: an agent hands the command to a person (A1). Entries land in
`data/curated/regulations/synopsis/region-*.json`.

## Review marks and curation

```bash
python -m pipeline.regs.review.apply [--dry-run]   # every open domain: marks.jsonl to its final curated file
python -m pipeline.regs.review.export              # the decisions in force per domain, in memory
bash curation-review/run.sh                        # the review app (curation-review/README.md)
python -m pipeline.tools.audit.check_curated       # are the committed match artifacts fresh?
```

No build stage reads `marks.jsonl`; they read the curated files the apply paths write.

## Notes

`data/source/bc_fisheries_data.gpkg` (the FWA source; `bash scripts/bootstrap.sh` fetches it) must be present
for the atlas and tiles. After editing code, `graphify update .` keeps the knowledge graph current.
