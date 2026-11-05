# pipeline — main commands

Run everything with the venv: `.venv/bin/python`. The whole rebuild is one command; the steps below
are what it runs, for when you need one alone.

## A release (T408): build once, promote the same artifact

```bash
python -m pipeline release --staging --dry-run   # the plan: preflight, baseline, build plan, checks, promote, upload
python -m pipeline release --staging             # build -> checks -> promote LATEST -> deploy/staging/upload.sh
python -m pipeline release --staging --accept-diff data/generated/release/checks/<r-id>/binding_diff.json
python -m pipeline release --prod <r-id>         # HUMAN-ONLY: the same release to production (refuses an agent)
```

`pipeline/release.py`; the runbook is `handoff/PLAN/runbooks/release.md`. The build runs with `--hold-latest`: the new
release waits in `data/release/CANDIDATE` and LATEST moves only after the checks pass. This replaces the hand steps
(build, binding diff, upload guard, `upload.sh`).

## The rebuild

```bash
python -m pipeline build --graph          # the stage graph: what each stage reads, runs, writes
python -m pipeline build --dry-run        # each stage, its key, run or up to date — and WHY it runs
python -m pipeline build                  # clim + reach + tiles side by side (--jobs 2), then deliver
python -m pipeline build --jobs 1         # one stage after another, output on the console
python -m pipeline build --status         # the last run: per-stage time, peak memory, status, logs
python -m pipeline build --atlas          # + a side atlas data/generated/atlas/<build>_next and its parity
python -m pipeline build --promote        # adopt <build>_next, then rebuild against it
python -m pipeline build --side DIR [--atlas]   # every stage into DIR; nothing live, no LATEST (a proof run)
python -m pipeline build --hold-latest    # the new release named in data/release/CANDIDATE, LATEST untouched
```

See `pipeline/build.py` and AGENTS rule 62. Heavy: run it through `scripts/heavy.sh` (one heavy job at a
time; heavy.sh is re-entrant for its own lock holder). Each stage is skipped while its inputs' digests
(atlas, corpus, curated tree, source stamps, its own code) match its last successful run
(`data/generated/build-manifest.json`). With `--jobs 2` a stage logs to `build-logs/<run>/<stage>.log`.

## Review marks → curated files (T266, T408)

```bash
python -m pipeline.regs.review.apply             # every open domain: marks.jsonl -> its final curated file
python -m pipeline.regs.review.apply --dry-run   # what would land; writes nothing
python -m pipeline.regs.review.export            # print the decisions in force per domain (in memory, no files)
```

The writers compute the marks' decisions in memory (`review.export.load`); there is no intermediate
`decided/` copy. No build stage reads `marks.jsonl`; they read the curated files the apply paths write.

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

Entries land in `data/curated/regulations/synopsis/region-*.json`. An agent never runs
`parse`, `all`, `review` or `repass`: hand the command to a person.

## Curation

```bash
bash curation-review/run.sh               # review entries, edit splits (validated, backed up)
python -m pipeline.tools.audit.check_curated    # are the committed match artifacts fresh?
```

## Notes

- `data/source/bc_fisheries_data.gpkg` (~9.6 GB FWA source) must be present for the atlas and tiles.
- How the book is read: `pipeline/docs/RULINGS.md`. Rules for this repo: `AGENTS.md`.
- After editing code, `graphify update .` keeps the knowledge graph current.
