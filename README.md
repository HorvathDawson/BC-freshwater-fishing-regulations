# Can I Fish This?

**[canifishthis.ca](https://canifishthis.ca)** — a map of British Columbia's freshwater fishing
regulations: tap a stream or lake and see what applies there, today.

BC's rules are spread across the synopsis PDF, its zone and water tables, DFO in-season notices and
licensing schedules. This project reads them once, binds every rule to the stretches of water it
covers, and decides every answer ahead of time, so the app only looks answers up.

How to work in the repo (branches, pull requests, commit format) is [Contributing](#contributing) below; the binding
rules are [`AGENTS.md`](AGENTS.md). The v1 code (`archive/`) was removed in T335: `git show d1b36ca44:archive/…`.

## How it works

```
source data ─┐            ┌─ clim ───────────┐
             ├─ atlas ────┼─ reach ──────────┼─► deliver ─────────────────────────► data/release/r-*/
curated data ┘  graph,    │  every rule      │   bundle → verdicts → status index     (+ LATEST)
                sections, │  bound to        │   → UI export → answers → store
                registry  └─ tiles (PMTiles) ┘   → release
```

`python -m pipeline build --graph` prints the stages in run order with what each reads.

- **Clim** (`pipeline/gauges/feed/climatology.py`): the gauge envelope (`clim.json`) from HYDAT.
- **Atlas** (`pipeline/atlas/`): the stream network and lakes from the BC Freshwater Atlas, cut into
  *sections* at the curated splits; a registry of named waters. Every artifact downstream keys
  sections by the atlas's handle table, `section_handles.txt`.
- **Reach** (`pipeline/atlas/reach/`): binds each rule of each catalogue entry to the sections it
  covers (extents, tributary walks, areas). Deterministic.
- **Tiles** (`pipeline/deliver/tiles/`): the map, with a sidecar recording which atlas and registry
  it was cut from.
- **Deliver** (`pipeline/deliver/`): the SQLite bundle, then the reader's every answer
  (`verdicts.sqlite`), the status index, the UI export, the answers and the release files the app reads. The
  app decides nothing.

The current maps: [`pipeline/docs/MAP.md`](pipeline/docs/MAP.md) (stages, modules, inputs, outputs) and
[`app/MAP.md`](app/MAP.md) (packages, the data path, fixtures, gates).

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
  tools/             audits, emitters of generated app code, the mini release
  build.py           the one rebuild command
  release.py         the release command (build → checks → LATEST → staging; --cut makes the release branch)
  tests/             pytest (markers: slow, smoke, needs_bundle, needs_atlas, …)
  docs/              the pipeline map, the rulings, design notes
app/                 pnpm workspace: packages/{core,data,ui,ui-native,map}, apps/mobile (Expo: native + web),
                     conformance/, seo/ (the static site), workers/feeds (the feeds Worker), tools/ (the gates)
curation-review/     the curation tool (FastAPI backend + frontend) for reviewing entries
deploy/              staging and production deploy scripts (uploads, site, workers, store builds)
scripts/             fast-fail, heavy.sh, before_push.py, the test tiers, bootstrap
handoff/PLAN/        the ticket ledger and tickets (app-integration/tickets/), runbooks, owner to-do, status
data/
  source/            fetched inputs (FWA GeoPackage, gauges, synopsis PDF, …)
  curated/           human-owned decisions — never regenerable
  generated/         everything the pipeline writes
```

## Getting started

Tools: Python 3.13, Node (the version in `app/.nvmrc`) with pnpm 9 (`app/package.json` `packageManager`), and
[tippecanoe](https://github.com/felt/tippecanoe) + pmtiles for tiles (`brew install tippecanoe pmtiles`).

```bash
git config core.hooksPath .githooks           # once per clone: the versioned commit-msg and pre-push hooks
bash scripts/bootstrap.sh                     # tools check, .venv, the pinned source data (data/source/pins.json)
pnpm -C app install --frozen-lockfile         # the app workspace (the commit-msg hook's commitlint lives here too)
```

`bootstrap.sh` fetches the exact source bytes the live release was built from; refreshing from upstream instead is
`python data/fetch_data.py`. The whole fresh-clone path, with the measured download and build cost, is
`handoff/PLAN/runbooks/fresh-machine.md`. The atlas is not in git: the first build is
`python -m pipeline build --atlas --promote` (below).

### Regulations (only when the synopsis changes)

```bash
python -m pipeline.regs.extraction.extract_synopsis   # data/source/fishing_synopsis.pdf → synopsis rows
bash pipeline/regs/parsing/run_parse.sh               # rows → data/curated/regulations/synopsis/
```

The parse runs Claude and spends credits: **a person runs it**, never an agent. The annual update is
`handoff/PLAN/runbooks/annual-synopsis.md`.

### Rebuild

```bash
python -m pipeline build --dry-run        # the plan: each stage, its key, run or up to date
python -m pipeline build                  # every stage that is out of date (the order: pipeline/docs/MAP.md)
python -m pipeline build --atlas          # also build a side atlas (<build>_next) + parity report
python -m pipeline build --promote        # promote <build>_next, then rebuild against it
python -m pipeline build --force reach    # rerun a stage and everything after it
python -m pipeline build --status         # the last run: time and peak memory per stage
```

A stage reruns when anything it reads changed (curated data, source stamps, the corpus, its own
code) or its outputs are missing. Each run is recorded in `data/generated/build-manifest.json`, and
the run ends with a strict check that the tiles and the bundle come from one atlas and one
registry. A build is a heavy job: run it through `scripts/heavy.sh` (one heavy job at a time, AGENTS 18). Every
pipeline command is in [`pipeline/README.md`](pipeline/README.md).

### Release (build → checks → LATEST → staging)

```bash
python -m pipeline release --cut                 # at scope lock: the version from the commits (below), release/<version>
python -m pipeline release --staging --dry-run   # the plan
python -m pipeline release --staging             # one command; stops at the first failure, a rerun resumes
```

Production re-uploads the same checked release and is the owner's: `handoff/PLAN/runbooks/release.md`.

### Tests

```bash
.venv/bin/python -m pytest -q             # the fast suite (slow tests deselected, pytest.ini)
.venv/bin/python -m pytest -q -m slow     # stage agreement and determinism
pnpm -C app check                         # every app gate, knip, the typechecks and vitest
bash scripts/test-smoke.sh                # Tier 1: the app check + pytest -m smoke
python scripts/before_push.py             # what the PR's required checks ask, for this tree (--list shows the steps)
```

A test that reads generated or fetched data carries a `needs_*` marker and **fails** when that
data is missing, printing the command that makes it. CI runs the no-data tier by deselecting those
markers (`.github/workflows/pipeline-ci.yml`). The test tiers are AGENTS 18; Tier 2 is `scripts/test-full.sh`.

### The app and the curation tool

```bash
pnpm -C app tiles                         # serve the local tiles, bundle and feeds
pnpm -C app dev:web                       # the phone UI in a browser
./scripts/dev-build.sh                    # refresh the gauge feeds (the envelope is a pipeline build stage)
bash curation-review/run.sh               # the curation tool (see curation-review/README.md)
```

The app's layers and commands: [`app/README.md`](app/README.md).

## Contributing

`main` is the protected trunk (T995, AGENTS W47). Every change reaches it through a pull request; nobody pushes to
`main`.

1. **Branch** from a fresh `origin/main`, named for the kind of change and its ticket: `feat/`, `fix/`, `perf/`,
   `docs/`, `chore/`, `ci/` or `test/` + the ticket id (`fix/T994-gauge-history`). The work list is the ticket ledger,
   `handoff/PLAN/app-integration/tickets/README.md`; a ticket file and its ledger row exist before a commit names it
   (`node app/tools/check-tickets.mjs`).
2. **Commit** in Conventional Commits form, `<type>(<scope>)!: <subject>`, header at most 100 characters, the scope
   the ticket id: `fix(T983): …`, `docs(T987): …` (T998, AGENTS W48). The types and what each does to the app version
   are `app/tools/versioning.json`. commitlint checks it three times: the commit-msg hook (`.githooks/commit-msg`),
   `scripts/before_push.py`, and docs-ci's `commits` job on the PR.
3. **Push**: the pre-push hook (`.githooks/pre-push`) runs `scripts/before_push.py --changed` for the areas the branch
   changed. `git push --no-verify` is an emergency skip only, said in the commit or PR.
4. **Open a PR** whose title follows the same format (it becomes the merge commit's subject):
   `gh pr create --base main`, then `gh pr merge --auto --merge`. It merges when the four required checks pass, with a
   merge commit (no squash, no rebase: the ledger cites lane commit hashes).

The required checks are each workflow's last job (`scripts/ci_paths.py` `REQUIRED`). Every PR runs all four
workflows; a `changes` job skips the jobs whose area the PR did not touch (a skip is a pass).

| workflow (`.github/workflows/`) | required check | what it runs |
|---|---|---|
| `docs-ci.yml` | `docs ok` | the ticket ledger and handoff docs guard (`app/tools/check-tickets.mjs`), the commit format |
| `app-ci.yml` | `app ok` | `pnpm check:fast` (every static gate and the typechecks), vitest on 4 shards, the web and native bundles |
| `feeds-worker-ci.yml` | `feeds worker ok` | the feeds worker's suites (`app/workers/feeds`) |
| `pipeline-ci.yml` | `pipeline ok` | curated paths, committed artifacts against their rosters, generated files current, the no-data pytest tier |

`feeds-hydat.yml` and `feeds-runtiming.yml` are not checks: they publish feeds, started by hand or by the feeds worker
(never a GitHub schedule).

**Versions.** The app version is never edited by hand. `python -m pipeline release --cut` computes the next SemVer
from the app's commits since the last `v*` tag (`pipeline/release_version.py`: `feat` → minor, `fix` / `perf` → patch,
a breaking change → major; minor before 1.0.0) and makes `release/<version>`; a fix lands on `main` first and is
cherry-picked onto the release branch. At ship the release tags `data/<r-id>` and `v<version>`.

## License

Not affiliated with or endorsed by the Government of British Columbia. Regulation data is from
publicly available provincial and federal documents. Always check the official sources before you
fish.
