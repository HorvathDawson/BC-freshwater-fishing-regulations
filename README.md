# Can I Fish This?

**[canifishthis.ca](https://canifishthis.ca)**: a map of British Columbia's freshwater fishing regulations. Tap a stream or lake and see what
applies there, today.

BC's rules are spread across the synopsis PDF, its zone and water tables, DFO in-season notices and licensing schedules. `pipeline/` reads them
once, binds every rule to the stretches of water it covers and decides every answer ahead of time into a release (`data/release/r-*`); `app/` only
looks answers up. Not affiliated with the Government of British Columbia: always check the official sources before you fish.

## The map

| folder | what |
|---|---|
| `pipeline/` | Python. The water network, the rules and the release: `atlas/` (FWA graph, sections, reach builder), `regs/` (reading the book, DFO salmon, review library), `gauges/`, `deliver/` (bundle, verdicts, answers, tiles, release), `common/`, `runtiming/`, `tools/`, `tests/`; `bathymetry/` and `stocking/` are prepared, not fed |
| `app/` | pnpm workspace: the Expo / React Native app for iOS, Android and web (`apps/mobile`, `packages/{core,data,ui,ui-native,map}`), `conformance/`, the feeds worker (`workers/feeds`), the gates and tools (`tools/`) |
| `data/` | `source/` fetched inputs, `curated/` human-owned decisions (never regenerable), `generated/` what the pipeline writes (not in git) |
| `tools/curation-review/` | the owner's review tool (FastAPI backend + Vite frontend); writes the owner's marks |
| `deploy/` | staging and production uploads, site and worker deploys, store builds |
| `scripts/` | dev and CI helpers: `heavy.sh`, `before_push.py`, the test tiers, bootstrap |
| `docs/` | the plans, runbooks, restart pages and decisions |
| `tickets/` | the open tickets, and the index of the closed ones |
| `.github/`, `.githooks/` | the nine workflows (four are required checks); the commit-msg and pre-push hooks |

Each folder, pipeline stage and app package has its own `README.md`.

## Start reading here

- Data side: [`pipeline/docs/MAP.md`](pipeline/docs/MAP.md) (stages, inputs, outputs), then `pipeline/build.py` (one rebuild) and
  `pipeline/deliver/__main__.py` (the release steps). How the book is read: [`pipeline/docs/RULINGS.md`](pipeline/docs/RULINGS.md), which wins over any other note.
- App side: [`app/MAP.md`](app/MAP.md), then `app/apps/mobile/src/App.tsx` (the one shell) and `app/packages/ui-native/src/Shell.tsx`.
- Docs index: [`docs/README.md`](docs/README.md) says what each page and folder in `docs/` is.
- Rules for working here: [`AGENTS.md`](AGENTS.md); read the rule you are about to touch. Work lives in tickets (AGENTS W49): `tickets/README.md`.

## The three everyday things

```bash
# 0. once per clone
git config core.hooksPath .githooks && bash scripts/bootstrap.sh && pnpm -C app install --frozen-lockfile

# 1. the app (from app/): the phone UI in a browser, with local tiles, release and feeds
pnpm -C app tiles          # one terminal
pnpm -C app dev:web        # another

# 2. a pipeline build (heavy: through scripts/heavy.sh, one at a time)
python -m pipeline build --dry-run    # the plan: each stage, run or up to date
python -m pipeline build --graph      # the stages in run order, with what each reads
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build

# 3. the tests
.venv/bin/python -m pytest -q         # the fast pipeline suite
pnpm -C app check                     # every app gate, knip, typecheck, vitest
python scripts/before_push.py         # what a PR's required checks ask, for this tree
```

Tools: Python 3.13, Node (`app/.nvmrc`) with pnpm 9, and tippecanoe + pmtiles for tiles. The whole fresh-machine path is
`docs/runbooks/fresh-machine.md`. `bootstrap.sh --setup-only` is the cheap path (hooks, `.venv`, JS install, no download); the full run also downloads about 16 GB of pinned source data. A release, start to finish by hand: [`docs/runbooks/release-by-hand.md`](docs/runbooks/release-by-hand.md) (the flags are in `docs/runbooks/release.md`; production is the owner's).
The synopsis parse spends credits: **a person runs it**, never an agent (AGENTS A1; `pipeline/regs/parsing/README.md`).

## Contributing

`main` is protected: every change is a pull request (AGENTS W47) with Conventional Commits, `type(Tnnn): subject`, header at most 100 characters
(W48). The pre-push hook runs `scripts/before_push.py --changed`.

## License

See `LICENSE` and `NOTICE`; the OpenStreetMap-derived data is ODbL (`data/LICENSE-ODbL.md`).
