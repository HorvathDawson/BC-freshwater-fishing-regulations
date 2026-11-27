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

## The five everyday things

`make` lists every verb (the root `Makefile`; `scripts/README.md`: what each calls). Tools first: Python 3.13, Node (`app/.nvmrc`) with
pnpm 9, tippecanoe + pmtiles (the whole path: `docs/runbooks/fresh-machine.md`). Heavy verbs run through `scripts/heavy.sh`, macOS-only.

```bash
make setup         # once per clone: git hooks, .venv, the app's install; fetches nothing (about 35 s)
make test          # CI's no-data tier, green on a fresh clone: pipeline in chunks (~14 min), app vitest (~5 min)
make test-py ARGS="<file>"   # a part: plain pytest is the no-data tier, its NOT RUN line counts the data tests; --with-data adds them
make build-first   # the first data build, after `make fetch-data` (a 16 GB download): ~1h15, ~24 GB free memory; later: make build
make app           # the app in a browser (http://localhost:39218): tile server + Expo web, Ctrl-C stops both; the map needs a build
make release       # prints the steps of docs/runbooks/release-by-hand.md (the person's page), then the cut's dry run
```

Elsewhere a heavy verb prints the command to run. `make check` is what a PR's required checks ask; the release's flags are
`docs/runbooks/release.md`. Production is the owner's (`make prod` only prints it). The synopsis parse spends credits: **a person
runs it**, never an agent (AGENTS A1; `pipeline/regs/parsing/README.md`).

## Contributing

`main` is protected: every change is a pull request (AGENTS W47) with Conventional Commits, `type(Tnnn): subject`, header at most 100 characters
(W48). The pre-push hook runs `scripts/before_push.py --changed`.

## License

See `LICENSE` and `NOTICE`; the OpenStreetMap-derived data is ODbL (`data/LICENSE-ODbL.md`).
