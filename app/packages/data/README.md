# @app/data: the data seam

The only package that reads a release. One interface, `WaterSource` (`src/source.ts`), and one implementation per platform
over the same release files; the regs store reader; the release loader and installer; the SQL drivers; the user's spots;
and the gauge, in-season and salmon feed clients. Bulk reads take arrays because the map colours a viewport, not a feature.

- Entry: `src/index.ts`. Sub-exports in `package.json`: `./release`, `./release/platform`, `./release/node` (reads a release
  directory, for tests), `./regs`, `./spots`, `./fixture`.
- `src/release/`: manifest, `loader.ts`, `install.ts` (phone: to disk; web: fetched on first need), prefetch, `bgDownload.ts`,
  `offline.ts`, `source.ts`; `src/regs/` the regs store; `src/sql/` the drivers (`expo-sqlite` native, `sql.js` web);
  `src/feed/` the feeds worker's files; `src/notify/` the phone's background alert check; `src/spots/` the user's own data.
- Reads: `data/release/r-*/` locally, R2 in staging and production (the `env` in `apps/mobile`); the feeds under `feeds/`.
- Writes: the phone's installed release set and the spots file (user data no update touches).
- `dev/`: frozen test releases and feeds (`fixture-release`, `regs-release`, `mini-release`, `feeds`); see `app/MAP.md` "Fixtures".
- `pack.generated.ts` (and `src/regs/fflateWorker.generated.ts`) are generated (`pipeline.tools.emit.pack`,
  `app/tools/emit-fflate-worker.mjs`): never edited by hand.

## Rules

A query layer, not a view layer: no React (`layers.json`). `fetchVerified` checks the decoded bytes of every release file. A new `WaterSource` method must be in
the conformance suite.

## Tests

`src/**/*.test.ts` beside the code; `app/conformance` runs one suite against every implementation.
