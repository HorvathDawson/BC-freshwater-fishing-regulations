# @app/core: every answer and every sentence

Pure TypeScript with no React and no platform imports (AGENTS 22): the day's status of a section or water, the
Regulations page's presenters (`src/regs/`: a hydrated `RegsInputs` in, a `RegsPage` out), conditions and flow, search,
menu words, freshness, alerts, update messages, the clock (`clock.ts`) and the failure reporter (`failure.ts`). It runs
identically on a phone, in a browser and in a test.

- Entry: `src/index.ts`, a pure barrel (no logic lives there); `./regs/testing` is the other export.
- Reads: nothing from disk or network. Its inputs are plain objects handed in by `@app/data` / `@app/ui`.
- Generated files, never edited by hand (`src/*.generated.*`, `src/regs/fish.generated.ts`, `src/nearBcOutline.ts`): written by
  `pipeline.deliver.answers.model` (answers), `pipeline.tools.emit.{calendar,gauge_policy,pack,feed_formats}`,
  `app/tools/emit-fish.mjs` and `pipeline.deliver.tiles.near_bc`. Each has a `--check` run in `pnpm check` (AGENTS 40).

## Rules

- Status is rendered by one function in core, never re-implemented per screen (AGENTS 23); the app decides no regulation,
  it looks answers up (AGENTS 56).
- The device clock is read only through `clock.ts` (`now`, `bcNow`, `bcToday`); `app/tools/clock.test.ts` enforces it.
- A caught failure is thrown on or reported through `failure.ts` `reportFailure`, never swallowed (AGENTS W46).

## Tests

`src/**/*.test.ts` beside the code (vitest, `pnpm test` in `app/`); the cross-implementation suites are in `app/conformance`.
