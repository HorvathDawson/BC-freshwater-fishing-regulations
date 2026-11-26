# conformance: one suite, every implementation

The tests that hold two implementations of the same contract to the same answers: every `WaterSource`
(`src/suite.ts`) and the regs store (`src/regs/regsConformance.ts`), run against the fixture releases, the live release
and the Python-side oracles (page v36 and the Python decoder, `app/fixtures/`).

- Entry: `vitest` through `pnpm test` in `app/`. Files: `*.fixture.test.ts` (committed fixtures), `*.release.test.ts` (the
  live release on disk), `*.parity.test.ts` (against an oracle).
- Reads: `app/packages/data/dev/*` fixtures, `app/fixtures/{golden,reference}`, and `data/release/LATEST` (through the
  helpers in `src/repoData.ts`, `latestRelease.mjs`; never `data/` directly: `pnpm anchors` fails that).
- Writes nothing. The golden fixtures are re-cut by `node app/tools/regs-recut/sync.mjs`, not here.

## Rules

A release-reading test needs a release on disk and is skipped without one, unless `REGS_STORE_REQUIRED=1` (set by
`scripts/test-full.sh`), which makes it fail instead. No test may type a section number or release id (`pnpm anchors`).
