# app/fixtures: frozen and release-backed test inputs

Inputs the conformance and tool tests read. Nothing here ships in the app.

| where | what | made by |
|---|---|---|
| `golden/` | release-backed oracles: `taps.json` (the Python decoder's answers), `page-oracle.json` (page v36's own code over the same release), `states.json`, `trout-a11y.json` | `node app/tools/regs-recut/sync.mjs`; the release re-cuts them on its candidate (AGENTS W46) |
| `reference/` | page v36 (html, js) and the calendar copy, frozen; the parity oracle (own README) | `python -m pipeline.tools.emit.calendar` (calendar only) |
| `trout/` | the trout and char table fixtures (`regs-release/`, `section-lines.json`, `trout-hand.json`) | `scripts/trout-refresh.sh` (through `pipeline.deliver.release.regs_fixture`); never by hand, except `trout-hand.json` |
| `reader-states.json` | states for the reader tool | by hand |
| `dfo-feed-2026-11-13.json` | a captured DFO feed for tests | captured once |

Read through the helpers in `app/conformance/src/regs/` and `app/tools/regs-recut/`, never by a literal path in a new test
(`pnpm anchors`). Other fixtures live beside what they test: `app/packages/data/dev/` (releases, feeds).
