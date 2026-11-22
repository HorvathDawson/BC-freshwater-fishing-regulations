# Pipeline design docs

**Start with [`MAP.md`](MAP.md)** (the pipeline as it is: stages, modules, inputs, outputs) and
[`RULINGS.md`](RULINGS.md) (how the book is read: every user ruling, with its test), then the root `AGENTS.md` (the
rules for working here). The root `README.md` has the layout and the rebuild command.

| Doc | What it is |
|---|---|
| **[MAP](MAP.md)** | **The one current map of the pipeline** |
| **[RULINGS](RULINGS.md)** | **Every interpretation ruling, its example and what enforces it** |
| **[GOTCHAS](GOTCHAS.md)** | **Data that looks like a bug, a change or a duplicate, and is not** |
| [01-domain-model](01-domain-model.md) | Waters, reaches, regulations — the vocabulary |
| [03-graph-design](03-graph-design.md) | Inverted graph, lakes as nodes, the two flow guards |
| [04-section-split-design](04-section-split-design.md) | Sectionizer, anchor types, curated cuts |
| [05-name-variations](05-name-variations.md) | Name tuples, variants, the display flag |
| [06-ui-data-contract](06-ui-data-contract.md) | What the UI is handed, and what it is never asked to work out (2026-09 design; live code cites it) |
| [07-gear-representation](07-gear-representation.md) | The gear / while / conduct model |
| [15-live-data-flow](15-live-data-flow.md) | Gauges + stocking: bundle vs feed, who computes the percentile |
| [18-how-regulations-are-stored](18-how-regulations-are-stored.md) | The catalogue format the parser writes |
| [19-dfo-salmon-pipeline](19-dfo-salmon-pipeline.md) | DFO salmon: fetch, parse, untangle, curate |
| [REACH-BUILDER](REACH-BUILDER.md) | The reach builder as built: how a rule binds to sections |

Numbering has gaps because superseded docs were removed; the numbers are kept so links resolve.

## History (removed in T335)

The superseded designs under `archive/` (the v1 pipeline map, the delivery plans `10-plan` / `13-build-plan`, the
September–October state notes, the old work lists) and the v1 code (`archive/`) were deleted on 2026-11-06. Any path a
comment still names reads with `git show d1b36ca44:<path>`. Moved up because live code cites them: `REACH-BUILDER`,
`06-ui-data-contract`. The two historical handoffs moved up then (`HANDOFF-curated-layout`, the curated-data
restructure, done; `HANDOFF-dfo-curation`, the 2026-09 curation sitting guide) were removed in T987:
`git show caf593bb6:pipeline/docs/<name>.md`. The 2026-08 locator research that notes in `data/curated/waters/cuts.json` cite as their
source reads with `git show d1b36ca44:pipeline/docs/archive/waterbody-splits.json`.

## Where the work is

The ticket ledger, `handoff/PLAN/app-integration/tickets/README.md`, is the one work list.
⛔ **Parser runs spend credits and are human-only** — see the root `AGENTS.md` rule A1.
