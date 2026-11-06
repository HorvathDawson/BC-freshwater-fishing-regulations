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
| [07-gear-representation](07-gear-representation.md) | The gear / while / conduct model |
| [15-live-data-flow](15-live-data-flow.md) | Gauges + stocking: bundle vs feed, who computes the percentile |
| [18-how-regulations-are-stored](18-how-regulations-are-stored.md) | The catalogue format the parser writes |
| [19-dfo-salmon-pipeline](19-dfo-salmon-pipeline.md) | DFO salmon: fetch, parse, untangle, curate |

Numbering has gaps because superseded docs were archived; the numbers are kept so links resolve.

## History (removed in T335)

The superseded designs under `archive/` (the v1 pipeline map, the delivery plans `10-plan` / `13-build-plan`, the
September–October state notes, the old work lists) and the v1 code (`archive/`) were deleted on 2026-11-06. Any path a
comment still names reads with `git show d1b36ca44:<path>`. Moved up because live code cites them: `REACH-BUILDER`,
`HANDOFF-curated-layout`, `HANDOFF-dfo-curation` (the curation sitting guide), `06-ui-data-contract`. Kept in place:
`archive/waterbody-splits.json`, which the curated `data/curated/waters/cuts.json` names as its research source.

## Where the work is

The ticket ledger, `handoff/PLAN/app-integration/tickets/README.md`, is the one work list.
⛔ **Parser runs spend credits and are human-only** — see the root `AGENTS.md` rule 1.
