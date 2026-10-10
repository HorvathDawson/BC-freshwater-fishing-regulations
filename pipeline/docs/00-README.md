# Pipeline design docs

**Start with [`RULINGS.md`](RULINGS.md)** (how the book is read: every user ruling, with its test)
and the root `AGENTS.md` (the rules for working here). The root `README.md` has the layout and the
rebuild command.

| Doc | What it is |
|---|---|
| **[RULINGS](RULINGS.md)** | **Every interpretation ruling, its example and what enforces it** |
| **[GOTCHAS](GOTCHAS.md)** | **Data that looks like a bug, a change or a duplicate, and is not** |
| [01-domain-model](01-domain-model.md) | Waters, reaches, regulations — the vocabulary |
| [03-graph-design](03-graph-design.md) | Inverted graph, lakes as nodes, the two flow guards |
| [04-section-split-design](04-section-split-design.md) | Sectionizer, anchor types, curated cuts |
| [05-name-variations](05-name-variations.md) | Name tuples, variants, the display flag |
| [06-ui-data-contract](06-ui-data-contract.md) | What crosses into the client, and what it is never asked to work out (partly stale: answers/2 replaced the client-side reading) |
| [07-gear-representation](07-gear-representation.md) | The gear / while / conduct model |
| [15-live-data-flow](15-live-data-flow.md) | Gauges + stocking: bundle vs feed, who computes the percentile |
| [18-how-regulations-are-stored](18-how-regulations-are-stored.md) | The catalogue format the parser writes |
| [19-dfo-salmon-pipeline](19-dfo-salmon-pipeline.md) | DFO salmon: fetch, parse, untangle, curate |
| [CURRENT-STATE](CURRENT-STATE.md), [NEXT](NEXT.md) | Older state notes and parked work (partly stale; the live plan is outside git) |

Numbering has gaps because superseded docs moved to `archive/`; the numbers are kept so links resolve.

## `archive/`

Superseded designs, kept for provenance; nothing reads them. Among them: the v1 pipeline map (`02`),
`10-plan` and `13-build-plan` (the delivery plan, done), `REACH-BUILDER` and `RESOLVER-HANDOFF` (the
reach builder is built), `12-testing`, `05-table-generation` (the table was removed 2026-09-22),
`16-curated-data-layout` and `HANDOFF-curated-layout` (done), `HANDOFF-data`, `HANDOFF-dfo-curation`
(the curation sitting guide), and `curation-review-queue.md` (an old work list).

## Where the work is (2026-10-09)

```
curated + source ─▶ atlas ─────────▶ reach ─────────▶ deliver ─────────────▶ tiles
                     21,147 items      1,525 entries    bundle · verdicts ·     PMTiles
                     1.96 M sections   3,406 rules      status · export ·
                                                         answers/2
```

Every entry still awaits a person's check against the book (the curation-review app's queue).
⛔ **Parser runs spend credits and are human-only** — see the root `CLAUDE.md`.
