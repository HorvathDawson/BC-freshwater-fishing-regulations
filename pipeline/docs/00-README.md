# Pipeline design docs

**Read [`10-plan.md`](10-plan.md) then [`13-build-plan.md`](13-build-plan.md).**

`10` is where we are, how reaches resolve, the content schema, and every known issue with a proposed
solution. It absorbs the old `06`, `09`, `15`, `16`, `17` and `18`.

`13` is how it gets delivered: durable addressing (what binds to `item_id` vs `section_id`), one
content store feeding **separate web and mobile packagers**, the **greenfield mobile-first app**
architecture, **the reach builder** (the next thing to build), live feeds (gauges / stocking /
in-season) versioned apart from the bundle, bathymetry, and ordered steps with acceptance criteria. It
**supersedes `10` §4 (artifacts), §5 (data flow) and §7 (sequencing)**; the rest of `10` stands.

| Doc | What it is |
|---|---|
| **[10-plan](10-plan.md)** | **Status · resolution · schema · the 43 issues** (§4/§5/§7 superseded by 13) |
| **[13-build-plan](13-build-plan.md)** | **Delivery: content store → web + mobile packagers · live feeds · ordered steps + acceptance criteria** |
| [01-domain-model](01-domain-model.md) | Waters, reaches, regulations — the vocabulary |
| [03-graph-design](03-graph-design.md) | Inverted graph, lakes as nodes, the two flow guards |
| [04-section-split-design](04-section-split-design.md) | Sectionizer, anchor types, curated cuts |
| [05-name-variations](05-name-variations.md) | Name tuples, variants, the display flag |
| **[GOTCHAS](GOTCHAS.md)** | **Data that looks like a bug, a change or a duplicate, and is not** |
| **[NEXT](NEXT.md)** | **Parked work with the thinking done — PMTiles, tributary walk, open bugs** |
| **[REACH-BUILDER](REACH-BUILDER.md)** | **Structure sketch of the next thing to build** |
| [RESOLVER-HANDOFF](RESOLVER-HANDOFF.md) | Everything found about `resolve_extent`, for whoever is in it |
| [12-testing](12-testing.md) | Test strategy, spikes S1–S4, sanity gates |
| [15-live-data-flow](15-live-data-flow.md) | Gauges + stocking: bundle vs feed, who computes the percentile |
| **[06-ui-data-contract](06-ui-data-contract.md)** | **What crosses into the client, and what it is never asked to work out** |
| [05-table-generation](05-table-generation.md) | How a table was built from rules — **the code is removed; this is the reference to rebuild from** |

Numbering has gaps because superseded docs moved to `archive/`; the remaining numbers are unchanged
so existing links still resolve.

## Working files, not design docs

`curation-review-queue.md` — a curation work list.

## `archive/`

Superseded designs and one-off data, kept for provenance: the v1 pipeline map (`02`), early
architecture (`07`), pre-registry data structures (`08`), the delivered implementation plan (`11`),
and the six docs `10-plan` replaces. Nothing reads these.

Added 2026-09-22, when the settling layer was removed: `HANDOFF-regs-tables.md` (its state, open
defects and next steps — all of the code it hands over is gone), `DUPLICATION.md` (the review of
the `regs-v3` prototype, which is now a design reference rather than a running page) and
`optimize-web-bundle.md` (a scratch note about the deployed site's JS bundle).

Added 2026-09-23, when the prose parse format was cleaned out: `DESIGN-regs-to-sections.md` (the
long-form regs → sections design, written against the prose `Entry`), `14-waterbody-split-curation.md`
and its data `waterbody-splits.json` (the split-curation flow over prose rules; the JSON is kept as the
provenance of the curated splits), `SESSION-HANDOFF.md`, and `curation-review-BUILD-PLAN.md` (the
review app's original build plan, around a confirm/lock the catalogue does not have).

## Where the work is

```
splits ─▶ graph+registry ─▶ parse (HUMAN) ─▶ curation review ─▶ bundle ─▶ clients
  391          19,722            1,392          (no sign-off       ❌        ❌
 splits     49,639 sections     entries          state is kept)  (doc 10)  (doc 10)
```

⛔ **Parser runs spend credits and are human-only** — see the root `CLAUDE.md`.
