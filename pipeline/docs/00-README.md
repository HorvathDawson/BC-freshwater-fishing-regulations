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
| **[NEXT](NEXT.md)** | **Parked work with the thinking done — PMTiles, tributary walk, open bugs** |
| **[REACH-BUILDER](REACH-BUILDER.md)** | **Structure sketch of the next thing to build** |
| [RESOLVER-HANDOFF](RESOLVER-HANDOFF.md) | Everything found about `resolve_extent`, for whoever is in it |
| [12-testing](12-testing.md) | Test strategy, spikes S1–S4, sanity gates |
| [14-waterbody-split-curation](14-waterbody-split-curation.md) | Curating splits |
| [DESIGN-regs-to-sections](DESIGN-regs-to-sections.md) | Long-form design of regs → sections |

Numbering has gaps because superseded docs moved to `archive/`; the remaining numbers are unchanged
so existing links still resolve.

## Working files, not design docs

`waterbody-splits.json` · `waterbody-splits.md` · `waterbody-splits-regs.md` ·
`curation-review-queue.md` — read by `pipeline/hack/` scripts.
`SESSION-HANDOFF.md` — referenced by `curation-review/README.md`.

## `archive/`

Superseded designs and one-off data, kept for provenance: the v1 pipeline map (`02`), early
architecture (`07`), pre-registry data structures (`08`), the delivered implementation plan (`11`),
and the six docs `10-plan` replaces. Nothing reads these.

## Where the work is

```
splits ─▶ graph+registry ─▶ parse (HUMAN) ─▶ curation review ─▶ bundle ─▶ clients
  391          19,722            1,392           108 of 1,392      ❌        ❌
 splits     49,639 sections     entries         confirmed        (doc 10)  (doc 10)
```

⛔ **Parser runs spend credits and are human-only** — see the root `CLAUDE.md`.
