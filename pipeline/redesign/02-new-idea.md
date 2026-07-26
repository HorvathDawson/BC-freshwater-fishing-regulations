# 02 — The New Idea (section model), cleaned up

## Raw idea (verbatim, preserved)

> Combine all stream segments for all streams. Will need to split on the lakes too keeping
> stream under lake separate from not. The combined streams should be a much smaller graph
> like what is currently there. When one stream goes into another with multiple nodes they
> are collapsed in the graph relationship for tributaries. Apply name overrides before
> combining (Seabird Island).
>
> Identify all nodes on the stream (confluences of some kind).
>
> Split on predefined boundaries for different rivers hand created (Section river A between
> x and y). A section is identified by gnis name / location identifier (null if no splits) /
> lake wbk.
>
> These will be further split by zone (or zone regs handled completely separate on the UI?
> Needs thought).
>
> After divisions each segment gets all nodes that exist on the segment, usable with the
> tree to find all upstream items.
>
> Split regulations into simple and complex matching.
>
> Goal: keep the info of the massive stream network while dropping the massive stream
> network. Faster to process, less complicated. Define every transformation.
>
> Alt: use OSM stream network to avoid the combine problem?

## Restated as a model

### The atom: a **Section**

A section is a contiguous run of one named stream (one blue line / gnis identity) between
two boundaries. Boundaries are one of:
- the stream's outlet (mouth) or headwaters (natural ends),
- a **lake** the stream passes through (splits under-lake from open-channel),
- a **confluence** (a major tributary joining, if we choose to split there),
- a **hand-defined split point** (see `04-section-split-design.md`).

**Section identity** = `(gnis_id/name, location_identifier, lake_wbk)`:
- `gnis_id` — the named-stream identity (or an override/inherited name for unnamed runs).
- `location_identifier` — `null` when the stream has no splits; otherwise a generated
  descriptor like `"upstream of Adams Lake"` / `"downstream of Adams Lake"` /
  `"between X and Y"`. This is derived from the section's bounding split points.
- `lake_wbk` — set when the section IS an under-lake run (or a lake body itself); null for
  open channel.

A section **contains** an ordered set of FWA `linear_feature_id`s (the micro-segments that
compose it). The micro-segments still exist in the atlas/tiles for geometry; the section is
the logical grouping laid on top.

### The structure: a **contracted directed tree**

Instead of the 2.37 GB micro-segment igraph, build a small directed graph where:
- **nodes** = confluences / split points / lake in-out points,
- **edges** = sections.

When stream A flows into stream B through many micro-segments, that collapses to a single
"A → B" tributary relationship. Upstream lookup for tributaries becomes a walk over this
small tree (find all sections upstream of a section), replacing the per-reg BFS over the
full graph.

### The transformation order (target)

1. **Load FWA streams** + apply **name overrides before combining** (so Seabird Island etc.
   carry the right name into the grouping).
2. **Combine** micro-segments into named-stream chains; **split on lakes** (under-lake vs
   open channel kept separate).
3. **Identify confluence nodes** along each stream.
4. **Apply hand-defined splits** → cut chains into sections; each section gets its
   `location_identifier` auto-generated from its bounds.
5. **(Decision) zone handling** — either split sections by zone OR keep zone as an attribute
   and resolve on the UI. See `03-gap-analysis.md` §Zones — **recommendation: do NOT split
   geometry by zone.**
6. **Attach nodes to sections** — each section knows the confluence nodes on it, so the
   contracted tree can answer "all upstream sections."
7. **Split regulations into simple vs complex matching** — simple = resolvable directly to a
   section by name/wbk; complex = needs hand overrides / spatial / multi-boundary logic.

### What we keep vs drop

- **Keep:** the *information* of the full network — connectivity (as the contracted tree),
  names (propagated), watershed codes, stream order/magnitude (aggregated per section),
  under-lake distinction, tributary reachability.
- **Drop from the runtime path:** the 2.37 GB per-micro-segment graph as the thing we BFS
  over per regulation. (It is still needed once at build time to *derive* the contracted
  tree and section membership — see feasibility doc.)

## On the OSM alternative — short answer: no

OSM `waterway=river/stream` ways come pre-named and pre-combined, which is tempting. But
OSM lacks: FWA `fwa_watershed_code` (drives mainstem/parent logic), `blue_line_key`,
`waterbody_key` (the lake join key regulations match on), stream order/magnitude (drives
minzoom), and consistent flow directionality/connectivity. Regulation matching is built on
gnis/wbk/wsc identifiers that OSM does not carry. Adopting OSM would trade the "combine
problem" (solvable, one-time, build-time) for the loss of every join key the matcher
depends on. **Stay on FWA as the spine; solve combining ourselves.**
