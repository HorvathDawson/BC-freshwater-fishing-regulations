# T311 — Provincial parks missing (Garibaldi closes the Pitt)

Status: DONE (style; ships with the next app build) — owner 2026-10-31: "the parks like Garibaldi park and other provincial parks didn't show up. Garibaldi is the
important one at least because it closes the Pitt river". Scope narrowed by the owner: "The rules are correct there just no park polygon" — drawing only; no legend entry (the legend is for regulation items only).

## Work
Investigate: is Garibaldi Provincial Park in the atlas park/area layer and the tiles (drawn?), and do the Synopsis rules for
waters in Garibaldi Park (e.g. the upper Pitt closure) bind to the sections inside the park? Find why parks don't show
(layer filtered, wrong source, zoom, style) and why/if the Pitt park closure doesn't bind. Fix: parks drawn (outline +
name, theme tokens, legend), park-scoped rules bind via the park polygon (walk-then-clip, base+deltas). Binding diff
before promote; list every rule that changes.

## Acceptance
Garibaldi drawn and named; upper Pitt inside the park shows the park closure on map + regs pane; binding diff explained.

Depends: none. Size: M. Owner: no.

## Result (2026-10-31)
**Cause: drawn, but invisible.** All 782 provincial parks (Garibaldi included) are in `areas.json` `parks_bc` → tile layer
`park` (in `atlas.pmtiles`, z6–13; tile 9/81/173 carries "Garibaldi Park") → style layer `park` (always on). It was painted
`color.park.fill` at `opacity.park` 0.12 with NO outline by design, and runtime-style removes the basemap's own park fills, so
nothing showed. Not a source, export, filter or zoom fault.

**Fix (style only, no tiles rebuild, no binding change):** `park` gets a solid green edge from z7 (`color.park.edge`, new
`width.park.edge` ramp 0.6→2.0 px, new `opacity.park.edge`); `opacity.park` 0.12→0.32 (light/cvd), 0.14→0.30 (dark); light/cvd
edge darkened #7d9c5c→#5F8A3C. The "NO OUTLINE" comment is rewritten citing this ticket. Label unchanged (z8+, ahead of basemap
labels). No legend row (owner). `tools/label-shots.mjs` gained `garibaldi` / `upper-pitt` places (not in its default set).

**Binding:** unchanged. `r2:pitt_river@2-8` r1 "No Fishing within Garibaldi Park" (printed p.25) binds via
`within_area: area:park:garibaldi_park` + split `pitt_river__garibaldi_pitt`; the owner confirmed the rules are correct.

Shots: `T311-shots/` — garibaldi + upper-pitt at z8/z11, light/dark/cvd.
