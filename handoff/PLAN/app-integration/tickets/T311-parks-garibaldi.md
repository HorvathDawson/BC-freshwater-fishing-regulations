# T311 — Provincial parks missing (Garibaldi closes the Pitt)

Status: OPEN (owner 2026-10-31: "the parks like Garibaldi park and other provincial parks didn't show up. Garibaldi is the
important one at least because it closes the Pitt river").

## Work
Investigate: is Garibaldi Provincial Park in the atlas park/area layer and the tiles (drawn?), and do the Synopsis rules for
waters in Garibaldi Park (e.g. the upper Pitt closure) bind to the sections inside the park? Find why parks don't show
(layer filtered, wrong source, zoom, style) and why/if the Pitt park closure doesn't bind. Fix: parks drawn (outline +
name, theme tokens, legend), park-scoped rules bind via the park polygon (walk-then-clip, base+deltas). Binding diff
before promote; list every rule that changes.

## Acceptance
Garibaldi drawn and named; upper Pitt inside the park shows the park closure on map + regs pane; binding diff explained.

Depends: none. Size: M. Owner: no.
