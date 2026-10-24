# T179 — One cut-placement pass: auto cuts merge into names, real cuts stay apart

Status: OPEN. Owner 2026-10-24: "would it be better to revise how this is done? So that splits can merge with lake or
gauge or other auto generated splits, but also handle real splits close together? This just feels fragile."

## Why (what is fragile today)
`pipeline/atlas/splits/sectionizer.py` cuts one split at a time and decides "same place?" as each arrives
(`_pickup`), so the result depends on ORDER and on a radius:
- One field, two meanings: `proximity_m` is the projection tolerance (default 500 m) AND the pickup radius; a value of
  exactly 500 is read as "inherited, not authored" (`_DEFAULT_PROJECTION_M` sentinel, sectionizer.py:111). Curated
  splits set 30/60/80/100/300 m by hand where two cuts share a small area.
- Authorship is read off the id spelling (`_auto_split`: `gauge__…`, `area:…`, `border:…`, `length:…`) — a list that
  has already been wrong once (the code says it "belongs on SplitPoint").
- A pickup RELABELS the boundary it reuses; names were lost that way (Sproat Lake / `gauge__08HB008`, Spuzzum / the
  R2/3 MU line) and are patched by carrying aliases; 25 boundaries once showed a station code because the gauge ran
  second. Known gaps left on purpose: an auto split may still relabel an INTERIOR lake edge.

## Design (proposal)
Two phases per blue line, order-independent:
1. **Collect** every candidate position first — curated point/line/confluence/MU cuts, lake edges, area, border,
   gauge, length — each with its measure, `authored: bool` (set where each family is minted, never parsed from the
   id) and its id/label.
2. **Place**: cluster positions along the line.
   - An AUTO position within the merge distance of any other position joins it: one boundary, the authored name
     wins, every other id is an alias (the gauge's id stays bindable). Auto never relabels a name.
   - Two AUTHORED positions are never merged by distance — they are both real, however close; a piece between them
     shorter than SLIVER_M is reported as a curation conflict (fix the source), not silently collapsed.
   - An authored cut meeting a lake edge / natural end within the merge distance takes that place (today's Stamp and
     Bella Coola cases).
   Then cut once. `proximity_m` keeps only its projection meaning; per-split pickup radii and the 500 sentinel go.

## Acceptance
- Unit tests: gauge 0.5 m from a lake edge (Sproat) keeps the lake's name + gauge alias; MU line on a confluence
  (Spuzzum) keeps both names; two curated cuts 20 m apart stay two cuts; the same inputs in any order give identical
  boundaries (shuffle test).
- Atlas rebuild + `binding_diff` reviewed (rule AND gauge bindings, memory: split fix changes the trib walk); every
  moved binding explained or fixed before promoting. The curated `proximity_m` pickup values are removed or shown
  unneeded.

Depends: after T61 (2.8 normaliser moves bindings too — not in the same rebuild). Size: L (heavy: atlas rebuild).
Owner: review the binding diff.
