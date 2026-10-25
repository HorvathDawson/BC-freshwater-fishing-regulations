# T179 — One cut-placement pass: auto cuts merge into names, real cuts stay apart

Status: PARTIAL (code deef0cec; build + binding diff pending). Owner 2026-10-24: "would it be better to revise how this is done? So that splits can merge with lake or
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

## Outcome — PARTIAL (2026-10-24; code deef0cec, build pending)

Built as designed, in `pipeline/atlas/splits/sectionizer.py` (`_plan_blk` collect → place → cut once;
`place_points` is the plan, idempotent under `split_graph_at`):
- Authorship = `SplitPoint.source` / new `SectionBoundary.source`, set where each family is minted;
  `_auto_split`/`_pickup` and the 500 m sentinel are gone (`minted_id` stays for readers holding only an
  id: review app, bundle).
- Authored: takes a lake edge / natural end / minted boundary within 5 m (SLIVER_M); never merged with
  another authored cut by distance unless one spot (<1 m, AT_CUT_M); 1–5 m apart both are cut and the pair
  goes to `split_conflicts.json` (the sliver gate then stops the build — 2026-10-06 ruling).
- `proximity_m` is projection-only. The 8 curated pickup radii became `"at_edge": true` (reach 300 m to a
  lake edge, end or minted boundary — never another authored cut); their `proximity_m` values stay as
  projection tolerances (unchanged projection).
- Auto: gauge joins the nearest name within 400 m on EITHER side (old: one-sided, order-dependent, read
  off the gauge's 400 m projection value); area/border/length join only the same point; no auto takes a
  bare end. Naming authored > lake > border > area > gauge > length; every other id an alias.
- build.py: curated + gauge + curated-area points placed together before the braid sweep; the sweep skips
  minted points that joined a place; braid pass `exact=True`; length cuts placed too. POLICY_VERSION 34.

Unit tests (`test_sectionizer.py`): Sproat gauge 0.5 m keeps lake name + alias; Spuzzum MU on confluence
keeps both; two curated 20 m apart stay two; 2.5 m apart cut + reported; 40-shuffle order test
(mutation-checked: unsorted authored loop fails it); plan == cut; two-pass order within 5 m.

Predicted binding diff (from the live `splits.resolved.json`; lake edges need the graph):
- 7 gauges below a curated cut now join it (08MH024 CPR bridge Mission 20 m, 08MH076 Kanaka 21 m,
  08MH035 Pitt 43 m, 08HD007 Salmon/Kay Ck 80 m, 08EC001 Fort Babine 88 m, 08MH001 Vedder +200 m,
  08NK030 Elko dam 232 m) and 08GC006/007 (97 m) share one boundary: those slivers vanish, section ids on
  those blks shift, rules ending there gain/lose the sliver, those stations' section_gauge changes.
- Area cuts standing on a gauge boundary now name it (area canonical, gauge alias): end tokens only.
- The 8 `at_edge` waters: unchanged if each radius took a lake edge / end; any that took another
  curated cut now cut separately — check Waneta (Pend d'Oreille) and Corra Linn first.
- split_conflicts.json expected `[]` (current authored pairs are all < 0.03 m).

Left: atlas rebuild, reach run, rule AND gauge binding diff reviewed (subtract an old-vs-old diff: the
atlas is not deterministic); every moved binding explained before promote.
