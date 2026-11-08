# T517 — One part per connected run of same-rule sections, always in sync with the paint

Status: OPEN — paused 2026-11-08 for the owner's fresh audit of part naming / sectioning (coordinator); WIP in
`git stash` "T517 WIP: contiguous parts…" (derived.part_rows + schema UNIQUE). In-season work paused separately (notice
audit). Supersedes T509's "What is left" (`short` on `part_stretch`, dropped). Part of T495 gate 1.

Owner 2026-11-08: "Yes one part per connected adjacent stretch. And we need to make sure it will always be synced to the
painted colors. They can never diverge." Also: salmon paint must match the salmon rules the same way; data size roughly
flat; old-release fallbacks may go (in-season part paused).

## Measured (live bundle `data/generated/bundle/bundle.sqlite`, release r-6dce77a24e6a)
- **Dry run of the split** (`derived.part_rows` cutting each rule-set group by `topology.stretches`, numbered by km from
  the mouth): parts 20,708 → 20,933 (+225, +1.1 %); 160 waters gain parts; 472 waters change which part an index names
  (river order renumbers every multi-part water whose old order was not river order). `part_stretch` today: 869 rows on
  182 waters; after, only T228 salmon pieces would remain. The Fraser goes 17 → 20 parts, tidal piece first.
- Fixture tests with the WIP (derived, part_geo, part_line, release, slices, regs_store, …): 228 pass. One live-bundle
  test fails with or without it: `test_part_geo::test_the_fraser_reaches_the_sea…` (the Fraser's exit is now Morey
  Channel `gnis:14905`, a T497 effect on the live bundle, not this change).
- **In-season spill, live staging** (links name r-6dce77a24e6a, so phones read the per-section `places`): 0 of 32
  notices land on a piece they do not cover. 9 notice/part pairs sit on a part of several pieces; every piece has
  covered sections. Fallback (mark walk, phones on another release): no spill on today's data either. 5 of the 14 partial
  extents resolve and match a piece-level walk; 9 have labels that are not part marks, so they show whole-water "partial".
  Ends mid-part: 5 cases, all `some` (Fraser part 2 stretch 0 25/28 sections; Pitt parts 0 and 2; Kaslo; Snehumption).
  The spill is a hazard in how the code is built, not a bug users see today.
- **Salmon:** `salmon_section` has 287 chains over 1.95 M sections. `salmon_edge_gap` (a chain change with no named split)
  has 1 row, so T228 pieces with mixed chains are the only salmon-paint divergence path today.

## Not started
Place key, `short_label` per run, app side (waterGroup / mergeNav / partName, SEO rulesKey, picker grouping,
targetFor, problem reports), contract bump, paint-sync build check + mutation test, golden-tap declarations, side build,
size before/after, shots. All wait for the audited design.
