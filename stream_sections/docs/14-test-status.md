# 14 — Test Status & "Where We Are" Dashboard

One place to see what's built, what's tested, and what's next. Detailed per-test visuals
(ASCII diagrams of each anchor/section/tributary case) live in `12-section-tests.md`; this doc
is the **status map**. Run: `.venv/bin/python -m pytest stream_sections/tests -q`
→ **46 passed, 11 skipped**.

## Pipeline status (build order)

```
 fids ─▶ blk-chains ─▶ names ─▶ INVERTED GRAPH ─▶ curated splits ─▶ tributaries ─▶ [ match ] ─▶ [ bundle/tiles ]
         (merge)      (tuples)   (lakes=nodes,      (sectionizer,     (guarded       regs→        section_id
                                  WSC filter,        location_id,      ancestors,     sections     tiles, client
                                  2300 barrier)      bounds)           between, lake) + MU overlay)
   ✅ DONE       ✅ DONE     ✅ DONE      ✅ DONE            ✅ DONE           ✅ DONE        ⏳ NEXT        ⏳ FUTURE
                                                                          + name variations ✅ (compiler + graph apply)
```

Legend: ✅ implemented + tested · ◻ implemented, tests are stale stubs · ⏳ not built yet.

## Test files (57 tests: 46 pass, 11 skip)

| File | Area | Tests | Status | Proves |
|------|------|:----:|:------:|--------|
| `test_graph.py` | inverted graph + guards | 12 | ✅ pass | mainstem=one node; lake split; no wetland split; WSC-descendant edge filter; 2300 barrier; **real-data regressions** Chehalis/Harrison + Columbia/Kootenay (skip w/o gpkg) |
| `test_cutting.py` | geometry + ids | 7 | ✅ pass | substring cut (~cm); 2-point fallback; endpoint/section id stability |
| `test_anchors.py` | anchor resolution | 8 | ✅ pass | point/line/confluence(blk\|wsc)/lake/mu_boundary → `(blk, measure)`; proximity gate; adjacency; real `splits.json` on Atnarko (skip w/o gpkg) |
| `test_splits.py` | splits.json schema | 4 | ✅ pass | load/validate; target/anchor rules; dupe-id fail-loud |
| `test_sectionizer.py` | curated split + roll-up | 5 | ✅ pass | split→section nodes + labels; `tributaries_between` (A–B); edge reattach by measure; anchor→sectionizer |
| `test_tributaries.py` | lakes + range | 4 | ✅ pass | lake inlets/outlets; lake tributaries (drop mainstem); "X from lake A to C" range; closure through lakes |
| `test_name_variants.py` | name application | 6 | ✅ pass | `display:true` beats gazette on a reach; searchable-only alias; multi-blk target; **wetland overlay** via member_wbks; shouty casing |
| `test_blk_chains.py` | blk merge | 4 | ◻ **stale stub** | module IS implemented (blk_chains.py); tests still reference old `pipeline.sections` — need real tests |
| `test_names.py` | name tuples | 3 | ◻ **stale stub** | module IS implemented (names.py: gazette+side_channel); tests are stubs — need real tests |
| `test_section_regs.py` | regs → sections | 4 | ⏳ future skip | the **match** step (not built): reg→section by name+location, MU overlay, tributary expansion |

## What's solid right now
The **whole geometry + graph + naming chain** is built and tested: merge → names → inverted graph
(lakes as nodes, both flow guards baked in) → curated sections (structured bounds, fid-free reach
splits) → guarded tributary reachability (incl. reach-scoped `between` and lake in/out) → unified
name variations (compiled file + graph application with the display flag and wetland overlay).
Validated end-to-end on real extracts (Adams, Chehalis/Harrison, Columbia/Kootenay, Penticton/Two
Forty-One, Atnarko).

## What's next
1. **Match step** (`test_section_regs` un-skips): resolve regulations → section ids using
   `name_tuples` (search) + `location_identifier`/structured bounds (range); MU overlay for base
   regs; tributary expansion via `tributaries_between`/`tributary_node_ids`.
2. **Backfill real tests** for `test_blk_chains` + `test_names` (drop the stale stubs).
3. **Bundle / tiles / client** on the `section_id` spine (docs 06).
4. Follow-up flagged in docs/13: node named wetland/other-layer waterbodies a reg references.
