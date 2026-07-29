# 14 — Test Status & "Where We Are" Dashboard

One place to see what's built, what's tested, and what's next. Detailed per-test visuals
(ASCII diagrams of each anchor/section/tributary case) live in `12-section-tests.md`; this doc
is the **status map**. Run: `.venv/bin/python -m pytest stream_sections/tests -q`
→ **53 passed, 11 skipped**.

## Pipeline status (build order)

```
 fids ─▶ blk-chains ─▶ names ─▶ INVERTED GRAPH ─▶ BORDER ─▶ curated splits ─▶ tributaries ─▶ [ match ] ─▶ [ bundle ]
         (merge)      (tuples)   (lakes=nodes,     (out-of-  (sectionizer,     (guarded       regs→        section_id
                                  WSC filter,       BC split, location_id,      ancestors,     sections     tiles,
                                  2300 barrier)     flag)     +pickup,concern)  between, lake, + MU overlay) client
                                                                               EXCEPT algebra)
   ✅ DONE       ✅ DONE     ✅ DONE      ✅ DONE        ✅ DONE   ✅ DONE          ✅ DONE        ⏳ NEXT      ⏳ FUTURE
                                                                          + name variations ✅ (compiler + graph apply)
```

Legend: ✅ implemented + tested · ◻ implemented, tests are stale stubs · ⏳ not built yet.

## Test files (64 tests: 53 pass, 11 skip)

| File | Area | Tests | Status | Proves |
|------|------|:----:|:------:|--------|
| `test_graph.py` | inverted graph + guards | 12 | ✅ pass | mainstem=one node; lake split; no wetland split; WSC-descendant edge filter; 2300 barrier; **real-data regressions** Chehalis/Harrison + Columbia/Kootenay (skip w/o gpkg) |
| `test_cutting.py` | geometry + ids | 7 | ✅ pass | substring cut (~cm); 2-point fallback; endpoint/section id stability |
| `test_anchors.py` | anchor resolution | 10 | ✅ pass | point/line/confluence(blk\|wsc)/lake/mu_boundary → `(blk, measure)`; **confluence WSC-descendant concern**; **MU multi-crossing → one split**; proximity gate; adjacency; real `splits.json` on Bella Coola (Hunlen Falls/Young/Burnt Bridge, skip w/o gpkg) |
| `test_splits.py` | splits.json schema | 4 | ✅ pass | load/validate; target/anchor rules; dupe-id fail-loud |
| `test_sectionizer.py` | curated split + roll-up | 7 | ✅ pass | split→section nodes + labels; `tributaries_between` (A–B); edge reattach by measure; anchor→sectionizer; **proximity pickup reuses/relabels a nearby boundary**; far split still cuts |
| `test_tributaries.py` | lakes + range + EXCEPT | 5 | ✅ pass | lake inlets/outlets; lake tributaries (drop mainstem); "X from lake A to C" range; closure through lakes; **"[Includes Tributaries] EXCEPT …" set algebra** (Atnarko/Bella Coola) |
| `test_border.py` | BC border | 2 | ✅ pass | cross-border BLK → `border` splits at both outline crossings; middle (US) piece `out_of_bc`, geometry kept, not a barrier |
| `test_name_variants.py` | name application | 6 | ✅ pass | `display:true` beats gazette on a reach; searchable-only alias; multi-blk target; **wetland overlay** via member_wbks; shouty casing |
| `test_blk_chains.py` | blk merge | 4 | ◻ **stale stub** | module IS implemented (blk_chains.py); tests still reference old `pipeline.sections` — need real tests |
| `test_names.py` | name tuples | 3 | ◻ **stale stub** | module IS implemented (names.py: gazette+side_channel); tests are stubs — need real tests |
| `test_section_regs.py` | regs → sections | 4 | ⏳ future skip | the **match** step (not built): reg→section by name+location, MU overlay, tributary expansion |

## What's solid right now
The **whole geometry + graph + naming chain** is built and tested: merge → names → inverted graph
(lakes as nodes, both flow guards baked in) → **border split** (`out_of_bc` pieces) → curated
sections (structured bounds, fid-free reach splits, **proximity pickup**, **`concern`**) → guarded
tributary reachability (reach-scoped `between`, lake in/out, and **"[Includes Tributaries]
EXCEPT …"** set algebra) → unified name variations (compiled file + graph application with the
display flag and wetland overlay). Anchors: point/line/lake/**confluence (WSC-validated)**/
**mu_boundary (single split)**. Validated end-to-end on real extracts (Adams, Chehalis/Harrison,
Columbia/Kootenay, Penticton/Two Forty-One, Bella Coola: Hunlen Falls / Young / Burnt Bridge).
The debug **gpkg** now also carries `split_points`, `full_name`/`out_of_bc` on sections, and an
`obstacles` layer (FISS fish-passage points) when fetched.

## What's next
1. **Match step** (`test_section_regs` un-skips): resolve regulations → section ids using
   `name_tuples` (search) + `location_identifier`/structured bounds (range); MU overlay for base
   regs; tributary expansion via `tributaries_between`/`tributary_node_ids`.
2. **Backfill real tests** for `test_blk_chains` + `test_names` (drop the stale stubs).
3. **Bundle / tiles / client** on the `section_id` spine (docs 06).
4. Follow-up flagged in docs/13: node named wetland/other-layer waterbodies a reg references.
