# 12 — Testing (everything up to matching)

One doc for the whole test story up to the (unbuilt) match step: the **approach**, the
**section & anchor catalogue** (what each test proves, visually), and the **status dashboard**.
Run: `.venv/bin/python -m pytest pipeline/tests -q` → **73 passed, 11 skipped**.

Goal: never be confused about *why* something happened. Every step is independently testable
against a small, inspectable input before it touches the 4.9 M-feature dataset.

## Approach — three tiers per step

1. **Synthetic unit tests** — build a tiny hand-made graph/chain in code and assert exact
   behavior. Fast, deterministic, no data dependency. This is where most logic is pinned.
2. **Small real-extract tests** — pull one or a few named watersheds from the gpkg (Adams,
   Fraser@Seabird, Chehalis/Harrison, Kootenay/Columbia, Similkameen, Bella Coola) and assert
   real-world outcomes. These are the "known hard cases" (they `@pytest.mark.skip` when the gpkg
   is absent).
3. **Full-build validation** — counts, coverage, and golden parity against the legacy pipeline
   on the whole dataset, run before cutover.

Each module has a matching `pipeline/tests/test_<module>.py`. Two modules
(`test_blk_chains`, `test_names`) still carry **stale stubs** that reference the old
`pipeline.sections` package — the modules themselves are implemented; the tests need
backfilling.

## Pipeline status (build order)

```
 fids ─▶ blk-chains ─▶ names ─▶ INVERTED GRAPH ─▶ BORDER ─▶ curated splits ─▶ tributaries ─▶ [ match ] ─▶ [ bundle ]
         (merge)      (tuples)   (lakes=nodes,     (out-of-  (sectionizer,     (guarded       regs→        section_id
                                  WSC filter,       BC split, location_id,      ancestors,     sections     tiles,
                                  2300 barrier)     flag)     +pickup,concern)  between, lake, + MU overlay) client
                                                              + area_boundary   EXCEPT algebra)
                                                              (in_areas flag)
   ✅ DONE       ✅ DONE     ✅ DONE      ✅ DONE        ✅ DONE   ✅ DONE          ✅ DONE        ⏳ NEXT      ⏳ FUTURE
                                                                          + name variations ✅ (compiler + graph apply)
```

Legend: ✅ implemented + tested · ◻ implemented, tests are stale stubs · ⏳ not built yet.

## The gating spikes (run FIRST, settled)

These decided whether the biggest simplifications were valid. Both guards now run **at
graph-build time** (WSC-descendant edge filter + `EDGE_TYPE=2300` `is_barrier` node), and both
regressions are **un-skipped**: `test_graph.py::test_chehalis_harrison_no_leak_via_wsc_filter`
and `::test_kootenay_columbia_no_leak_via_2300_barrier` (they build a small real extract and
skip only when the gpkg is absent). Empirical note: the WSC filter alone severs *both* named
cases (the Columbia/Kootenay canal drains into a different WSC branch); the 2300 barrier is
kept as independent defense for same-watershed canals, and a synthetic test pins it in
isolation.

| Spike | Question | Result | Finding |
|-------|----------|--------|---------|
| **S1 Braiding** | Can SCC condensation replace the watershed-code exclusion for the Chehalis/Harrison leak? | ❌ **NO** | FWA is already a DAG (0 non-trivial SCCs) → condensation is a **no-op**. Naive walk leaked 24/43 Harrison segs; the leak is confluence topology (mouth node's predecessor is the Harrison mainstem). **WSC-descendant filter** fixes it (145/145 Chehalis, 0 Harrison). **Keep excluded-WSC logic.** |
| **S2 Lake barrier** | Can lake-collapse replace the edge_type-2300 rule for Kootenay/Columbia? | ❌ **NO** | The canal **bypasses** Columbia Lake's polygon; the 6 `2300` segs (blk 356366076) sit under other waterbodies → lake-collapse catches 0/6, stops leak only by accident. Blocking `2300` segs → leak 86→0. **Keep the 2300 barrier.** |
| **S3 Geometry cut** | Is measure-`substring` safe? | ✅ mostly | 99.995% segment contiguity; measure-cut safe for ~99.3% of multi-segment BLKs. **0.69% are entirely 2-point** → need interpolation fallback. |
| **S4 Structural** | Is BLK a safe section atom? | ✅ **YES** | GNIS spanning >1 BLK = 0.03% (4/11,604). Lake-cut: 84.7% single section, 96.6% ≤3, max 180. |

**Outcome: keep BOTH the WSC-hierarchy filter and the 2300 barrier** — the spike disproved
the hope of deleting them. The simplification is now: BLK-atom sections + WSC-subtree
tributary filter (reframed as the drainage subtree, not a hack) + 2300 barrier + lake barrier
as a complement. The two regression tests assert the leak is stopped **by those rules**.

## Test files (73 tests: 62 pass, 11 skip)

| File | Area | Tests | Status | Proves |
|------|------|:----:|:------:|--------|
| `test_graph.py` | inverted graph + guards | 12 | ✅ pass | mainstem=one node; lake split; no wetland split; WSC-descendant edge filter; 2300 barrier; **real-data regressions** Chehalis/Harrison + Columbia/Kootenay (skip w/o gpkg) |
| `test_cutting.py` | geometry + ids | 7 | ✅ pass | substring cut (~cm); 2-point fallback; endpoint/section id stability |
| `test_anchors.py` | anchor resolution | 10 | ✅ pass | point/line/confluence(blk\|wsc)/lake/mu_boundary → `(blk, measure)`; **confluence WSC-descendant concern**; **MU multi-crossing → one split**; proximity gate; adjacency; real `splits.json` on Bella Coola (Hunlen Falls/Young/Burnt Bridge, skip w/o gpkg) |
| `test_splits.py` | splits.json schema | 4 | ✅ pass | load/validate; target/anchor rules; dupe-id fail-loud |
| `test_sectionizer.py` | curated split + roll-up | 7 | ✅ pass | split→section nodes + labels; `tributaries_between` (A–B); edge reattach by measure; anchor→sectionizer; **proximity pickup reuses/relabels a nearby boundary**; far split still cuts |
| `test_tributaries.py` | lakes + range + EXCEPT | 5 | ✅ pass | lake inlets/outlets; lake tributaries (drop mainstem); "X from lake A to C" range; closure through lakes; **"[Includes Tributaries] EXCEPT …" set algebra** (Atnarko/Bella Coola) |
| `test_border.py` | BC border | 2 | ✅ pass | cross-border BLK → `border` splits at both outline crossings; middle (US) piece `out_of_bc`, geometry kept, not a barrier |
| `test_area_boundary.py` | admin/park closure split | 9 | ✅ pass | WSC-descendant targeting; `area_boundary` cut at every crossing; through/ends-in/wholly-in pieces flagged `in_areas`; **"within {area}" identifier** (incl. ends-in-park); outside + out-of-scope-WSC negatives; determinism |
| `test_name_variants.py` | name application | 6 | ✅ pass | `display:true` beats gazette on a reach; searchable-only alias; multi-blk target; **wetland overlay** via member_wbks; shouty casing |
| `test_blk_chains.py` | blk merge | 4 | ◻ **stale stub** | module IS implemented (blk_chains.py); tests still reference old `pipeline.sections` — need real tests |
| `test_names.py` | name tuples | 3 | ◻ **stale stub** | module IS implemented (names.py: gazette+side_channel); tests are stubs — need real tests |
| `test_section_regs.py` | regs → sections | 4 | ⏳ future skip | the **match** step (not built): reg→section by name+location, MU overlay, tributary expansion |

## Section & anchor test catalogue (what each test proves, visually)

Every case below is synthetic (a hand-built `FidRow` graph, no gpkg) and fast. Mainstems lie on
the `y=0` axis so a route measure equals an x-coordinate. `┃` = a cut/boundary, `▲` = a tributary
mouth, `[Lake]` = a lake node. Convention: a stream **piece** node id is `"{blk}:{int(down_m)}"`;
a **lake** node is `"lake:{wbk}"`.

### `test_anchors.py` — one anchor kind → one resolved `(blk, measure)`

**point → project onto the line**
```
X:  0 ───────────────●(120,0)──────────────── 300      point coord (120,0)
                     ┃ measure 120
```
`test_point_anchor_projects_to_measure`: a `point` coord projects onto the target blue line →
measure 120. (A `proximity_m` gate rejects a coord that lands too far from the line.)

**line → intersection crossing**
```
                         │ cut line
X:  0 ───────────────────┼(210,0)──────────── 300
                         ┃ measure 210
```
`test_line_anchor_crosses_at_measure`: a cut `line` crossing the mainstem at x=210 → measure 210.

**confluence → the tributary's mouth on the parent**
```
X:  0 ───────────────────▲(200,0)──────────── 300
                         ┃ Y Creek joins here → measure 200
Y:                       │ (tributary, mouth at 200,0)
```
`test_confluence_anchor_by_blk_uses_tributary_mouth`: give the tributary Y by BLK → its mouth
projects onto X at 200; the boundary is labelled "Y Creek". Y's WSC `100-3` is a descendant of
X's `100` → **no concern**.
`test_confluence_anchor_by_wsc_picks_main_channel`: give Y by WSC instead → the main channel of
that WSC is chosen, same measure.

**confluence WSC self-validation**
```
X (wsc 100): 0 ─────────▲(200,0)──────── 300
Y (wsc 200-3): joins here ─ but 200-3 is NOT under 100  →  split kept, concern set
```
`test_confluence_wsc_mismatch_flags_concern`: the tributary's trimmed WSC must be a strict
descendant of the parent's; when it isn't the split still resolves but carries a `concern`. This
is what makes `tributary_wsc` safer than `tributary_blk` — the hierarchy is checkable.

**lake → the waterbody-run boundary (a no-op split, for matching)**
```
X:  0 ──────[Lake W: 100..200]────────────── 300
            ┃ run boundary → measure 100
```
`test_lake_anchor_resolves_to_run_boundary`: a `lake` anchor resolves to the lake-run boundary
(100). The lake already split the BLK in the graph build; this exists so a lake-anchored reg
resolves to the existing boundary.

**mu_boundary → shared edge of two ADJACENT MUs**
```
   MU 2-17            │ shared edge x=180        MU 3-15
X:  0 ────────────────┼(180,0)──────────────── 300
                      ┃ measure 180
```
`test_mu_boundary_anchor_splits_where_shared_edge_crosses`: two adjacent MU polygons share the
edge x=180; X crosses it → measure 180.
`test_mu_boundary_non_adjacent_makes_no_split`: MUs with a gap share no boundary → no split.

**mu_boundary → ONE split even when the river weaves across it**
```
   boundary x=180 (straight)          MU a │ MU b
X weaves:  0 ──▶(180)──▶ 200 ──▶(180)──▶ ◀──(180)──▶ 200        crosses 3×
              keep the MEDIAN crossing; concern = "crossed 3x"
```
`test_mu_boundary_collapses_multiple_crossings_to_one`: a river running along the region boundary
(the Fraser case) yields exactly one split — the median crossing — with a `concern` recording the
count. Real Fraser data (`2-18/3-14`) crosses once, so the collapse is a safeguard, not the norm.

Real-data regression `test_real_splits_json_resolves_on_bella_coola_extract` (skip w/o gpkg):
loads the authored `splits.json` against the real Bella Coola extract — `hunlen_falls` lands on
Hunlen Creek (blk 360862431) at m≈1685; `young_hwy20` on Young Creek (blk 360862631); the
`burnt_bridge_at_sitkatapa` confluence on Burnt Bridge (blk 360883785) with the WSC-descendant
check passing and the authored Sitkatapa `concern` preserved.

### `test_sectionizer.py` — splits become graph nodes, labelled, tributaries reattach

Fixture: mainstem X over 0..300, tributaries **L**@50, **M**@150, **U**@280; two curated splits
**A**@120 and **B**@250.
```
        L▲            M▲                    U▲
X:  0 ───┃50──A┃120──────┃150───────B┃250─────┃280──── 300
        └ X:0 ─┘└─ X:120 (A..B) ─┘└─── X:250 ───┘
```
- `test_split_creates_section_nodes_and_labels`: X → pieces `X:0` / `X:120` / `X:250`, all still
  named "X River", qualifiers `downstream of A` / `between A and B` / `upstream of B`.
- `test_tributaries_between_excludes_upstream_mainstem`: **"tributaries of X between A and B"** =
  `tributaries_between(X:120)` = `{M}` only — NOT U or the upper mainstem (the continuation edge
  above B is subtracted).
- `test_tributary_edge_reattaches_by_measure`: after splitting at A only, L (@50) stays on the
  lower piece; M (@150) and U (@280) move to the upper piece — edges reattach by confluence measure.
- `test_anchor_resolves_point_to_measure` / `..._proximity_gate_skips_far_blk`: end-to-end anchor
  resolution feeding the sectionizer, incl. the proximity gate skipping far side-channels.

**proximity pickup — reuse a nearby boundary instead of cutting a duplicate**
```
first split A at 120 ─┐        curated "Idaho border" resolves at 123 (≤ proximity 10)
X:  0 ────────────────┃120────────── 300
                      ▲ pickup: relabel the 120 boundary "Idaho border", NO new node
```
- `test_proximity_pickup_reuses_nearby_boundary`: a curated split within `proximity_m` of the
  existing 120 boundary reuses + **relabels** it; the node set is unchanged; the applied record has
  `picked_up=True`. This is how Kootenay's "Idaho border" / "Koocanusa Reservoir" points snap onto
  the auto border/lake splits.
- `test_proximity_pickup_far_split_still_cuts`: a split outside `proximity_m` cuts normally
  (`picked_up=False`) — no false pickups.

### `test_tributaries.py` — lakes as nodes, the range reach, and EXCEPT

Fixture: mainstem X threads three lakes **A**[50,100], **B**[150,200], **C**[250,300]; a side
creek dips into each lake's interior. Flow runs source(300) → mouth(0).
```
   mouth 0 ── X:0 ──[Lake A]── X:100 ──[Lake B]── X:200 ──[Lake C] ── 300 source
                       ▲SA          ▲SB              ▲SC
```
- `test_lake_inlets_and_outlets`: `lake:B` inlets = `{X:200 (upstream mainstem), SB}`, outlet =
  `{X:100}` — straight off graph adjacency.
- `test_lake_tributaries_drop_the_mainstem`: **"tributaries of Lake B"** = `{SB}` only — the
  through-mainstem inflow `X:200` and its river system are dropped.
- `test_range_reach_from_lake_A_to_lake_C_spans_lake_B`: **"X River from Lake A to Lake C"** =
  `sections_in_reach(X, 100, 250)` = `{X:100, X:200}` — spanning Lake B.
- `test_full_closure_reaches_through_lakes`: the guarded closure of the mouth piece reaches every
  lake + side creek + upper piece (lakes are non-barrier nodes the walk passes through).

**"[Includes Tributaries] EXCEPT …" — set difference over split pieces**
```
   ocean ── Bella Coola ─────────────────────────────  headwaters
              ▲Ordinary  ▲Young┊Hwy20  ▲BurntBridge┊Sitkatapa  ▲Atnarko─┊─Hunlen┊Hunlen Falls
   base   = tribs(Bella Coola) ∪ tribs(Atnarko)          (both rivers + everything upstream)
   except = HU:20 ∪ BB:40 ∪ YO:40      (each "X upstream of Y" = the upper piece the split made)
   result = base − except              (keeps Ordinary Cr. + the reaches BELOW each ┊)
```
`test_includes_tributaries_except_upstream_of_splits`: builds the real Atnarko/Bella Coola shape,
applies the three splits, resolves each excepted upper piece via `piece_above(blk, label)`, and
asserts `reach_except` drops exactly `{HU:20, BB:40, YO:40}` while keeping both mainstems, the
un-split Ordinary creek, and each creek's below-split reach.

### `test_border.py` — cross-border BLK split + `out_of_bc` flag

Fixture: a box "BC" outline `(0,0)-(100,100)` and a BLK that loops out and back (the Kootenay
shape), leaving at x=100 and returning at x=100.
```
   BC outline │x=100
   X: (50,50)─┼─▶(150,50)          m=50  exit  ┐
              │      │(150,10)                 │  X:50 = US loop → out_of_bc, geometry KEPT
      (50,10)◀┼──────┘             m=190 re-entry ┘
```
- `test_border_split_points_finds_both_crossings`: the BLK ∩ outline yields two `border` splits at
  m=50 and m=190, labelled "BC boundary".
- `test_out_of_bc_piece_flagged_kept_and_not_barrier`: the **middle** piece `X:50` is
  `out_of_bc=True` (geometry present for dotted display, **not** a flow barrier); the two BC-side
  ends are `out_of_bc=False`.

### `test_area_boundary.py` — admin/park closure split + `in_areas` flag

Same cut machinery as `test_border.py`, but the INSIDE pieces are flagged (a closure marker) and
get the **"within {area}"** identifier. Fixture: a box "park" outline `(0,0)-(100,100)` (WSC
`100-025956`, "Pitt River" trunk) with streams that pass through / end inside / sit inside / sit
outside, plus one inside stream on a **different** WSC (out of scope).
```
   park = box(0,0,100,100)
   TH (through):    (50,-50) ▶ (50,50) ▶ (50,150)   enters m=50, exits m=150 → middle inside
   EN (ends-in):    (30,-30) ▶ (30,30)              enters m=30; headwaters INSIDE
   IN (wholly in):  (70,20)  ▶ (70,80)              never crosses; whole piece inside
   OUT (wholly out):(150,20) ▶ (150,80)             in WSC scope, but entirely outside
   OOS (out-scope): (40,20)  ▶ (40,80)              inside geometry, but a DIFFERENT WSC
```
- `test_wsc_descendants_prefix_match`: `wsc_descendants=True` targets every Pitt-WSC descendant
  (`TH,EN,IN,OUT`) by WSC prefix; the exact-WSC mode targets none (regression guard).
- `test_area_split_points_at_every_crossing`: cuts at every outline crossing — `TH` at `[50,150]`,
  `EN` at `[30]`, none for wholly-in `IN` / wholly-out `OUT`, and nothing for out-of-scope `OOS`.
- `test_through_stream_yields_three_pieces_middle_inside`: `TH` → `TH:0` / `TH:50` / `TH:150`; only
  the middle `TH:50` has `in_areas=("Garibaldi Park",)`; geometry KEPT on every piece.
- `test_identifiers_through_stream`: `TH:0` = "downstream of Garibaldi Park", `TH:50` = "within
  Garibaldi Park", `TH:150` = "upstream of Garibaldi Park".
- `test_identifier_ends_in_park_says_within_not_upstream`: **the hard case** — the boundary→
  headwaters piece `EN:30` inside the park reads **"within"**, not the misleading "upstream of"
  the bounds alone would give.
- `test_wholly_inside_tributary_flagged_uncut`: `IN` is never cut (`IN:0` only), flagged `in_areas`
  and reads "within Garibaldi Park".
- `test_outside_tributary_untouched`: `OUT` is in WSC scope but outside the geometry → no flag, no
  identifier.
- `test_out_of_scope_wsc_not_flagged_even_if_inside`: `OOS` sits inside the box but on the wrong
  WSC → ignored (scope is WSC-gated, not geometry-gated).
- `test_deterministic_across_two_builds`: node set + `in_areas` identical across two builds.

### `test_graph.py` — the graph + guards (see also 03)

Mainstem-is-one-node, lake split, no-split-on-wetland, the WSC-descendant edge filter, the 2300
barrier (`ancestors(guarded=)`), plus the two real-data regressions (Chehalis/Harrison,
Columbia/Kootenay) that skip when the gpkg is absent.

## Per-module coverage — the modules without a visual above

- **blk-chains** (`test_blk_chains.py`, stub): fid ordering + contiguity; `up_m-down_m == length`;
  under-lake runs from wbk (not edge_type); sentinel-BLK skip.
- **names** (`test_names.py`, stub): NameSource priority; Seabird→Fraser side-channel inheritance;
  upstream-inherited for unnamed headwaters; override wins.
- **cutting** (`test_cutting.py`): substring ~cm; 2-point fallback; section_id stable under an
  unrelated split, changes on an in-section split.
- **splits** (`test_splits.py`): every anchor → `(blk, measure)`; resolved sidecar determinism;
  dupe-id fail-loud.
- **match/section_regs** (`test_section_regs.py`, future skip): Similkameen one section across MU;
  Fraser curated `mu_boundary` split → one reg set each; base regs are MU overlay; ported
  `effective_includes_tributaries`.

## Golden parity (before cutover)

- Capture legacy outputs as fixtures: current `tier0.json` + a sample of fid→reg assignments for
  the known-hard rivers.
- Assert the v2 pipeline reproduces the *regulation assignments* (modulo intended improvements:
  real Adams split, corrected Chehalis/Kootenay). Differences must be explainable.
- Compare aggregate counts: #sections vs #named streams; #regs assigned; total tributary reach
  length per reg (should be ≈ legacy, minus the corrected leaks).

## Full-build sanity gates

- Section coverage == input BLK coverage (no dropped geometry).
- No orphan under-lake segments; every lake wbk → exactly one node.
- All `location_identifier`s unique within their gnis.
- Tile byte budget respected (tippecanoe `--maximum-tile-bytes`), minzoom sane.
- Client smoke: load webapp against `v2/`; click Adams (two sections), Fraser@Seabird, a
  `tributary_only` lake; confirm regs + tributaries + labels.

## Test data hygiene

- Prefer synthetic graphs for logic; use small real extracts only for the named cases.
- Keep a fixtures module that pulls the named watersheds by GNIS/WSC once and caches, so
  real-extract tests stay fast.
- Note: `pytest` must be installed in the run env (the repo's base conda interpreter lacks it).

## What's solid right now

The **whole geometry + graph + naming chain** is built and tested: merge → names → inverted graph
(lakes as nodes, both flow guards baked in) → **border split** (`out_of_bc` pieces) → **area
closure split** (`in_areas`, "within {area}") → curated sections (structured bounds, fid-free
reach splits, **proximity pickup**, **`concern`**) → guarded tributary reachability (reach-scoped
`between`, lake in/out, and **"[Includes Tributaries] EXCEPT …"** set algebra) → unified name
variations (compiled file + graph application with the display flag and wetland overlay). Anchors:
point/line/lake/**confluence (WSC-validated)**/**mu_boundary (single split)**/**area_boundary**.
Validated end-to-end on real extracts (Adams, Chehalis/Harrison, Columbia/Kootenay,
Penticton/Two Forty-One, Bella Coola: Hunlen Falls / Young / Burnt Bridge). The debug **gpkg**
carries `split_points`, `full_name`/`out_of_bc`/`in_areas` on sections, and an `obstacles` layer
(FISS fish-passage points) when fetched.

## What's next

> **Stale as of 2026-08.** Items 1 and 2 are done: matching, LLM parsing, the registry and the
> curation-review app all exist, and the suite is 275 passing / 8 skipped rather than 62/11. Item 3
> (bundle / tiles / client) is the live gap — see `10-plan.md`.

1. **Match step** (`test_section_regs` un-skips): resolve regulations → section ids using
   `name_tuples` (search) + `location_identifier`/structured bounds (range); MU overlay for base
   regs; tributary expansion via `tributaries_between`/`tributary_node_ids`. See `16-phase5-match-plan`.
2. **Backfill real tests** for `test_blk_chains` + `test_names` (drop the stale stubs).
3. **Bundle / tiles / client** on the `section_id` spine (docs `09`).
4. Follow-up flagged in docs `05`: node named wetland/other-layer waterbodies a reg references.
