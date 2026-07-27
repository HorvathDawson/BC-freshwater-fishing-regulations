# 12 — Section & Anchor Test Catalogue (what each test proves, visually)

Every test is synthetic (a hand-built `FidRow` graph, no gpkg) and fast. Mainstems lie on the
`y=0` axis so a route measure equals an x-coordinate. `┃` = a cut/boundary, `▲` = a tributary
mouth, `[Lake]` = a lake node. Run: `.venv/bin/python -m pytest stream_sections/tests -q`.

Convention: a stream **piece** node id is `"{blk}:{int(down_m)}"`; a **lake** node is
`"lake:{wbk}"`.

---

## `test_anchors.py` — one anchor kind → one resolved `(blk, measure)`

### point → project onto the line
```
X:  0 ───────────────●(120,0)──────────────── 300      point coord (120,0)
                     ┃ measure 120
```
`test_point_anchor_projects_to_measure`: a `point` coord projects onto the target blue line →
measure 120. (A `proximity_m` gate rejects a coord that lands too far from the line.)

### line → intersection crossing
```
                         │ cut line
X:  0 ───────────────────┼(210,0)──────────── 300
                         ┃ measure 210
```
`test_line_anchor_crosses_at_measure`: a cut `line` crossing the mainstem at x=210 → measure 210.

### confluence → the tributary's mouth on the parent
```
X:  0 ───────────────────▲(200,0)──────────── 300
                         ┃ Y Creek joins here → measure 200
Y:                       │ (tributary, mouth at 200,0)
```
`test_confluence_anchor_by_blk_uses_tributary_mouth`: give the tributary Y by BLK → its mouth
projects onto X at 200; the boundary is labelled "Y Creek".
`test_confluence_anchor_by_wsc_picks_main_channel`: give Y by WSC instead → the main channel of
that WSC is chosen, same measure.

### lake → the waterbody-run boundary (a no-op split, for matching)
```
X:  0 ──────[Lake W: 100..200]────────────── 300
            ┃ run boundary → measure 100
```
`test_lake_anchor_resolves_to_run_boundary`: a `lake` anchor resolves to the lake-run boundary
(100). NOTE the lake already split the BLK in the graph build; this exists so a lake-anchored
reg resolves to the existing boundary.

### mu_boundary → shared edge of two ADJACENT MUs
```
   MU 2-17            │ shared edge x=180        MU 3-15
X:  0 ────────────────┼(180,0)──────────────── 300
                      ┃ measure 180
```
`test_mu_boundary_anchor_splits_where_shared_edge_crosses`: two adjacent MU polygons share the
edge x=180; X crosses it → measure 180.
`test_mu_boundary_non_adjacent_makes_no_split`: MUs with a gap between them share no boundary →
no split (adjacency is enforced by construction — a non-shared boundary is empty).

---

## `test_sectionizer.py` — splits become graph nodes, labelled, tributaries reattach

Fixture: mainstem X over 0..300, tributaries **L**@50 (below), **M**@150 (mid), **U**@280
(above); two curated splits **A**@120 and **B**@250.
```
        L▲            M▲                    U▲
X:  0 ───┃50──A┃120──────┃150───────B┃250─────┃280──── 300
        └ X:0 ─┘└─ X:120 (A..B) ─┘└─── X:250 ───┘
```
- `test_split_creates_section_nodes_and_labels`: X → pieces `X:0` / `X:120` / `X:250`, all still
  named "X River", with qualifiers `downstream of A` / `between A and B` / `upstream of B`.
- `test_tributaries_between_excludes_upstream_mainstem`: **"tributaries of X between A and B"** =
  `tributaries_between(X:120)` = `{M}` only — NOT U or the upper mainstem (the continuation edge
  above B is subtracted). The full closure of `X:0` is the whole upstream network.
- `test_tributary_edge_reattaches_by_measure`: after splitting at A only, L (@50) stays on the
  lower piece; M (@150) and U (@280) move to the upper piece — edges reattach by confluence
  measure.
- `test_anchor_resolves_point_to_measure` / `..._proximity_gate_skips_far_blk`: end-to-end anchor
  resolution feeding the sectionizer, incl. the proximity gate skipping far side-channels.

---

## `test_tributaries.py` — lakes as nodes, and the range reach

Fixture: mainstem X threads three lakes **A**[50,100], **B**[150,200], **C**[250,300]; a side
creek dips into each lake's interior node. Flow runs source(300) → mouth(0).
```
   mouth 0 ── X:0 ──[Lake A]── X:100 ──[Lake B]── X:200 ──[Lake C] ── 300 source
                       ▲SA          ▲SB              ▲SC
```
- `test_lake_inlets_and_outlets`: `lake:B` inlets = `{X:200 (upstream mainstem), SB}`, outlet =
  `{X:100 (downstream mainstem)}` — straight off graph adjacency.
- `test_lake_tributaries_drop_the_mainstem`: **"tributaries of Lake B"** = `{SB}` only — the
  through-mainstem inflow `X:200` and its river system are dropped.
- `test_range_reach_from_lake_A_to_lake_C_spans_lake_B`: **"X River from Lake A to Lake C"** =
  `sections_in_reach(X, 100, 250)` = `{X:100, X:200}` — spanning Lake B — with identifiers
  `between Lake A and Lake B` / `between Lake B and Lake C`.
- `test_full_closure_reaches_through_lakes`: the guarded closure of the mouth piece reaches every
  lake + side creek + upper piece (lakes are non-barrier nodes the walk passes through).

---

## `test_graph.py` — the graph + guards (see also 03/10)

Mainstem-is-one-node, lake split, no-split-on-wetland, the WSC-descendant edge filter, the 2300
barrier (`ancestors(guarded=)`), plus the two real-data regressions (Chehalis/Harrison,
Columbia/Kootenay) that skip when the gpkg is absent.
