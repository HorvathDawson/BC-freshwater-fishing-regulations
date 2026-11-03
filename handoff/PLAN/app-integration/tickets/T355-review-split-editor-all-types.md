# T355 — Review app split editor makes every split type, and proposal ids become real split ids

Status: DONE 2026-11-02 (Claude) — proposals only; owner to try it in the DFO tab.

The new-split feature in the review app should produce every kind of split that splits.json supports, not just a
hand-placed point. splits.json today holds: point 318, confluence 102, lake 18, area_boundary 4, lake_outlet 1,
border 1.

1. **One "new split" panel with a type picker.** Each type fills the matching SplitAnchor fields
   (pipeline/common/models/splits.py):
   - **point:** click on the line; it snaps to the blk and measure.
   - **confluence:** click a tributary mouth, or pick the tributary from a list of the water's tributaries.
   - **lake / lake_outlet:** pick the lake on the map; the inlet or outlet comes from the graph.
   - **area_boundary:** pick a polygon from a layer (parks_bc, parks_nat, admin). The name fills in from the layer's
     field (e.g. PROTECTED_LANDS_NAME). Options: side (upstream/downstream), wsc_descendants, exclude_gnis.
   - **border:** the provincial or national border crossing.
   - **offset:** the split's offset attribute on any type, +/- N m along the river. It already works for points;
     extend it to every type.
2. **Live preview** of where the cut lands, before saving, using the build's own resolver. No geometry logic in
   TypeScript (see memory cascade-resolved-in-python).
3. **Edit a proposed split in place:** change its type, anchor or offset.
4. **Real ids.** A proposal's local id (e.g. "n1-2") becomes a real splits.json id when it lands. apply_proposals
   already assigns a unique slug id (water__label, with _2, _3 … on a clash) and remaps the extent to that id.
   - Verify that every path (DFO, entries, tidal) remaps every reference, so no "n…" id is ever written to a curated
     file.
   - Add a guard test.

Found 2026-11-02: the Little Qualicum area_boundary save was refused because the proposal view dropped the area name.
Fixed in 59e75d36b. Add a test so every anchor type survives the proposal → save round trip.

Size: M.

## Done (2026-11-02)

- **Panel** (DfoReview `NewSplits`): one type picker — point (bridge / dam / sign / map point), confluence
  (click the mouth or pick from the water's tributaries), lake (outlet / inlet / both ends: click the lake or
  search), area boundary (click a polygon of parks_bc / parks_nat / osm_admin_boundaries; name from the layer's
  field; end, WSC subtree, leave-uncut gnis), border (optional click picks the crossing). `NewSplit` gained
  `lake`, `border`, `area_layer`, `area_field`, `wsc_descendants`, `exclude_gnis`; `split_record` writes each
  into the SplitAnchor fields the build reads.
- **Offsets**: point, confluence, lake (all edges), existing split. Area boundary and border REFUSE one (the
  build's `SplitAnchor.from_dict` cannot offset them) — the model refuses, the panel says so.
- **Live preview**: `GET /api/dfo/locations/{id}/split-preview` → `curation-review/backend/split_preview.py`:
  `split_record` → `SplitDef.from_dict` → `resolve_split_defs` on the build's own chains (`load_stream_fids`
  with a new `where` filter + `build_blk_chains`), area polygon by `get_area_polys`, border by
  `border_split_points`. Orange on the map (cuts, offset reference, area edge). `check_anchor` uses it for
  lake / border / area too. Not modelled: added lakes/streams, placement.
- **Edit in place**: "edit" on a cut in this mark or on any already-proposed cut. The edit rides in the mark
  as `NewSplit.amends` (the app's one write stays a mark); `proposal_groups` replaces that cut keeping its id;
  an earlier override's set no longer applies to an edited cut.
- **Real ids**: verified DFO / entries / tidal all land slugs; guard `proposals._only_real_split_ids` in
  `materialize` refuses any local ref (no `__` and no `:`) in an extent, a new split id or a tidal flag.
- Tests: `pipeline/tests/test_review_split_types.py` (round trip per type, offsets, edit, guard + mutation),
  `pipeline/tests/test_curation_review_split_preview.py` (resolver vs the served atlas: Moyie border 42640.55,
  Little Qualicum park ends, offsets).
- Follow-up (owner): the area name, lake and tributary fields are autocomplete dropdowns (150 ms, ↑↓ Enter, click):
  area names from `GET …/area-names` (`split_preview.area_names`: contains-match, prefix first, then nearest the
  water, cached per layer, the layer's own spelling); lakes from the registry search; tributaries filter the
  water's loaded list. Picking runs the preview.
- Follow-up (owner): P is set to n (labelled "placement wrong — fix proposed") when a new split / new point is
  added; P y with one is held in the screen; the refused-saves list offers "retry with P ?" / "retry with P n…".
- Follow-up (owner): REAL IDS from the start. `proposals.place_groups` / `assign_ids` give every proposed cut its
  splits.json slug (`<water>__<name>`, `_2`… on a clash with splits.json or another pending cut, clashes numbered
  by anchor digest, never order); the build lands, the review app shows, new marks store, the preview returns the
  same id. Old marks' local refs are re-keyed at read (`dfo.rekeyed`), never edited — 1,179 proposals (8 DFO,
  1,171 tidal). `amends` names the edited cut by its anchor, not an id. The materialize guard stays.
