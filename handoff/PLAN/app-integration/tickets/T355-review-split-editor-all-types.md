# T355 — Review app split editor makes every split type, and proposal ids become real split ids

Status: OPEN (owner 2026-11-02).

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
