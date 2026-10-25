# T191 — Curation review: a one-row top bar, and a proposed extent fix in the mark

Status: DONE (031eeb0c). Owner to try it in the pilot (T136).

Source: the owner during the T136 pilot on the T189 screen: the filter bar is too tall; "we have no way to change
extents? like choose op and 1 or 2 splits. if something is wrong we cant fix it". Correction: the ops are whole,
upstream, downstream, between (the catalogue's own names, shown in those words) — not "from"/"to".

## Built

1. **One-row top bar** (`FilterBar.tsx`): region, review, the pilot toggle and a slim progress (`r6 3/120 · all
   …`) plus a 2 px progress line stay visible; status, order, tier, refresh, the per-tier progress and the
   "output" commands fold under **filters ▾** (the button counts the filters set inside it; open/closed is
   remembered per browser). The detail pane gains the height.
2. **Fix extent…** in every rule block and in "This entry" (not on a zone entry: it is marked rule by rule).
   - **op**: every op the catalogue's extent model has (`entry_models.Op`), by its own name, shown in the
     owner's words (`marks.OP_POINTS` / `OP_WORDS`): whole (0 points), upstream (1), downstream (1),
     between (2), within an area (an area instead of points), the rest of the water, steelhead waters (0).
   - **points**, each from: a split on this map (the splits the rules name + the items' curated boundaries);
     any curated split by name (`GET /api/splits/search`); or a NEW point by anchor type (T164): confluence of
     a named tributary, a lake's inlet, a lake's outlet (`GET /api/items/search`), or a map click (lon/lat).
   - **on**: which items (the entry's waters, ticked; any other water by search); none = the entry's waters.
   - **tributaries**: no call / include / exclude.
   - **preview on map** (`GET /api/entries/{id}/extent-preview?fix=`, a READ): the builder's own
     `resolve_extent` + `build_reach` on an in-memory copy of the entry, drawn magenta; a rule fix shows what
     the rule would bind, an entry fix the stretch it would scope to. A NEW point cannot be previewed until it
     is a split — the UI says so ("resolved once T185 builds it"); so do `rest` / `steelhead_waters`.
3. **In the mark**: `Mark.extent_fix: list[ExtentFix]`, `ExtentFix {scope: "entry" | rule_id, op, points:
   [FixPoint {kind: split | confluence | lake_inlet | lake_outlet | coord, split_id?, name?, item_id?,
   coord?}], items, tributaries: bool | null, area?}` (`pipeline/regs/review/marks.py`). Validated: the op is
   the catalogue's, the point count is the op's, each point is anchored (a map point inside BC), one fix per
   scope, a rule mark fixes only its rule, and only on a wrong or unsure mark (the ones that become tickets;
   refused in the reviewer's words otherwise). A mark without a fix writes the line it always did.
4. **In tickets** (`pipeline.regs.review.tickets`): `- **Extent fix:** `[…json…]`` (read back by
   `parse_ticket`, so T185 builds the proposed edit from it) and a "## Proposed extent" section in words.

HARD RULE kept (T185, AGENTS 66/67): the new routes are GETs; the one write is still `POST /api/marks`, and
`test_curation_review_safety` / `test_curation_review_screen` (which now calls the search and preview routes with
real arguments while proving nothing under data/curated changed) stay green.

## Checks
`test_review_marks.py` (ops = the catalogue's, round trip, refusals, rule-mark scope, ticket round trip),
`test_curation_review_screen.py` (search, preview, NEW-point and 422 answers), the curation-review and review
suites (293 passed), frontend `tsc -b`. Screenshots: Iskut River (r6) at 900 px tall, bar folded and the editor
open with a preview.
