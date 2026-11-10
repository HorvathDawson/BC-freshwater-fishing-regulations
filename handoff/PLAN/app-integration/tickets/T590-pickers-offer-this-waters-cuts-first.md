# T590 — Cut pickers offer this water's cuts first, and the nearest existing cut

Status: OPEN

Source: T532 leftover d; owner 2026-11-10 on T573: "if one is near one that already existed use the already
existing one … applies to the pickers' proposals in general".

## Today
The extent editor's "any curated split…" search ranks by text, so a wording that names another water (a tributary's
confluence) lists that water's cuts above the place's own. The NEAREST-EXISTING rule is built for entry B only
(`dfo.nearest_cut`, `NEAR_M` = 200 m along the line: shown on every new end, offered in the editor after a map click).

## Work
1. Search: this place's waters first, then the rest (server side, `searchSplits` with the place's items).
2. The same nearest-existing line and "use it" button in the place editor's NEW-cut flow (`NewSplits` in
   `DfoReview.tsx`) and on the suggestions (`dfo_fix_proposals.json`): a proposed new cut within 200 m of an existing
   one is proposed AS the existing one, the distance said.
3. Tests for both; the 200 m lives in one place.

Depends: —. Size: M. Owner: no.
