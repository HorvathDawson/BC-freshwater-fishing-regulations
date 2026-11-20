# T590 — Cut pickers offer this water's cuts first, and the nearest existing cut

Status: PARTIAL — af76fda95, 2026-11-10. Done: 1 (search: this water's cuts first; test) and 2 for entry B and the place editor's words (`dfo.check_anchor` answers `nearest`; within 200 m it says to use it; entry B has the one-click "Use it"). left (Claude): a one-click "use it" in the PLACE editor (needs its draft state), and 3 (stored suggestions in dfo_fix_proposals.json / suggest.py proposed AS the existing cut when within 200 m). Not looked at in a browser.

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
