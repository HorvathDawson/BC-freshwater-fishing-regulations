# T677 — Next rebuild: what the side build left

Status: PARTIAL — items 4–7 closed (by T678/T685, RULINGS E21; below); item 2 answered by the owner 2026-11-12 (keep as built: the Strathcona Park rule covers the first 33 m of the Ucona); left (Claude): item 1 (measure the name rule), item 3 (by-place binding diff)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T677 | Next rebuild leftovers: item names follow piece count, a park rule gained 33 m, binding diff by place, the 32 new held lines on a map page, lines ending at the sea | PARTIAL — left (Claude): items 1, 3 | — | items 4–7 closed by T678/T685 (RULINGS E21); item 2 answered 2026-11-12 (keep the Ucona 33 m as built) |

Source: the rebuild lane, 2026-11-11 (the side-build results, git history: next-rebuild-results.md).

Done: item 2 kept as built (owner 2026-11-12: `strathcona_park_waters.r1` covers the first 33 m of the Ucona River
(354153463)); item 4 (the 32 new held lines on `T665-main-rivers-map.html`) superseded by the T678 coast rule and its owner list (`coast_owner.json`, `shore_held.json`); item 5
settled by RULINGS E21 ("a water reaching the sea is fresh to the shoreline"); item 6 RULINGS E21 (+ E20–E25) and
`pipeline/docs/MAP.md` (T678/T685); item 7 lake:329461947 joined the Okanagan River Oxbows (T678).

## Work (left)
1. **An item's name follows its piece count.** `registry.build._primary_name` takes the most common display name
   among the item's sections (ties: the longest). Dropping one shore cut renamed `wsc:100-011602` from "Power House
   Creek" to "Byrne Slough" in the first side build (2 + 2 pieces became 1 + 2). Any cut added or removed can flip a
   name. Decide the rule (by length, or by the lowest line) and measure how many names move before changing it.
3. **The binding diff is by id.** `reach.binding_diff` cannot see a rule that kept `B:0` while `B:0` grew from a
   crossing piece to the whole creek. The by-place diff used for the side build
   (`measure_geo.py` in `data/generated/side/next-rebuild/tools.tar`: what each rule covers on each changed blue line, in
   metres) belongs in `binding_diff` (and in the release check). M, not small: a by-place diff needs both atlases'
   section spans in the release check (the live atlas's pre-promote copy is not always kept). Its own lane.

Note (2026-11-11): 29 Fraser delta lines that touch the tidal polygon have no section, because the braid-loop prune
dropped them (16 at least 95% inside the polygon). Line 356337576 is still to look at.

## Acceptance
- Each item done, or closed with the owner's answer.

Depends: T664, T665. Size: M.

## Still open (from the deleted next-rebuild-results.md, 2026-11-25)
The results page (2026-11-11 side build) is in git history; its owner items are mostly settled by T678's coast rule. Left:
- Brem River 360854323 (102 m) and three unnamed creeks into it (360743108, 360827624, 360883856), and 360802896: not cut; the Brem's line ends at the sea with no water mapped below it.
- Four creeks newly cut into Fraser side channels (354135384 252 m, 356070273 6 m, Crescent Slough 356326465 27 m, 356349946 195 m): the owner says if any is held.
- Standing: the `mouth`-rows patch (`T665-tidal-limits-mouth-rows.patch`, never applied), the Nakina half-width (a mark cannot say "centre line to the northern bank").
