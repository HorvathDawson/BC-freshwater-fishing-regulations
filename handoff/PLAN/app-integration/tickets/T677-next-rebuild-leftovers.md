# T677 — Next rebuild: what the side build left

Status: OPEN

Source: the rebuild lane, 2026-11-11 (`handoff/PLAN/app-integration/next-rebuild-results.md`).

## Work
1. **An item's name follows its piece count.** `registry.build._primary_name` takes the most common display name
   among the item's sections (ties: the longest). Dropping one shore cut renamed `wsc:100-011602` from "Power House
   Creek" to "Byrne Slough" in the first side build (2 + 2 pieces became 1 + 2). Any cut added or removed can flip a
   name. Decide the rule (by length, or by the lowest line) and measure how many names move before changing it.
2. **A park rule gained 33 m when a shore cut went away.** `strathcona_park_waters.r1` now covers the first 33 m of
   the Ucona River (354153463): membership of a provincial park is by overlap of the whole piece. Owner item 5 in the
   results file; if the answer is "keep it out", the fix is a cut or clean membership for that park, not the shore rule.
3. **The binding diff is by id.** `reach.binding_diff` cannot see a rule that kept `B:0` while `B:0` grew from a
   crossing piece to the whole creek. The by-place diff used for the side build
   (`measure_geo.py` in `data/generated/side/next-rebuild/tools.tar`: what each rule covers on each changed blue line, in
   metres) belongs in `binding_diff` (and in the release check).
4. **The 32 new held lines are not on a map page.** `T665-main-rivers-map.html` shows 61; add Cohilukthan and Deas
   sloughs, Kainet Creek and the 29 of `shore.HELD_NOT_ON_THE_OWNERS_MAP`, each with what a tick brings (31 creeks
   follow the main rivers; 24 braid cases at their polygon exits).
5. **A line that ends at the sea with nothing mapped below it** (the Brem River) is never tidal by the rule, though
   its last 102 m lie inside the polygon. Count such lines and ask the owner once.
6. The shore rule into `pipeline/docs/RULINGS.md` and `pipeline/docs/MAP.md` (T665's leftover).
7. `lake:329461947`: fold it into the Oxbows or not (owner item 6).

## Acceptance
- Each item done, or closed with the owner's answer.

Depends: T664, T665. Size: M.

## Note (2026-11-11): Fraser delta lines with no section
29 Fraser delta lines that touch the tidal polygon have no section, because the braid-loop prune dropped them (16 at
least 95% inside the polygon). Line 356337576 is still to look at. The Ewen Slough and 356242813 stubs are settled by
the 25 m rule in T678.
