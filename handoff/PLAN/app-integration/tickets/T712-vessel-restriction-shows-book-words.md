# T712 — Vessel restriction section shows the book's words with the location text

Status: CLOSED (owner 2026-11-13 by dialog: the two vessel entries in build 30 are fine; nothing to change). owner: OWNER-TODO "t712-harrison-vessel-words"

Source: owner 2026-11-13, Harrison River harrison_river.r4: "in the vessel restriuction section it should say the words with the location text and a user can infer it". Book: "Speed restriction or no powered boats in small bays along the river as signed"; the app shows a bare "No powered boats".
## Work
1. Show the book's wording including its location text in the vessel restriction section, not a bare "No powered boats".
2. Survey other waters with the same shape (a vessel rule whose book text carries a location/qualifier that the structured form drops); list them here and fix them the same way. Not yet surveyed.
## Acceptance
Harrison River r4 shows the full book phrase; survey count recorded; every same-shape water shows its location text; no open/closed change.
Depends: none. Size: S-M. Owner: no. Default: quote the book phrase verbatim. Priority: open for main to place (changes what the app shows).

## Finding (2026-11-13, pipeline lane)

Already built: c63427bbd + 2841fb605 (owner 2026-11-12 "Keep them as two entries", RULINGS P51), both in build 30. The
live export holds `harrison_river.r3` / `r4` with `in_part: "in small bays along the river as signed"`; the gear frame
lists them in `in_part` and the app's vessel section draws one chip per kind with the book's locator
(`boats.ts` `partChips`): "No powered boats — in small bays along the river as signed" and "Speed restriction — in
small bays along the river as signed" (pinned: `pipeline/tests/test_vessel_in_part_t209.py`, `boats.test.ts`). The
"r4 No powered boats" the owner read was the boundary-name pack's own summary line, not the app.

Survey (catalogue, every `vessel_rule`): 23 carry an `undrawn_part` and all get the locator chip (same mechanism);
Pitt `pitt_river.r2` "at Grant Narrows" joins them with T708 (874bbd00a, next release). Rules whose sentence names a
place the structured form drops, outside that mechanism: 13; 10 are drawn stretches (`between` / `upstream_of` /
`downstream_of`: the chip shows only on that stretch, the label names it), Weaver Lake r2 binds the lake by item, and
Alouette River r3 "on mainstem" is the river without tributaries. The one of a different shape is McArthur Island
Slough r1 (whole slough, place words refused beside `whole` by the catalogue model): listed for the owner with T708.
No change made. Owner: is two chips with the locator enough, or should the chip be the one printed line?
