# T710 — Parts list: a side channel is listed next to the main-stem part it runs beside

Status: OPEN.

Source: owner 2026-11-13, boundary-name review (pack: ~/bcfish-session-backup-2026-11-12/boundary-names/PACK.md, Skeena Reserve). Split accepted; owner chose "List it where it sits". Today side channels are numbered after all main-stem parts (the closed Reserve side channel is part 18).
## Work
1. COUNT FIRST, change nothing: how many waters and parts the reorder touches (waters with at least one side-channel part, and parts whose number moves). Report the numbers to the owner before any change.
2. Then order parts so a side channel sits beside the main-stem part it runs beside (list reads in order going upstream). Check that part numbers are not stored anywhere that breaks (marks, anchors, deep links, tests).
3. Mention to the owner (undecided): the side channel's name "Side channel (from the Exchamsiks River confluence up to Skeena River Ecological Reserve boundary)" reads backwards next to its neighbours.
## Acceptance
Count reported to the owner and acknowledged; after the change the Skeena list reads in upstream order with the Reserve side channel beside its main-stem part; no open/closed answer changes (binding diff clean); tests pin the order.
Depends: none. Size: M. Owner: yes (sees the count; side-channel name wording). Default: build the ordering after the count; leave the name as is. Priority: open for main to place (changes what the app shows).

## Count (Work 1, 2026-11-13, pipeline lane; live export r-adc4e248a163; nothing changed)

Which parts are "after all main-stem parts": a part made only of side-channel runs gets a km (and so its place in river
order) from its branches' `km_from`, the main-stem measure where it leaves the river; 16 such parts already sit by that
km (Burman, Bedwell, Bella Coola, …). The ones listed LAST are side-channel parts whose branch runs carry no `km_from`:
**111 such parts on 110 waters** that also have a main stem. Placed beside the main-stem part they run beside (the one
whose main run shares the most end names with theirs, else the first one they touch; 3 have neither and stay last),
**101 of the 110 are already in place** (the side channel touches the water's last main-stem part).

**9 waters reorder; 35 part numbers move** (Skeena alone 15):
- Skeena River (gnis:2936): Reserve side channel 18 → after part 3 (Exchamsiks → Reserve boundary); parts 4–17 shift by one.
- San Juan River (gnis:21201): side part 4 → after part 0; 1–3 shift.
- Forsyth Creek (gnis:11815): side part 3 → after part 0; 1–2 shift.
- Kitsumkalum River (gnis:3942): side part 3 → after part 0; 1–2 shift.
- Old Kootenay River Channel (gnis:2123): side parts 1 and 2 swap.
- Also Mahood Creek (12114), Sustut River (25386), Snehumption Creek (27390), Wood River (8372): side part 2 ↔ part 1.

Where the order is decided (once): `pipeline/deliver/bundle/derived.py::part_rows` — "RIVER ORDER: by km from the mouth
(`part_km`), then the parts with no km (a lake, side channels only)" (`with_km` sorted, then the km-less appended);
`_runs_of` gives each run's km. `pipeline/deliver/bundle/topology.py::order_parts` re-ranks the same way and
`part_geo` stops the build if it disagrees ("part_ix is not river order"); `pipeline/tools/audit/water_map_export.py`
uses `order_parts` too. Method (read-only): scratch script over `codec.expand` of the export, `parts[].runs` /
`touches`.
