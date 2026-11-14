# T710 — Parts list: a side channel is listed next to the main-stem part it runs beside

Status: DONE (shipped in r-a9914d6b25b7 on staging: 023c61c9d, 523d2e45c, e25736f4b).

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

## Built (023c61c9d, 2026-11-13; owner decided "List it where it sits")
`topology.order_parts` is the one ranking (bundle `part_rows`, the `part_geo` check, the answers picker, `water_map_export`):
a side-channel part with no km goes right after the main-stem part it runs beside (`side_host`: most shared end names,
else the first part it touches; a tie to the part nearer the mouth; several beside one host keep part order; none: last).
Tests: `test_part_geo.py` (Skeena shape, tie, touch fallback, no host, a side part with a km keeps its place).

**Measured: 8 waters, 33 part numbers** (the count's 9th, Old Kootenay River Channel's "swap", was the count script
inserting each side part directly after its host, which reversed two side parts beside one host; built, they keep part
order). Before → after (old part number at each new position), same on the live bundle and the candidate r-c874b18502c9:
- Skeena (gnis:2936): Reserve side channel 18 → 4 (after part 3); old 4–17 → 5–18.
- San Juan (gnis:21201): side 4 → 1 (Deering Road bridge side channel, after the estuary part 0); old 1–3 → 2–4.
- Forsyth Creek (gnis:11815): side 3 → 1; old 1–2 → 2–3.
- Kitsumkalum (gnis:3942): side 3 → 1; old 1–2 → 2–3.
- Mahood Creek (12114), Sustut River (25386), Snehumption Creek (27390), Wood River (8372): side 2 → 1, old 1 → 2.
No binding change (part order is bundle-only).

Part numbers stored elsewhere (checked): marks.jsonl has no part field (no rename); links carry a FWA place, never a part
(T369; `ix=` is the browser tools' key, re-cut per release); app tests name Skeena parts by their ends (`sectionNames`).
Follows at the release: the goldens re-cut, and the Skeena declarations in `app/tools/regs-recut/taps.py` that name a
store part index >= 4 move +1 (x19p1 4→5, x19p5 5→6, x19p6 7→8, x19p7 8→9) — left for the release's re-cut step (it
stops and names them; not editable before the candidate is LATEST).
Not done: the side channel's name wording (Work 3, owner, undecided).
