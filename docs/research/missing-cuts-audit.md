# Missing cuts and unmatched lakes: what is still open

Conclusions only, from the read-only audit of the synopsis rules against release r-adc4e248a163 (1,393 entries, 3,043
rules) and the unmatched-lake research. Open ticket T708 holds the owner decisions; what it fixed is built and released
(Pitt r2 label, Ruby r1 `between`, Gagnon cut, Deena and Mamin onto bridges, the Kootenay dolphin, Hidden, Secret and
Square matched, the McArthur, Lakelse and Tlell wording).

## Findings that stand

- **Haida Gwaii has no missing cut.** All 8 rows are in the data and every printed boundary has a cut the build uses. The
  Haida Gwaii tidal boundary stays at the mouth for Copper, Deena and Mamin (the book writes no boundary); on the Mamin
  FWA's tidal crossing is about 2.1 km up. The Dean River ("to tidal boundary", Region 5) is the same kind of case.
- **Noted on the water, never drawn: 85.** 80 are lake closure areas or lake parts (Mahood, Shuswap, Pitt Lake, Sakinaw,
  Lake Revelstoke, Upper Arrow drawdown, "as buoyed and signed" swim areas), the known deferred class. 3 are on streams
  (Harrison River r3 and r4 "in small bays as signed", Columbia River r12 "in wetlands"): no boundary to cut. 2 are
  carve-out notes held as notes (`kinbasket_lake_tributaries.r2`, `kootenay_lake_tributaries.r2`).
- **30 "on part(s)" vessel rules** (Region 1 17, Region 2 9, Region 5 2, Region 8 2) have no place in the book, are
  extent `whole`, and colour the whole water, where AGENTS.md lists "on parts" as `undrawn_part`. No cut is possible; the
  owner decides whether they become `undrawn_part`.
- **Guessed cuts:** 34 cuts carry a confidence mark in the bundle (`split.confidence`: 12 low, 22 medium). Deena and Mamin
  are `low` (no second or third bridge was mapped; they sit on the nearest crossings). McRae Creek falls at 2.33 km
  against "approximately 4 km upstream of Christina Lake" (Region 8, p.70), closed with no change.
- **Left as recorded, not built:** the Kootenay dolphin cut is an alias of the standing lake boundary
  (`sectionizer._coincident`), so the part end name stays "Kootenay Lake — Main Body"; Gagnon's cut label
  ("Its mouth (placed 200 m above the unnamed lake)") fails `test_part_names_unnamed_lake` and wants a name such as
  "fishing boundary signs (200 m above the lake below Williston)" (another atlas build); Lakelse (Region 6, p.52,
  "mouth designated by signs 1.7 km downstream of the logging road bridge") has no cut beyond the power-line one;
  Kootenay River lower r1 ends at the lake edge, 0.67 km into the 2 km the book names (the rest is in the deferred lake
  class).
- **Method that found them:** flag rules whose wording has a place word (upstream, downstream, between, from to, bridge,
  signs, outlet, mouth, within N m, at Name) but whose extent is `whole`, then read each by hand (about 110), and read
  every "N km" wording beside its cut position (about 90). Not checked: that each cut sits on the right ground feature,
  zone and provincial rows, the DFO feed, licensing extents, tributary walks, and how the app shows combined stretches.

## Unmatched lakes still unresolved (they bind nowhere; 17 rules in `AWAITING_WATER_ROWS`)

The key source was the Province's FISS data on openmaps WFS: `WHSE_FISH.FISS_LAKE_SURVEYS_SVW`,
`FISS_FISH_OBSRVTN_PNT_SP` and the EcoCat report titles carry local names and a 50K waterbody identifier that equals the
FWA lake's `WATERBODY_KEY_50K` plus `WATERSHED_GROUP_CODE_50K`, so a match is exact, never "nearest". The Region 5 MU
5-6 rows follow a 1997 AIM Ecological Consultants survey list (Cowboy (Maydoe), Banana, Crazy Bear (Ginny), Whitton
(Square), Hidden, Secret, Cohen, Stewart).

- **Frog Lake (MU 5-6; No Ice Fishing, quota 1, bait ban, single barbless):** no FISS, EcoCat or BCGNIS record puts a Frog
  Lake near 5-6. The 2008 Cariboo harmonization table groups it with Hidden and Secret, so it is probably an unnamed
  lake of the Stewart Lake drainage: wbk 328962870 (01256ATNA, 10.9 ha), 329021897 (01267ATNA, 8.6 ha), 329021982
  (01272ATNA, 4.4 ha) and four more unnamed candidates. None eliminated; do not bind. Next: the 1994 survey forms, the
  scanned EcoCat 49174 report, or ask Region 5 (Williams Lake) fisheries.
- **Redfern Lake (MU 5-15; quota 2, bait ban, single barbless):** BCGNIS has only Ruth Redfern Creek in the Cariboo; the
  only Redfern Lake is in Redfern-Keily Park (MU 7-42). The creek rises at unnamed lake wbk 329354762 (00034MAHD,
  33 ha), which FISS and EcoCat call "Goat Lake", and flows through Deception Lake, which has its own 5-15 row with the
  same rules. The name may be coined from the creek or a typo.
- **Squirrel Lake (`r6:squirrel_lake@6-1`):** BC's only Squirrel Lake is in MU 5-1 (the registry's Paddy/Squirrel Lake,
  wbk 329354753). The other 6-1 rows were found from the 1993 Region 5 winter-limnology series; none names Squirrel.
- The 8 intentional duplicates (Region 5 copies of the MU 6-1 lakes, Endako River 7A) and the 5 Kootenay pointer rows
  (Arrow Lakes to Upper Arrow wbk 328961689 plus Lower Arrow wbk 328961720; Seven Mile and Waneta reservoir tributaries
  to the Pend d'Oreille River row) need no binding.
