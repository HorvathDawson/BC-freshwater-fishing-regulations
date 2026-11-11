# T665 dry run: the polygon rule on the live atlas, in memory

2026-11-11. Rerun with the owner's answers (dry run 2). The build-path code (`pipeline/atlas/splits/shore.py`,
wired into `build.cut_shores`) ran on the live atlas (build 27, `data/generated/atlas/full`), in memory, through
`scripts/heavy.sh` (lane 2, about 75 s). Nothing was built or written into the atlas. The driver stayed in the scratchpad.

Maps for the owner (private artifacts, open on a phone):
- Cuts, removed lines, D2 creeks: https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc (`T665-dryrun-map.html`)
- Held rivers and creeks, one card each to tick: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q (`T665-main-rivers-map.html`)

Also here: `T665-orphans.json`, `T665-tidal-limits-mouth-rows.patch` (proposed, not applied).

## Owner answers applied (2026-11-11)
1. A cut that leaves nothing tidal is not made. A creek is cut only where the piece below the cut goes tidal
   (`A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE`).
2. D4 is withdrawn ("just do the 33 cuts and then remove tidal part"). The 33 mouth-tick creeks are cut at the polygon
   exit like every other creek. The same applies to the 59 further `mouth`-row creeks; that is the coordinator's reading,
   and they are listed separately below so the owner can object. Their `mouth` rows are superseded where a cut is made
   (24 rows): see `T665-tidal-limits-mouth-rows.patch`. **Proposed only**: reviewed data, not applied. The build works
   either way, because a `mouth` alias stays at the line end and does not touch the cut.
3. The eight main rivers with no limit of their own are held as today (no cut) until the owner ticks each
   (`HELD_FOR_OWNER`).
4. Kept as a default the owner may reverse: a water with its own `split`/`whole` limit is never cut by the polygon
   (`THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER`). Without it, the rule cut the Pitt 3.4 km and the Nitinat 23.9 km above
   their limits.

**Reference for waters with no listed limit.** DFO's glossary, "tidal boundary"
(https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/definitions-eng.html): unless otherwise specified, the tidal boundary is a
straight line drawn between the two most seaward points of land on either side of the mouth of a river or stream. A
fishery officer may also mark one with a triangular sign. Other specified boundaries are on the Pacific Fishery
Management Area maps. The DFO polygon can run further up an estuary than that line (the Pitt case), so the cuts where it
runs up a narrow channel are held too (below). The rule itself is not changed yet.

## Counts (dry run 2)

| | count |
|---|---|
| lines meeting the polygon | 16,292 runs of line |
| **lines wholly inside, removed (rule A)** | **215 lines not already tidal, 67.7 km** (246 listed; the other 31 are Fraser side channels already tidal in the reach run) |
| **shore cuts** | **201**: 110 into curated tidal water, 91 into a removed line (D2); 127.4 km goes tidal below them |
| against build 27's 532 shore cuts | 169 lines cut again (moved to the polygon exit), 363 dropped, 32 new |
| not cut, piece below would stay freshwater (answer 1) | 214 |
| not cut, the water's own limit governs | 26 |
| **held for the owner** | **61 lines**: 8 main rivers + 53 up the channel |
| mouth outside the polygon (D6) | 28 |
| lake on the line before the exit | 1 (360226010) |
| leave and re-enter, cut at the first exit (D3) | 10 cuts |
| exit within 5 m of a standing boundary (alias) | 0 |
| added (municipal) streams cut | 9 |
| the 33 mouth-tick creeks | 19 cut, 11 held (up the channel), 3 not cut (2 would stay fresh, 1 joins fresh water) |
| the 59 further mouth-row creeks | 5 cut, 14 held, 40 not cut (would stay fresh or join fresh water) |
| curated items whose water changes (`T665-orphans.json`) | 106 items, named 110 times in curated files; 0 would stop a promote |

Only one item becomes wholly tidal: Brae Slough (blk:356232808), an owner-found name, dropped from search by D5.
Owner hand matches touched (all partly tidal, none emptied): DFO places in regions 1, 2, 3, 5b, 6 (Cowichan, Nanaimo,
Chemainus, Bella Coola, Kitlope, Kemano, Kitimat and others) and 3 in-season notice waters. McLennan Creek (DFO
region 2 place) goes from partly tidal back to fresh.

### The atlas's tidal set vs the reach's
At atlas time there is no registry yet, so the rule decides "tidal water off the map" from the curated limits on the
graph (`shore.atlas_tidal`: the limit's line below its cut, plus same-gnis lines connected below it, plus `whole`
items by gnis). On build 27 that gives 70 sections; the reach run has 124. The 54 missing are side channels with no gnis
that the registry folds into the river (Fraser 16, Chemainus 15, whole waters 12, Nanaimo 4, Campbell 4 and others).
Run with the reach's set instead, the rule's cuts differ by 10:
- 3 only with the atlas set: 355991970, 355992231, 355992232 (Williamson Slough / Fraser side channels).
- 7 only with the reach set: 354135384, 356070273, 356285962, 356326465, 356342435, 356349946, 356358883.

Closing this needs registry knowledge in the atlas cut stage (T664's area). For now these 10 are known, listed
differences.

## Held for the owner: main rivers with no limit of their own (8)
| river | blue line | would go tidal (m from the line end) | line end (lat, lon) |
|---|---|---|---|
| Apple River | 360229887 | 648 | 50.71560, -125.40285 |
| Apple River | 360229888 | 703 | 50.71647, -125.41223 |
| Apple River | 360229889 | 534 | 50.71860, -125.40534 |
| Bedwell River | 354088440 | 317 | 49.36269, -125.77542 |
| Bella Coola River | 360218493 | 212 | 52.37788, -126.77007 |
| Kshwan River | 360225716 | 99 | 55.63495, -129.80656 |
| Mamin River | 360222775 | 443 | 53.62522, -132.31134 |
| Sarita River | 354088016 | 419 | 48.90121, -125.00376 |

## Held for the owner: the polygon reaches up the channel (53)
The measurement walks down from the polygon exit while the line is within 50 m of the polygon edge, so the polygon there
is a channel narrower than about 100 m. Cuts where that run is longer than 100 m are held. This is a proxy for "past the
two most seaward points" and needs the owner's eye.

| creek | blue line | up the channel (m) | would go tidal (m) | line end (lat, lon) |
|---|---|---|---|---|
| Rogers Creek | 354154568 | 1140 | 1182 | 49.25503, -124.81291 |
| Oona River | 360776500 | 900 | 922 | 53.94747, -130.25620 |
| Nunns Creek | 354149928 | 845 | 1425 | 50.04440, -125.25519 |
| (unnamed) | 360675720 | 710 | 760 | 54.01492, -128.65577 |
| (unnamed) | 354148021 | 670 | 673 | 48.89482, -123.68230 |
| Chemainus River | 354087938 | 580 | 1112 | 48.90356, -123.69080 |
| Franklin River | 354150804 | 575 | 577 | 49.10302, -124.81694 |
| Tsowwin River | 354150904 | 570 | 574 | 49.77825, -126.63827 |
| Chester Creek | 356237969 | 565 | 763 | 49.15186, -122.39957 |
| Cayeghle Creek | 354150572 | 480 | 1487 | 50.34246, -127.43988 |
| Sarita River | 354155136 | 465 | 1786 | 48.89706, -125.01690 |
| Colonial Creek | 354154029 | 460 | 464 | 50.33121, -127.43368 |
| Pa-aat River | 360871265 | 460 | 2961 | 53.80915, -130.00553 |
| Scribner Creek | 360817979 | 435 | 593 | 52.18328, -127.98271 |
| Trent River | 354148132 | 325 | 326 | 49.64412, -124.93005 |
| Sydney River | 354147245 | 300 | 2041 | 49.51494, -126.29488 |
| Tranquil Creek | 354154428 | 300 | 303 | 49.20924, -125.67406 |
| (unnamed) | 356249719 | 300 | 701 | 49.12923, -123.06193 |
| (unnamed) | 354100939 | 295 | 856 | 50.04404, -125.25568 |
| Kitkiata Creek | 360852130 | 295 | 1077 | 53.63855, -129.27230 |
| (unnamed) | 354119673 | 290 | 295 | 50.03814, -125.25447 |
| Bulson Creek | 354152039 | 285 | 656 | 49.26045, -125.72908 |
| (unnamed) | 354148274 | 260 | 261 | 49.52947, -126.30532 |
| Kitsucksus Creek | 354145435 | 255 | 400 | 49.25682, -124.81462 |
| (unnamed) | 360535529 | 250 | 253 | 50.95023, -126.17727 |
| Nanaimo River | 354087624 | 245 | 633 | 49.13359, -123.90985 |
| (unnamed) | 360861076 | 215 | 580 | 52.75696, -127.87277 |
| (unnamed) | 360438268 | 205 | 346 | 52.45927, -127.31439 |
| Gundersen Slough | 356286798 | 185 | 1489 | 49.16970, -122.92743 |
| (unnamed) | 360445710 | 185 | 499 | 52.37534, -126.76881 |
| McLennan Creek | 356195562 | 170 | 297 | 49.10942, -122.33929 |
| Tamboline Slough | 356165068 | 165 | 701 | 49.10146, -123.14632 |
| Chemainus River | 354087940 | 160 | 481 | 48.90470, -123.68362 |
| (unnamed) | 356232974 | 160 | 346 | 49.20494, -122.70169 |
| Kokish River | 354154484 | 150 | 153 | 50.53153, -126.87161 |
| (unnamed) | 356330529 | 145 | 479 | 49.16730, -122.50983 |
| (unnamed) | 360832937 | 145 | 150 | 53.81623, -129.93854 |
| (unnamed) | 356152742 | 130 | 244 | 49.22140, -123.20201 |
| Katzie Slough | 356339436 | 125 | 406 | 49.24352, -122.73773 |
| Kitlope River | 360227039 | 125 | 179 | 53.23168, -127.86447 |
| (unnamed) | 356227245 | 120 | 316 | 49.21499, -122.88140 |
| Matsqui Slough | 356310903 | 120 | 420 | 49.11325, -122.32478 |
| (unnamed) | 356321196 | 120 | 530 | 49.14249, -123.04396 |
| London Slough | 356346058 | 120 | 600 | 49.10286, -123.15125 |
| (unnamed) | 360812872 | 115 | 547 | 52.37672, -126.77359 |
| (unnamed) | 360868424 | 115 | 796 | 50.87820, -126.16575 |
| (unnamed) | 360700529 | 110 | 118 | 52.91997, -128.02944 |
| (unnamed) | 354118035 | 105 | 108 | 49.52054, -126.30223 |
| (unnamed) | 360586479 | 105 | 108 | 53.08534, -128.41015 |
| (unnamed) | 360665787 | 105 | 311 | 52.10262, -127.37933 |
| Carlisle Creek | 360764607 | 105 | 3215 | 54.14160, -130.09261 |
| (unnamed) | 360840369 | 105 | 107 | 52.18698, -127.98306 |
| (unnamed) | 360880247 | 105 | 107 | 53.64778, -129.30739 |

## The 33 creeks with the owner's mouth tick (D4 withdrawn)
"Inside" is how far the mapped line runs inside the DFO polygon, from its end to where it leaves the polygon.

| # | creek | blue line | inside (m) | line end (lat, lon) | polygon edge (lat, lon) | outcome |
|---|---|---|---|---|---|---|
| 1 | Baker Creek | 356164168 | 332 | 49.23032, -122.76739 | 49.23140, -122.77144 | cut 332 m up, tidal below |
| 2 | Barker Creek | 356274648 | 278 | 49.21843, -122.85370 | 49.21593, -122.85396 | cut 278 m up, tidal below |
| 3 | Benson Canal | 356179037 | 134 | 49.16844, -122.46063 | 49.16744, -122.46030 | cut 134 m up, tidal below |
| 4 | Boneyard Creek | 360885692 | 2506 | 54.15070, -130.07197 | 54.17250, -130.08115 | cut 2506 m up, tidal below |
| 5 | Carlisle Creek | 360764607 | 3214 | 54.14160, -130.09261 | 54.11364, -130.08077 | held (up the channel, 3215 m) |
| 6 | Chester Creek | 356237969 | 763 | 49.15186, -122.39957 | 49.15226, -122.39151 | held (up the channel, 763 m) |
| 7 | Cohilukthan Slough | 356285962 | 884 | 49.09398, -123.08656 | 49.08778, -123.07921 | not cut: would stay freshwater |
| 8 | Como Creek | 356150276 | 224 | 49.22241, -122.85113 | 49.22438, -122.85057 | cut 224 m up, tidal below |
| 9 | Cougar Canyon Creek | 356170069 | 336 | 49.15808, -122.94585 | 49.15617, -122.94243 | cut 336 m up, tidal below |
| 10 | Crescent Slough | 356326465 | 27 | 49.10468, -123.07251 | 49.10451, -123.07227 | not cut: joins fresh water |
| 11 | Deas Slough | 356358883 | 2935 | 49.11496, -123.08401 | 49.12654, -123.05220 | not cut: would stay freshwater |
| 12 | Ewen Slough | 356355111 | 1224 | 49.10523, -123.15462 | 49.10114, -123.16996 | cut 1224 m up, tidal below |
| 13 | Gundersen Slough | 356286798 | 1489 | 49.16970, -122.92743 | 49.17740, -122.91496 | held (up the channel, 1489 m) |
| 14 | Hanna Creek | 356323547 | 361 | 49.14046, -122.38857 | 49.13895, -122.39286 | cut 361 m up, tidal below |
| 15 | Katzie Slough | 356339436 | 406 | 49.24352, -122.73773 | 49.24169, -122.73353 | held (up the channel, 406 m) |
| 16 | Kildala Creek | 360864443 | 2838 | 54.14344, -130.08791 | 54.11895, -130.07673 | cut 2839 m up, tidal below |
| 17 | Kitsucksus Creek | 354145435 | 400 | 49.25682, -124.81462 | 49.26022, -124.81469 | held (up the channel, 400 m) |
| 18 | London Slough | 356346058 | 600 | 49.10286, -123.15125 | 49.10004, -123.15757 | held (up the channel, 600 m) |
| 19 | MacMillan Creek | 360874166 | 2556 | 54.14492, -130.08533 | 54.16619, -130.09982 | cut 2556 m up, tidal below |
| 20 | Matsqui Slough | 356310903 | 420 | 49.11325, -122.32478 | 49.11034, -122.32121 | held (up the channel, 420 m) |
| 21 | McCoy Creek | 354130563 | 318 | 49.26795, -124.85280 | 49.26845, -124.85658 | cut 318 m up, tidal below |
| 22 | McDonald Slough | 356248171 | 760 | 49.21524, -123.17662 | 49.21099, -123.18359 | cut 760 m up, tidal below |
| 23 | McLennan Creek | 356195562 | 297 | 49.10942, -122.33929 | 49.10754, -122.34095 | held (up the channel, 297 m) |
| 24 | Nelson Creek | 356013125 | 38 | 49.22957, -122.87702 | 49.22944, -122.87653 | cut 38 m up, tidal below |
| 25 | Nunns Creek | 354149928 | 1424 | 50.04440, -125.25519 | 50.03480, -125.25528 | held (up the channel, 1425 m) |
| 26 | Palmateer Creek | 356230481 | 590 | 49.16758, -122.52523 | 49.16243, -122.52575 | cut 590 m up, tidal below |
| 27 | Rogers Creek | 354154568 | 1182 | 49.25503, -124.81291 | 49.25786, -124.80397 | held (up the channel, 1182 m) |
| 28 | Salmon River | 356358833 | 95 | 49.17738, -122.58574 | 49.17689, -122.58681 | cut 95 m up, tidal below |
| 29 | Silverdale Creek | 356346138 | 277 | 49.13115, -122.35871 | 49.13361, -122.35827 | cut 277 m up, tidal below |
| 30 | Tamboline Slough | 356165068 | 701 | 49.10146, -123.14632 | 49.09542, -123.14887 | held (up the channel, 701 m) |
| 31 | Tones Creek | 356327002 | 170 | 49.11919, -122.35988 | 49.11796, -122.36127 | cut 170 m up, tidal below |
| 32 | Windebank Creek | 356341732 | 370 | 49.12608, -122.33549 | 49.12909, -122.33351 | cut 370 m up, tidal below |
| 33 | Woodward Slough | 356303771 | 426 | 49.10755, -123.12265 | 49.11117, -123.12349 | cut 426 m up, tidal below |

## The 59 further mouth-row creeks (coordinator's reading of the answer; the owner may object)
| # | creek | blue line | inside (m) | line end (lat, lon) | polygon edge (lat, lon) | outcome |
|---|---|---|---|---|---|---|
| 1 | Adam River | 354154447 | 299 | 50.46587, -126.27747 | 50.46446, -126.28094 | not cut: would stay freshwater |
| 2 | Anderson Creek | 360859311 | 392 | 49.64047, -123.98757 | 49.63926, -123.98374 | not cut: would stay freshwater |
| 3 | Apple River | 360884532 | 1371 | 50.71691, -125.41556 | 50.72143, -125.39969 | not cut: would stay freshwater |
| 4 | Bedwell River | 354154798 | 924 | 49.36425, -125.77683 | 49.36986, -125.76801 | not cut: would stay freshwater |
| 5 | Bella Coola River | 360885253 | 1320 | 52.39012, -126.77769 | 52.38107, -126.76640 | not cut: would stay freshwater |
| 6 | Bonsall Creek | 354133599 | 461 | 48.88543, -123.66487 | 48.88564, -123.67028 | not cut: would stay freshwater |
| 7 | Brem River | 360854323 | 102 | 50.43562, -124.66633 | 50.43645, -124.66691 | cut 102 m up, tidal below |
| 8 | Brittain River | 360882453 | 279 | 49.99744, -124.00964 | 49.99878, -124.01199 | not cut: would stay freshwater |
| 9 | Brothers Creek | 360836484 | 46 | 49.32783, -123.13041 | 49.32819, -123.13065 | not cut: would stay freshwater |
| 10 | Bulson Creek | 354152039 | 656 | 49.26045, -125.72908 | 49.26537, -125.72808 | held (up the channel, 656 m) |
| 11 | Cascade Creek | 354150623 | 39 | 49.70705, -126.10618 | 49.70718, -126.10669 | not cut: would stay freshwater |
| 12 | Cayeghle Creek | 354150572 | 1487 | 50.34246, -127.43988 | 50.33087, -127.43339 | held (up the channel, 1487 m) |
| 13 | Colonial Creek | 354154029 | 464 | 50.33121, -127.43368 | 50.32944, -127.43913 | held (up the channel, 464 m) |
| 14 | Cowichan River | 354155148 | 951 | 48.75324, -123.63630 | 48.75745, -123.64660 | not cut: would stay freshwater |
| 15 | Deer Creek | 354149342 | 70 | 49.69586, -126.11783 | 49.69628, -126.11851 | not cut: would stay freshwater |
| 16 | Draney Creek | 360878108 | 817 | 51.40709, -127.25239 | 51.40535, -127.24133 | not cut: would stay freshwater |
| 17 | Franklin River | 354150804 | 577 | 49.10302, -124.81694 | 49.10020, -124.81075 | held (up the channel, 577 m) |
| 18 | Glacier Creek | 360815596 | 214 | 55.96121, -129.97136 | 55.96060, -129.96813 | not cut: would stay freshwater |
| 19 | Gold River | 354154308 | 4075 | 49.68114, -126.11254 | 49.70973, -126.10246 | not cut: would stay freshwater |
| 20 | Kainet Creek | 360871314 | 16 | 52.75644, -127.87717 | 52.75640, -127.87695 | cut 16 m up, tidal below |
| 21 | Kemano River | 360885559 | 813 | 53.48393, -128.12479 | 53.48848, -128.11682 | not cut: would stay freshwater |
| 22 | Khutze River | 360878779 | 1068 | 53.08524, -128.41215 | 53.08383, -128.39733 | not cut: would stay freshwater |
| 23 | Kimsquit River | 360887414 | 974 | 52.88314, -127.07739 | 52.88968, -127.08429 | not cut: would stay freshwater |
| 24 | Kingcome River | 360874718 | 4556 | 50.92731, -126.18741 | 50.96275, -126.19107 | not cut: would stay freshwater |
| 25 | Kitimat River | 360885824 | 1250 | 54.01089, -128.66227 | 54.02102, -128.65669 | not cut: would stay freshwater |
| 26 | Kitkiata Creek | 360852130 | 1077 | 53.63855, -129.27230 | 53.64717, -129.27314 | held (up the channel, 1077 m) |
| 27 | Kokish River | 354154484 | 153 | 50.53153, -126.87161 | 50.53030, -126.87194 | held (up the channel, 153 m) |
| 28 | Kowesas River | 360883956 | 810 | 53.33685, -128.12306 | 53.33069, -128.12571 | not cut: would stay freshwater |
| 29 | Kshwan River | 360860569 | 698 | 55.63021, -129.80703 | 55.63499, -129.80271 | not cut: would stay freshwater |
| 30 | Ksi X'anmas | 360882263 | 1641 | 54.77545, -130.15004 | 54.77330, -130.12539 | not cut: would stay freshwater |
| 31 | Kwatna River | 360858825 | 3125 | 52.11187, -127.38610 | 52.09617, -127.35330 | not cut: would stay freshwater |
| 32 | Mamin River | 360873432 | 1573 | 53.62133, -132.31050 | 53.62050, -132.29528 | not cut: would stay freshwater |
| 33 | Marmot River | 360883324 | 41 | 55.87865, -130.01530 | 55.87852, -130.01469 | not cut: would stay freshwater |
| 34 | Monmouth Creek | 360877261 | 341 | 49.70652, -123.17432 | 49.70917, -123.17583 | not cut: would stay freshwater |
| 35 | Moyeha River | 354154217 | 959 | 49.41797, -125.91242 | 49.42616, -125.91285 | not cut: would stay freshwater |
| 36 | Mussel River | 360884170 | 635 | 52.92057, -128.03248 | 52.92472, -128.02657 | not cut: would stay freshwater |
| 37 | Necleetsconnay River | 360884575 | 617 | 52.38900, -126.77438 | 52.39036, -126.76576 | not cut: would stay freshwater |
| 38 | Nieumiamus Creek | 360869552 | 120 | 52.38930, -126.83423 | 52.38971, -126.83584 | not cut: would stay freshwater |
| 39 | Oona River | 360776500 | 922 | 53.94747, -130.25620 | 53.95270, -130.26583 | held (up the channel, 922 m) |
| 40 | Pa-aat River | 360871265 | 2961 | 53.80915, -130.00553 | 53.78498, -130.00859 | held (up the channel, 2961 m) |
| 41 | Quaal River | 360886968 | 2357 | 53.64819, -129.30339 | 53.65154, -129.33041 | not cut: would stay freshwater |
| 42 | Sarita River | 354155136 | 1786 | 48.89706, -125.01690 | 48.89325, -124.99779 | held (up the channel, 1786 m) |
| 43 | Scribner Creek | 360817979 | 593 | 52.18328, -127.98271 | 52.18821, -127.98310 | held (up the channel, 593 m) |
| 44 | Silverado Creek | 360833243 | 77 | 55.93035, -129.98297 | 55.93018, -129.98180 | not cut: would stay freshwater |
| 45 | Stagoo Creek | 360877620 | 399 | 55.28582, -129.74446 | 55.28344, -129.74085 | not cut: would stay freshwater |
| 46 | Sydney River | 354147245 | 2042 | 49.51494, -126.29488 | 49.52957, -126.30559 | held (up the channel, 2041 m) |
| 47 | Theodosia River | 360856269 | 441 | 50.08042, -124.65483 | 50.08345, -124.65158 | not cut: would stay freshwater |
| 48 | Toba River | 360883346 | 2753 | 50.48861, -124.36585 | 50.50587, -124.34184 | cut 2753 m up, tidal below |
| 49 | Tom Browne Creek | 360870027 | 855 | 50.66328, -125.73161 | 50.65679, -125.72902 | not cut: would stay freshwater |
| 50 | Tranquil Creek | 354154428 | 303 | 49.20924, -125.67406 | 49.21195, -125.67394 | held (up the channel, 303 m) |
| 51 | Trent River | 354148132 | 326 | 49.64412, -124.93005 | 49.64169, -124.93240 | held (up the channel, 326 m) |
| 52 | Tsowwin River | 354150904 | 574 | 49.77825, -126.63827 | 49.78002, -126.63214 | held (up the channel, 574 m) |
| 53 | Tzouhalem Creek | 354153794 | 2266 | 48.76140, -123.63262 | 48.77599, -123.64370 | cut 2266 m up, tidal below |
| 54 | Ucona River | 354153463 | 80 | 49.70904, -126.10318 | 49.70866, -126.10224 | not cut: would stay freshwater |
| 55 | Vinson Creek | 360744582 | 69 | 49.32214, -123.13912 | 49.32258, -123.13978 | not cut: would stay freshwater |
| 56 | Wahoo Creek | 360887120 | 40 | 53.49184, -128.11919 | 53.49214, -128.11900 | cut 40 m up, tidal below |
| 57 | Wathl Creek | 360875560 | 928 | 53.97886, -128.65156 | 53.97366, -128.64087 | not cut: would stay freshwater |
| 58 | Winachee Creek | 360808890 | 292 | 55.95051, -129.97405 | 55.95244, -129.97172 | not cut: would stay freshwater |
| 59 | Zeballos River | 354151763 | 336 | 49.98185, -126.85263 | 49.98442, -126.85053 | not cut: would stay freshwater |

## What changed in the code since dry run 1
- `shore.py`: the T660 crossing-run rule (`shore_cuts`, the 50 % share, the braid-span skip) is replaced by the polygon
  rule. New pieces: `atlas_tidal`, `owner_limited`, the fixpoint that drops cuts which would leave the piece below fresh,
  `HELD_FOR_OWNER` and `HELD_UP_THE_CHANNEL`, the `inside_tidal.json` writer and reader, and `RULE = polygon/1` in
  `shore_cuts.json`.
- `build.cut_shores` runs it on the build's `BlkChain`s and writes `shore_cuts.json` and `inside_tidal.json`. The braid
  sweep still runs on the cuts and stops the build if it would cut anything.
- `reach/outside.py`: owner `inside:<blk>` (`tidal_inside_owner`, `A_LINE_THE_ATLAS_LISTS_INSIDE_IS_TIDAL`).
  `is_cut_owner` takes it, so the bundle proof and the tiles take the same set. `reach.cli` / `build_reaches` pass it,
  `tiles/tidal.tidal_pieces` adds it, and the review app's tidal set adds it.
- `pipeline/build.py`: the reach and tiles stage keys gain `inside_tidal`, but only when the atlas lists lines, so an
  older atlas keeps its keys.
- `POLICY_VERSION` 36 → 37, with its changelog line and the pin updated.
- An atlas whose `shore_cuts.json` names the rule must carry `inside_tidal.json` (refused otherwise). An older atlas
  lists none, by its record.

## What the side build must include
The side build must contain this commit, T664 (Oxbows) and the nine DFO cuts, built once with
`python -m pipeline build --atlas` (side atlas). After the build:
1. Atlas parity.
2. A reach run (POLICY_VERSION 37).
3. `binding_diff`. It will stop until `--accept-diff`.
4. `atlas.promote --accept-boundary-loss`, because 363 shore cuts are dropped.
5. Check the straddle, unplaced and sliver gates and the DFO drowned check.
6. Re-resolve the in-season notices, then re-cut the app fixtures.

The owner decides on the mouth-row patch before or after; either works. RULINGS and MAP still need the shore rule
written in.

## Not verified
- No atlas build. The braid sweep, sliver gate, straddle and unplaced gates have not run at the new cut points, and
  there is no reach run or binding diff.
- The dry run rebuilds lines from the already-cut graph (`graph_lines`). The build uses `BlkChain`s with their real
  lake runs, so lake ends may differ (1–2 cases seen).
- `atlas_tidal` vs the reach set: the 10 cut differences above.
- "Up the channel" is a distance-to-edge proxy, not the DFO two-points line.
- The `sidecars.py` writer for `inside_tidal.json` was not added. A sidecar alone, without the moved cuts, would leave
  an atlas half on each rule.
- The orphans scan matches item ids as text in the curated JSON files (marks.jsonl skipped). Gauges are keyed by
  station, so they may be missed.
