# T1053 — rebuilt list: every head lake climbed, a stream's own row wins (read again before it ships)

Built on branch `feat/T1053-walk-climbs-head-lakes` (side runs on the live atlas; nothing released). Built from your
answers of 2026-11-24: "All 22, but a stream's own row wins"; "make sure upstream includes tributarties still overrides
lower includes tributtaries that it conflicts with"; "keep earlier rule of closure always winning unless it is exempt)
this does not include no fishiong all year rulings tho those will yield in that case. only the dated closures."; and
"tributary rules dont apply to the lake either only streams".

> **2026-11-25: Dove Creek's reopening rests on a WRONG binding.** The row "“ANDERSON” LAKE" (p.14, MU 1-3) is an unnamed
> lake in the Walbran Creek watershed (FWA `wbk:329504289`), not the gazetted Anderson Lake near Comox that it is bound to
> today (`wbk:329095566`, MU 1-6, on Dove Creek). Rebinding it (proposal in the T1053 ticket) makes the Dove Creek /
> Anderson Lake reopening below disappear: nothing reopens outside the head lakes, and Frosst Creek stays the only "own row
> kept". The Comox lake and Dove Creek also lose rules they wrongly carry in the live release.

**In short.**
- The walk is unchanged from the first list: 22 head lakes pass their river's rules up to the streams above them
  (15,894 stream pieces, about 19,500 km; 39 rules climb, plus 3 B.C.-wide steelhead rules and one steelhead licence
  record (`steelhead_targeting_known`) that gain one piece above Babine Lake as a knock-on; all gains, nothing lost, no
  DFO place moved).
- **Own row kept** (new): where a river's ALL-YEAR "No Fishing" reaches a stream that has a row of its own, or a nearer
  ✱ row, it gives way. Above the head lakes this changes ONE stream: **Frosst Creek answers exactly as today** (its own
  "No Fishing Oct 15-Apr 15", open Apr 16-30 and Jul 1-Oct 14), not Sweltzer River's all-year closure.
- DATED closures still close, even a stream with its own row, unless it is exempt (unchanged: the Granby River stays
  closed Jul 25-Sept 15 under the Kettle's tributaries). A stream with no row still closes with its river: Dick Creek
  closes with Eagle Creek.
- **The climb also LOOSENS in places** (section below): the Lardeau River's printed exemptions reach Trout Lake's
  streams (a rainbow on Asher Creek on Dec 1: release only today, 5 a day after), and Perry Creek's brook trout 20 a day
  reaches the streams above Richmond Lake. **It newly closes Cameron River above Cameron Lake (32.4 km) all year**, under
  the Little Qualicum's "No Fishing - All tributaries".
- **Outside any head lake the same own-row rule reopens Dove Creek's top piece above Anderson Lake and 19 unnamed stream
  pieces around the lake (20 pieces, 21.6 km; map `dove-creek-reopens.png`)** for catch and release, except Jul 15-Aug 31:
  the Tsolum River's all-year ✱ "No Fishing" gives way to Anderson Lake's ✱ "Trout and kokanee catch and release", the
  nearer row. The fish that reopen: cutthroat, Dolly Varden / bull trout, brook trout, brown trout, lake trout,
  kokanee, rainbow and steelhead (CT, DV, EB, GB, KO, LT, RB, ST); every other game fish (bass, perch, whitefish, burbot,
  sturgeon, pike, walleye …) stays closed. Everywhere else the rule only changes WHICH closure is named (both closed).
- Lakes: no lake that is a water of its own is covered by any tributary rule. Only ponds the process lists as a piece of
  a stream (oxbows, slough lakes, widenings) follow that stream: 3,657 such polygons today, 3,668 with this change.

## Where the climb closes, and where it opens or loosens (per fish, live release → this branch)

Every stretch of stream whose rule set changes was asked, per game fish, on the 1st and 15th of every month: closed /
release only / a daily number (plain daily limits; a rule held only while doing something, or for foul-hooked fish, is
left out). One section stands for each group of sections that share an answer, before and after.

**Closes or tightens.** Top of the list: **Cameron River above Cameron Lake (32.4 km, 5 pieces) is newly closed all year**
under the Little Qualicum River's "No Fishing - All tributaries" (with B. P., Cop, Kammat and Yellows creeks: 177 pieces).

| Head lake | The climbed rule(s) | Pieces | What tightens (fish) |
|---|---|---|---|
| Cameron Lake | `little_qualicum_river.r1` No Fishing July 15-Aug 31<br>`little_qualicum_river.r2` No Fishing - All tributaries | 177 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Slocan Lake | `slocan_river.r1` No Fishing July 15-Aug 31 from 12 pm to midnight (EXCEPT Koch Creek upstream of falls loca<br>`slocan_river.r2` Bait ban (where open), June 15-Oct 31<br>`slocan_river.r3` Trout/char catch and release (EXCEPT Koch Creek upstream of falls and Little Slocan Lake's | 3,398 | keep->release: CT, EB, GB, LT, RB, ST |
| Yakoun Lake | `yakoun_river.r2` Bait ban Nov 1-Apr 30<br>`yakoun_river.r3` Cutthroat trout catch and release<br>`yakoun_river.r4` No powered boats | 186 | keep->release: CT |
| Lower Elk Lake | `elk_river_s_tributaries.r1` No Fishing Sept 1-Oct 31<br>`elk_river_s_tributaries.r2` Trout/char daily quota = 1 (none under 30 cm)<br>`elk_river_s_tributaries.r3` bait ban, June 15-Aug 31<br>`elk_river_s_tributaries.r4` Bull trout catch and release | 98 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LT, LW, MW, RB, ST; keep->release: DV; quota down: EB, GB, LT |
| Cultus Lake | `sweltzer_creek.r1` No Fishing | 59 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, WSG, YP |
| Keogh Lake | `keogh_river.r2` No Fishing in all parts, Dec 1-May 31 | 35 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Beaver Lake | `colquitz_river.r1` No Fishing | 13 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Dick Lake | `eagle_creek.r1` No Fishing | 13 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Gooseneck Lake | `miller_creek.r1` No Fishing | 11 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Butchart Lake | `goldstream_river.r1` No Fishing | 6 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Puntledge Lake | `puntledge_river.r5` No Fishing tributaries | 3 | closes: BB, BCB, CT, DV, EB, GB, GE, GR, IN, KO, LMB, LT, LW, MW, NP, RB, SMB, ST, WP, YP |
| Richmond Lake | `perry_creek.r1` Bait ban<br>`perry_creek.r2` Cutthroat trout and bull trout catch and release<br>`perry_creek.r3` brook trout daily quota = 20 | 2 | keep->release: CT, DV |
| Babine Lake — Main Body | `babine_river.r6` Bait ban<br>`steelhead.r1b` The annual province-wide quota for hatchery steelhead is 10.<br>`steelhead.r2b` All wild steelhead must be released.<br>`steelhead.r4b` You must immediately record your retention of hatchery steelhead on your basic angling lic | 1 | keep->release: ST |

**Opens or loosens.**

| Head lake | The climbed rule(s) | Pieces | What loosens (fish) |
|---|---|---|---|
| Trout Lake | `lardeau_river.r2` Exempt from spring closure (Apr 1-Jun 14)<br>`lardeau_river.r3` exempt from trout/char release (Nov 1-Mar 31)<br>`lardeau_river.r4` Rainbow trout daily quota = 5 (any size)<br>`lardeau_river.r5` bull trout daily quota = 2 (any size) | 1,194 | release->keep: CT, EB, GB, LT, RB, ST |
| Yakoun Lake | `yakoun_river.r2` Bait ban Nov 1-Apr 30<br>`yakoun_river.r3` Cutthroat trout catch and release<br>`yakoun_river.r4` No powered boats | 186 | quota up: RB |
| Richmond Lake | `perry_creek.r1` Bait ban<br>`perry_creek.r2` Cutthroat trout and bull trout catch and release<br>`perry_creek.r3` brook trout daily quota = 20 | 2 | quota up: EB |
| Babine Lake — Main Body | `babine_river.r6` Bait ban<br>`steelhead.r1b` The annual province-wide quota for hatchery steelhead is 10.<br>`steelhead.r2b` All wild steelhead must be released.<br>`steelhead.r4b` You must immediately record your retention of hatchery steelhead on your basic angling lic | 1 | quota up: RB |

The Lardeau River's printed exemptions climb through Trout Lake: e.g. Asher Creek, a rainbow on Dec 1 — release only
today (Region 4's winter trout/char release), 5 a day after (`lardeau_river.r3` lifts the release; `lardeau_river.r4`
"Rainbow trout daily quota = 5"). Perry Creek's "brook trout daily quota = 20" raises the brook trout number above
Richmond Lake. The pieces above Babine Lake that become book-known steelhead water (one piece) answer rainbow by the
steelhead rules.

**No change to any fish's keep / release / closed answer** (the climbed rule is a bait ban, a boat rule, a note, or
repeats what the zone already says): Babine Lake — Main Body (4,994); Morice Lake (2,589); Cowichan Lake (1,234); Meziadin Lake (1,139); Lakelse Lake (474); Sooke Lake (86); Burnaby Lake (83); Nass Lake (49); Jacobs Lake (18); Trout Lake (18); Slocan Lake (13); Cultus Lake (1).

## Own row kept (the climbed rule gives way)

| Stream | Above | The climbed rule that gives way | Its own row that answers | Answer |
|---|---|---|---|---|
| Frosst Creek (own row) | Cultus Lake | Sweltzer River ✱ "No Fishing" (all year) | "No Fishing Oct 15-Apr 15" | as today: closed Oct 15-Apr 15 and May 1-Jun 30 (Region 2 streams), open Apr 16-30 and Jul 1-Oct 14 |

Every other stream above a climbed head lake that has a row of its own keeps BOTH: its own rules speak where the two
say the same kind of thing (quota, release, bait, gear), and a climbed DATED closure still closes it (none of the 22
lakes' climbed closures is dated except the Cowichan's and the Slocan's summer closures, which close as printed).

## The same rule outside any climb (for the record)

Only **Dove Creek's top piece above Anderson Lake and 19 unnamed stream pieces around Anderson Lake** (20 pieces,
21.6 km, Region 1) change what an angler may do (above; read over every answer key, named or not). In 122 more stream parts an all-year
closure gives way to ANOTHER all-year "No Fishing" of a nearer row: the stream stays closed and the nearer row is the
one named.

| The farther row's all-year closure | yields to (nearer / own row) | how | waters (same answer, closed either way) |
|---|---|---|---|
| `r2:brunette_river@2-8::brunette_river.r1` No Fishing upstream of Burnaby Lake or from Caribo | `r2:burnaby_lake_s_tributaries@2-8::burnaby_lake_s_tributaries.r1` No Fishing | nearer_row | 95: Angelo Creek, Ardley Creek, Beaver Trib.1, Beaver Trib.1-1, Beaver Trib.2, Beaver Trib.2-1, Beaver Trib.3, Beaver Trib.4 … |
| `r2:brunette_river_s_tributaries@2-8::brunette_river_s_tributaries.r1` No Fishing | `r2:burnaby_lake_s_tributaries@2-8::burnaby_lake_s_tributaries.r1` No Fishing | nearer_row | 95: Angelo Creek, Ardley Creek, Beaver Trib.1, Beaver Trib.1-1, Beaver Trib.2, Beaver Trib.2-1, Beaver Trib.3, Beaver Trib.4 … |
| `r2:burnaby_lake_s_tributaries@2-8::burnaby_lake_s_tributaries.r1` No Fishing | `r2:still_creek@2-8::still_creek.r1` No Fishing | nearer_row | 33: Ardley Creek, Beecher Trib.1, Beecher Trib.2, Beecher Trib.3, Beecher Trib.4, Black Bear Creek, Bog Creek, Chub Creek … |
| `r1:puntledge_river@1-6::puntledge_river.r5` No Fishing tributaries | `r1:cruickshank_river@1-6::cruickshank_river.r1` No Fishing | nearer_row | 7: Capes Creek, Comox Creek, Datsio Creek, Eric Creek, Idiens Creek, Kweishun Creek, Rees Creek |
| `r5:dean_river@5-9::dean_river.r2` No Fishing from Iltasyuko River to Crag Creek | `r5:takia_river@5-10::takia_river.r1` No Fishing | nearer_row | 5: Compass Creek, Kohasganko Creek, Tahyesco River, Taiataeszi Creek, Tzeetsaytsul Creek |
| `r2:squamish_river_s_tributaries@2-6::squamish_river_tributaries.r1` No Fishing tributaries EXCEPT: Ashlu Creek, Cheaka | `r2:mashiter_creek@2-7::mashiter_creek.r1` No Fishing | own_row | 1: Mashiter Creek |
| `r2:burnaby_lake_s_tributaries@2-8::burnaby_lake_s_tributaries.r1` No Fishing | `r2:still_creek@2-8::still_creek.r1` No Fishing | own_row | 1: Still Creek |
| `r1:harris_creek@1-3::harris_creek.r2` No Fishing upstream of and including Hemmingsen Cr | `r1:hemmingsen_creek@1-3::hemmingsen_creek.r1` No Fishing | own_row | 1: Hemmingsen Creek |
| `r2:mosquito_creek@2-8::mosquito_creek.r1` No Fishing | `r2:wagg_creek@2-8::wagg_creek.r1` No Fishing | own_row | 1: Wagg Creek |
| `r5:dean_river@5-9::dean_river.r2` No Fishing from Iltasyuko River to Crag Creek | `r5:takia_river@5-10::takia_river.r1` No Fishing | own_row | 1: Takia River |
| `r1:puntledge_river@1-6::puntledge_river.r5` No Fishing tributaries | `r1:cruickshank_river@1-6::cruickshank_river.r1` No Fishing | own_row | 1: Cruickshank River |
| `r2:squamish_river_s_tributaries@2-6::squamish_river_tributaries.r1` No Fishing tributaries EXCEPT: Ashlu Creek, Cheaka | `r2:cheekye_river@2-7::cheekye_river.r1` No Fishing (including Brohm Creek) | own_row | 1: Cheekye River |
| `r2:squamish_river_s_tributaries@2-6::squamish_river_tributaries.r1` No Fishing tributaries EXCEPT: Ashlu Creek, Cheaka | `r2:cheekye_river@2-7::cheekye_river.r1` No Fishing (including Brohm Creek) | nearer_row | 1: Brohm River |
| `r2:squamish_river_s_tributaries@2-6::squamish_river_tributaries.r1` No Fishing tributaries EXCEPT: Ashlu Creek, Cheaka | `r2:shovelnose_creek@2-6::shovelnose_creek.r1` No Fishing | own_row | 1: Shovelnose Creek |
| `r1:puntledge_river@1-6::puntledge_river.r5` No Fishing tributaries | `r1:browns_river@1-6::browns_river.r1` No Fishing | own_row | 1: Browns River |
| `r6:hays_creek_in_prince_rupert@6-14::hays_creek.r1` No Fishing upstream of fishing boundary signs down | `r6:oldfield_creek@6-14::oldfield_creek.r1` No Fishing | own_row | 1: Oldfield Creek |


## The lakes that change

| Head lake | Lake has its own row? | River | The river's rule(s) that now climb | Streams gained | Pieces / km |
|---|---|---|---|---|---|
| Babine Lake — Main Body | yes | Babine River | `babine_river.r6` Bait ban | 61 named: Betty Creek (9.0 km), Big Loon Creek (12.0 km), Bristol Creek (7.0 km), Bristow Creek (16.4 km), Broughton Creek (20.9 km), Byron Creek (17.1 km), Coldwater Creek (8.9 km), Covert Creek (8.0 km) … and 53 more named; unnamed: 4650 pieces, 6477.5 km; plus 10+ small polygons counted as part of those streams; 1 dead-end piece(s) of those streams | 4,995 / 7,487.1 |
| Slocan Lake | yes | Slocan River | `slocan_river.r1` No Fishing July 15-Aug 31 from 12 pm to midnight (EXCEPT Koch Creek upstream of falls located approximately 700 m downstream of the Little Slocan Forest Service Road Koch Creek bridge crossing and Little Slocan Lake's tributaries; see Lemon Creek)<br>`slocan_river.r2` Bait ban (where open), June 15-Oct 31<br>`slocan_river.r3` Trout/char catch and release (EXCEPT Koch Creek upstream of falls and Little Slocan Lake's tributaries) | 91 named: Algiers Creek (2.2 km), Allen Creek (3.0 km), Arthur Creek (5.1 km), Avison Creek (3.4 km), Aylwin Creek (7.1 km), Baby Ruth Creek (3.1 km), Bartlett Creek (5.6 km), Beatrice Creek (13.1 km) … and 83 more named; unnamed: 3200 pieces, 3104.0 km; plus 10+ small polygons counted as part of those streams | 3,411 / 3,765.7 |
| Morice Lake | no | Morice River | `morice_river.r1` Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays, Sept 1-Oct 31<br>`morice_river.r6` Bait ban<br>`morice_river.r7` No angling from boats, Aug 15-Dec 31 (all parts) | 14 named: Atna River (31.2 km), Bergeland Creek (27.3 km), Copper Cliff Creek (13.7 km), Fenton Creek (9.2 km), Glacier Creek (17.8 km), Loring Creek (6.6 km), McBride Creek (18.5 km), Mortella Creek (5.5 km) … and 6 more named; unnamed: 2506 pieces, 2651.5 km; plus 10+ small polygons counted as part of those streams; 3 dead-end piece(s) of those streams | 2,589 / 2,860.3 |
| Cowichan Lake | yes | Cowichan River | `cowichan_river.r2` No Fishing July 15-Aug 31 | 25 named: Ashburnham Creek (6.6 km), Beaver Creek (5.6 km), Coonskin Creek (3.4 km), Cottonwood Creek (11.4 km), Croft Creek (2.3 km), Death Creek (0.7 km), East Robertson River (10.3 km), Gnarly Creek (1.0 km) … and 17 more named; unnamed: 1198 pieces, 1059.6 km | 1,234 / 1,239.3 |
| Trout Lake | yes | Lardeau River | `lardeau_river.r2` Exempt from spring closure (Apr 1-Jun 14)<br>`lardeau_river.r3` exempt from trout/char release (Nov 1-Mar 31)<br>`lardeau_river.r4` Rainbow trout daily quota = 5 (any size)<br>`lardeau_river.r5` bull trout daily quota = 2 (any size) | 51 named: Abrahamson Creek (7.8 km), Alpha Creek (4.8 km), American Creek (5.8 km), Asher Creek (18.8 km), Batys Creek (3.7 km), Benson Creek (3.9 km), Bigger Creek (5.1 km), Broadview Creek (2.7 km) … and 43 more named; unnamed: 1132 pieces, 1090.6 km; plus 9 small polygons counted as part of those streams | 1,212 / 1,396.3 |
| Meziadin Lake | no | Meziadin River | `meziadin_river.r1` Bait ban | 8 named: Crook Creek (6.0 km), Divi Creek (6.2 km), Glace Creek (8.4 km), Goldsmith Creek (4.2 km), Hanna Creek (38.6 km), Strohn Creek (22.7 km), Surprise Creek (21.6 km), Tintina Creek (35.3 km); unnamed: 1089 pieces, 1163.2 km; plus 10+ small polygons counted as part of those streams; 4 dead-end piece(s) of those streams | 1,139 / 1,306.2 |
| Lakelse Lake | yes | Lakelse River | `lakelse_river.r3` Bait ban<br>`lakelse_river.r4` no powered boats<br>`lakelse_river.r5` Note: The mouth of the river is designated by signs 1.7 km downstream of Lakelse River logging road bridge | 8 named: Andalas Creek (4.7 km), Clearwater Creek (6.5 km), Ena Creek (1.8 km), Furlong Creek (6.8 km), Hatchery Creek (13.2 km), Schulbuckhand Creek (16.3 km), Sockeye Creek (12.0 km), Williams Creek (40.7 km); unnamed: 456 pieces, 508.1 km; plus 3 small polygons counted as part of those streams; 1 dead-end piece(s) of those streams | 474 / 610.2 |
| Yakoun Lake | no | Yakoun River | `yakoun_river.r2` Bait ban Nov 1-Apr 30<br>`yakoun_river.r3` Cutthroat trout catch and release<br>`yakoun_river.r4` No powered boats | 4 named: Baddeck Creek (5.6 km), Delta Creek (5.5 km), Etheline Creek (5.2 km), Sandstone Creek (8.2 km); unnamed: 182 pieces, 165.9 km | 186 / 190.4 |
| Cameron Lake | no | Little Qualicum River | `little_qualicum_river.r1` No Fishing July 15-Aug 31<br>`little_qualicum_river.r2` No Fishing - All tributaries | 5 named: B. P. Creek (7.5 km), Cameron River (32.4 km), Cop Creek (4.6 km), Kammat Creek (2.4 km), Yellows Creek (3.5 km); unnamed: 163 pieces, 171.0 km; plus 3 small polygons counted as part of those streams | 177 / 221.3 |
| Lower Elk Lake | yes | Elk River | `elk_river_s_tributaries.r1` No Fishing Sept 1-Oct 31<br>`elk_river_s_tributaries.r2` Trout/char daily quota = 1 (none under 30 cm)<br>`elk_river_s_tributaries.r3` bait ban, June 15-Aug 31<br>`elk_river_s_tributaries.r4` Bull trout catch and release | 1 named: Nivelle Creek (6.7 km); unnamed: 96 pieces, 80.0 km | 98 / 86.7 |
| Sooke Lake | no | Sooke River | `sooke_river.r2` No Fishing July 15-Aug 31 | 2 named: Judge Creek (7.3 km), Rithet Creek (8.0 km); unnamed: 84 pieces, 78.1 km | 86 / 93.4 |
| Burnaby Lake | yes | Brunette River | `brunette_river_s_tributaries.r1` No Fishing | 76 named: Angelo Creek (1.0 km), Ardley Creek (0.6 km), Beaver Trib.1 (0.2 km), Beaver Trib.1-1 (0.4 km), Beaver Trib.2 (1.1 km), Beaver Trib.2-1 (0.1 km), Beaver Trib.3 (0.6 km), Beaver Trib.4 (0.5 km) … and 68 more named; unnamed: 4 pieces, 1.7 km; plus 1 small polygons counted as part of those streams | 83 / 56.8 |
| Cultus Lake | yes | Sweltzer River | `sweltzer_creek.r1` No Fishing | 11 named: Amadis Creek (2.8 km), Ascaphus Creek (2.2 km), Clear Creek (3.1 km), Fin Creek (1.0 km), Frosst Creek (8.2 km), Laurel Creek (0.7 km), Smith Falls Creek (2.1 km), Teapot Creek (2.4 km) … and 3 more named; unnamed: 42 pieces, 39.4 km; plus 3 small polygons counted as part of those streams | 60 / 71.1 |
| Nass Lake | no | Nass River | `nass_river.r1` Bait ban | 0 named: ; unnamed: 49 pieces, 62.2 km | 49 / 62.2 |
| Keogh Lake | no | Keogh River | `keogh_river.r2` No Fishing in all parts, Dec 1-May 31 | 0 named: ; unnamed: 35 pieces, 37.6 km | 35 / 37.6 |
| Jacobs Lake | no | North Alouette River | `north_alouette_river.r2` No powered boats | 3 named: Gwendoline Creek (2.3 km), Jacobs Creek (3.8 km), Mayfly Creek (2.7 km); unnamed: 11 pieces, 6.2 km; plus 1 small polygons counted as part of those streams | 18 / 15.0 |
| Beaver Lake | yes | Colquitz River | `colquitz_river.r1` No Fishing | 1 named: O'Donnel Creek (1.4 km); unnamed: 4 pieces, 1.2 km; plus 4 small polygons counted as part of those streams | 13 / 2.6 |
| Dick Lake | no | Eagle Creek | `eagle_creek.r1` No Fishing | 1 named: Dick Creek (2.4 km); unnamed: 6 pieces, 2.0 km; plus 3 small polygons counted as part of those streams | 13 / 4.5 |
| Gooseneck Lake | no | Miller Creek | `miller_creek.r1` No Fishing | 0 named: ; unnamed: 11 pieces, 16.9 km | 11 / 16.9 |
| Butchart Lake | no | Goldstream River | `goldstream_river.r1` No Fishing | 0 named: ; unnamed: 6 pieces, 1.6 km | 6 / 1.6 |
| Puntledge Lake | no | Puntledge River | `puntledge_river.r5` No Fishing tributaries | 0 named: ; unnamed: 3 pieces, 0.8 km | 3 / 0.8 |
| Richmond Lake | no | Perry Creek | `perry_creek.r1` Bait ban<br>`perry_creek.r2` Cutthroat trout and bull trout catch and release<br>`perry_creek.r3` brook trout daily quota = 20 | 0 named: ; unnamed: 2 pieces, 1.2 km | 2 / 1.2 |

Named counts are by water name (one creek can be several pieces). Unnamed = FWA streams with no name. Sums count a stream once per lake.

## Also moves (a knock-on, above Babine Lake)
One stream piece above Babine Lake (`360565541:1251`, already in the Skeena River's tributary walk) becomes book-known steelhead water, because the Babine River's bait ban (a steelhead row's rule) now binds it: the B.C.-wide steelhead rules `zp:steelhead` r1b/r2b/r4b and the steelhead licence record `steelhead_targeting_known` gain that one piece.

## Lake polygons

3,657 FWA lake polygons are covered by walked tributary rules today and 3,668 with this change (+11, e.g. three ponds on
Dick Creek). Every one is listed by the existing process as a piece of a stream (an unnamed widening on the creek's
line, an oxbow, a slough lake: Bowman Slough). No lake that is a water of its own — named, or held by any registry water
— is covered by any tributary rule; head lakes and lakes in the middle of a river are walked through, never covered.

## The head lakes from the question that do NOT change, and why

| Head lake | River | Why nothing changes |
|---|---|---|
| Pitt Lake | Pitt River | its rule is "No Fishing within Garibaldi Park": bound inside the park only, it never reaches the lake |
| Tsacha Lake | West Road (Blackwater) River | the river goes on above the lake (the lake is in the middle of the river, already walked) |
| Naglico Lake | West Road (Blackwater) River | climbed, but the rows' walks are held to their region/area, and nothing new lies inside it |
| Trophy Lake | Klinaklini River | climbed; the streams above were already the rule's |
| Weaver Lake | Weaver Creek | the rule is bound to the lake itself (the row's own water), not the creek |
| unnamed lakes on the Gitnadoix, Skeena (2), Swift, Granby, Squamish, Flathead | — | climbed; the streams above were already the rule's |
| unnamed lake on the Morice; St. Mary | — | the river itself goes on above it (not a head lake) |

## What an angler reads (before = live release, after = this branch: the climb AND the own-row rule)


### Dick Creek (`gnis:23637`)

Part 1: From the mouth up to the top of the river

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Dec 31: base | Jan 1–Dec 31: closed (No fishing) |
| NEW rule (trib) | — | **No fishing** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Eagle Creek. Covers Eagle Creek and its tributary streams (✱, p.4). / Inherited from Eagle Creek: its row covers its tributaries. |


#### Sources lines of `r2:eagle_creek@2-8::eagle_creek.r1`

| | before | after |
|---|---|---|
| applies | This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Eagle Creek. | This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Eagle Creek. Covers Eagle Creek and its tributary streams (✱, p.4). |
| applies_trib | — | Inherited from Eagle Creek: its row covers its tributaries. |

### Frosst Creek (`gnis:12564`)

Part 1: Does not include Sumas River · From the mouth up to the B.C. border

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Apr 15: closed (No fishing, Oct 15–Apr 15)<br>Apr 16–Apr 30: own<br>May 1–Jun 30: closed (No fishing, May 1–Jun 30)<br>Jul 1–Oct 14: own<br>Oct 15–Dec 31: closed (No fishing, Oct 15–Apr 15) | Jan 1–Apr 15: closed (No fishing, Oct 15–Apr 15)<br>Apr 16–Apr 30: own<br>May 1–Jun 30: closed (No fishing, May 1–Jun 30)<br>Jul 1–Oct 14: own<br>Oct 15–Dec 31: closed (No fishing, Oct 15–Apr 15) |
| NEW rule (trib) | — | **No fishing** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Sweltzer River. Covers Sweltzer River and its tributary streams (✱, p.4). / Inherited from Sweltzer River: its row covers its tributaries. |

Part 2: None

| | before (live release) | after (this branch) |
|---|---|---|
| the year | — | — |


### Dove Creek (`gnis:21542`)

Part 1: From the mouth up to the Anderson Lake outlet

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Jul 14: closed (No fishing)<br>Jul 15–Aug 31: closed (No fishing; No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: closed (No fishing) | Jan 1–Jul 14: closed (No fishing)<br>Jul 15–Aug 31: closed (No fishing; No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: closed (No fishing) |

Part 2: From Anderson Lake up to the top of the river

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Jul 14: closed (No fishing)<br>Jul 15–Aug 31: closed (No fishing; No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: closed (No fishing) | Jan 1–Jul 14: own<br>Jul 15–Aug 31: closed (No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: own |


### Robertson River (`gnis:6402`)

Part 1: From the mouth up to the top of the river (through 1 lake)

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Jul 14: base<br>Jul 15–Aug 31: closed (No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: base | Jan 1–Jul 14: own<br>Jul 15–Aug 31: closed (No fishing, Jul 15–Aug 31; No fishing in streams, Jul 15–Aug 31)<br>Sep 1–Dec 31: own |
| NEW rule (trib) | — | **No fishing, Jul 15–Aug 31** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Cowichan River. Covers Cowichan River and its tributary streams. / Inherited from Cowichan River: its row covers its tributaries. |


### Schulbuckhand Creek (`gnis:3201`)

Part 1: From the mouth up to the top of the river

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–May 14: own<br>May 15–Jun 15: own<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own | Jan 1–May 14: own<br>May 15–Jun 15: own<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own |
| NEW rule (trib) | — | **Bait ban** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Lakelse River. Covers Lakelse River and its tributary streams (✱, p.4). / Inherited from Lakelse River: its row covers its tributaries. |
| NEW rule (trib) | — | **No powered boats** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Lakelse River. Covers Lakelse River and its tributary streams (✱, p.4). / Inherited from Lakelse River: its row covers its tributaries. |
| NEW rule (trib) | — | **Note: The mouth of the river is designated by signs 1.7 km downstream of Lakelse River logging road bridge** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Lakelse River. Covers Lakelse River and its tributary streams (✱, p.4). / Inherited from Lakelse River: its row covers its tributaries. |


### Fulton River (`gnis:12586`)

Part 1: Two stretches: from the mouth up to the downstream end of the stretch opposite the spawning channels, and from the upstream end of the stretch opposite the spawning channels up to the top of the river (through 2 lakes)

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own |
| NEW rule (trib) | — | **Bait ban** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Babine River. Covers Babine River and its tributary streams (✱, p.4). / Inherited from Babine River: its row covers its tributaries. |

Part 2: From the downstream end of the stretch opposite the spawning channels up to the upstream end of the stretch opposite the spawning channels

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own |
| NEW rule (trib) | — | **Bait ban** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Babine River. Covers Babine River and its tributary streams (✱, p.4). / Inherited from Babine River: its row covers its tributaries. |

Part 3: Two stretches: from the mouth up to the downstream end of the stretch opposite the spawning channels, and from the upstream end of the stretch opposite the spawning channels up to the top of the river (through 2 lakes)

| | before (live release) | after (this branch) |
|---|---|---|
| the year | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own | Jan 1–Apr 30: own<br>May 1–May 14: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>May 15–Jun 15: closed (No fishing, May 1–Jun 15; No fishing in streams, Jan 1–Jun 15)<br>Jun 16–Jun 30: own<br>Jul 1–Sep 14: own<br>Sep 15–Oct 31: own<br>Nov 1–Nov 30: own<br>Dec 1–Dec 31: own |
| NEW rule (trib) | — | **Bait ban** — This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Babine River. Covers Babine River and its tributary streams (✱, p.4). / Inherited from Babine River: its row covers its tributaries. |



"base" = only the zone's rules; "own" = the water's own rules decide; "closed" = no fishing. A rule listed NEW on Frosst
Creek is on its Sources page but gives way (its answer is unchanged).
